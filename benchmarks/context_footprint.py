#!/usr/bin/env python3
# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Context-footprint measurement for the bundled Lightflow example workflows.

Drives the seven example workflows end to end through the Lightflow CLI with
scripted operator inputs and records, per CLI call, what an agent would have
emitted (the command line, plus any file it authored) and read back (stdout and
stderr). It also records the size of the state Lightflow keeps on disk
(`passport.json`) and of the workflow sources (`lightflow.yaml` + `actions.py`).

This is a measurement, not a comparison. There is no baseline arm, so no
column is a saving: the passport and source sizes are reported as sizes.
The live comparison against a prose runbook is Part I of the benchmarks
(`reliability_benchmark.py`).

Usage (from the repository root):

    python3 benchmarks/context_footprint.py                  # report
    python3 benchmarks/context_footprint.py --json           # machine-readable
    python3 benchmarks/context_footprint.py --check-readme   # exit 1 if stale
    python3 benchmarks/context_footprint.py --update-readme  # rewrite table

Temp paths, PIDs and timestamps are normalised, so the numbers are
reproducible bit for bit. Token counts are the 4-chars-per-token heuristic.
"""

from __future__ import annotations

import argparse
import contextlib
import dataclasses
import difflib
import io
import json
import os
import re
import shlex
import shutil
import sys
import tempfile
from typing import Any
import warnings

# pylint: disable=g-import-not-at-top,g-bad-import-order
try:
  from ..lightflow import engine
  from ..lightflow import runner
except (ImportError, ValueError):
  _REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
  if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)
  from lightflow import engine  # type: ignore[no-redef]  # pyrefly: ignore[missing-import]
  from lightflow import runner  # type: ignore[no-redef]  # pyrefly: ignore[missing-import]
# pylint: enable=g-import-not-at-top,g-bad-import-order

_OSS_ROOT = os.path.realpath(
    os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
)
_README_PATH = os.path.join(_OSS_ROOT, "benchmarks", "README.md")
_RUNNER_SKILL = os.path.join("skills", "run_lightflows", "SKILL.md")
_AUTHOR_SKILL = os.path.join("skills", "create_lightflows", "SKILL.md")
_CANONICAL_WORK_DIR = "/tmp/lf_work"
_CANONICAL_PATH_RE = re.compile(
    r"(?:/tmp/lf_work|/tmp/lf_state|/repo/lightflow)(?:(?:\\\\|\\|/)[A-Za-z0-9_.-]+)+"
)
_PID_RE = re.compile(r"PID \d+")
_JSON_PID_RE = re.compile(r'"worker_pid": \d+')
_TS_RE = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z?")
# The passport records the operator identity; pin it so the measurement does
# not depend on the invoking user name, and drop agent-session provenance.
_PINNED_ENV = {"LIGHTFLOW_OPERATOR": "operator"}
_SCRUBBED_ENV = ("ANTIGRAVITY_CONVERSATION_ID",)

README_BEGIN = "<!-- context_footprint:begin -->"
README_END = "<!-- context_footprint:end -->"
WORKFLOW_NAMES = (
    "quarterly_revenue_brief",
    "hn_digest",
    "usgs_seismic_alert",
    "pypi_upgrade_guard",
    "async_job_watcher",
    "create_lightflow",
    "tenant_gitops_onboarding",
)
# These two examples spawn `sys.executable`; hosts without a usable
# interpreter path (some hermetic test runners) can only measure the rest.
WORKFLOWS_SPAWNING_PYTHON = frozenset({"async_job_watcher", "create_lightflow"})


def estimate_tokens(chars: int) -> int:
  """Estimates LLM token count using the standard 4-chars-per-token heuristic."""
  return (chars + 3) // 4


@dataclasses.dataclass
class StepMetric:
  """What the agent emitted and read back for one CLI invocation."""

  label: str
  command_str: str
  exit_code: int
  stdout_chars: int
  cmd_chars: int

  @property
  def total_chars(self) -> int:
    return self.cmd_chars + self.stdout_chars

  @property
  def total_tokens(self) -> int:
    return estimate_tokens(self.total_chars)

  def to_dict(self) -> dict[str, Any]:
    return {
        "label": self.label,
        "command": self.command_str,
        "exit_code": self.exit_code,
        "cmd_chars": self.cmd_chars,
        "stdout_chars": self.stdout_chars,
        "total_chars": self.total_chars,
        "total_tokens": self.total_tokens,
    }


@dataclasses.dataclass
class WorkflowFootprint:
  """Context footprint of one example workflow driven end to end."""

  name: str
  description: str
  is_authoring_workflow: bool
  steps: list[StepMetric]
  source_chars: int
  passport_chars: int
  passport_outputs_chars: int

  @property
  def source_tokens(self) -> int:
    return estimate_tokens(self.source_chars)

  @property
  def runtime_cmd_chars(self) -> int:
    return sum(s.cmd_chars for s in self.steps)

  @property
  def runtime_stdout_chars(self) -> int:
    return sum(s.stdout_chars for s in self.steps)

  @property
  def runtime_total_chars(self) -> int:
    return sum(s.total_chars for s in self.steps)

  @property
  def runtime_total_tokens(self) -> int:
    return estimate_tokens(self.runtime_total_chars)

  @property
  def passport_tokens(self) -> int:
    return estimate_tokens(self.passport_chars)

  @property
  def passport_outputs_tokens(self) -> int:
    return estimate_tokens(self.passport_outputs_chars)

  @property
  def cumulative_multiturn_tokens(self) -> int:
    """Prompt tokens summed over turns, as turn k re-sends turns 1..k-1."""
    running = 0
    cumulative = 0
    for s in self.steps:
      running += s.total_tokens
      cumulative += running
    return cumulative

  def to_dict(self) -> dict[str, Any]:
    return {
        "name": self.name,
        "description": self.description,
        "is_authoring_workflow": self.is_authoring_workflow,
        "steps": [s.to_dict() for s in self.steps],
        "source_chars": self.source_chars,
        "source_tokens": self.source_tokens,
        "runtime_cmd_chars": self.runtime_cmd_chars,
        "runtime_stdout_chars": self.runtime_stdout_chars,
        "runtime_total_chars": self.runtime_total_chars,
        "runtime_total_tokens": self.runtime_total_tokens,
        "cumulative_multiturn_tokens": self.cumulative_multiturn_tokens,
        "passport_chars": self.passport_chars,
        "passport_tokens": self.passport_tokens,
        "passport_outputs_chars": self.passport_outputs_chars,
        "passport_outputs_tokens": self.passport_outputs_tokens,
    }


def _exit_code(exc: SystemExit) -> int:
  code = exc.code
  if code is None:
    return 0
  if isinstance(code, int):
    return code
  return 1


def python_subprocess_available() -> bool:
  """True when examples that spawn `sys.executable` can run on this host."""
  return bool(sys.executable) and os.path.isfile(sys.executable)


def _normalize_traceback(text: str) -> str:
  """Strips Python 3.11+ caret and multiline frame continuations from tracebacks."""
  if "Traceback (most recent call last):" not in text:
    return text
  out: list[str] = []
  in_tb = False
  skip_indented = False
  for line in text.splitlines(keepends=True):
    if line.startswith("Traceback (most recent call last):"):
      in_tb = True
      skip_indented = False
      out.append(line)
    elif in_tb and line.startswith('  File "'):
      skip_indented = False
      out.append(line)
    elif in_tb and line.startswith("    "):
      if not skip_indented:
        out.append(line)
        skip_indented = True
    else:
      in_tb = False
      skip_indented = False
      out.append(line)
  return "".join(out)


class FootprintRunner:
  """Runs each example workflow in-process against an isolated state dir."""

  def __init__(self) -> None:
    self.state_dir = os.path.realpath(
        tempfile.mkdtemp(prefix="lightflow_fp_state_")
    )
    self.work_dir = os.path.realpath(
        tempfile.mkdtemp(prefix="lightflow_fp_work_")
    )

  def close(self) -> None:
    shutil.rmtree(self.state_dir, ignore_errors=True)
    shutil.rmtree(self.work_dir, ignore_errors=True)

  def __enter__(self) -> FootprintRunner:
    return self

  def __exit__(self, *exc_info: Any) -> None:
    self.close()

  def _normalize_text(self, text: str) -> str:
    """Normalizes OS-specific temp paths, PIDs, and timestamps for determinism."""
    norm = text
    for raw_root, canonical in (
        (self.work_dir, _CANONICAL_WORK_DIR),
        (self.state_dir, "/tmp/lf_state"),
        (_OSS_ROOT, "/repo/lightflow"),
    ):
      norm = norm.replace(json.dumps(raw_root)[1:-1], canonical)
      norm = norm.replace(raw_root, canonical)
    norm = _CANONICAL_PATH_RE.sub(
        lambda m: m.group(0).replace("\\\\", "/").replace("\\", "/"), norm
    )
    norm = _PID_RE.sub("PID 12345", norm)
    norm = _JSON_PID_RE.sub('"worker_pid": 12345', norm)
    norm = _TS_RE.sub("2026-01-01T00:00:00Z", norm)
    return _normalize_traceback(norm)

  def _file_chars(self, rel_path: str) -> int:
    full = os.path.join(_OSS_ROOT, rel_path)
    if not os.path.exists(full):
      raise FileNotFoundError(f"Expected file missing: {full}")
    with open(full, "r", encoding="utf-8") as f:
      return len(f.read())

  def skill_chars(self) -> tuple[int, int]:
    """Returns (runner skill chars, authoring skill chars)."""
    return self._file_chars(_RUNNER_SKILL), self._file_chars(_AUTHOR_SKILL)

  def _measure_source(self, example_name: str) -> int:
    base = os.path.join("examples", example_name)
    return self._file_chars(os.path.join(base, "lightflow.yaml")) + (
        self._file_chars(os.path.join(base, "actions.py"))
    )

  def _measure_passport(self, log_id: str) -> tuple[int, int]:
    passport_path = os.path.join(
        self.state_dir, f"lightflow_state_{log_id}", "passport.json"
    )
    if not os.path.exists(passport_path):
      return 0, 0
    with open(passport_path, "r", encoding="utf-8") as f:
      raw = self._normalize_text(f.read())
    data = json.loads(raw)
    for stamp in data.get("stamps", []):
      if stamp.get("status") == "PAUSED" and stamp.get("instructions"):
        summary = engine.summarise(stamp["instructions"])
        stamp["message"] = f"Suspended waiting for operator action: {summary}"
      elif isinstance(stamp.get("message"), str):
        stamp["message"] = _normalize_traceback(stamp["message"])
    raw = json.dumps(data, indent=2)
    outputs = data.get("payload", {}).get("outputs", {})
    outputs_str = self._normalize_text(json.dumps(outputs, sort_keys=True))
    return len(raw), len(outputs_str)

  def _invoke(self, args: list[str], sink: io.StringIO) -> int:
    """Runs one `lightflow <args>` call in-process; returns its exit code."""
    saved_argv = sys.argv
    saved_cwd = os.getcwd()
    touched = (*_SCRUBBED_ENV, *_PINNED_ENV, "LIGHTFLOW_STATE_DIR")
    saved_env = {k: os.environ.get(k) for k in touched}
    sys.argv = ["lightflow", *args]
    os.chdir(_OSS_ROOT)
    os.environ["LIGHTFLOW_STATE_DIR"] = self.state_dir
    os.environ.update(_PINNED_ENV)
    for key in _SCRUBBED_ENV:
      os.environ.pop(key, None)
    try:
      with (
          contextlib.redirect_stdout(sink),
          contextlib.redirect_stderr(sink),
          warnings.catch_warnings(),
      ):
        # `async_job_watcher` detaches its worker on purpose; under
        # `-W error::ResourceWarning` the interpreter would print an
        # "Exception ignored" block into the sink and skew the measurement.
        warnings.simplefilter("ignore", ResourceWarning)
        try:
          runner.run_cli()
        except SystemExit as e:
          return _exit_code(e)
      return 0
    finally:
      sys.argv = saved_argv
      os.chdir(saved_cwd)
      for key, value in saved_env.items():
        if value is None:
          os.environ.pop(key, None)
        else:
          os.environ[key] = value

  def _run_step(
      self,
      label: str,
      args: list[str],
      expected_exit: int,
      extra_input_chars: int = 0,
  ) -> StepMetric:
    """Runs one CLI call and records what was emitted and read back."""
    norm_args = [self._normalize_text(a) for a in args]
    norm_cmd = "lightflow " + " ".join(shlex.quote(a) for a in norm_args)
    sink = io.StringIO()
    code = self._invoke(args, sink)
    if code != expected_exit:
      raise RuntimeError(
          f"Step '{label}' exited with {code} (expected"
          f" {expected_exit}):\n{sink.getvalue()}"
      )
    norm_out = self._normalize_text(sink.getvalue())
    return StepMetric(
        label=label,
        command_str=norm_cmd,
        exit_code=code,
        stdout_chars=len(norm_out),
        cmd_chars=len(norm_cmd) + extra_input_chars,
    )

  def _finish(
      self,
      name: str,
      description: str,
      log_id: str,
      steps: list[StepMetric],
      is_authoring_workflow: bool = False,
  ) -> WorkflowFootprint:
    """Measures the passport, runs `cleanup`, and packs the result."""
    p_chars, p_out_chars = self._measure_passport(log_id)
    steps.append(
        self._run_step("cleanup", ["cleanup", f"--log_id={log_id}"], 0)
    )
    return WorkflowFootprint(
        name=name,
        description=description,
        is_authoring_workflow=is_authoring_workflow,
        steps=steps,
        source_chars=self._measure_source(name),
        passport_chars=p_chars,
        passport_outputs_chars=p_out_chars,
    )

  def measure_quarterly_revenue_brief(self) -> WorkflowFootprint:
    """Measures the SQLite + two-gate revenue brief workflow."""
    log_id = "bench_q3"
    out_md = os.path.join(self.work_dir, "q3_revenue_brief.md")
    steps = [
        self._run_step(
            "start (SQL query + variance analysis -> pause at"
            " draft_commentary)",
            [
                "start",
                "--lightflow=examples/quarterly_revenue_brief",
                f"--log_id={log_id}",
                "--payload=" + json.dumps({"output_path": out_md}),
            ],
            expected_exit=2,
        ),
        self._run_step(
            "resume draft_commentary (render_report -> pause at approve_send)",
            [
                "resume",
                f"--log_id={log_id}",
                "--stage=draft_commentary",
                "--resolution=APPROVE",
                (
                    '--payload={"approved": true, "commentary": "Q3 revenue'
                    " grew +12.4% QoQ led by NA and APAC; EMEA trailed plan by"
                    ' 6.2% on Q4 renewal timing (closed Oct 4)."}'
                ),
            ],
            expected_exit=2,
        ),
        self._run_step(
            "resume approve_send (send_report -> complete)",
            [
                "resume",
                f"--log_id={log_id}",
                "--stage=approve_send",
                "--resolution=APPROVE",
            ],
            expected_exit=0,
        ),
    ]
    return self._finish(
        "quarterly_revenue_brief",
        "SQLite query -> Python variance math -> commentary gate -> render"
        " -> send gate -> receipt",
        log_id,
        steps,
    )

  def measure_hn_digest(self) -> WorkflowFootprint:
    """Measures the offline Hacker News digest with one editorial gate."""
    log_id = "bench_hn"
    out_md = os.path.join(self.work_dir, "hn_digest.md")
    steps = [
        self._run_step(
            "start (fetch -> pause at editorial_gate)",
            [
                "start",
                "--lightflow=examples/hn_digest",
                f"--log_id={log_id}",
                '--payload={"offline": true, "limit": 3}',
            ],
            expected_exit=2,
        ),
        self._run_step(
            "resume APPROVE (publish_digest)",
            [
                "resume",
                f"--log_id={log_id}",
                "--stage=editorial_gate",
                "--resolution=APPROVE",
                "--payload="
                + json.dumps({"editor_note": "LGTM", "output_path": out_md}),
            ],
            expected_exit=0,
        ),
    ]
    return self._finish(
        "hn_digest",
        "Fetch top stories -> human editorial gate -> publish Markdown",
        log_id,
        steps,
    )

  def measure_usgs_seismic_alert(self) -> WorkflowFootprint:
    """Measures the offline USGS feed workflow with a conditional gate."""
    log_id = "bench_usgs"
    out_md = os.path.join(self.work_dir, "usgs_bulletin.md")
    steps = [
        self._run_step(
            "start (fetch feed -> pause at incident_commander_gate)",
            [
                "start",
                "--lightflow=examples/usgs_seismic_alert",
                f"--log_id={log_id}",
                '--payload={"offline": true}',
            ],
            expected_exit=2,
        ),
        self._run_step(
            "resume APPROVE (emit_bulletin)",
            [
                "resume",
                f"--log_id={log_id}",
                "--stage=incident_commander_gate",
                "--resolution=APPROVE",
                "--payload="
                + json.dumps({"severity": "ADVISORY", "output_path": out_md}),
            ],
            expected_exit=0,
        ),
    ]
    return self._finish(
        "usgs_seismic_alert",
        "Poll USGS M4.5+ feed -> conditional gate -> emit bulletin",
        log_id,
        steps,
    )

  def measure_pypi_upgrade_guard(self) -> WorkflowFootprint:
    """Measures the PyPI audit with a smoke failure, rollback and recovery."""
    log_id = "bench_pypi"
    reqs_path = os.path.join(self.work_dir, "requirements.txt")
    steps = [
        self._run_step(
            "start (audit PyPI -> pause at approve_upgrades)",
            [
                "start",
                "--lightflow=examples/pypi_upgrade_guard",
                f"--log_id={log_id}",
                "--payload="
                + json.dumps({"offline": True, "requirements_path": reqs_path}),
            ],
            expected_exit=2,
        ),
        self._run_step(
            "resume APPROVE (smoke failure + automatic rollback)",
            [
                "resume",
                f"--log_id={log_id}",
                "--stage=approve_upgrades",
                "--resolution=APPROVE",
                '--payload={"simulate_smoke_failure": true}',
            ],
            expected_exit=1,
        ),
        self._run_step(
            "resume surgical recovery (preserves upstream stages)",
            [
                "resume",
                f"--log_id={log_id}",
                '--payload={"simulate_smoke_failure": false}',
            ],
            expected_exit=0,
        ),
    ]
    return self._finish(
        "pypi_upgrade_guard",
        "Audit PyPI -> gate -> smoke-test failure + auto-rollback -> resume",
        log_id,
        steps,
    )

  def measure_async_job_watcher(self) -> WorkflowFootprint:
    """Measures the background-job workflow with a polling_policy loop."""
    log_id = "bench_async"
    status_file = os.path.join(self.work_dir, "async_job_status.json")
    steps = [
        self._run_step(
            "start (spawn worker -> poll until READY -> verify -> cleanup)",
            [
                "start",
                "--lightflow=examples/async_job_watcher",
                f"--log_id={log_id}",
                "--payload="
                + json.dumps({"delay_seconds": 1.0, "status_file": status_file}),
            ],
            expected_exit=0,
        ),
    ]
    return self._finish(
        "async_job_watcher",
        "Spawn background job -> polling_policy loop -> checksum verify",
        log_id,
        steps,
    )

  def measure_create_lightflow(self) -> WorkflowFootprint:
    """Measures the authoring workflow, including the files it writes."""
    log_id = "bench_create"
    scaffold_dir = os.path.join(self.work_dir, "scaffold_demo")
    os.makedirs(scaffold_dir, exist_ok=True)

    s1 = self._run_step(
        "start (pause at align_on_design)",
        [
            "start",
            "--lightflow=examples/create_lightflow",
            f"--log_id={log_id}",
        ],
        expected_exit=2,
    )
    s2 = self._run_step(
        "resume align_on_design (pause at implement_and_test_actions)",
        [
            "resume",
            f"--log_id={log_id}",
            "--stage=align_on_design",
            "--resolution=APPROVE",
            "--payload="
            + json.dumps({
                "target_dir": scaffold_dir,
                "workflow_name": "demo_flow",
                "summary": "Single-stage demo workflow",
                "has_python_actions": True,
                "action_names": ["step_one"],
            }),
        ],
        expected_exit=2,
    )
    actions_src = (
        "def step_one(payload, dry_run=False, **kwargs):\n"
        "    return {'done': True}, 'Step one complete'\n"
    )
    tests_src = (
        "import unittest, actions\n"
        "class T(unittest.TestCase):\n"
        "    def test_1(self):\n"
        "        self.assertTrue(actions.step_one({})[0]['done'])\n"
    )
    with open(
        os.path.join(scaffold_dir, "actions.py"), "w", encoding="utf-8"
    ) as f:
      f.write(actions_src)
    with open(
        os.path.join(scaffold_dir, "actions_test.py"), "w", encoding="utf-8"
    ) as f:
      f.write(tests_src)
    s3 = self._run_step(
        "write actions.py + actions_test.py & resume"
        " implement_and_test_actions",
        [
            "resume",
            f"--log_id={log_id}",
            "--stage=implement_and_test_actions",
            "--resolution=APPROVE",
            (
                '--payload={"actions_file": "actions.py", "test_file":'
                ' "actions_test.py", "sample_payload": {}}'
            ),
        ],
        expected_exit=2,
        extra_input_chars=len(actions_src) + len(tests_src),
    )
    manifest_src = (
        'name: "demo_flow"\n'
        "strict_expressions: true\n"
        "actions:\n"
        '  - id: "step_one"\n'
        '    python_import: "actions.step_one"\n'
        "stages:\n"
        '  - name: "step_one"\n'
        '    description: "First step"\n'
        '    python_action: "step_one"\n'
    )
    with open(
        os.path.join(scaffold_dir, "lightflow.yaml"), "w", encoding="utf-8"
    ) as f:
      f.write(manifest_src)
    s4 = self._run_step(
        "write lightflow.yaml & resume draft_and_explain_manifest",
        [
            "resume",
            f"--log_id={log_id}",
            "--stage=draft_and_explain_manifest",
            "--resolution=APPROVE",
            (
                '--payload={"manifest_file": "lightflow.yaml",'
                ' "explained_to_user": true}'
            ),
        ],
        expected_exit=0,
        extra_input_chars=len(manifest_src),
    )
    return self._finish(
        "create_lightflow",
        "3 human authoring gates + AST/test verifier + dry-run compiler",
        log_id,
        [s1, s2, s3, s4],
        is_authoring_workflow=True,
    )

  def measure_tenant_gitops_onboarding(self) -> WorkflowFootprint:
    """Measures the 22-stage multi-branch tenant_gitops_onboarding workflow."""
    log_id = "bench_tenant"
    sandbox_dir = os.path.join(self.work_dir, "tenant_sandbox")
    steps = [
        self._run_step(
            "start (init spec + role check -> pause at verify_spec)",
            [
                "start",
                "--lightflow=examples/tenant_gitops_onboarding",
                f"--log_id={log_id}",
                "--payload="
                + json.dumps({
                    "alias": "payments-eu",
                    "compliance_tier": "pci",
                    "sandbox_dir": sandbox_dir,
                    "require_manual_quota_override": True,
                }),
            ],
            expected_exit=2,
        ),
        self._run_step(
            "resume verify_spec (Phase 2 IdP/SCIM/IAM -> pause at"
            " prompt_iam_quota_override)",
            [
                "resume",
                f"--log_id={log_id}",
                "--stage=verify_spec",
                "--resolution=APPROVE",
            ],
            expected_exit=2,
        ),
        self._run_step(
            "resume prompt_iam_quota_override (Phases 3-4 KMS/GitOps/ArgoCD ->"
            " complete)",
            [
                "resume",
                f"--log_id={log_id}",
                "--stage=prompt_iam_quota_override",
                "--resolution=APPROVE",
                '--payload={"quota_override_approved": true}',
            ],
            expected_exit=0,
        ),
    ]
    return self._finish(
        "tenant_gitops_onboarding",
        "22-stage multi-branch GitOps onboarding (2 gates, 3 branch"
        " patterns, 2 PRs, 6 polls)",
        log_id,
        steps,
    )

  def measure(self, name: str) -> WorkflowFootprint:
    return getattr(self, f"measure_{name}")()

  def run_all(
      self, names: tuple[str, ...] = WORKFLOW_NAMES
  ) -> list[WorkflowFootprint]:
    return [self.measure(name) for name in names]


def _cold_start_chars(
    r: WorkflowFootprint, runner_skill_chars: int, author_skill_chars: int
) -> int:
  skill = runner_skill_chars
  if r.is_authoring_workflow:
    skill += author_skill_chars
  return r.runtime_total_chars + skill


def _row(r: WorkflowFootprint, runner_skill: int, author_skill: int) -> str:
  rt = r.runtime_total_chars
  cold = _cold_start_chars(r, runner_skill, author_skill)
  return (
      f"| [`{r.name}`](../examples/{r.name}/) | {len(r.steps)} |"
      f" {rt:,} ch (~{estimate_tokens(rt):,} tok) |"
      f" {cold:,} ch (~{estimate_tokens(cold):,} tok) |"
      f" ~{r.cumulative_multiturn_tokens:,} tok |"
      f" {r.passport_chars:,} ch (~{r.passport_tokens:,} tok) |"
      f" {r.source_chars:,} ch (~{r.source_tokens:,} tok) |"
  )


def _total_row(results: list[WorkflowFootprint]) -> str:
  rt = sum(r.runtime_total_chars for r in results)
  cum = sum(r.cumulative_multiturn_tokens for r in results)
  pp = sum(r.passport_chars for r in results)
  src = sum(r.source_chars for r in results)
  return (
      f"| **Total ({len(results)} workflows, warm)** |"
      f" **{sum(len(r.steps) for r in results)}** |"
      f" **{rt:,} ch (~{estimate_tokens(rt):,} tok)** | — |"
      f" **~{cum:,} tok** | **{pp:,} ch (~{estimate_tokens(pp):,} tok)** |"
      f" **{src:,} ch (~{estimate_tokens(src):,} tok)** |"
  )


def readme_table(
    results: list[WorkflowFootprint], runner_skill: int, author_skill: int
) -> str:
  """Renders the Markdown table that `README.md` embeds between the markers."""
  lines = [
      (
          "| Workflow | CLI calls | Warm runtime (`cmd + stdout`) | Cold start"
          " (incl. `SKILL.md`) | Multi-turn cumulative | `passport.json` size |"
          " Source size (`yaml + actions.py`) |"
      ),
      "| :-- | :--: | :--: | :--: | :--: | :--: | :--: |",
  ]
  lines.extend(_row(r, runner_skill, author_skill) for r in results)
  lines.append(_total_row(results))
  return "\n".join(lines)


def format_report(
    results: list[WorkflowFootprint], runner_skill: int, author_skill: int
) -> str:
  """Formats the table plus a per-step breakdown for each workflow."""
  lines = [
      "# Lightflow context footprint (measurement, not a comparison)",
      "",
      readme_table(results, runner_skill, author_skill),
      "",
      "## Per-step breakdown",
      "",
  ]
  for r in results:
    lines.append(f"### `{r.name}` — {r.description}")
    for idx, s in enumerate(r.steps, 1):
      lines.append(
          f"- Step {idx} (`{s.label}`, exit `{s.exit_code}`): input"
          f" {s.cmd_chars} ch + output {s.stdout_chars} ch ="
          f" {s.total_chars:,} ch (~{s.total_tokens} tok)"
      )
    lines.append(
        f"- `passport.json`: {r.passport_chars:,} ch"
        f" (~{r.passport_tokens:,} tok), of which `payload.outputs` ="
        f" {r.passport_outputs_chars:,} ch"
    )
    lines.append("")
  lines.append(
      f"Cold start adds `{_RUNNER_SKILL}` ({runner_skill:,} ch,"
      f" ~{estimate_tokens(runner_skill):,} tok) to every workflow and"
      f" `{_AUTHOR_SKILL}` ({author_skill:,} ch,"
      f" ~{estimate_tokens(author_skill):,} tok) to the authoring one."
      " Tokens are chars/4."
  )
  return "\n".join(lines)


def _split_cells(line: str) -> list[str]:
  return [c.strip() for c in line.strip().strip("|").split("|")]


def _readme_block(readme_text: str) -> list[str]:
  """Returns the table lines between the README markers."""
  begin = readme_text.find(README_BEGIN)
  end = readme_text.find(README_END)
  if begin < 0 or end < 0 or end < begin:
    raise ValueError(
        f"README markers {README_BEGIN!r} / {README_END!r} not found"
    )
  block = readme_text[begin + len(README_BEGIN) : end]
  return [l for l in block.splitlines() if l.strip().startswith("|")]


def check_readme_table(
    readme_text: str,
    results: list[WorkflowFootprint],
    runner_skill: int,
    author_skill: int,
) -> list[str]:
  """Compares the README table with fresh results.

  Rows are compared cell by cell after whitespace normalisation. Only the
  workflows present in `results` are checked; the total row is checked when
  every workflow was measured.

  Args:
    readme_text: Contents of `benchmarks/README.md`.
    results: Fresh measurements, one per workflow.
    runner_skill: Size in chars of `skills/run_lightflows/SKILL.md`.
    author_skill: Size in chars of `skills/create_lightflows/SKILL.md`.

  Returns:
    One human-readable problem per stale or missing row; empty when current.
  """
  actual_rows = {_split_cells(l)[0]: l for l in _readme_block(readme_text)}
  expected = readme_table(results, runner_skill, author_skill).splitlines()
  complete = len(results) == len(WORKFLOW_NAMES)
  problems: list[str] = []
  for line in expected:
    key = _split_cells(line)[0]
    if key.startswith(":"):
      continue  # alignment row
    if key.startswith("**Total") and not complete:
      continue  # the total only matches when every workflow was measured
    actual = actual_rows.get(key)
    if actual is None:
      problems.append(f"missing row: {key}")
    elif _split_cells(actual) != _split_cells(line):
      diff = difflib.unified_diff(
          [actual.strip()], [line.strip()], "README.md", "fresh run", n=0
      )
      problems.append("\n".join(diff))
  return problems


def update_readme_table(
    readme_text: str,
    results: list[WorkflowFootprint],
    runner_skill: int,
    author_skill: int,
) -> str:
  """Returns README text with the table between the markers replaced."""
  begin = readme_text.find(README_BEGIN)
  end = readme_text.find(README_END)
  if begin < 0 or end < 0 or end < begin:
    raise ValueError("README markers not found")
  head = readme_text[: begin + len(README_BEGIN)]
  tail = readme_text[end:]
  return (
      head
      + "\n"
      + readme_table(results, runner_skill, author_skill)
      + "\n"
      + tail
  )


def main(argv: list[str] | None = None) -> int:
  parser = argparse.ArgumentParser(description=__doc__)
  parser.add_argument(
      "--json", action="store_true", help="Emit machine-readable JSON metrics."
  )
  parser.add_argument(
      "--check-readme",
      action="store_true",
      help="Exit 1 if the table in benchmarks/README.md is stale.",
  )
  parser.add_argument(
      "--update-readme",
      action="store_true",
      help="Rewrite the table in benchmarks/README.md from a fresh run.",
  )
  args = parser.parse_args(argv)

  with FootprintRunner() as fp:
    results = fp.run_all()
    runner_skill, author_skill = fp.skill_chars()

  if args.check_readme or args.update_readme:
    with open(_README_PATH, "r", encoding="utf-8") as f:
      readme_text = f.read()
    if args.update_readme:
      new_text = update_readme_table(
          readme_text, results, runner_skill, author_skill
      )
      if new_text != readme_text:
        with open(_README_PATH, "w", encoding="utf-8") as f:
          f.write(new_text)
        print(f"Updated table in {_README_PATH}")
      else:
        print("README table already current.")
      return 0
    problems = check_readme_table(
        readme_text, results, runner_skill, author_skill
    )
    if problems:
      print("README table is stale:\n" + "\n".join(problems))
      print("Run `python3 benchmarks/context_footprint.py --update-readme`.")
      return 1
    print("README table is current.")
    return 0

  if args.json:
    print(
        json.dumps(
            {
                "runner_skill_chars": runner_skill,
                "runner_skill_tokens": estimate_tokens(runner_skill),
                "author_skill_chars": author_skill,
                "author_skill_tokens": estimate_tokens(author_skill),
                "workflows": [r.to_dict() for r in results],
            },
            indent=2,
        )
    )
  else:
    print(format_report(results, runner_skill, author_skill))
  return 0


if __name__ == "__main__":
  sys.exit(main())
