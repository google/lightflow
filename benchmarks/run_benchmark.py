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
"""Reproducible token-efficiency benchmark for Lightflow example workflows.

Executes all 5 included example Lightflows end-to-end and measures:
1. Runtime Agent Context (CLI command invocations + authored files + stdout).
2. Multi-Turn Cumulative Input Tokens (accounting for transcript re-reads across
   turns in a stateful LLM conversation).
3. Out-of-Context State (`passport.json` payload + stamps kept on disk).
4. Source Files Avoided (`lightflow.yaml` + `actions.py` kept out of the agent's
   context window via the Zero-Context Entry Rule).
5. Cold-start overhead of reading `skills/run_lightflows/SKILL.md` (and
   `skills/create_lightflows/SKILL.md` when authoring) vs. warm/marginal runs.
"""

from __future__ import annotations

import argparse
import dataclasses
import json
import os
import re
import shlex
import shutil
import subprocess
import sys
import tempfile
from typing import Any

_OSS_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
_CANONICAL_WORK_DIR = "/tmp/lf_bench"
_PID_RE = re.compile(r"\bPID \d+\b")
_JSON_PID_RE = re.compile(r'"worker_pid":\s*\d+')
_TS_RE = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z")


def estimate_tokens(chars: int) -> int:
  """Estimates LLM token count using the standard 4-chars-per-token heuristic."""
  return round(chars / 4.0)


@dataclasses.dataclass
class StepMetric:
  """Metrics for a single agent tool invocation (CLI command or file write)."""

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
        "command_str": self.command_str,
        "exit_code": self.exit_code,
        "cmd_chars": self.cmd_chars,
        "stdout_chars": self.stdout_chars,
        "total_chars": self.total_chars,
        "total_tokens": self.total_tokens,
    }


@dataclasses.dataclass
class WorkflowBenchmark:
  """End-to-end benchmark metrics for a single Lightflow workflow."""

  name: str
  description: str
  is_authoring_workflow: bool
  steps: list[StepMetric]
  yaml_chars: int
  actions_chars: int
  readme_chars: int
  passport_chars: int
  passport_outputs_chars: int

  @property
  def blueprint_chars(self) -> int:
    return self.yaml_chars + self.actions_chars

  @property
  def blueprint_tokens(self) -> int:
    return estimate_tokens(self.blueprint_chars)

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
  def zero_context_savings_ratio(self) -> float:
    """Fraction of context saved by Zero-Context Entry vs reading yaml+actions.py first."""
    denom = self.runtime_total_chars + self.blueprint_chars
    return (self.blueprint_chars / denom) if denom else 0.0

  @property
  def cumulative_multiturn_tokens(self) -> int:
    """Cumulative prompt tokens billed across turns (Turn k re-sends Turns 1..k-1)."""
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
        "yaml_chars": self.yaml_chars,
        "actions_chars": self.actions_chars,
        "readme_chars": self.readme_chars,
        "blueprint_chars": self.blueprint_chars,
        "blueprint_tokens": self.blueprint_tokens,
        "runtime_cmd_chars": self.runtime_cmd_chars,
        "runtime_stdout_chars": self.runtime_stdout_chars,
        "runtime_total_chars": self.runtime_total_chars,
        "runtime_total_tokens": self.runtime_total_tokens,
        "cumulative_multiturn_tokens": self.cumulative_multiturn_tokens,
        "passport_chars": self.passport_chars,
        "passport_tokens": self.passport_tokens,
        "passport_outputs_chars": self.passport_outputs_chars,
        "passport_outputs_tokens": self.passport_outputs_tokens,
        "zero_context_savings_ratio": round(self.zero_context_savings_ratio, 4),
    }


class BenchmarkRunner:
  """Runs each example workflow in an isolated state directory and records sizes."""

  def __init__(self) -> None:
    self.state_dir = tempfile.mkdtemp(prefix="lightflow_bench_state_")
    self.work_dir = tempfile.mkdtemp(prefix="lightflow_bench_work_")
    self.env = os.environ.copy()
    self.env["PYTHONPATH"] = _OSS_ROOT
    self.env["LIGHTFLOW_STATE_DIR"] = self.state_dir
    self.env.pop("ANTIGRAVITY_CONVERSATION_ID", None)

  def close(self) -> None:
    shutil.rmtree(self.state_dir, ignore_errors=True)
    shutil.rmtree(self.work_dir, ignore_errors=True)

  def _normalize_text(self, text: str) -> str:
    """Normalizes OS-specific temp paths, PIDs, and timestamps for determinism."""
    norm = text.replace(self.work_dir, _CANONICAL_WORK_DIR)
    norm = norm.replace(self.state_dir, "/tmp/lf_state")
    norm = norm.replace(_OSS_ROOT, "/repo/lightflow")
    norm = _PID_RE.sub("PID 12345", norm)
    norm = _JSON_PID_RE.sub('"worker_pid": 12345', norm)
    norm = _TS_RE.sub("2026-01-01T00:00:00Z", norm)
    return norm

  def _file_chars(self, rel_path: str) -> int:
    full = os.path.join(_OSS_ROOT, rel_path)
    if not os.path.exists(full):
      return 0
    with open(full, "r", encoding="utf-8") as f:
      return len(f.read())

  def _measure_blueprint(self, example_name: str) -> tuple[int, int, int]:
    base = os.path.join("examples", example_name)
    return (
        self._file_chars(os.path.join(base, "lightflow.yaml")),
        self._file_chars(os.path.join(base, "actions.py")),
        self._file_chars(os.path.join(base, "README.md")),
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
    outputs = data.get("payload", {}).get("outputs", {})
    outputs_str = self._normalize_text(json.dumps(outputs, sort_keys=True))
    return len(raw), len(outputs_str)

  def _run_step(
      self,
      label: str,
      args: list[str],
      expected_exit: int,
      extra_input_chars: int = 0,
  ) -> StepMetric:
    raw_cmd = "lightflow " + " ".join(shlex.quote(a) for a in args)
    norm_cmd = self._normalize_text(raw_cmd)
    cmd = [sys.executable, "-m", "lightflow"] + args
    res = subprocess.run(
        cmd, cwd=_OSS_ROOT, env=self.env, capture_output=True, text=True
    )
    if res.returncode != expected_exit:
      raise RuntimeError(
          f"Step '{label}' exited with {res.returncode} (expected"
          f" {expected_exit}):\n{res.stdout}\n{res.stderr}"
      )
    norm_out = self._normalize_text(res.stdout + res.stderr)
    return StepMetric(
        label=label,
        command_str=norm_cmd,
        exit_code=res.returncode,
        stdout_chars=len(norm_out),
        cmd_chars=len(norm_cmd) + extra_input_chars,
    )

  def bench_hn_digest(self) -> WorkflowBenchmark:
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
                "--lightflow=examples/hn_digest",
                f"--log_id={log_id}",
                "--stage=editorial_gate",
                "--resolution=APPROVE",
                (
                    '--payload={"editor_note": "LGTM", "output_path":'
                    f' "{out_md}"}}'
                ),
            ],
            expected_exit=0,
        ),
    ]
    p_chars, p_out_chars = self._measure_passport(log_id)
    steps.append(
        self._run_step("cleanup", ["cleanup", f"--log_id={log_id}"], 0)
    )
    y_ch, a_ch, r_ch = self._measure_blueprint("hn_digest")
    return WorkflowBenchmark(
        name="hn_digest",
        description=(
            "Fetch top stories -> human editorial gate -> publish Markdown"
        ),
        is_authoring_workflow=False,
        steps=steps,
        yaml_chars=y_ch,
        actions_chars=a_ch,
        readme_chars=r_ch,
        passport_chars=p_chars,
        passport_outputs_chars=p_out_chars,
    )

  def bench_usgs_seismic_alert(self) -> WorkflowBenchmark:
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
                "--lightflow=examples/usgs_seismic_alert",
                f"--log_id={log_id}",
                "--stage=incident_commander_gate",
                "--resolution=APPROVE",
                (
                    '--payload={"severity": "ADVISORY", "output_path":'
                    f' "{out_md}"}}'
                ),
            ],
            expected_exit=0,
        ),
    ]
    p_chars, p_out_chars = self._measure_passport(log_id)
    steps.append(
        self._run_step("cleanup", ["cleanup", f"--log_id={log_id}"], 0)
    )
    y_ch, a_ch, r_ch = self._measure_blueprint("usgs_seismic_alert")
    return WorkflowBenchmark(
        name="usgs_seismic_alert",
        description="Poll USGS M4.5+ feed -> conditional gate -> emit bulletin",
        is_authoring_workflow=False,
        steps=steps,
        yaml_chars=y_ch,
        actions_chars=a_ch,
        readme_chars=r_ch,
        passport_chars=p_chars,
        passport_outputs_chars=p_out_chars,
    )

  def bench_pypi_upgrade_guard(self) -> WorkflowBenchmark:
    log_id = "bench_pypi"
    reqs_path = os.path.join(self.work_dir, "requirements.txt")
    steps = [
        self._run_step(
            "start (audit PyPI -> pause at approve_upgrades)",
            [
                "start",
                "--lightflow=examples/pypi_upgrade_guard",
                f"--log_id={log_id}",
                (
                    '--payload={"offline": true, "requirements_path":'
                    f' "{reqs_path}"}}'
                ),
            ],
            expected_exit=2,
        ),
        self._run_step(
            "resume APPROVE (smoke failure + automatic rollback)",
            [
                "resume",
                "--lightflow=examples/pypi_upgrade_guard",
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
                "--lightflow=examples/pypi_upgrade_guard",
                f"--log_id={log_id}",
                '--payload={"simulate_smoke_failure": false}',
            ],
            expected_exit=0,
        ),
    ]
    p_chars, p_out_chars = self._measure_passport(log_id)
    steps.append(
        self._run_step("cleanup", ["cleanup", f"--log_id={log_id}"], 0)
    )
    y_ch, a_ch, r_ch = self._measure_blueprint("pypi_upgrade_guard")
    return WorkflowBenchmark(
        name="pypi_upgrade_guard",
        description=(
            "Audit PyPI -> gate -> smoke-test failure + auto-rollback -> resume"
        ),
        is_authoring_workflow=False,
        steps=steps,
        yaml_chars=y_ch,
        actions_chars=a_ch,
        readme_chars=r_ch,
        passport_chars=p_chars,
        passport_outputs_chars=p_out_chars,
    )

  def bench_async_job_watcher(self) -> WorkflowBenchmark:
    log_id = "bench_async"
    status_file = os.path.join(self.work_dir, "async_job_status.json")
    steps = [
        self._run_step(
            "start (spawn worker -> poll until READY -> verify -> cleanup)",
            [
                "start",
                "--lightflow=examples/async_job_watcher",
                f"--log_id={log_id}",
                (
                    '--payload={"delay_seconds": 1.0, "status_file":'
                    f' "{status_file}"}}'
                ),
            ],
            expected_exit=0,
        ),
    ]
    p_chars, p_out_chars = self._measure_passport(log_id)
    steps.append(
        self._run_step("cleanup", ["cleanup", f"--log_id={log_id}"], 0)
    )
    y_ch, a_ch, r_ch = self._measure_blueprint("async_job_watcher")
    return WorkflowBenchmark(
        name="async_job_watcher",
        description=(
            "Spawn background job -> polling_policy loop -> checksum verify"
        ),
        is_authoring_workflow=False,
        steps=steps,
        yaml_chars=y_ch,
        actions_chars=a_ch,
        readme_chars=r_ch,
        passport_chars=p_chars,
        passport_outputs_chars=p_out_chars,
    )

  def bench_create_lightflow(self) -> WorkflowBenchmark:
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
            "--lightflow=examples/create_lightflow",
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
        "def step_one(payload, **kwargs):\n"
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
            "--lightflow=examples/create_lightflow",
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
            "--lightflow=examples/create_lightflow",
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
    p_chars, p_out_chars = self._measure_passport(log_id)
    s5 = self._run_step("cleanup", ["cleanup", f"--log_id={log_id}"], 0)
    y_ch, a_ch, r_ch = self._measure_blueprint("create_lightflow")
    return WorkflowBenchmark(
        name="create_lightflow",
        description=(
            "3 human authoring gates + AST/test verifier + dry-run compiler"
        ),
        is_authoring_workflow=True,
        steps=[s1, s2, s3, s4, s5],
        yaml_chars=y_ch,
        actions_chars=a_ch,
        readme_chars=r_ch,
        passport_chars=p_chars,
        passport_outputs_chars=p_out_chars,
    )

  def run_all(self) -> list[WorkflowBenchmark]:
    return [
        self.bench_hn_digest(),
        self.bench_usgs_seismic_alert(),
        self.bench_pypi_upgrade_guard(),
        self.bench_async_job_watcher(),
        self.bench_create_lightflow(),
    ]


def format_markdown_report(
    results: list[WorkflowBenchmark],
    runner_skill_chars: int,
    author_skill_chars: int,
) -> str:
  """Formats a Markdown table and fairness breakdown of the benchmark results."""
  runner_skill_tokens = estimate_tokens(runner_skill_chars)
  author_skill_tokens = estimate_tokens(author_skill_chars)
  lines: list[str] = []
  lines.append("# Lightflow Token-Efficiency Benchmark Results")
  lines.append("")
  lines.append("## 1. Per-Workflow Context Footprint (Measured End-to-End)")
  lines.append("")
  lines.append(
      "| Workflow | CLI Calls | Warm Runtime (`cmd + stdout`) | Cold Start"
      " (`N=1` incl. `SKILL.md`) | Multi-Turn Cumulative | Out-of-Context State"
      " (`passport.json`) | Source Avoided (`yaml + actions.py`) |"
      " Zero-Context Savings (Warm) |"
  )
  lines.append(
      "| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: |"
  )

  tot_runtime_ch = 0
  tot_cum_tok = 0
  tot_passport_ch = 0
  tot_blueprint_ch = 0

  for r in results:
    n_calls = len(r.steps)
    rt_ch = r.runtime_total_chars
    rt_tok = r.runtime_total_tokens
    skill_ch = (
        runner_skill_chars + author_skill_chars
        if r.is_authoring_workflow
        else runner_skill_chars
    )
    n1_cold_ch = rt_ch + skill_ch
    n1_cold_tok = estimate_tokens(n1_cold_ch)
    cum_tok = r.cumulative_multiturn_tokens
    pp_ch = r.passport_chars
    pp_tok = r.passport_tokens
    bp_ch = r.blueprint_chars
    bp_tok = r.blueprint_tokens
    savings = r.zero_context_savings_ratio

    tot_runtime_ch += rt_ch
    tot_cum_tok += cum_tok
    tot_passport_ch += pp_ch
    tot_blueprint_ch += bp_ch

    lines.append(
        f"| `{r.name}` | {n_calls} | {rt_ch:,} ch (~**{rt_tok:,} tok**) |"
        f" {n1_cold_ch:,} ch (~{n1_cold_tok:,} tok) | ~{cum_tok:,} tok |"
        f" {pp_ch:,} ch (~{pp_tok:,} tok) | {bp_ch:,} ch (~{bp_tok:,} tok) |"
        f" **{savings * 100:.0f}%** |"
    )

  tot_runtime_tok = estimate_tokens(tot_runtime_ch)
  tot_pp_tok = estimate_tokens(tot_passport_ch)
  tot_bp_tok = estimate_tokens(tot_blueprint_ch)
  warm_savings = tot_blueprint_ch / (tot_runtime_ch + tot_blueprint_ch)

  lines.append(
      "| **Total (5 workflows, warm)** |"
      f" **{sum(len(r.steps) for r in results)}** | **{tot_runtime_ch:,} ch"
      f" (~{tot_runtime_tok:,} tok)** | — | **~{tot_cum_tok:,} tok** |"
      f" **{tot_passport_ch:,} ch (~{tot_pp_tok:,} tok)** |"
      f" **{tot_blueprint_ch:,} ch (~{tot_bp_tok:,} tok)** |"
      f" **{warm_savings * 100:.0f}%** |"
  )
  session_cold_ch = tot_runtime_ch + runner_skill_chars + author_skill_chars
  session_cold_tok = estimate_tokens(session_cold_ch)
  cold_savings = tot_blueprint_ch / (session_cold_ch + tot_blueprint_ch)
  lines.append(
      "| **Session Total (`N=5`, cold start incl. both `SKILL.md`s)** |"
      f" **{sum(len(r.steps) for r in results)} + 2 reads** | — |"
      f" **{session_cold_ch:,} ch (~{session_cold_tok:,} tok)** | — |"
      f" **{tot_passport_ch:,} ch (~{tot_pp_tok:,} tok)** |"
      f" **{tot_blueprint_ch:,} ch (~{tot_bp_tok:,} tok)** |"
      f" **{cold_savings * 100:.0f}%** |"
  )
  lines.append("")
  lines.append("## 2. Step-by-Step Breakdown")
  lines.append("")
  for r in results:
    lines.append(f"### `{r.name}` — {r.description}")
    for idx, s in enumerate(r.steps, 1):
      lines.append(
          f"- **Step {idx} (`{s.label}`, exit `{s.exit_code}`)**: input"
          f" `{s.cmd_chars}` ch + stdout `{s.stdout_chars}` ch ="
          f" **{s.total_chars:,} ch (~{s.total_tokens} tok)**"
      )
    lines.append(
        "- **Persisted in `passport.json` (kept out of LLM context)**:"
        f" `{r.passport_chars:,}` ch (~`{r.passport_tokens:,}` tok), of which"
        f" `payload.outputs` = `{r.passport_outputs_chars:,}` ch"
    )
    lines.append("")
  lines.append(
      "*Note: Cold-start skill sizes are `skills/run_lightflows/SKILL.md` ="
      f" {runner_skill_chars:,} chars (~{runner_skill_tokens:,} tokens) for"
      " running existing workflows, plus `skills/create_lightflows/SKILL.md` ="
      f" {author_skill_chars:,} chars (~{author_skill_tokens:,} tokens) when"
      " authoring a new workflow via `create_lightflow`.*"
  )
  return "\n".join(lines)


def main() -> int:
  parser = argparse.ArgumentParser(description=__doc__)
  parser.add_argument(
      "--json", action="store_true", help="Emit machine-readable JSON metrics."
  )
  args = parser.parse_args()

  runner = BenchmarkRunner()
  try:
    results = runner.run_all()
    runner_skill_chars = runner._file_chars("skills/run_lightflows/SKILL.md")
    author_skill_chars = runner._file_chars("skills/create_lightflows/SKILL.md")
  finally:
    runner.close()

  if args.json:
    payload = {
        "runner_skill_chars": runner_skill_chars,
        "runner_skill_tokens": estimate_tokens(runner_skill_chars),
        "author_skill_chars": author_skill_chars,
        "author_skill_tokens": estimate_tokens(author_skill_chars),
        "workflows": [r.to_dict() for r in results],
    }
    print(json.dumps(payload, indent=2))
  else:
    print(
        format_markdown_report(results, runner_skill_chars, author_skill_chars)
    )
  return 0


if __name__ == "__main__":
  sys.exit(main())
