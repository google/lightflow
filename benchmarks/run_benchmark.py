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

Executes all 6 included example Lightflows end-to-end and measures:
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
  """End-to-end head-to-head benchmark metrics for a workflow pair (Arm A vs Arm B)."""

  name: str
  description: str
  is_authoring_workflow: bool
  steps: list[StepMetric]
  yaml_chars: int
  actions_chars: int
  readme_chars: int
  passport_chars: int
  passport_outputs_chars: int
  order_match: bool = True
  no_duplicate_side_effects: bool = True
  gate_discipline: bool = True
  external_state_valid: bool = True
  completed_stages: list[str] = dataclasses.field(default_factory=list)
  baseline_skill_chars: int = 0
  baseline_steps: list[StepMetric] = dataclasses.field(default_factory=list)
  baseline_external_state_valid: bool = True

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
  def runtime_cmd_tokens(self) -> int:
    return estimate_tokens(self.runtime_cmd_chars)

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
  def baseline_skill_tokens(self) -> int:
    return estimate_tokens(self.baseline_skill_chars)

  @property
  def baseline_cmd_chars(self) -> int:
    return sum(s.cmd_chars for s in self.baseline_steps)

  @property
  def baseline_cmd_tokens(self) -> int:
    return estimate_tokens(self.baseline_cmd_chars)

  @property
  def baseline_stdout_chars(self) -> int:
    return sum(s.stdout_chars for s in self.baseline_steps)

  @property
  def baseline_total_chars(self) -> int:
    return sum(s.total_chars for s in self.baseline_steps)

  @property
  def baseline_total_tokens(self) -> int:
    return estimate_tokens(self.baseline_total_chars)

  @property
  def baseline_cumulative_tokens(self) -> int:
    running = 0
    cumulative = 0
    for s in self.baseline_steps:
      running += s.total_tokens
      cumulative += running
    return cumulative

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

  @property
  def all_checks_passed(self) -> bool:
    return (
        self.order_match
        and self.no_duplicate_side_effects
        and self.gate_discipline
        and self.external_state_valid
        and self.baseline_external_state_valid
    )

  def to_dict(self) -> dict[str, Any]:
    """Serializes the workflow benchmark metrics to a dictionary."""
    baseline_dict = None
    if self.baseline_steps:
      baseline_dict = {
          "skill_chars": self.baseline_skill_chars,
          "skill_tokens": self.baseline_skill_tokens,
          "cmd_chars": self.baseline_cmd_chars,
          "cmd_tokens": self.baseline_cmd_tokens,
          "stdout_chars": self.baseline_stdout_chars,
          "warm_total_chars": self.baseline_total_chars,
          "warm_total_tokens": self.baseline_total_tokens,
          "cold_start_chars": (
              self.baseline_skill_chars + self.baseline_total_chars
          ),
          "cold_start_tokens": estimate_tokens(
              self.baseline_skill_chars + self.baseline_total_chars
          ),
          "cumulative_multiturn_tokens": self.baseline_cumulative_tokens,
          "external_state_valid": self.baseline_external_state_valid,
          "steps": [s.to_dict() for s in self.baseline_steps],
      }
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
        "runtime_cmd_tokens": self.runtime_cmd_tokens,
        "runtime_stdout_chars": self.runtime_stdout_chars,
        "runtime_total_chars": self.runtime_total_chars,
        "runtime_total_tokens": self.runtime_total_tokens,
        "cumulative_multiturn_tokens": self.cumulative_multiturn_tokens,
        "passport_chars": self.passport_chars,
        "passport_tokens": self.passport_tokens,
        "passport_outputs_chars": self.passport_outputs_chars,
        "passport_outputs_tokens": self.passport_outputs_tokens,
        "zero_context_savings_ratio": round(self.zero_context_savings_ratio, 4),
        "baseline_traditional_skill": baseline_dict,
        "correctness": {
            "order_match": self.order_match,
            "no_duplicate_side_effects": self.no_duplicate_side_effects,
            "gate_discipline": self.gate_discipline,
            "external_state_valid": self.external_state_valid,
            "all_checks_passed": self.all_checks_passed,
            "completed_stages": self.completed_stages,
        },
    }


def _posix_path(path: str) -> str:
  """Normalizes a filesystem path to forward slashes for cross-platform JSON."""
  return path.replace("\\", "/")


class BenchmarkRunner:
  """Runs each example workflow in an isolated state directory and records sizes."""

  def __init__(self) -> None:
    self.state_dir = tempfile.mkdtemp(prefix="lightflow_bench_state_")
    self.work_dir = tempfile.mkdtemp(prefix="lightflow_bench_work_")
    site_dir = os.path.join(self.state_dir, "_bench_site")
    os.makedirs(site_dir, exist_ok=True)
    with open(
        os.path.join(site_dir, "sitecustomize.py"), "w", encoding="utf-8"
    ) as sf:
      sf.write(
          "import json, os, sys\n"
          "_tf = os.environ.get('LIGHTFLOW_BENCH_TRACE_FILE', '').strip()\n"
          "if _tf:\n"
          "  def _prof(frame, event, arg):\n"
          "    if event != 'call': return\n"
          "    c = frame.f_code\n"
          "    if c.co_name.startswith(('_', '<')): return\n"
          "    fn = c.co_filename.replace('\\\\', '/')\n"
          "    if '/examples/' in fn and fn.endswith('/actions.py'):\n"
          "      loc = frame.f_locals\n"
          "      p = loc.get('payload') if isinstance(loc.get('payload'), dict)"
          " else {}\n"
          "      if loc.get('dry_run') or p.get('dry_run'): return\n"
          "      with open(_tf, 'a', encoding='utf-8') as f:\n"
          "        f.write(json.dumps({'func': c.co_name}) + '\\n')\n"
          "  sys.setprofile(_prof)\n"
      )
    self.env = os.environ.copy()
    self.env["PYTHONPATH"] = os.pathsep.join([site_dir, _OSS_ROOT])
    self.env["LIGHTFLOW_STATE_DIR"] = self.state_dir
    self.env["PYTHONDONTWRITEBYTECODE"] = "1"
    self.env.pop("ANTIGRAVITY_CONVERSATION_ID", None)
    self.env.pop("LIGHTFLOW_OPERATOR", None)

  def close(self) -> None:
    shutil.rmtree(self.state_dir, ignore_errors=True)
    shutil.rmtree(self.work_dir, ignore_errors=True)

  def _normalize_text(self, text: str) -> str:
    """Normalizes OS-specific temp paths, PIDs, and timestamps for determinism."""
    norm = text
    for raw_dir, canonical in (
        (self.work_dir, _CANONICAL_WORK_DIR),
        (self.state_dir, "/tmp/lf_state"),
        (_OSS_ROOT, "/repo/lightflow"),
    ):
      norm = norm.replace(raw_dir, canonical)
      norm = norm.replace(_posix_path(raw_dir), canonical)
      norm = norm.replace(json.dumps(raw_dir)[1:-1], canonical)
    norm = _PID_RE.sub("PID 12345", norm)
    norm = _JSON_PID_RE.sub('"worker_pid": 12345', norm)
    norm = _TS_RE.sub("2026-01-01T00:00:00Z", norm)
    return norm

  def file_chars(self, rel_path: str) -> int:
    full = os.path.join(_OSS_ROOT, rel_path)
    if not os.path.exists(full):
      return 0
    with open(full, "r", encoding="utf-8") as f:
      return len(f.read())

  def _file_chars(self, rel_path: str) -> int:
    return self.file_chars(rel_path)

  def _measure_blueprint(self, example_name: str) -> tuple[int, int, int]:
    base = os.path.join("examples", example_name)
    return (
        self.file_chars(os.path.join(base, "lightflow.yaml")),
        self.file_chars(os.path.join(base, "actions.py")),
        self.file_chars(os.path.join(base, "README.md")),
    )

  def _load_passport_data(self, log_id: str) -> dict[str, Any]:
    passport_path = os.path.join(
        self.state_dir, f"lightflow_state_{log_id}", "passport.json"
    )
    if not os.path.exists(passport_path):
      return {}
    with open(passport_path, "r", encoding="utf-8") as f:
      return json.loads(f.read())

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

  def _verify_passport_trace(
      self,
      log_id: str,
      expected_completed_order: list[str],
      gate_stages: list[str],
  ) -> tuple[bool, bool, bool, list[str]]:
    """Checks stage order, single-execution of completed stages, and gate pauses."""
    data = self._load_passport_data(log_id)
    stamps = data.get("stamps", [])
    completed_order: list[str] = []
    completed_counts: dict[str, int] = {}
    stage_statuses: dict[str, list[str]] = {}

    for st in stamps:
      sname = st.get("stage_name", "")
      status = st.get("status", "")
      stage_statuses.setdefault(sname, []).append(status)
      if status == "COMPLETED":
        completed_order.append(sname)
        completed_counts[sname] = completed_counts.get(sname, 0) + 1

    order_match = completed_order == expected_completed_order
    no_dup = all(count == 1 for count in completed_counts.values())
    gate_ok = True
    for g in gate_stages:
      statuses = stage_statuses.get(g, [])
      if "PAUSED" not in statuses or "COMPLETED" not in statuses:
        gate_ok = False
      elif statuses.index("PAUSED") > statuses.index("COMPLETED"):
        gate_ok = False
    return order_match, no_dup, gate_ok, completed_order

  def _run_step(
      self,
      label: str,
      args: list[str],
      expected_exit: int,
      extra_input_chars: int = 0,
  ) -> StepMetric:
    raw_cmd = "lightflow " + " ".join(shlex.quote(a) for a in args)
    norm_cmd = self._normalize_text(raw_cmd)
    cmd = [sys.executable, "-B", "-m", "lightflow"] + args
    res = subprocess.run(
        cmd,
        cwd=_OSS_ROOT,
        env=self.env,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
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
    out_md = _posix_path(os.path.join(self.work_dir, "hn_digest.md"))
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
                "--payload="
                + json.dumps({"editor_note": "LGTM", "output_path": out_md}),
            ],
            expected_exit=0,
        ),
    ]
    p_chars, p_out_chars = self._measure_passport(log_id)
    order_ok, no_dup, gate_ok, comp = self._verify_passport_trace(
        log_id,
        ["fetch_top_stories", "editorial_gate", "publish_digest"],
        ["editorial_gate"],
    )
    ext_ok = os.path.exists(out_md) and os.path.getsize(out_md) > 0
    steps.append(
        self._run_step("cleanup", ["cleanup", f"--log_id={log_id}"], 0)
    )

    # Arm B: Traditional Skill (baseline_skills/hn_digest/SKILL.md + actions.py)
    b_state = _posix_path(os.path.join(self.work_dir, "hn_b_state.json"))
    b_out_md = _posix_path(os.path.join(self.work_dir, "hn_b_digest.md"))
    bs1, _ = self._run_python_snippet(
        "B step 1: fetch_top_stories -> state.json",
        "import json, sys; sys.path.insert(0, 'examples/hn_digest');"
        " import actions; p = {'offline': True, 'limit': 3};"
        " d, m = actions.fetch_top_stories(p);"
        " p['outputs'] = {'fetch_top_stories': d};"
        f" json.dump(p, open({repr(b_state)}, 'w')); print(m)",
    )
    bs2, _ = self._run_python_snippet(
        "B step 2: editorial_gate + publish_digest",
        "import json, sys; sys.path.insert(0, 'examples/hn_digest');"
        f" import actions; p = json.load(open({repr(b_state)}));"
        " p.update("
        + repr({"editor_note": "LGTM", "output_path": b_out_md})
        + "); d, m = actions.publish_digest(p); print(m)",
    )
    bs3, _ = self._run_python_snippet(
        "B step 3: cleanup state.json",
        f"import os; os.remove({repr(b_state)})",
    )
    b_ext_ok = (
        os.path.exists(b_out_md)
        and os.path.getsize(b_out_md) > 0
        and not os.path.exists(b_state)
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
        order_match=order_ok,
        no_duplicate_side_effects=no_dup,
        gate_discipline=gate_ok,
        external_state_valid=ext_ok,
        completed_stages=comp,
        baseline_skill_chars=self.file_chars(
            "benchmarks/baseline_skills/hn_digest/SKILL.md"
        ),
        baseline_steps=[bs1, bs2, bs3],
        baseline_external_state_valid=b_ext_ok,
    )

  def bench_usgs_seismic_alert(self) -> WorkflowBenchmark:
    log_id = "bench_usgs"
    out_md = _posix_path(os.path.join(self.work_dir, "usgs_bulletin.md"))
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
                "--payload="
                + json.dumps({"severity": "ADVISORY", "output_path": out_md}),
            ],
            expected_exit=0,
        ),
    ]
    p_chars, p_out_chars = self._measure_passport(log_id)
    order_ok, no_dup, gate_ok, comp = self._verify_passport_trace(
        log_id,
        ["fetch_usgs_feed", "incident_commander_gate", "emit_bulletin"],
        ["incident_commander_gate"],
    )
    ext_ok = os.path.exists(out_md) and os.path.getsize(out_md) > 0
    steps.append(
        self._run_step("cleanup", ["cleanup", f"--log_id={log_id}"], 0)
    )

    # Arm B: Traditional Skill (usgs_seismic_alert/SKILL.md + actions.py)
    b_state = _posix_path(os.path.join(self.work_dir, "usgs_b_state.json"))
    b_out_md = _posix_path(os.path.join(self.work_dir, "usgs_b_bulletin.md"))
    bs1, _ = self._run_python_snippet(
        "B step 1: fetch_usgs_feed -> state.json",
        "import json, sys; sys.path.insert(0, 'examples/usgs_seismic_alert');"
        " import actions; p = {'offline': True};"
        " d = actions.fetch_usgs_feed(p);"
        " p['outputs'] = {'fetch_usgs_feed': d};"
        f" json.dump(p, open({repr(b_state)}, 'w'));"
        " print(f\"M{d['max_magnitude']} at {d['strongest_place']}\")",
    )
    bs2, _ = self._run_python_snippet(
        "B step 2: conditional gate + emit_bulletin",
        "import json, sys; sys.path.insert(0, 'examples/usgs_seismic_alert');"
        f" import actions; p = json.load(open({repr(b_state)}));"
        " p.update("
        + repr({"severity": "ADVISORY", "output_path": b_out_md})
        + "); d, m = actions.emit_bulletin(p); print(m)",
    )
    bs3, _ = self._run_python_snippet(
        "B step 3: cleanup state.json",
        f"import os; os.remove({repr(b_state)})",
    )
    b_ext_ok = (
        os.path.exists(b_out_md)
        and os.path.getsize(b_out_md) > 0
        and not os.path.exists(b_state)
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
        order_match=order_ok,
        no_duplicate_side_effects=no_dup,
        gate_discipline=gate_ok,
        external_state_valid=ext_ok,
        completed_stages=comp,
        baseline_skill_chars=self.file_chars(
            "benchmarks/baseline_skills/usgs_seismic_alert/SKILL.md"
        ),
        baseline_steps=[bs1, bs2, bs3],
        baseline_external_state_valid=b_ext_ok,
    )

  def bench_pypi_upgrade_guard(self) -> WorkflowBenchmark:
    log_id = "bench_pypi"
    reqs_path = _posix_path(os.path.join(self.work_dir, "requirements.txt"))
    s1 = self._run_step(
        "start (audit PyPI -> pause at approve_upgrades)",
        [
            "start",
            "--lightflow=examples/pypi_upgrade_guard",
            f"--log_id={log_id}",
            "--payload="
            + json.dumps({"offline": True, "requirements_path": reqs_path}),
        ],
        expected_exit=2,
    )
    s2 = self._run_step(
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
    )
    with open(reqs_path, "r", encoding="utf-8") as f:
      rolled_back_text = f.read()
    rollback_ok = "PyYAML==6.0\n" in rolled_back_text and not os.path.exists(
        reqs_path + ".bak"
    )
    s3 = self._run_step(
        "resume surgical recovery (preserves upstream stages)",
        [
            "resume",
            "--lightflow=examples/pypi_upgrade_guard",
            f"--log_id={log_id}",
            '--payload={"simulate_smoke_failure": false}',
        ],
        expected_exit=0,
    )
    with open(reqs_path, "r", encoding="utf-8") as f:
      upgraded_text = f.read()
    steps = [s1, s2, s3]
    p_chars, p_out_chars = self._measure_passport(log_id)
    order_ok, no_dup, gate_ok, comp = self._verify_passport_trace(
        log_id,
        ["audit_pypi_versions", "approve_upgrades", "apply_and_smoke_test"],
        ["approve_upgrades"],
    )
    ext_ok = rollback_ok and ("PyYAML==6.0.2" in upgraded_text)
    steps.append(
        self._run_step("cleanup", ["cleanup", f"--log_id={log_id}"], 0)
    )

    # Arm B: Traditional Skill (pypi_upgrade_guard/SKILL.md + actions.py)
    b_state = _posix_path(os.path.join(self.work_dir, "pypi_b_state.json"))
    b_reqs = _posix_path(os.path.join(self.work_dir, "pypi_b_reqs.txt"))
    bs1, _ = self._run_python_snippet(
        "B step 1: audit_pypi_versions -> state.json",
        "import json, sys; sys.path.insert(0, 'examples/pypi_upgrade_guard');"
        " import actions; p ="
        f" {repr({'offline': True, 'requirements_path': b_reqs})};"
        " d, m = actions.audit_pypi_versions(p);"
        " p['outputs'] = {'audit_pypi_versions': d};"
        f" json.dump(p, open({repr(b_state)}, 'w')); print(m)",
    )
    bs2, _ = self._run_python_snippet(
        "B step 2: apply_and_smoke_test (smoke failure + manual rollback)",
        "import json, sys; sys.path.insert(0, 'examples/pypi_upgrade_guard');"
        f" import actions; p = json.load(open({repr(b_state)}));"
        " p.update({'approved': True, 'simulate_smoke_failure': True})\n"
        "try:\n"
        "  actions.apply_and_smoke_test(p)\n"
        "except Exception as e:\n"
        "  _, rb_m = actions.restore_requirements_backup(p)\n"
        f"  json.dump(p, open({repr(b_state)}, 'w'))\n"
        "  print(f'FAILED: {e} | ROLLBACK: {rb_m}')",
    )
    with open(b_reqs, "r", encoding="utf-8") as f:
      b_rb_ok = "PyYAML==6.0\n" in f.read() and not os.path.exists(
          b_reqs + ".bak"
      )
    bs3, _ = self._run_python_snippet(
        "B step 3: surgical recovery apply_and_smoke_test",
        "import json, sys; sys.path.insert(0, 'examples/pypi_upgrade_guard');"
        f" import actions; p = json.load(open({repr(b_state)}));"
        " p['simulate_smoke_failure'] = False;"
        " d, m = actions.apply_and_smoke_test(p);"
        " p['outputs']['apply_and_smoke_test'] = d;"
        f" json.dump(p, open({repr(b_state)}, 'w')); print(m)",
    )
    bs4, _ = self._run_python_snippet(
        "B step 4: cleanup state.json",
        f"import os; os.remove({repr(b_state)})",
    )
    with open(b_reqs, "r", encoding="utf-8") as f:
      b_up_ok = "PyYAML==6.0.2" in f.read()
    b_ext_ok = b_rb_ok and b_up_ok and not os.path.exists(b_state)

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
        order_match=order_ok,
        no_duplicate_side_effects=no_dup,
        gate_discipline=gate_ok,
        external_state_valid=ext_ok,
        completed_stages=comp,
        baseline_skill_chars=self.file_chars(
            "benchmarks/baseline_skills/pypi_upgrade_guard/SKILL.md"
        ),
        baseline_steps=[bs1, bs2, bs3, bs4],
        baseline_external_state_valid=b_ext_ok,
    )

  def bench_async_job_watcher(self) -> WorkflowBenchmark:
    log_id = "bench_async"
    status_file = _posix_path(
        os.path.join(self.work_dir, "async_job_status.json")
    )
    steps = [
        self._run_step(
            "start (spawn worker -> poll until READY -> verify -> cleanup)",
            [
                "start",
                "--lightflow=examples/async_job_watcher",
                f"--log_id={log_id}",
                "--payload="
                + json.dumps(
                    {"delay_seconds": 1.0, "status_file": status_file}
                ),
            ],
            expected_exit=0,
        ),
    ]
    p_chars, p_out_chars = self._measure_passport(log_id)
    order_ok, no_dup, gate_ok, comp = self._verify_passport_trace(
        log_id,
        [
            "spawn_background_job",
            "await_job_completion",
            "verify_exported_artifact",
            "cleanup_job_status_file",
        ],
        [],
    )
    ext_ok = not os.path.exists(status_file)
    steps.append(
        self._run_step("cleanup", ["cleanup", f"--log_id={log_id}"], 0)
    )

    # Arm B: Traditional Skill (async_job_watcher/SKILL.md + actions.py)
    b_status_file = _posix_path(
        os.path.join(self.work_dir, "async_b_status.json")
    )
    bs1, _ = self._run_python_snippet(
        "B step 1: spawn -> manual poll loop -> verify -> cleanup",
        "import sys, time; sys.path.insert(0, 'examples/async_job_watcher');"
        " import actions; p ="
        f" {repr({'delay_seconds': 1.0, 'status_file': b_status_file})};"
        " p['outputs'] = {};"
        " d1, m1 = actions.spawn_background_job(p);"
        " p['outputs']['spawn_background_job'] = d1; print(m1)\n"
        "for _ in range(15):\n"
        "  d2, m2 = actions.check_job_status_file(p)\n"
        "  if d2.get('job_status') == 'READY':\n"
        "    p['outputs']['await_job_completion'] = d2; print(m2); break\n"
        "  time.sleep(0.5)\n"
        "else:\n"
        "  raise RuntimeError('Poll timeout')\n"
        "d3, m3 = actions.verify_exported_artifact(p);"
        " p['outputs']['verify_exported_artifact'] = d3; print(m3)\n"
        "d4, m4 = actions.cleanup_job_status_file(p);"
        " p['outputs']['cleanup_job_status_file'] = d4; print(m4)",
    )
    b_ext_ok = not os.path.exists(b_status_file)

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
        order_match=order_ok,
        no_duplicate_side_effects=no_dup,
        gate_discipline=gate_ok,
        external_state_valid=ext_ok,
        completed_stages=comp,
        baseline_skill_chars=self.file_chars(
            "benchmarks/baseline_skills/async_job_watcher/SKILL.md"
        ),
        baseline_steps=[bs1],
        baseline_external_state_valid=b_ext_ok,
    )

  def bench_create_lightflow(self) -> WorkflowBenchmark:
    log_id = "bench_create"
    scaffold_dir = _posix_path(os.path.join(self.work_dir, "scaffold_demo"))
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
    order_ok, no_dup, gate_ok, comp = self._verify_passport_trace(
        log_id,
        [
            "align_on_design",
            "implement_and_test_actions",
            "verify_actions",
            "draft_and_explain_manifest",
            "prove_manifest_dry_run",
        ],
        [
            "align_on_design",
            "implement_and_test_actions",
            "draft_and_explain_manifest",
        ],
    )
    ext_ok = os.path.exists(os.path.join(scaffold_dir, "visualizer.html"))
    s5 = self._run_step("cleanup", ["cleanup", f"--log_id={log_id}"], 0)
    y_ch, a_ch, r_ch = self._measure_blueprint("create_lightflow")
    return WorkflowBenchmark(
        name="create_lightflow",
        description=(
            "3 human/agent authoring checkpoints + AST/test verifier + dry-run"
            " compiler"
        ),
        is_authoring_workflow=True,
        steps=[s1, s2, s3, s4, s5],
        yaml_chars=y_ch,
        actions_chars=a_ch,
        readme_chars=r_ch,
        passport_chars=p_chars,
        passport_outputs_chars=p_out_chars,
        order_match=order_ok,
        no_duplicate_side_effects=no_dup,
        gate_discipline=gate_ok,
        external_state_valid=ext_ok,
        completed_stages=comp,
    )

  def bench_tenant_gitops_onboarding(self) -> WorkflowBenchmark:
    """Benchmarks the 22-stage multi-branch tenant_gitops_onboarding workflow."""
    log_id = "bench_tenant"
    sandbox_dir = _posix_path(os.path.join(self.work_dir, "tenant_sandbox"))
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
                "--lightflow=examples/tenant_gitops_onboarding",
                f"--log_id={log_id}",
                "--stage=verify_spec",
                "--resolution=APPROVE",
                '--payload={"approved": true}',
            ],
            expected_exit=2,
        ),
        self._run_step(
            "resume prompt_iam_quota_override (Phases 3-4 KMS/GitOps/ArgoCD ->"
            " complete)",
            [
                "resume",
                "--lightflow=examples/tenant_gitops_onboarding",
                f"--log_id={log_id}",
                "--stage=prompt_iam_quota_override",
                "--resolution=APPROVE",
                '--payload={"approved": true, "quota_override_approved": true}',
            ],
            expected_exit=0,
        ),
    ]
    p_chars, p_out_chars = self._measure_passport(log_id)
    order_ok, no_dup, gate_ok, comp = self._verify_passport_trace(
        log_id,
        [
            "init_spec",
            "check_admin",
            "verify_spec",
            "create_ticket",
            "wait_governance_approval",
            "provision_identity_groups",
            "wait_scim_sync",
            "create_workload_iam_role",
            "prompt_iam_quota_override",
            "wait_iam_role_ready",
            "provision_external_resources",
            "provision_dedicated_kms_key",
            "checkpoint_catalog",
            "scaffold_k8s_gitops_pr",
            "wait_k8s_gitops_pr",
            "wait_argocd_namespace_sync",
            "package_mesh_and_catalog_pr",
            "wait_mesh_and_catalog_pr",
            "wait_argocd_mesh_sync",
        ],
        ["verify_spec", "prompt_iam_quota_override"],
    )
    cp_path = os.path.join(sandbox_dir, "control_plane_state.json")
    ext_ok = os.path.exists(cp_path)
    steps.append(
        self._run_step("cleanup", ["cleanup", f"--log_id={log_id}"], 0)
    )

    # Arm B: Traditional Skill (tenant_gitops_onboarding/SKILL.md + actions.py)
    b_state = _posix_path(os.path.join(self.work_dir, "tenant_b_state.json"))
    b_sandbox = _posix_path(os.path.join(self.work_dir, "tenant_b_sandbox"))
    bs1, _ = self._run_python_snippet(
        "B step 1: Phase 1 (init_spec + check_admin) -> pause at verify_spec",
        "import json, sys; sys.path.insert(0,"
        " 'examples/tenant_gitops_onboarding'); import actions; p ="
        f" {repr({'alias': 'payments-eu', 'compliance_tier': 'pci', 'sandbox_dir': b_sandbox, 'require_manual_quota_override': True})};"
        " p['outputs'] = {}; d1, m1 = actions.initialize_spec(p);"
        " p['outputs']['init_spec'] = d1; print(m1); d2, m2 ="
        " actions.check_operator_is_admin(p); p['outputs']['check_admin'] ="
        f" d2; print(m2); json.dump(p, open({repr(b_state)}, 'w'))",
    )
    bs2, _ = self._run_python_snippet(
        "B step 2: Phase 2 (ticket + poll + IdP/SCIM + IAM) -> pause at"
        " prompt_iam_quota_override",
        "import json, sys; sys.path.insert(0,"
        " 'examples/tenant_gitops_onboarding'); import actions; p ="
        f" json.load(open({repr(b_state)})); p['outputs']['verify_spec'] ="
        " {'approved': True}\n"
        "d, m = actions.create_provisioning_request(p);"
        " p['outputs']['create_ticket'] = d; print(m)\n"
        "if p['outputs']['check_admin']['is_admin']:\n"
        "  while True:\n"
        "    d, m = actions.check_governance_approval(p)\n"
        "    if d.get('governance_approved'):"
        " p['outputs']['wait_governance_approval'] = d; print(m); break\n"
        "d, m = actions.provision_identity_groups(p);"
        " p['outputs']['provision_identity_groups'] = d; print(m)\n"
        "while True:\n"
        "  d, m = actions.check_scim_ready(p)\n"
        "  if d.get('scim_ready'): p['outputs']['wait_scim_sync'] = d;"
        " print(m); break\n"
        "d, m = actions.create_workload_iam_role(p);"
        " p['outputs']['create_workload_iam_role'] = d; print(m)\n"
        f"json.dump(p, open({repr(b_state)}, 'w'))",
    )
    bs3, _ = self._run_python_snippet(
        "B step 3: Phases 3-4 (IAM poll + PCI KMS branch + 2 GitOps PRs + 4"
        " polls)",
        "import json, sys; sys.path.insert(0,"
        " 'examples/tenant_gitops_onboarding'); import actions; p ="
        f" json.load(open({repr(b_state)}));"
        " p['outputs']['prompt_iam_quota_override'] = {'approved': True,"
        " 'quota_override_approved': True}\n"
        "while True:\n"
        "  d, m = actions.check_iam_role_ready(p)\n"
        "  if d.get('iam_role_ready'): p['outputs']['wait_iam_role_ready'] ="
        " d; print(m); break\n"
        "d, m = actions.provision_external_resources(p);"
        " p['outputs']['provision_external_resources'] = d; print(m)\n"
        "tier = p['outputs']['init_spec']['compliance_tier']\n"
        "if tier == 'pci':\n"
        "  d, m = actions.provision_dedicated_kms_key(p);"
        " p['outputs']['provision_dedicated_kms_key'] = d; print(m)\n"
        "else:\n"
        "  d, m = actions.apply_shared_kms_policy(p);"
        " p['outputs']['apply_shared_kms_policy'] = d; print(m)\n"
        "d, m = actions.checkpoint_registry(p);"
        " p['outputs']['checkpoint_catalog'] = d; print(m)\n"
        "d, m = actions.scaffold_k8s_gitops_pr(p);"
        " p['outputs']['scaffold_k8s_gitops_pr'] = d; print(m)\n"
        "while True:\n"
        "  d, m = actions.check_pr_merged(p,"
        " target_stage='scaffold_k8s_gitops_pr', target_pr_key='k8s_pr_id')\n"
        "  if d.get('pr_merged'): p['outputs']['wait_k8s_gitops_pr'] = d;"
        " print(m); break\n"
        "while True:\n"
        "  d, m = actions.check_argocd_synced(p, app_name='tenant-namespace',"
        " required_pr_stage='wait_k8s_gitops_pr')\n"
        "  if d.get('app_synced'):"
        " p['outputs']['wait_argocd_namespace_sync'] = d; print(m); break\n"
        "d, m = actions.package_mesh_and_catalog_pr(p);"
        " p['outputs']['package_mesh_and_catalog_pr'] = d; print(m)\n"
        "while True:\n"
        "  d, m = actions.check_pr_merged(p,"
        " target_stage='package_mesh_and_catalog_pr',"
        " target_pr_key='catalog_pr_id')\n"
        "  if d.get('pr_merged'): p['outputs']['wait_mesh_and_catalog_pr'] ="
        " d; print(m); break\n"
        "while True:\n"
        "  d, m = actions.check_argocd_synced(p, app_name='mesh-and-catalog',"
        " required_pr_stage='wait_mesh_and_catalog_pr')\n"
        "  if d.get('app_synced'): p['outputs']['wait_argocd_mesh_sync'] = d;"
        " print(m); break\n"
        f"json.dump(p, open({repr(b_state)}, 'w'))",
    )
    bs4, _ = self._run_python_snippet(
        "B step 4: cleanup state.json",
        f"import os; os.remove({repr(b_state)})",
    )
    b_cp_path = os.path.join(b_sandbox, "control_plane_state.json")
    b_ext_ok = os.path.exists(b_cp_path) and not os.path.exists(b_state)

    y_ch, a_ch, r_ch = self._measure_blueprint("tenant_gitops_onboarding")
    return WorkflowBenchmark(
        name="tenant_gitops_onboarding",
        description=(
            "22-stage multi-branch GitOps onboarding (2 gates, 3 branch"
            " patterns, 2 PRs, 6 polls)"
        ),
        is_authoring_workflow=False,
        steps=steps,
        yaml_chars=y_ch,
        actions_chars=a_ch,
        readme_chars=r_ch,
        passport_chars=p_chars,
        passport_outputs_chars=p_out_chars,
        order_match=order_ok,
        no_duplicate_side_effects=no_dup,
        gate_discipline=gate_ok,
        external_state_valid=ext_ok,
        completed_stages=comp,
        baseline_skill_chars=self.file_chars(
            "benchmarks/baseline_skills/tenant_gitops_onboarding/SKILL.md"
        ),
        baseline_steps=[bs1, bs2, bs3, bs4],
        baseline_external_state_valid=b_ext_ok,
    )

  def bench_blue_green_release(self) -> WorkflowBenchmark:
    """Benchmarks the 8-stage blue_green_release workflow across Arm A and Arm B."""
    log_id = "bench_bg"
    a_state_path = _posix_path(
        os.path.join(self.work_dir, "bg_arm_a_state.json")
    )
    s1 = self._run_step(
        "start (parallel validation + warmup poll -> pause at cutover gate)",
        [
            "start",
            "--lightflow=examples/blue_green_release",
            f"--log_id={log_id}",
            "--payload="
            + json.dumps({
                "release_state_path": a_state_path,
                "simulate_canary_regression": True,
            }),
        ],
        expected_exit=2,
    )
    s2 = self._run_step(
        "resume APPROVE (canary 5xx breach + automatic revert_canary_traffic)",
        [
            "resume",
            "--lightflow=examples/blue_green_release",
            f"--log_id={log_id}",
            "--stage=approve_canary_cutover",
            "--resolution=APPROVE",
            "--payload="
            + json.dumps({
                "approved": True,
                "canary_percent": 10,
                "simulate_canary_regression": True,
            }),
        ],
        expected_exit=1,
    )
    with open(a_state_path, "r", encoding="utf-8") as f:
      a_rb_state = json.load(f)
    a_rb_ok = (
        bool(a_rb_state.get("rolled_back"))
        and a_rb_state.get("active_slot") == "blue"
        and a_rb_state.get("canary_weight") == 0
    )
    s3 = self._run_step(
        "resume surgical recovery (preserves stages 1-5 -> promote + drain)",
        [
            "resume",
            "--lightflow=examples/blue_green_release",
            f"--log_id={log_id}",
            '--payload={"simulate_canary_regression": false}',
        ],
        expected_exit=0,
    )
    with open(a_state_path, "r", encoding="utf-8") as f:
      a_final_state = json.load(f)
    ext_ok = (
        a_rb_ok
        and a_final_state.get("active_slot") == "green"
        and bool(a_final_state.get("old_slot_decommissioned"))
    )
    steps = [s1, s2, s3]
    p_chars, p_out_chars = self._measure_passport(log_id)
    order_ok, no_dup, gate_ok, comp = self._verify_passport_trace(
        log_id,
        [
            "build_release_bundle",
            "run_security_scan",
            "run_integration_suite",
            "warm_green_environment",
            "approve_canary_cutover",
            "shift_and_verify_canary",
            "promote_green_to_prod",
            "decommission_old_slot",
        ],
        ["approve_canary_cutover"],
    )
    steps.append(
        self._run_step("cleanup", ["cleanup", f"--log_id={log_id}"], 0)
    )

    # Arm B: Traditional Skill (blue_green_release/SKILL.md + actions.py)
    b_rstate = _posix_path(os.path.join(self.work_dir, "bg_arm_b_rstate.json"))
    b_state = _posix_path(os.path.join(self.work_dir, "bg_b_state.json"))
    bs1, _ = self._run_python_snippet(
        "B step 1: stages 1-4 (bundle + parallel checks + warmup poll)",
        "import json; from examples.blue_green_release import actions\n"
        f"p = {{'release_state_path': {repr(b_rstate)}, 'outputs': {{}}}}\n"
        "d, m = actions.build_release_bundle(p);"
        " p['outputs']['build_release_bundle'] = d; print(m)\n"
        "d, m = actions.run_security_scan(p);"
        " p['outputs']['run_security_scan'] = d; print(m)\n"
        "d, m = actions.run_integration_suite(p);"
        " p['outputs']['run_integration_suite'] = d; print(m)\n"
        "while True:\n"
        "  d, m = actions.warm_green_environment(p)\n"
        "  if d.get('green_ready'):"
        " p['outputs']['warm_green_environment'] = d; print(m); break\n"
        f"json.dump(p, open({repr(b_state)}, 'w'))",
    )
    bs2, _ = self._run_python_snippet(
        "B step 2: gate approval + shift_and_verify_canary (5xx + rollback)",
        "import json; from examples.blue_green_release import actions\n"
        f"p = json.load(open({repr(b_state)}))\n"
        "p['outputs']['approve_canary_cutover'] = {'approved': True,"
        " 'canary_percent': 10}\n"
        "p['simulate_canary_regression'] = True\n"
        "try:\n"
        "  actions.shift_and_verify_canary(p)\n"
        "except Exception as e:\n"
        "  _, rb_m = actions.revert_canary_traffic(p)\n"
        f"  json.dump(p, open({repr(b_state)}, 'w'))\n"
        "  print(f'FAILED: {e} | ROLLBACK: {rb_m}')",
    )
    with open(b_rstate, "r", encoding="utf-8") as f:
      b_rb_state = json.load(f)
    b_rb_ok = (
        bool(b_rb_state.get("rolled_back"))
        and b_rb_state.get("active_slot") == "blue"
        and b_rb_state.get("canary_weight") == 0
    )
    bs3, _ = self._run_python_snippet(
        "B step 3: surgical recovery stages 6-8 (canary -> promote -> drain)",
        "import json; from examples.blue_green_release import actions\n"
        f"p = json.load(open({repr(b_state)}))\n"
        "p['simulate_canary_regression'] = False\n"
        "d, m = actions.shift_and_verify_canary(p);"
        " p['outputs']['shift_and_verify_canary'] = d; print(m)\n"
        "d, m = actions.promote_green_to_prod(p);"
        " p['outputs']['promote_green_to_prod'] = d; print(m)\n"
        "d, m = actions.decommission_old_slot(p);"
        " p['outputs']['decommission_old_slot'] = d; print(m)\n"
        f"json.dump(p, open({repr(b_state)}, 'w'))",
    )
    bs4, _ = self._run_python_snippet(
        "B step 4: cleanup state.json",
        f"import os; os.remove({repr(b_state)})",
    )
    with open(b_rstate, "r", encoding="utf-8") as f:
      b_final_state = json.load(f)
    b_ext_ok = (
        b_rb_ok
        and b_final_state.get("active_slot") == "green"
        and bool(b_final_state.get("old_slot_decommissioned"))
        and not os.path.exists(b_state)
    )

    y_ch, a_ch, r_ch = self._measure_blueprint("blue_green_release")
    return WorkflowBenchmark(
        name="blue_green_release",
        description=(
            "8-stage blue/green canary release (parallel validation, warmup"
            " poll, gate, canary rollback & recovery)"
        ),
        is_authoring_workflow=False,
        steps=steps,
        yaml_chars=y_ch,
        actions_chars=a_ch,
        readme_chars=r_ch,
        passport_chars=p_chars,
        passport_outputs_chars=p_out_chars,
        order_match=order_ok,
        no_duplicate_side_effects=no_dup,
        gate_discipline=gate_ok,
        external_state_valid=ext_ok,
        completed_stages=comp,
        baseline_skill_chars=self.file_chars(
            "benchmarks/baseline_skills/blue_green_release/SKILL.md"
        ),
        baseline_steps=[bs1, bs2, bs3, bs4],
        baseline_external_state_valid=b_ext_ok,
    )

  def bench_incident_db_failover(self) -> WorkflowBenchmark:
    """Benchmarks the 9-stage incident_db_failover workflow across Arm A and Arm B."""
    log_id = "bench_dbf"
    a_cstate_path = _posix_path(
        os.path.join(self.work_dir, "dbf_arm_a_state.json")
    )
    s1 = self._run_step(
        "start (outage -> elect -> WAL replay poll -> ALL_DONE fence -> gate)",
        [
            "start",
            "--lightflow=examples/incident_db_failover",
            f"--log_id={log_id}",
            "--payload="
            + json.dumps({
                "failover_state_path": a_cstate_path,
                "replication_lag_bytes": 4096,
                "simulate_pooler_write_error": True,
            }),
        ],
        expected_exit=2,
    )
    s2 = self._run_step(
        "resume APPROVE (promote standby -> pooler cutover failure + rollback)",
        [
            "resume",
            "--lightflow=examples/incident_db_failover",
            f"--log_id={log_id}",
            "--stage=approve_replica_promotion",
            "--resolution=APPROVE",
            "--payload="
            + json.dumps({
                "approved": True,
                "incident_ticket": "INC-2026-0841",
                "simulate_pooler_write_error": True,
            }),
        ],
        expected_exit=1,
    )
    with open(a_cstate_path, "r", encoding="utf-8") as f:
      a_rb_state = json.load(f)
    a_rb_ok = (
        a_rb_state.get("pooler_status") == "PAUSED_MAINTENANCE"
        and a_rb_state.get("promotion_count") == 1
    )
    s3 = self._run_step(
        "resume surgical recovery (preserves standby promotion -> ledger)",
        [
            "resume",
            "--lightflow=examples/incident_db_failover",
            f"--log_id={log_id}",
            '--payload={"simulate_pooler_write_error": false}',
        ],
        expected_exit=0,
    )
    with open(a_cstate_path, "r", encoding="utf-8") as f:
      a_final_state = json.load(f)
    ext_ok = (
        a_rb_ok
        and a_final_state.get("pooler_status") == "ONLINE_RW"
        and bool(a_final_state.get("audit_published"))
        and a_final_state.get("promotion_count") == 1
    )
    steps = [s1, s2, s3]
    p_chars, p_out_chars = self._measure_passport(log_id)
    order_ok, no_dup, gate_ok, comp = self._verify_passport_trace(
        log_id,
        [
            "detect_primary_outage",
            "elect_failover_candidate",
            "replay_missing_wal_segments",
            "fence_old_primary",
            "approve_replica_promotion",
            "promote_standby_replica",
            "cutover_pooler_and_verify_writes",
            "publish_failover_ledger",
        ],
        ["approve_replica_promotion"],
    )
    steps.append(
        self._run_step("cleanup", ["cleanup", f"--log_id={log_id}"], 0)
    )

    # Arm B: Traditional Skill (incident_db_failover/SKILL.md + actions.py)
    b_cstate_path = _posix_path(
        os.path.join(self.work_dir, "dbf_arm_b_state.json")
    )
    b_state = _posix_path(os.path.join(self.work_dir, "dbf_b_state.json"))
    bs1, _ = self._run_python_snippet(
        "B step 1: stages 1-5 (outage -> elect -> WAL branch -> fence)",
        "import json; from examples.incident_db_failover import actions\n"
        f"p = {{'failover_state_path': {repr(b_cstate_path)},"
        " 'replication_lag_bytes': 4096, 'outputs': {}}\n"
        "d, m = actions.detect_primary_outage(p);"
        " p['outputs']['detect_primary_outage'] = d; print(m)\n"
        "d, m = actions.elect_failover_candidate(p);"
        " p['outputs']['elect_failover_candidate'] = d; print(m)\n"
        "if d.get('lag_mode') == 'wal_replay_needed':\n"
        "  while True:\n"
        "    d, m = actions.replay_missing_wal_segments(p)\n"
        "    if d.get('wal_caught_up'):"
        " p['outputs']['replay_missing_wal_segments'] = d; print(m); break\n"
        "else:\n"
        "  d, m = actions.verify_zero_loss_sync(p);"
        " p['outputs']['verify_zero_loss_sync'] = d; print(m)\n"
        "d, m = actions.fence_old_primary(p);"
        " p['outputs']['fence_old_primary'] = d; print(m)\n"
        f"json.dump(p, open({repr(b_state)}, 'w'))",
    )
    bs2, _ = self._run_python_snippet(
        "B step 2: gate + promote_standby + cutover failure + rollback",
        "import json; from examples.incident_db_failover import actions\n"
        f"p = json.load(open({repr(b_state)}))\n"
        "p['outputs']['approve_replica_promotion'] = {'approved': True,"
        " 'incident_ticket': 'INC-2026-0841'}\n"
        "d, m = actions.promote_standby_replica(p);"
        " p['outputs']['promote_standby_replica'] = d; print(m)\n"
        "p['simulate_pooler_write_error'] = True\n"
        "try:\n"
        "  actions.cutover_pooler_and_verify_writes(p)\n"
        "except Exception as e:\n"
        "  _, rb_m = actions.revert_pooler_to_maintenance(p)\n"
        f"  json.dump(p, open({repr(b_state)}, 'w'))\n"
        "  print(f'FAILED: {e} | ROLLBACK: {rb_m}')",
    )
    with open(b_cstate_path, "r", encoding="utf-8") as f:
      b_rb_state = json.load(f)
    b_rb_ok = (
        b_rb_state.get("pooler_status") == "PAUSED_MAINTENANCE"
        and b_rb_state.get("promotion_count") == 1
    )
    bs3, _ = self._run_python_snippet(
        "B step 3: surgical recovery stages 8-9 (cutover -> ledger)",
        "import json; from examples.incident_db_failover import actions\n"
        f"p = json.load(open({repr(b_state)}))\n"
        "p['simulate_pooler_write_error'] = False\n"
        "d, m = actions.cutover_pooler_and_verify_writes(p);"
        " p['outputs']['cutover_pooler_and_verify_writes'] = d; print(m)\n"
        "d, m = actions.publish_failover_ledger(p);"
        " p['outputs']['publish_failover_ledger'] = d; print(m)\n"
        f"json.dump(p, open({repr(b_state)}, 'w'))",
    )
    bs4, _ = self._run_python_snippet(
        "B step 4: cleanup state.json",
        f"import os; os.remove({repr(b_state)})",
    )
    with open(b_cstate_path, "r", encoding="utf-8") as f:
      b_final_state = json.load(f)
    b_ext_ok = (
        b_rb_ok
        and b_final_state.get("pooler_status") == "ONLINE_RW"
        and bool(b_final_state.get("audit_published"))
        and b_final_state.get("promotion_count") == 1
        and not os.path.exists(b_state)
    )

    y_ch, a_ch, r_ch = self._measure_blueprint("incident_db_failover")
    return WorkflowBenchmark(
        name="incident_db_failover",
        description=(
            "9-stage regional DB failover (mutually exclusive WAL branch,"
            " ALL_DONE fence, IC gate, standby promotion, pooler rollback)"
        ),
        is_authoring_workflow=False,
        steps=steps,
        yaml_chars=y_ch,
        actions_chars=a_ch,
        readme_chars=r_ch,
        passport_chars=p_chars,
        passport_outputs_chars=p_out_chars,
        order_match=order_ok,
        no_duplicate_side_effects=no_dup,
        gate_discipline=gate_ok,
        external_state_valid=ext_ok,
        completed_stages=comp,
        baseline_skill_chars=self.file_chars(
            "benchmarks/baseline_skills/incident_db_failover/SKILL.md"
        ),
        baseline_steps=[bs1, bs2, bs3, bs4],
        baseline_external_state_valid=b_ext_ok,
    )

  def _run_python_snippet(
      self, label: str, code: str
  ) -> tuple[StepMetric, str]:
    """Executes a baseline Python one-liner and returns its StepMetric and stdout."""
    raw_cmd = f"python3 -c {shlex.quote(code)}"
    norm_cmd = self._normalize_text(raw_cmd)
    res = subprocess.run(
        [sys.executable, "-B", "-c", code],
        cwd=_OSS_ROOT,
        env=self.env,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=False,
    )
    if res.returncode != 0:
      raise RuntimeError(
          f"Baseline snippet '{label}' failed:\n{res.stdout}\n{res.stderr}"
      )
    norm_out = self._normalize_text(res.stdout + res.stderr)
    return (
        StepMetric(
            label=label,
            command_str=norm_cmd,
            exit_code=res.returncode,
            stdout_chars=len(norm_out),
            cmd_chars=len(norm_cmd),
        ),
        res.stdout,
    )

  def bench_pypi_conventional_baseline(
      self, arm_a: WorkflowBenchmark
  ) -> dict[str, Any]:
    """Executes Arm B state-passing variants for pypi_upgrade_guard.

    Compares Arm A (Lightflow DAG) against three conventional prose-skill
    execution patterns using `examples/pypi_upgrade_guard/actions.py`:
      - Variant B0 (Steelmanned `/tmp/state.json`): Measured directly in
        `arm_a.baseline_steps`.
      - Variant B1 (Context State-Bus): Step 1 prints `audit_pypi_versions`
        JSON to stdout so the agent can pass `outputs.audit_pypi_versions` in
        the inline payload to Steps 2 and 3 without `/tmp` files or re-running
        `audit_pypi_versions`.
      - Variant B2 (Naive Re-Exec): Steps 2 and 3 re-call `audit_pypi_versions`
        in-process rather than persisting state, causing `audit_pypi_versions`
        to execute 3 times instead of 1.

    Args:
      arm_a: Benchmark metrics from the `pypi_upgrade_guard` run.

    Returns:
      Dictionary containing comparative metrics for Arm A, B0, B1, and B2.
    """
    baseline_skill_chars = self.file_chars(
        "benchmarks/baseline_skills/pypi_upgrade_guard/SKILL.md"
    )
    actions_chars = self.file_chars("examples/pypi_upgrade_guard/actions.py")

    # Variant B1: State-Bus (agent passes `outputs.audit_pypi_versions` across
    # turns)
    reqs_b1 = _posix_path(os.path.join(self.work_dir, "reqs_b1.txt"))
    b1_s1, b1_out1 = self._run_python_snippet(
        "B1 step 1: audit_pypi_versions -> stdout JSON",
        "import json, sys; sys.path.insert(0, 'examples/pypi_upgrade_guard');"
        " import actions; d, m = actions.audit_pypi_versions({'offline':"
        " True}); print(json.dumps({'delta': d, 'message': m}))",
    )
    audit_delta = json.loads(b1_out1.strip())["delta"]
    b1_payload_fail = {
        "requirements_path": reqs_b1,
        "offline": True,
        "outputs": {"audit_pypi_versions": audit_delta},
        "simulate_smoke_failure": True,
    }
    b1_s2, _ = self._run_python_snippet(
        "B1 step 2: apply (smoke fail) + manual restore_requirements_backup",
        "import json, sys; sys.path.insert(0, 'examples/pypi_upgrade_guard');"
        " import actions; p = json.loads("
        + repr(json.dumps(b1_payload_fail))
        + ")\ntry:\n  actions.apply_and_smoke_test(p)\n"
        "except Exception as e:\n"
        "  rb, msg = actions.restore_requirements_backup(p)\n"
        "  print(json.dumps({'error': str(e), 'rollback': rb, 'msg': msg}))",
    )
    with open(reqs_b1, "r", encoding="utf-8") as f:
      b1_rolled_back = "PyYAML==6.0\n" in f.read() and not os.path.exists(
          reqs_b1 + ".bak"
      )
    b1_payload_ok = dict(b1_payload_fail, simulate_smoke_failure=False)
    b1_s3, _ = self._run_python_snippet(
        "B1 step 3: apply recovery (simulate_smoke_failure=False)",
        "import json, sys; sys.path.insert(0, 'examples/pypi_upgrade_guard');"
        " import actions; p = json.loads("
        + repr(json.dumps(b1_payload_ok))
        + "); d, m = actions.apply_and_smoke_test(p);"
        " print(json.dumps({'delta': d, 'message': m}))",
    )
    with open(reqs_b1, "r", encoding="utf-8") as f:
      b1_upgraded = "PyYAML==6.0.2" in f.read()
    b1_steps = [b1_s1, b1_s2, b1_s3]
    b1_warm_ch = sum(s.total_chars for s in b1_steps)
    b1_cum_ch = sum(
        s.total_chars * (len(b1_steps) - idx) for idx, s in enumerate(b1_steps)
    )

    # Variant B2: Naive Re-Exec (re-runs audit_pypi_versions on every turn)
    reqs_b2 = _posix_path(os.path.join(self.work_dir, "reqs_b2.txt"))
    b2_s1, _ = self._run_python_snippet(
        "B2 step 1: audit_pypi_versions",
        "import sys; sys.path.insert(0, 'examples/pypi_upgrade_guard');"
        " import actions; _, m = actions.audit_pypi_versions({'offline':"
        " True}); print(m)",
    )
    b2_s2, _ = self._run_python_snippet(
        "B2 step 2: re-run audit + apply (smoke fail) + rollback",
        "import json, sys; sys.path.insert(0, 'examples/pypi_upgrade_guard');"
        " import actions; p = json.loads("
        + repr(
            json.dumps({
                "requirements_path": reqs_b2,
                "offline": True,
                "simulate_smoke_failure": True,
            })
        )
        + "); d, _ = actions.audit_pypi_versions(p);"
        " p['outputs'] = {'audit_pypi_versions': d}\n"
        "try:\n  actions.apply_and_smoke_test(p)\n"
        "except Exception as e:\n"
        "  _, msg = actions.restore_requirements_backup(p);"
        " print(f'{e} -> {msg}')",
    )
    b2_s3, _ = self._run_python_snippet(
        "B2 step 3: re-run audit + apply recovery",
        "import json, sys; sys.path.insert(0, 'examples/pypi_upgrade_guard');"
        " import actions; p = json.loads("
        + repr(
            json.dumps({
                "requirements_path": reqs_b2,
                "offline": True,
                "simulate_smoke_failure": False,
            })
        )
        + "); d, _ = actions.audit_pypi_versions(p);"
        " p['outputs'] = {'audit_pypi_versions': d};"
        " _, m = actions.apply_and_smoke_test(p); print(m)",
    )
    with open(reqs_b2, "r", encoding="utf-8") as f:
      b2_upgraded = "PyYAML==6.0.2" in f.read()
    b2_steps = [b2_s1, b2_s2, b2_s3]
    b2_warm_ch = sum(s.total_chars for s in b2_steps)
    b2_cum_ch = sum(
        s.total_chars * (len(b2_steps) - idx) for idx, s in enumerate(b2_steps)
    )

    b1_cmd_ch = sum(s.cmd_chars for s in b1_steps)
    b2_cmd_ch = sum(s.cmd_chars for s in b2_steps)
    return {
        "baseline_skill_chars": baseline_skill_chars,
        "baseline_skill_tokens": estimate_tokens(baseline_skill_chars),
        "actions_py_chars": actions_chars,
        "actions_py_tokens": estimate_tokens(actions_chars),
        "arm_a_lightflow": {
            "cmd_chars": arm_a.runtime_cmd_chars,
            "cmd_tokens": estimate_tokens(arm_a.runtime_cmd_chars),
            "warm_chars": arm_a.runtime_total_chars,
            "warm_tokens": arm_a.runtime_total_tokens,
            "warm_cumulative_tokens": arm_a.cumulative_multiturn_tokens,
            "audit_executions": 1,
            "no_duplicate_side_effects": arm_a.no_duplicate_side_effects,
            "external_state_valid": arm_a.external_state_valid,
        },
        "arm_b0_state_file": {
            "cmd_chars": arm_a.baseline_cmd_chars,
            "cmd_tokens": arm_a.baseline_cmd_tokens,
            "warm_chars": arm_a.baseline_total_chars,
            "warm_tokens": arm_a.baseline_total_tokens,
            "warm_cumulative_tokens": arm_a.baseline_cumulative_tokens,
            "audit_executions": 1,
            "no_duplicate_side_effects": True,
            "external_state_valid": arm_a.baseline_external_state_valid,
        },
        "arm_b1_state_bus": {
            "cmd_chars": b1_cmd_ch,
            "cmd_tokens": estimate_tokens(b1_cmd_ch),
            "warm_chars": b1_warm_ch,
            "warm_tokens": estimate_tokens(b1_warm_ch),
            "warm_cumulative_tokens": estimate_tokens(b1_cum_ch),
            "cold_chars_with_skill_and_actions": (
                b1_warm_ch + baseline_skill_chars + actions_chars
            ),
            "cold_tokens_with_skill_and_actions": estimate_tokens(
                b1_warm_ch + baseline_skill_chars + actions_chars
            ),
            "audit_executions": 1,
            "no_duplicate_side_effects": True,
            "external_state_valid": b1_rolled_back and b1_upgraded,
        },
        "arm_b2_naive_reexec": {
            "cmd_chars": b2_cmd_ch,
            "cmd_tokens": estimate_tokens(b2_cmd_ch),
            "warm_chars": b2_warm_ch,
            "warm_tokens": estimate_tokens(b2_warm_ch),
            "warm_cumulative_tokens": estimate_tokens(b2_cum_ch),
            "cold_chars_with_skill_and_actions": (
                b2_warm_ch + baseline_skill_chars + actions_chars
            ),
            "cold_tokens_with_skill_and_actions": estimate_tokens(
                b2_warm_ch + baseline_skill_chars + actions_chars
            ),
            "audit_executions": 3,
            "no_duplicate_side_effects": False,
            "external_state_valid": b2_upgraded,
        },
    }

  def run_all(self) -> list[WorkflowBenchmark]:
    return [
        self.bench_hn_digest(),
        self.bench_usgs_seismic_alert(),
        self.bench_pypi_upgrade_guard(),
        self.bench_async_job_watcher(),
        self.bench_blue_green_release(),
        self.bench_incident_db_failover(),
        self.bench_tenant_gitops_onboarding(),
        self.bench_create_lightflow(),
    ]


def format_markdown_report(
    results: list[WorkflowBenchmark],
    runner_skill_chars: int,
    author_skill_chars: int,
    trials: int = 1,
    trial_runs: list[list[WorkflowBenchmark]] | None = None,
    baseline_comparison: dict[str, Any] | None = None,
) -> str:
  """Formats the unified A/B benchmark report in Markdown."""
  runner_skill_tokens = estimate_tokens(runner_skill_chars)
  author_skill_tokens = estimate_tokens(author_skill_chars)
  all_runs = trial_runs or [results]
  lines: list[str] = []
  lines.append(
      "# Lightflow A/B Benchmark: Lightflow DAG (`Arm A`) vs. Traditional Skill"
      " (`Arm B`)"
  )
  lines.append("")
  lines.append(
      "## 1. Unified 7-Pair Domain Execution Benchmark (`Arm A: Lightflow DAG`"
      f" vs. `Arm B: Traditional Skill`, `N={trials}`"
      f" {'trial' if trials == 1 else 'trials'})"
  )
  lines.append("")
  lines.append(
      "Both arms execute the exact same Python helper functions in"
      " `examples/<name>/actions.py`. **Arm A** uses the universal"
      " `skills/run_lightflows/SKILL.md` (`1x` per session) and the `lightflow`"
      " CLI (`passport.json`). **Arm B** uses a steelmanned per-workflow"
      " `benchmarks/baseline_skills/<name>/SKILL.md` and `python3 -c`"
      " commands with a local `state.json` file."
  )
  lines.append("")
  lines.append(
      "| Workflow Pair | Arm A / Arm B Checks | Skill Loaded (`Arm A` vs."
      " `Arm B`) | Agent Command Output `cmd` (`Arm A` vs. `Arm B`) | Warm"
      " Runtime `cmd + stdout` (`Arm A` vs. `Arm B`) | Session Total (`Skill +"
      " Warm`) (`Arm A` vs. `Arm B`) |"
  )
  lines.append("| :--- | :---: | :---: | :---: | :---: | :---: |")

  domain_indices = [
      (idx, r) for idx, r in enumerate(results) if not r.is_authoring_workflow
  ]
  tot_a_skill_ch = runner_skill_chars
  tot_b_skill_ch = 0
  tot_a_cmd_ch = 0
  tot_b_cmd_ch = 0
  tot_a_warm_ch = 0
  tot_b_warm_ch = 0
  tot_a_checks = 0
  tot_b_checks = 0
  tot_domain_trials = len(domain_indices) * len(all_runs)

  for domain_pos, (wf_idx, r) in enumerate(domain_indices):
    wf_trials = [run[wf_idx] for run in all_runs]
    a_pass = sum(1 for t in wf_trials if t.all_checks_passed)
    b_pass = sum(1 for t in wf_trials if t.baseline_external_state_valid)
    tot_a_checks += a_pass
    tot_b_checks += b_pass

    a_skill_ch = runner_skill_chars if domain_pos == 0 else 0
    a_skill_tok = estimate_tokens(a_skill_ch)
    b_skill_ch = r.baseline_skill_chars
    b_skill_tok = r.baseline_skill_tokens
    tot_b_skill_ch += b_skill_ch

    a_cmd_ch = r.runtime_cmd_chars
    a_cmd_tok = estimate_tokens(a_cmd_ch)
    b_cmd_ch = r.baseline_cmd_chars
    b_cmd_tok = r.baseline_cmd_tokens
    tot_a_cmd_ch += a_cmd_ch
    tot_b_cmd_ch += b_cmd_ch

    a_warm_ch = r.runtime_total_chars
    a_warm_tok = r.runtime_total_tokens
    b_warm_ch = r.baseline_total_chars
    b_warm_tok = r.baseline_total_tokens
    tot_a_warm_ch += a_warm_ch
    tot_b_warm_ch += b_warm_ch

    a_sess_tok = estimate_tokens(a_skill_ch + a_warm_ch)
    b_sess_tok = estimate_tokens(b_skill_ch + b_warm_ch)
    skill_cell = (
        f"~{a_skill_tok:,} tok (1st) vs. ~{b_skill_tok:,} tok"
        if domain_pos == 0
        else f"**0 tok** (reused) vs. ~{b_skill_tok:,} tok"
    )
    lines.append(
        f"| `{r.name}` | {a_pass}/{len(wf_trials)} vs."
        f" {b_pass}/{len(wf_trials)} | {skill_cell} | **~{a_cmd_tok:,} tok**"
        f" vs. ~{b_cmd_tok:,} tok | ~{a_warm_tok:,} tok vs. ~{b_warm_tok:,} tok"
        f" | **~{a_sess_tok:,} tok** vs. ~{b_sess_tok:,} tok |"
    )

  tot_a_skill_tok = estimate_tokens(tot_a_skill_ch)
  tot_b_skill_tok = estimate_tokens(tot_b_skill_ch)
  tot_a_cmd_tok = estimate_tokens(tot_a_cmd_ch)
  tot_b_cmd_tok = estimate_tokens(tot_b_cmd_ch)
  tot_a_warm_tok = estimate_tokens(tot_a_warm_ch)
  tot_b_warm_tok = estimate_tokens(tot_b_warm_ch)
  tot_a_sess_tok = estimate_tokens(tot_a_skill_ch + tot_a_warm_ch)
  tot_b_sess_tok = estimate_tokens(tot_b_skill_ch + tot_b_warm_ch)

  lines.append(
      f"| **7-Workflow Session Total** | **{tot_a_checks}/{tot_domain_trials}"
      f" vs. {tot_b_checks}/{tot_domain_trials}** | **~{tot_a_skill_tok:,} tok"
      f" (1 skill) vs. ~{tot_b_skill_tok:,} tok (7 skills)** |"
      f" **~{tot_a_cmd_tok:,} tok vs. ~{tot_b_cmd_tok:,} tok** |"
      f" **~{tot_a_warm_tok:,} tok vs. ~{tot_b_warm_tok:,} tok** |"
      f" **~{tot_a_sess_tok:,} tok vs. ~{tot_b_sess_tok:,} tok** |"
  )
  lines.append("")

  if baseline_comparison:
    a = baseline_comparison["arm_a_lightflow"]
    b0 = baseline_comparison["arm_b0_state_file"]
    b1 = baseline_comparison["arm_b1_state_bus"]
    b2 = baseline_comparison["arm_b2_naive_reexec"]
    lines.append(
        "## 2. State-Passing & Failure-Recovery Breakdown"
        " (`pypi_upgrade_guard`)"
    )
    lines.append("")
    lines.append(
        "| Arm | Mechanism | Agent Command Output (`cmd`) | Warm Runtime (`cmd"
        " + stdout`) | `audit_pypi_versions` Executions | No Duplicate Side"
        " Effects | Rollback & Final State Valid |"
    )
    lines.append("| :--- | :--- | :---: | :---: | :---: | :---: | :---: |")
    lines.append(
        "| **Arm A (Lightflow DAG)** | `lightflow start` / `resume` +"
        f" `passport.json` | **{a['cmd_chars']:,} ch (~{a['cmd_tokens']:,}"
        f" tok)** | **{a['warm_chars']:,} ch (~{a['warm_tokens']:,} tok)** |"
        f" **{a['audit_executions']}x** |"
        f" `{a['no_duplicate_side_effects']}` |"
        f" `{a['external_state_valid']}` |"
    )
    lines.append(
        "| **Arm B0 (Traditional Skill — `/tmp/state.json`)** |"
        " `baseline_skills/pypi_upgrade_guard/SKILL.md` + `python3 -c` +"
        f" `state.json` | {b0['cmd_chars']:,} ch (~{b0['cmd_tokens']:,} tok) |"
        f" {b0['warm_chars']:,} ch (~{b0['warm_tokens']:,} tok) |"
        f" {b0['audit_executions']}x | `{b0['no_duplicate_side_effects']}` |"
        f" `{b0['external_state_valid']}` |"
    )
    lines.append(
        "| **Arm B1 (Traditional Skill — Context State-Bus)** |"
        " `baseline_skills/pypi_upgrade_guard/SKILL.md` + `python3 -c` passing"
        f" `outputs` JSON | {b1['cmd_chars']:,} ch"
        f" (~{b1['cmd_tokens']:,} tok) | {b1['warm_chars']:,} ch"
        f" (~{b1['warm_tokens']:,} tok) |"
        f" {b1['audit_executions']}x | `{b1['no_duplicate_side_effects']}` |"
        f" `{b1['external_state_valid']}` |"
    )
    lines.append(
        "| **Arm B2 (Traditional Skill — Naive Re-Exec)** |"
        " `baseline_skills/pypi_upgrade_guard/SKILL.md` + `python3 -c`"
        f" re-running `audit_pypi_versions` | {b2['cmd_chars']:,} ch"
        f" (~{b2['cmd_tokens']:,} tok) | {b2['warm_chars']:,} ch"
        f" (~{b2['warm_tokens']:,} tok) |"
        f" **{b2['audit_executions']}x (duplicated!)** |"
        f" **`{b2['no_duplicate_side_effects']}`** |"
        f" `{b2['external_state_valid']}` |"
    )
    lines.append("")

  lines.append(
      "## 3. All 8 Reference Workflows (Including `create_lightflow` Authoring"
      " Workflow)"
  )
  lines.append("")
  lines.append(
      "| Workflow | CLI Calls | Order & Gate Check | No Dup Side Effects | Warm"
      " Runtime (`cmd + stdout`) | Out-of-Context State (`passport.json`) |"
      " Source Avoided (`yaml + actions.py`) | Zero-Context Savings (Warm) |"
  )
  lines.append(
      "| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: |"
  )

  tot_runtime_ch = 0
  tot_passport_ch = 0
  tot_blueprint_ch = 0
  for wf_idx, r in enumerate(results):
    n_calls = len(r.steps)
    rt_ch = r.runtime_total_chars
    rt_tok = r.runtime_total_tokens
    pp_ch = r.passport_chars
    pp_tok = r.passport_tokens
    bp_ch = r.blueprint_chars
    bp_tok = r.blueprint_tokens
    savings = r.zero_context_savings_ratio

    wf_trials = [run[wf_idx] for run in all_runs]
    order_passes = sum(
        1
        for t in wf_trials
        if t.order_match and t.gate_discipline and t.external_state_valid
    )
    nodup_passes = sum(1 for t in wf_trials if t.no_duplicate_side_effects)

    tot_runtime_ch += rt_ch
    tot_passport_ch += pp_ch
    tot_blueprint_ch += bp_ch

    lines.append(
        f"| `{r.name}` | {n_calls} | {order_passes}/{len(wf_trials)} |"
        f" {nodup_passes}/{len(wf_trials)} | {rt_ch:,} ch (~**{rt_tok:,}"
        f" tok**) | {pp_ch:,} ch (~{pp_tok:,} tok) | {bp_ch:,} ch"
        f" (~{bp_tok:,} tok) | **{savings * 100:.0f}%** |"
    )

  tot_runtime_tok = estimate_tokens(tot_runtime_ch)
  tot_pp_tok = estimate_tokens(tot_passport_ch)
  tot_bp_tok = estimate_tokens(tot_blueprint_ch)
  warm_savings = tot_blueprint_ch / (tot_runtime_ch + tot_blueprint_ch)
  n_wf = len(results)
  total_checks = n_wf * len(all_runs)
  passed_checks = 0
  for run in all_runs:
    for t in run:
      if t.all_checks_passed:
        passed_checks += 1
  lines.append(
      f"| **Total ({n_wf} workflows)** |"
      f" **{sum(len(r.steps) for r in results)}** |"
      f" **{passed_checks}/{total_checks}** |"
      f" **{passed_checks}/{total_checks}** | **{tot_runtime_ch:,} ch"
      f" (~{tot_runtime_tok:,} tok)** | **{tot_passport_ch:,} ch"
      f" (~{tot_pp_tok:,} tok)** | **{tot_blueprint_ch:,} ch"
      f" (~{tot_bp_tok:,} tok)** | **{warm_savings * 100:.0f}%** |"
  )
  lines.append("")

  lines.append("## 4. Step-by-Step Breakdown")
  lines.append("")
  for r in results:
    lines.append(f"### `{r.name}` — {r.description}")
    lines.append("- **Arm A (Lightflow DAG)**:")
    for idx, s in enumerate(r.steps, 1):
      lines.append(
          f"  - Step {idx} (`{s.label}`, exit `{s.exit_code}`): input"
          f" `{s.cmd_chars}` ch + stdout `{s.stdout_chars}` ch ="
          f" **{s.total_chars:,} ch (~{s.total_tokens} tok)**"
      )
    if r.baseline_steps:
      lines.append(
          "- **Arm B (Traditional Skill —"
          f" `benchmarks/baseline_skills/{r.name}/SKILL.md` ="
          f" `{r.baseline_skill_chars:,}` ch /"
          f" ~`{r.baseline_skill_tokens:,}` tok)**:"
      )
      for idx, s in enumerate(r.baseline_steps, 1):
        lines.append(
            f"  - Step {idx} (`{s.label}`, exit `{s.exit_code}`): input"
            f" `{s.cmd_chars}` ch + stdout `{s.stdout_chars}` ch ="
            f" **{s.total_chars:,} ch (~{s.total_tokens} tok)**"
        )
    lines.append(
        "- **Persisted in `passport.json` (kept out of LLM context)**:"
        f" `{r.passport_chars:,}` ch (~`{r.passport_tokens:,}` tok), of which"
        f" `payload.outputs` = `{r.passport_outputs_chars:,}` ch"
    )
    lines.append(
        "- **Ground-truth trace verification**:"
        f" `order_match={r.order_match}`,"
        f" `no_duplicate_side_effects={r.no_duplicate_side_effects}`,"
        f" `gate_discipline={r.gate_discipline}`,"
        f" `external_state_valid={r.external_state_valid}`,"
        f" `baseline_external_state_valid={r.baseline_external_state_valid}`"
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
  parser.add_argument(
      "--trials",
      type=int,
      default=1,
      help="Number of independent benchmark trials to execute (default: 1).",
  )
  args = parser.parse_args()
  trials = max(1, args.trials)

  trial_runs: list[list[WorkflowBenchmark]] = []
  baseline_comparison: dict[str, Any] = {}
  runner_skill_chars = 0
  author_skill_chars = 0
  for _ in range(trials):
    runner = BenchmarkRunner()
    try:
      run_results = runner.run_all()
      trial_runs.append(run_results)
      pypi_result = next(
          r for r in run_results if r.name == "pypi_upgrade_guard"
      )
      baseline_comparison = runner.bench_pypi_conventional_baseline(pypi_result)
      runner_skill_chars = runner.file_chars("skills/run_lightflows/SKILL.md")
      author_skill_chars = runner.file_chars(
          "skills/create_lightflows/SKILL.md"
      )
    finally:
      runner.close()

  results = trial_runs[0]
  if args.json:
    all_passed = all(
        all(t.all_checks_passed for t in run) for run in trial_runs
    )
    payload = {
        "trials": trials,
        "all_trials_passed": all_passed,
        "zero_token_variance_across_trials": (
            len({
                tuple(t.runtime_total_chars for t in run) for run in trial_runs
            })
            == 1
        ),
        "runner_skill_chars": runner_skill_chars,
        "runner_skill_tokens": estimate_tokens(runner_skill_chars),
        "author_skill_chars": author_skill_chars,
        "author_skill_tokens": estimate_tokens(author_skill_chars),
        "conventional_baseline_comparison": baseline_comparison,
        "workflows": [r.to_dict() for r in results],
    }
    print(json.dumps(payload, indent=2))
  else:
    print(
        format_markdown_report(
            results,
            runner_skill_chars,
            author_skill_chars,
            trials=trials,
            trial_runs=trial_runs,
            baseline_comparison=baseline_comparison,
        )
    )
  return 0


if __name__ == "__main__":
  sys.exit(main())
