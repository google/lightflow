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

"""Unit tests for Lightflow OSS PassportManager, LightflowRunnerCLI, and Visualizer."""

from __future__ import annotations

# pylint: disable=g-import-not-at-top,g-bad-import-order,missing-function-docstring,protected-access,unused-argument,g-import-not-at-top

import contextlib
import errno
import io
import importlib.machinery
import os
import re
import shutil
import sys
import tempfile
import threading
import time
import types
from typing import Any
import unittest
from unittest import mock

_OSS_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _OSS_ROOT not in sys.path:
  sys.path.insert(0, _OSS_ROOT)

# pylint: disable=g-import-not-at-top,g-bad-import-order
try:
  from ..lightflow import engine
  from ..lightflow import lib
  from ..lightflow import runner as runner_mod
  from ..lightflow import schema
  from ..lightflow import visualizer
except (ImportError, ValueError):
  from lightflow import engine  # pyrefly: ignore[missing-import]
  from lightflow import lib  # pyrefly: ignore[missing-import]
  from lightflow import runner as runner_mod  # pyrefly: ignore[missing-import]
  from lightflow import schema  # pyrefly: ignore[missing-import]
  from lightflow import visualizer  # pyrefly: ignore[missing-import]


_EXEC_TRACE: list[str] = []
_STAGE_2_SHOULD_FAIL = True


def act_step_1(
    payload: dict[str, Any], **static_kwargs: Any
) -> tuple[dict[str, Any], str]:
  del static_kwargs
  _EXEC_TRACE.append("step_1")
  return {"step_1_done": True, "summary": "Step 1 OK"}, "Step 1 done"


def act_step_2_repairable(
    payload: dict[str, Any], **static_kwargs: Any
) -> tuple[dict[str, Any], str]:
  del payload, static_kwargs
  _EXEC_TRACE.append("step_2")
  if _STAGE_2_SHOULD_FAIL:
    raise RuntimeError("Step 2 broken")
  return {"step_2_done": True}, "Step 2 repaired"


def act_silent(payload: dict[str, Any], **static_kwargs: Any) -> dict[str, Any]:
  """Returns only a payload delta, with no agent-facing message."""
  del payload, static_kwargs
  return {"fetched": True}


def act_step_3(
    payload: dict[str, Any], **static_kwargs: Any
) -> tuple[dict[str, Any], str]:
  del payload, static_kwargs
  _EXEC_TRACE.append("step_3")
  return {"step_3_done": True}, "Step 3 done"


class PassportAndLightflowRunnerTest(unittest.TestCase):
  """Tests state management, operator gates, subgraph re-arming, and HTML output."""

  def setUp(self) -> None:
    super().setUp()
    os.environ.pop("ANTIGRAVITY_CONVERSATION_ID", None)
    _EXEC_TRACE.clear()
    global _STAGE_2_SHOULD_FAIL
    _STAGE_2_SHOULD_FAIL = True
    self.temp_dir = tempfile.mkdtemp(prefix="lightflow_oss_test_")
    self.old_env = os.environ.get("LIGHTFLOW_STATE_DIR")
    os.environ["LIGHTFLOW_STATE_DIR"] = self.temp_dir

  def tearDown(self) -> None:
    if self.old_env is None:
      os.environ.pop("LIGHTFLOW_STATE_DIR", None)
    else:
      os.environ["LIGHTFLOW_STATE_DIR"] = self.old_env
    shutil.rmtree(self.temp_dir, ignore_errors=True)
    super().tearDown()

  def test_start_suspend_resume_approve_and_visualize(self) -> None:
    module_name = __name__
    manifest_path = os.path.join(self.temp_dir, "workflow.yaml")
    with open(manifest_path, "w", encoding="utf-8") as f:
      f.write(f"""
name: approval_pipeline
description: Test human gate and visualizer </SCRIPT>
actions:
  - id: step_1
    python_import: {module_name}.act_step_1
  - id: step_3
    python_import: {module_name}.act_step_3
stages:
  - name: extract
    python_action:
      action_id: step_1
  - name: human_gate
    run_after: [extract]
    operator_action:
      instructions: "'Review: ' + payload.outputs.extract.summary"
      json_schema: '{{"type": "object", "required": ["approved"]}}'
  - name: load
    run_after: [human_gate]
    run_if: "payload.outputs.human_gate.approved == true"
    python_action:
      action_id: step_3
""")

    cli = lib.LightflowRunnerCLI()
    # 1. Start workflow -> suspends at human_gate
    with self.assertRaises(engine.OperatorActionSuspended):
      cli.start(lightflow=manifest_path, log_id="run_gate_1")

    self.assertEqual(_EXEC_TRACE, ["step_1"])

    # 2. Resume with APPROVE and valid JSON schema payload
    cli.resume(
        lightflow=manifest_path,
        log_id="run_gate_1",
        stage="human_gate",
        resolution="APPROVE",
        payload={"approved": True},
    )
    self.assertEqual(_EXEC_TRACE, ["step_1", "step_3"])
    gate_passport = lib.PassportManager("run_gate_1").load_passport()
    self.assertIsNotNone(gate_passport)
    assert gate_passport is not None
    gate_payload = gate_passport.payload.to_dict()
    self.assertTrue(gate_payload["approved"])
    self.assertEqual(
        gate_payload["outputs"]["human_gate"],
        {"approved": True, "resolution": "APPROVE"},
    )

    # 3. Verify portable Visualizer HTML generation
    viz_path = os.path.join(self.temp_dir, "viz.html")
    html_out = cli.visualize(
        lightflow=manifest_path,
        log_id="run_gate_1",
        output=viz_path,
        title="OSS Test DAG",
    )
    self.assertEqual(html_out, viz_path)
    with open(viz_path, "r", encoding="utf-8") as f:
      html_text = f.read()
    self.assertNotIn("gstatic.com", html_text)
    self.assertNotIn("</SCRIPT>", html_text)
    self.assertIn("approval_pipeline", html_text)

  def test_subgraph_rearm_preserves_completed_upstream_stages(self) -> None:
    global _STAGE_2_SHOULD_FAIL
    module_name = __name__
    manifest_path = os.path.join(self.temp_dir, "rearm.yaml")
    with open(manifest_path, "w", encoding="utf-8") as f:
      f.write(f"""
name: rearm_pipeline
actions:
  - id: step_1
    python_import: {module_name}.act_step_1
  - id: step_2
    python_import: {module_name}.act_step_2_repairable
  - id: step_3
    python_import: {module_name}.act_step_3
stages:
  - name: s1
    python_action:
      action_id: step_1
  - name: s2
    run_after: [s1]
    python_action:
      action_id: step_2
  - name: s3
    run_after: [s2]
    python_action:
      action_id: step_3
""")

    cli = lib.LightflowRunnerCLI()
    # First run fails at s2 (s1 completes, s3 is blocked as collateral)
    with self.assertRaises(lib.LightflowRunFailedError):
      cli.start(lightflow=manifest_path, log_id="rearm_run")

    self.assertEqual(_EXEC_TRACE, ["step_1", "step_2"])

    # Repair s2 and resume -> s1 MUST NOT re-execute!
    _STAGE_2_SHOULD_FAIL = False
    cli.resume(
        lightflow=manifest_path,
        log_id="rearm_run",
        payload={"rearmed_override": "yes"},
    )
    self.assertEqual(_EXEC_TRACE, ["step_1", "step_2", "step_2", "step_3"])

    pm = lib.PassportManager("rearm_run")
    passport = pm.load_passport()
    self.assertIsNotNone(passport)
    self.assertEqual(passport.payload.get("rearmed_override"), "yes")
    latest = {s.stage_name: s.status for s in passport.stamps}
    self.assertEqual(latest["s1"], schema.StampStatus.COMPLETED)
    self.assertEqual(latest["s2"], schema.StampStatus.COMPLETED)
    self.assertEqual(latest["s3"], schema.StampStatus.COMPLETED)

  def test_passport_manager_blocks_path_traversal_and_non_state_dirs(
      self,
  ) -> None:
    with self.assertRaises(ValueError):
      lib.PassportManager("x/../../escape")
    with self.assertRaises(ValueError):
      lib.PassportManager("../escape")
    with self.assertRaises(ValueError):
      lib.PassportManager("~/.gemini/other_dir")
    pm_num = lib.PassportManager(20260926)  # type: ignore[arg-type]
    self.assertEqual(pm_num.workflow_id, "20260926")

  def test_start_force_validates_manifest_before_deleting_state(self) -> None:
    pm = lib.PassportManager("preserve_run")
    existing = schema.Passport(payload=schema.StructDict({"keep": True}))
    pm.save_passport(existing)

    broken_path = os.path.join(self.temp_dir, "broken.yaml")
    with open(broken_path, "w", encoding="utf-8") as f:
      f.write("name: broken\nstages:\n  - name: s1\n    run_after: [missing]\n")

    cli = lib.LightflowRunnerCLI()
    with self.assertRaises(engine.EngineError):
      cli.start(lightflow=broken_path, log_id="preserve_run", force=True)

    loaded = pm.load_passport()
    self.assertIsNotNone(loaded)
    assert loaded is not None
    self.assertTrue(loaded.payload.get("keep"))

    # Verify cleanup removes both state directory and sibling .lock file
    # without requiring --lightflow
    cli.cleanup(log_id="preserve_run")
    self.assertFalse(os.path.exists(pm.resolved_path))
    self.assertFalse(os.path.exists(pm.resolved_path + ".lock"))

  def test_dry_run_cascades_skip_and_previews_polling(self) -> None:
    module_name = __name__
    wf_path = os.path.join(self.temp_dir, "skip_cascade.yaml")
    with open(wf_path, "w", encoding="utf-8") as f:
      f.write(f"""
name: skip_cascade_wf
actions:
  - id: step_1
    python_import: {module_name}.act_step_1
stages:
  - name: s1_skipped
    run_if: "false"
    python_action:
      action_id: step_1
  - name: s2_downstream
    run_after: [s1_skipped]
    python_action:
      action_id: step_1
  - name: s3_all_done_poll
    run_after: [s1_skipped]
    trigger_rule: ALL_DONE
    polling_policy:
      condition: "payload.ready == true"
      poll_tick_action:
        action_id: step_1
""")
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
      lib.LightflowRunnerCLI().dry_run(lightflow=wf_path)
    out = buf.getvalue()
    self.assertIn(
        "Trigger Rule: Skipped: Upstream dependency was skipped.", out
    )
    self.assertNotIn("Action: [RUN] executes python_action 'step_1'", out)
    self.assertIn(
        "Action: [POLL] executes 'step_1' until 'payload.ready == true'", out
    )

  def test_resume_target_reflects_invocation_style(self) -> None:
    wf = schema.Lightflow(name="rt_wf")
    runner = engine.LightflowEngine(
        wf, lightflow_path="workflow.yaml", log_id="rt_run"
    )

    def _with_main(argv0: str, spec_name: str | None) -> str:
      fake_main = types.ModuleType("__main__")
      fake_main.__spec__ = (
          importlib.machinery.ModuleSpec(spec_name, loader=None)
          if spec_name
          else None
      )
      with (
          mock.patch.object(sys, "argv", [argv0, "dry_run"]),
          mock.patch.dict(sys.modules, {"__main__": fake_main}),
      ):
        return runner.resume_target()

    # python -m lightflow.runner / python -m lightflow
    self.assertEqual(
        _with_main("/x/lightflow/runner.py", "lightflow.runner"),
        "python3 -m lightflow.runner",
    )
    self.assertEqual(
        _with_main("/x/lightflow/__main__.py", "lightflow.__main__"),
        "python3 -m lightflow",
    )
    # Direct script from a source checkout echoes the typed path (quoted).
    self.assertEqual(
        _with_main("src/lightflow/runner.py", None),
        "python3 src/lightflow/runner.py",
    )
    self.assertEqual(
        _with_main("my dir/lightflow/runner.py", None),
        "python3 'my dir/lightflow/runner.py'",
    )
    # Installed console script.
    self.assertEqual(_with_main("/venv/bin/lightflow", None), "lightflow")
    # Unknown entry point falls back to empty (caller defaults to 'lightflow').
    self.assertEqual(_with_main("/opt/something_else.py", None), "")

  def test_unknown_flag_is_reported_with_suggestions(self) -> None:
    wrapper = runner_mod.CliWrapper(lib.LightflowRunnerCLI())
    with self.assertRaisesRegex(
        ValueError,
        r"Unknown flag '--logid' for 'status'\. Did you mean '--log_id'\?"
        r" Supported: --lightflow, --log_id, --verbose, --workflow\. Run"
        r" 'lightflow status --help' for details\.",
    ):
      runner_mod._dispatch_argv(wrapper, ["status", "--logid=x"])  # pylint: disable=protected-access

  def test_payload_file_missing_is_reported_with_flag_context(self) -> None:
    with self.assertRaisesRegex(
        ValueError,
        r"--payload / --payload_file: cannot read payload file"
        r" 'nope\.json': No such file",
    ):
      lib._parse_payload_arg("@nope.json", context_label="--payload")

  def test_polling_gate_timeout_prints_operator_yield_notice(self) -> None:
    wf = schema.Lightflow(
        name="poll_yield_wf", runner_target="my-custom-runner"
    )
    runner = engine.LightflowEngine(
        wf, lightflow_path="workflow.yaml", log_id="yield_run"
    )
    self.assertEqual(runner.resume_target(), "my-custom-runner")
    passport = schema.Passport()
    failed_stamp = passport.stamps.add()
    failed_stamp.stage_name = "gate"
    failed_stamp.status = schema.StampStatus.FAILED
    failed_stamp.message = (
        "Stage 'gate' polling timed out after 1800.0s waiting for external"
        " condition. Yielding control back to operator/agent. State is"
        " preserved."
    )
    pm = lib.PassportManager("yield_run")
    cli = lib.LightflowRunnerCLI()
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
      with self.assertRaises(lib.LightflowWaitingError):
        cli._execute_engine_loop(  # pylint: disable=protected-access
            runner=runner,
            ordered_stages=["gate"],
            passport=passport,
            pm=pm,
        )
    out = buf.getvalue()
    self.assertIn("⏳ Lightflow is waiting: polling stage 'gate'", out)
    self.assertIn("my-custom-runner resume", out)

  def test_windows_msvcrt_lock_fallback_and_cross_drive_commonpath(
      self,
  ) -> None:
    calls: list[tuple[int, int]] = []

    def fake_locking(fd: int, mode: int, nbytes: int) -> None:
      del fd
      calls.append((mode, nbytes))

    fake_msvcrt = types.SimpleNamespace(
        LK_NBLCK=2, LK_UNLCK=0, locking=fake_locking
    )

    pm = lib.PassportManager("win_lock_run")
    with (
        mock.patch.object(lib.state_lib, "fcntl", None),
        mock.patch.object(lib.state_lib, "msvcrt", fake_msvcrt),
    ):
      with pm.lock():
        pass
    self.assertEqual(calls, [(2, 1), (0, 1)])

    # Simulate Windows lock contention (PermissionError -> RuntimeError)
    calls.clear()

    def busy_locking(fd: int, mode: int, nbytes: int) -> None:
      del fd
      calls.append((mode, nbytes))
      raise PermissionError(13, "Permission denied")

    busy_msvcrt = types.SimpleNamespace(
        LK_NBLCK=2, LK_UNLCK=0, locking=busy_locking
    )

    with (
        mock.patch.object(lib.state_lib, "fcntl", None),
        mock.patch.object(lib.state_lib, "msvcrt", busy_msvcrt),
    ):
      with self.assertRaises(RuntimeError):
        with pm.lock():
          pass
    # Retried briefly (a status probe may hold it for an instant), never
    # unlocked since it was never acquired.
    self.assertEqual(set(calls), {(2, 1)})
    self.assertGreater(len(calls), 1)

    # Ensure OSError raised inside the lock body propagates untouched
    with self.assertRaises(FileNotFoundError):
      with pm.lock():
        raise FileNotFoundError("inner missing file")

    # Simulate cross-drive ValueError on os.path.commonpath
    real_commonpath = os.path.commonpath

    def cross_drive_commonpath(paths: list[str]) -> str:
      if os.path.normcase(paths[0]) != os.path.normcase(
          os.path.realpath(self.temp_dir)
      ):
        raise ValueError("Paths don't have the same drive")
      return real_commonpath(paths)

    with mock.patch("os.path.commonpath", side_effect=cross_drive_commonpath):
      pm_cross = lib.PassportManager("cross_drive_run")
      self.assertTrue(pm_cross.resolved_path.endswith("cross_drive_run"))

  def test_included_examples_and_stdlib_cli_dispatcher(self) -> None:
    try:
      from ..lightflow import runner  # pylint: disable=g-import-not-at-top
    except ImportError:
      from lightflow import runner  # type: ignore[no-redef]  # pylint: disable=g-import-not-at-top

    examples_dir = os.path.join(_OSS_ROOT, "examples")
    cli = lib.LightflowRunnerCLI()

    # 1. Verify all 5 example manifests compile and dry-run cleanly end-to-end
    dry_run_buf = io.StringIO()
    with contextlib.redirect_stdout(dry_run_buf):
      for name in (
          "async_job_watcher",
          "create_lightflow",
          "hn_digest",
          "pypi_upgrade_guard",
          "usgs_seismic_alert",
      ):
        wf_dir = os.path.join(examples_dir, name)
        wf_path = os.path.join(wf_dir, "lightflow.yaml")
        self.assertTrue(os.path.isfile(wf_path), f"Missing {wf_path}")
        self.assertEqual(schema.resolve_lightflow_path(wf_dir), wf_path)
        runner._dispatch_argv(  # pylint: disable=protected-access
            runner.CliWrapper(cli),
            ["dry_run", f"--lightflow={wf_path}"],
        )
      runner._dispatch_argv(  # pylint: disable=protected-access
          runner.CliWrapper(cli),
          [
              "start",
              f"--lightflow={os.path.join(examples_dir, 'usgs_seismic_alert')}",
              "--log_id=custom_preview_id",
              "--dry_run",
          ],
      )
    dry_run_out = dry_run_buf.getvalue()
    self.assertNotIn("Status: [SKIPPED]", dry_run_out)
    self.assertIn("Polled job status via dry_run: status=READY", dry_run_out)
    self.assertIn("Fetched 3 Hacker News stories", dry_run_out)
    self.assertIn("PyPI audit found 3 upgrade(s)", dry_run_out)
    self.assertIn("Fetched 2 M4.5+ seismic event(s) via dry_run", dry_run_out)
    self.assertIn("USGS reported 2 M4.5+ event(s)", dry_run_out)
    self.assertIn(
        "Output Namespace: payload.outputs.editorial_gate", dry_run_out
    )
    self.assertIn("/tmp/usgs_bulletin.md", dry_run_out)
    self.assertIn("--log_id=custom_preview_id", dry_run_out)
    self.assertIsNone(lib.PassportManager("custom_preview_id").load_passport())

    viz_out = os.path.join(self.temp_dir, "usgs_viz.html")
    with contextlib.redirect_stdout(io.StringIO()):
      runner._dispatch_argv(  # pylint: disable=protected-access
          runner.CliWrapper(cli),
          [
              "visualize",
              f"--lightflow={os.path.join(examples_dir, 'usgs_seismic_alert')}",
              f"--out={viz_out}",
          ],
      )
    self.assertTrue(os.path.isfile(viz_out))

    # Verify runner._parse_arg preserves strings inside decoded JSON dicts/lists
    parsed_payload = runner._parse_arg(  # pylint: disable=protected-access
        '{"comment": "true", "tag": "[draft]", "flag": false}'
    )
    self.assertEqual(
        parsed_payload,
        {"comment": "true", "tag": "[draft]", "flag": False},
    )
    self.assertIsInstance(parsed_payload["comment"], str)
    self.assertIsInstance(parsed_payload["tag"], str)

    # Verify stdlib JSON Schema fallback enforces property types, enums,
    # bounds, additionalProperties, and rejects unsupported keywords loudly.
    sample_schema = {
        "type": "object",
        "required": ["approved", "severity"],
        "additionalProperties": False,
        "properties": {
            "approved": {"type": "boolean", "const": True},
            "severity": {
                "type": "string",
                "enum": ["INFO", "ADVISORY", "WATCH"],
            },
            "score": {"type": ["integer", "null"], "minimum": 1, "maximum": 5},
        },
    }
    schema._validate_json_schema_stdlib(  # pylint: disable=protected-access
        {"approved": True, "severity": "ADVISORY", "score": 3}, sample_schema
    )
    schema._validate_json_schema_stdlib(  # pylint: disable=protected-access
        {"approved": True, "severity": "ADVISORY", "score": None},
        sample_schema,
    )
    with self.assertRaisesRegex(ValueError, "is not one of"):
      schema._validate_json_schema_stdlib(  # pylint: disable=protected-access
          {"approved": True, "severity": "CRITICAL"}, sample_schema
      )
    with self.assertRaisesRegex(ValueError, "Expected boolean at 'approved'"):
      schema._validate_json_schema_stdlib(  # pylint: disable=protected-access
          {"approved": "yes", "severity": "INFO"}, sample_schema
      )
    with self.assertRaisesRegex(ValueError, "Expected const True"):
      schema._validate_json_schema_stdlib(  # pylint: disable=protected-access
          {"approved": False, "severity": "INFO"}, sample_schema
      )
    with self.assertRaisesRegex(ValueError, "less than minimum"):
      schema._validate_json_schema_stdlib(  # pylint: disable=protected-access
          {"approved": True, "severity": "INFO", "score": 0}, sample_schema
      )
    with self.assertRaisesRegex(ValueError, "Additional properties"):
      schema._validate_json_schema_stdlib(  # pylint: disable=protected-access
          {"approved": True, "severity": "INFO", "extra": 1}, sample_schema
      )
    with self.assertRaisesRegex(
        ValueError, "Unsupported JSON Schema keyword.*pattern"
    ):
      schema._validate_json_schema_stdlib(  # pylint: disable=protected-access
          {"code": "abc"},
          {"type": "object", "properties": {"code": {"pattern": "^[0-9]+$"}}},
      )
    with self.assertRaisesRegex(
        ValueError, "'items' at 'tags' must be a schema dictionary"
    ):
      schema._validate_json_schema_stdlib(  # pylint: disable=protected-access
          {"tags": ["a"]},
          {
              "type": "object",
              "properties": {
                  "tags": {"type": "array", "items": [{"type": "string"}]}
              },
          },
      )
    with mock.patch.dict(sys.modules, {"jsonschema": None}):
      schema.check_json_schema(sample_schema)
      schema.validate_json_schema(
          {"approved": True, "severity": "INFO"}, sample_schema
      )
      with self.assertRaisesRegex(
          ValueError, "Unsupported JSON Schema keyword"
      ):
        schema.check_json_schema({"type": "object", "anyOf": []})

    # 2. Execute hn_digest offline end-to-end (start -> suspend -> resume)
    hn_wf = os.path.join(examples_dir, "hn_digest", "lightflow.yaml")
    digest_out = os.path.join(self.temp_dir, "hn_digest.md")
    hn_buf = io.StringIO()
    with contextlib.redirect_stdout(hn_buf):
      with self.assertRaises(engine.OperatorActionSuspended) as gate:
        cli.start(
            lightflow=hn_wf,
            log_id="hn_test_run",
            payload={"offline": True, "limit": 2},
        )
    self.assertIn("via offline_fixture", hn_buf.getvalue())
    # Issue #16: the approver is told the numbers come from a fixture, and
    # without `offline` a dead network is a failure, not silent sample data.
    self.assertIn(
        "[OFFLINE FIXTURE] Fetched 2 Hacker News", str(gate.exception)
    )
    with mock.patch(
        "urllib.request.urlopen", side_effect=OSError("network down")
    ):
      with self.assertRaises(lib.LightflowRunFailedError):
        cli.start(lightflow=hn_wf, log_id="hn_live_fail", payload={"limit": 2})
    failed = lib.PassportManager("hn_live_fail").load_passport()
    assert failed is not None
    fetch = [s for s in failed.stamps if s.stage_name == "fetch_top_stories"]
    self.assertIn("Live fetch failed", fetch[-1].message)
    self.assertIn('"offline": true', fetch[-1].message)
    cli.resume(
        lightflow=hn_wf,
        log_id="hn_test_run",
        stage="editorial_gate",
        resolution="APPROVE",
        payload={
            "editor_note": "Unit test note",
            "output_path": digest_out,
        },
    )
    self.assertTrue(os.path.isfile(digest_out))

    # 3. Execute pypi_upgrade_guard with simulated smoke failure -> rollback
    pypi_wf = os.path.join(examples_dir, "pypi_upgrade_guard", "lightflow.yaml")
    req_file = os.path.join(self.temp_dir, "requirements.txt")
    with open(req_file, "w", encoding="utf-8") as f:
      f.write("PyYAML==6.0\n")
    with self.assertRaises(engine.OperatorActionSuspended):
      cli.start(
          lightflow=pypi_wf,
          log_id="pypi_test_run",
          payload={"offline": True, "pinned_packages": {"PyYAML": "6.0"}},
      )
    fail_buf = io.StringIO()
    with contextlib.redirect_stdout(fail_buf):
      with self.assertRaises(lib.LightflowRunFailedError):
        cli.resume(
            lightflow=pypi_wf,
            log_id="pypi_test_run",
            stage="approve_upgrades",
            resolution="APPROVE",
            payload={
                "requirements_path": req_file,
                "simulate_smoke_failure": True,
            },
        )
    self.assertIn(
        "Resume after fixing the root cause (preserves completed stages):",
        fail_buf.getvalue(),
    )
    self.assertIn("apply_and_smoke_test: ", fail_buf.getvalue())
    self.assertIn(" · rolled back", fail_buf.getvalue())
    with open(req_file, "r", encoding="utf-8") as f:
      self.assertEqual(f.read().strip(), "PyYAML==6.0")
    status_buf = io.StringIO()
    with contextlib.redirect_stdout(status_buf):
      with self.assertRaises(lib.LightflowRunFailedError):
        cli.status(lightflow=pypi_wf, log_id="pypi_test_run", verbose=True)
    self.assertIn("[rolled back]", status_buf.getvalue())

    # 4. Dry-run and execute 22-stage tenant_gitops_onboarding across all paths
    tenant_wf = os.path.join(
        examples_dir, "tenant_gitops_onboarding", "lightflow.yaml"
    )
    self.assertTrue(os.path.isfile(tenant_wf))
    tenant_dry_buf = io.StringIO()
    with contextlib.redirect_stdout(tenant_dry_buf):
      runner._dispatch_argv(  # pylint: disable=protected-access
          runner.CliWrapper(cli),
          ["dry_run", f"--lightflow={tenant_wf}"],
      )
    self.assertIn(
        "Stage: wait_argocd_mesh_sync",
        tenant_dry_buf.getvalue(),
    )
    sandbox_dir = os.path.join(self.temp_dir, "tenant_sandbox")
    with contextlib.redirect_stdout(io.StringIO()):
      # 4a. Rejecting Gate #2 skips wait_iam_role_ready and Phases 3-4
      with self.assertRaises(engine.OperatorActionSuspended):
        cli.start(
            lightflow=tenant_wf,
            log_id="tenant_reject_run",
            payload={
                "alias": "payments-eu",
                "sandbox_dir": sandbox_dir,
                "require_manual_quota_override": True,
            },
        )
      with self.assertRaises(engine.OperatorActionSuspended):
        cli.resume(
            lightflow=tenant_wf,
            log_id="tenant_reject_run",
            stage="verify_spec",
            resolution="APPROVE",
            payload={"approved": True},
        )
      with self.assertRaisesRegex(
          lib.LightflowRunFailedError,
          r"🛑 Lightflow stopped: operator gate 'prompt_iam_quota_override'",
      ):
        cli.resume(
            lightflow=tenant_wf,
            log_id="tenant_reject_run",
            stage="prompt_iam_quota_override",
            resolution="REJECT",
        )
      rej_passport = lib.PassportManager("tenant_reject_run").load_passport()
      assert rej_passport is not None
      rej_stamps = {s.stage_name: s.status for s in rej_passport.stamps}
      self.assertEqual(
          rej_stamps["prompt_iam_quota_override"], schema.StampStatus.FAILED
      )
      self.assertEqual(
          rej_stamps["wait_iam_role_ready"], schema.StampStatus.SKIPPED
      )
      self.assertEqual(
          rej_stamps["wait_argocd_mesh_sync"], schema.StampStatus.SKIPPED
      )

      # 4b. Full 2-gate PCI approval path (dedicated KMS key branch)
      with self.assertRaises(engine.OperatorActionSuspended) as ctx1:
        cli.start(
            lightflow=tenant_wf,
            log_id="tenant_test_run",
            payload={
                "alias": "payments-eu",
                "compliance_tier": "pci",
                "sandbox_dir": sandbox_dir,
                "require_manual_quota_override": True,
            },
        )
      self.assertIn("verify_spec", str(ctx1.exception))
      with self.assertRaises(engine.OperatorActionSuspended) as ctx2:
        cli.resume(
            lightflow=tenant_wf,
            log_id="tenant_test_run",
            stage="verify_spec",
            resolution="APPROVE",
            payload={"approved": True},
        )
      self.assertIn("prompt_iam_quota_override", str(ctx2.exception))
      cli.resume(
          lightflow=tenant_wf,
          log_id="tenant_test_run",
          stage="prompt_iam_quota_override",
          resolution="APPROVE",
          payload={"approved": True, "quota_override_approved": True},
      )

      # 4c. Non-admin requester early handoff branch
      with self.assertRaises(engine.OperatorActionSuspended):
        cli.start(
            lightflow=tenant_wf,
            log_id="tenant_non_admin_run",
            payload={
                "alias": "payments-eu",
                "sandbox_dir": sandbox_dir,
                "simulate_non_admin": True,
            },
        )
      cli.resume(
          lightflow=tenant_wf,
          log_id="tenant_non_admin_run",
          stage="verify_spec",
          resolution="APPROVE",
          payload={"approved": True},
      )

      # 4d. Standard compliance tier (shared KMS branch) + auto-IAM (ALL_DONE
      #     bypass) + artifact failure rollback + surgical resume
      with self.assertRaises(engine.OperatorActionSuspended):
        cli.start(
            lightflow=tenant_wf,
            log_id="tenant_rollback_run",
            payload={
                "alias": "analytics-us",
                "compliance_tier": "standard",
                "sandbox_dir": sandbox_dir,
                "simulate_artifact_failure": True,
            },
        )
      with self.assertRaises(lib.LightflowRunFailedError):
        cli.resume(
            lightflow=tenant_wf,
            log_id="tenant_rollback_run",
            stage="verify_spec",
            resolution="APPROVE",
            payload={"approved": True},
        )
      cli.resume(
          lightflow=tenant_wf,
          log_id="tenant_rollback_run",
          stage="provision_external_resources",
          payload={"simulate_artifact_failure": False},
      )

    tenant_passport = lib.PassportManager("tenant_test_run").load_passport()
    self.assertIsNotNone(tenant_passport)
    assert tenant_passport is not None
    stamps_by_stage = {s.stage_name: s.status for s in tenant_passport.stamps}
    self.assertEqual(len(stamps_by_stage), 22)
    self.assertEqual(
        stamps_by_stage["provision_dedicated_kms_key"],
        schema.StampStatus.COMPLETED,
    )
    self.assertEqual(
        stamps_by_stage["apply_shared_kms_policy"], schema.StampStatus.SKIPPED
    )
    self.assertEqual(
        stamps_by_stage["wait_argocd_mesh_sync"], schema.StampStatus.COMPLETED
    )
    self.assertEqual(
        stamps_by_stage["stop_non_admin_stage"], schema.StampStatus.SKIPPED
    )
    self.assertEqual(
        stamps_by_stage["fail_on_iam_failed"], schema.StampStatus.SKIPPED
    )

    na_passport = lib.PassportManager("tenant_non_admin_run").load_passport()
    assert na_passport is not None
    na_stamps = {s.stage_name: s.status for s in na_passport.stamps}
    self.assertEqual(
        na_stamps["stop_non_admin_stage"], schema.StampStatus.COMPLETED
    )
    self.assertEqual(
        na_stamps["wait_argocd_mesh_sync"], schema.StampStatus.SKIPPED
    )

    rb_passport = lib.PassportManager("tenant_rollback_run").load_passport()
    assert rb_passport is not None
    rb_stamps = {s.stage_name: s.status for s in rb_passport.stamps}
    self.assertEqual(
        rb_stamps["prompt_iam_quota_override"], schema.StampStatus.SKIPPED
    )
    self.assertEqual(
        rb_stamps["wait_iam_role_ready"], schema.StampStatus.COMPLETED
    )
    self.assertEqual(
        rb_stamps["provision_dedicated_kms_key"], schema.StampStatus.SKIPPED
    )
    self.assertEqual(
        rb_stamps["apply_shared_kms_policy"], schema.StampStatus.COMPLETED
    )
    self.assertEqual(
        rb_stamps["wait_argocd_mesh_sync"], schema.StampStatus.COMPLETED
    )

  def test_stage_timeout_allows_all_done_cleanup_in_engine_loop(self) -> None:
    wf = schema.Lightflow(
        name="timeout_wf",
        actions=[
            schema.ActionDefinition(
                id="slow_act", python_import=f"{__name__}.act_step_1"
            ),
            schema.ActionDefinition(
                id="clean_act", python_import=f"{__name__}.act_step_3"
            ),
        ],
        stages=[
            schema.Stage(
                name="timed_out_stage",
                timeout_seconds=1,
                python_action=schema.PythonAction(action_id="slow_act"),
            ),
            schema.Stage(
                name="cleanup_stage",
                run_after=["timed_out_stage"],
                trigger_rule=schema.TriggerRule.ALL_DONE,
                python_action=schema.PythonAction(action_id="clean_act"),
            ),
        ],
    )
    pm = lib.PassportManager("timeout_all_done_run")
    passport = schema.Passport()
    runner = engine.LightflowEngine(wf)
    orig_exec = runner._execute_action_with_timeout

    def side_effect(stage_name, action_def, payload_dict, timeout=None):
      if stage_name == "timed_out_stage":
        raise engine.StageTimeoutError("Stage 'timed_out_stage' timed out")
      return orig_exec(stage_name, action_def, payload_dict, timeout=timeout)

    with mock.patch.object(
        runner, "_execute_action_with_timeout", side_effect=side_effect
    ):
      with self.assertRaisesRegex(
          lib.LightflowRunFailedError,
          "Root failure\\(s\\): timed_out_stage",
      ):
        lib.LightflowRunnerCLI()._execute_engine_loop(
            runner, ["timed_out_stage", "cleanup_stage"], passport, pm
        )

    saved = pm.load_passport()
    self.assertIsNotNone(saved)
    assert saved is not None
    latest = {s.stage_name: s.status for s in saved.stamps}
    self.assertEqual(latest["timed_out_stage"], schema.StampStatus.FAILED)
    self.assertEqual(latest["cleanup_stage"], schema.StampStatus.COMPLETED)
    self.assertTrue(saved.payload.get("step_3_done"))

  def test_create_lightflow_smoke_tests_actions_and_checks_upstream_outputs(
      self,
  ) -> None:
    try:
      from ..examples.create_lightflow import actions as cl_actions  # pylint: disable=g-import-not-at-top
    except ImportError:
      from examples.create_lightflow import actions as cl_actions  # type: ignore[no-redef]  # pylint: disable=g-import-not-at-top

    scaffold_dir = os.path.join(self.temp_dir, "scaffolded_wf")
    os.makedirs(scaffold_dir, exist_ok=True)
    with open(
        os.path.join(scaffold_dir, "actions.py"), "w", encoding="utf-8"
    ) as f:
      f.write(
          "def calc(payload, **kwargs):\n"
          "  return {'total': payload.get('x', 1) * 2}, 'ok'\n"
      )

    delta, _ = cl_actions.verify_actions({
        "outputs": {
            "align_on_design": {
                "target_dir": scaffold_dir,
                "action_names": ["calc"],
            },
            "implement_and_test_actions": {
                "actions_file": "actions.py",
                "sample_payload": {"x": 5},
            },
        }
    })
    self.assertEqual(delta["output_keys_by_action"], {"calc": ["total"]})

    # Referencing payload.outputs.s1 without run_after: [s1] is rejected
    bad_manifest = os.path.join(scaffold_dir, "bad.yaml")
    with open(bad_manifest, "w", encoding="utf-8") as f:
      f.write(
          "name: bad_wf\n"
          "actions:\n"
          "  - id: calc\n"
          "    python_import: actions.calc\n"
          "stages:\n"
          "  - name: s1\n"
          "    python_action:\n"
          "      action_id: calc\n"
          "  - name: s2\n"
          "    run_if: 'payload.outputs.s1.total > 0'\n"
          "    python_action:\n"
          "      action_id: calc\n"
      )
    with self.assertRaisesRegex(
        ValueError, "not an upstream dependency in `run_after`"
    ):
      cl_actions.validate_manifest_dry_run({
          "outputs": {
              "align_on_design": {"target_dir": scaffold_dir},
              "draft_and_explain_manifest": {"manifest_file": "bad.yaml"},
          }
      })

  def _record_notifications(self) -> list[tuple[str, str]]:
    events: list[tuple[str, str]] = []
    engine.register_notification_handler(
        lambda title, content: events.append((title, content))
    )
    self.addCleanup(engine.clear_notification_handlers)
    return events

  def test_status_notification_omits_mermaid_diagram(self) -> None:
    events = self._record_notifications()
    wf = schema.Lightflow(name="wf", stages=[schema.Stage(name="gate")])
    runner = engine.LightflowEngine(wf)
    passport = schema.Passport()
    s1 = passport.stamps.add()
    s1.stage_name = "gate"
    s1.status = schema.StampStatus.COMPLETED

    lib.LightflowRunnerCLI()._send_status_notification(
        runner, passport, "Lightflow completed successfully!"
    )
    self.assertEqual(
        events, [("Lightflow Completed", "Lightflow completed successfully!")]
    )

  def test_bare_stage_notifications_and_explicit_stdout_info(self) -> None:
    events = self._record_notifications()
    wf = schema.Lightflow(
        name="notif_wf",
        actions=[
            schema.ActionDefinition(
                id="silent_act", python_import=f"{__name__}.act_silent"
            ),
            schema.ActionDefinition(
                id="info_act", python_import=f"{__name__}.act_step_3"
            ),
        ],
        stages=[
            schema.Stage(
                name="fetch_stage",
                python_action=schema.PythonAction(action_id="silent_act"),
            ),
            schema.Stage(
                name="emit_stage",
                run_after=["fetch_stage"],
                python_action=schema.PythonAction(action_id="info_act"),
            ),
        ],
    )
    runner = engine.LightflowEngine(wf)
    passport = schema.Passport()
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
      runner.execute_stage("fetch_stage", passport)
      runner.execute_stage("emit_stage", passport)

    stdout_text = buf.getvalue()
    self.assertNotIn("[fetch_stage]", stdout_text)
    self.assertIn("ℹ️ [emit_stage] Step 3 done", stdout_text)
    self.assertEqual(passport.stamps[1].message, "")
    self.assertEqual(passport.stamps[3].message, "Step 3 done")
    self.assertEqual(
        events,
        [
            ("Stage Entered: fetch_stage", "🎬 fetch_stage: entered"),
            ("Stage Completed: fetch_stage", "✅ fetch_stage: completed"),
            ("Stage Entered: emit_stage", "🎬 emit_stage: entered"),
            ("Stage Completed: emit_stage", "✅ emit_stage: completed"),
        ],
    )

  def test_trigger_rule_notifies_only_terminal_outcomes(self) -> None:
    wf = schema.Lightflow(name="trigger_wf", stages=[schema.Stage(name="s")])
    cases = [
        (schema.StampStatus.FAILED, [("Stage Failed: s", "❌ s: failed")]),
        (schema.StampStatus.SKIPPED, [("Stage Skipped: s", "⏭️ s: skipped")]),
        # Still waiting on parents: not a lifecycle transition.
        (schema.StampStatus.STATUS_UNSPECIFIED, []),
    ]
    for status, expected in cases:
      with self.subTest(status=status):
        events = self._record_notifications()
        runner = engine.LightflowEngine(wf)
        with mock.patch.object(
            runner,
            "evaluate_trigger_rule",
            return_value=(False, status, "reason"),
        ):
          runner.execute_stage("s", schema.Passport())
        self.assertEqual(events, expected)
        engine.clear_notification_handlers()


class StatusStateTest(unittest.TestCase):
  """`status` prints one line per stage and signals the run's state."""

  # Literal on purpose: `status` must keep recognising the cascade message
  # already written into existing passports.
  _UPSTREAM_FAILED = "Failed: Upstream dependency failed."

  def setUp(self) -> None:
    super().setUp()
    self.temp_dir = tempfile.mkdtemp(prefix="lightflow_oss_status_")
    self.addCleanup(shutil.rmtree, self.temp_dir, ignore_errors=True)
    patcher = mock.patch.dict(
        os.environ, {"LIGHTFLOW_STATE_DIR": self.temp_dir}
    )
    patcher.start()
    self.addCleanup(patcher.stop)
    self.manifest = os.path.join(self.temp_dir, "status.yaml")
    self._write_manifest("""
name: status_workflow
stages:
  - name: a
    operator_action:
      instructions: "'Check a.'"
      json_schema: '{"type": "object", "required": ["approved"]}'
  - name: b
    run_after: [a]
    operator_action:
      instructions: "'Check b.'"
""")

  def _write_manifest(self, text: str) -> None:
    with open(self.manifest, "w", encoding="utf-8") as f:
      f.write(text)

  def _save(self, *stamps: tuple[str, schema.StampStatus, str]) -> None:
    passport = schema.Passport()
    for stage_name, status, message in stamps:
      stamp = passport.stamps.add()
      stamp.stage_name = stage_name
      stamp.status = status
      stamp.message = message
      if status == schema.StampStatus.PAUSED:
        stamp.instructions = "Check a."
        stamp.resume_command = "lightflow resume --stage=a"
    lib.PassportManager("status_run").save_passport(passport)

  def _status(
      self,
      expected_exc: type[Exception] | None = None,
      verbose: bool = False,
  ) -> str:
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
      if expected_exc is None:
        lib.LightflowRunnerCLI().status(
            lightflow=self.manifest, log_id="status_run", verbose=verbose
        )
      else:
        with self.assertRaises(expected_exc):
          lib.LightflowRunnerCLI().status(
              lightflow=self.manifest, log_id="status_run", verbose=verbose
          )
    return buf.getvalue()

  def test_completed_run_returns_normally(self) -> None:
    self._save(
        ("a", schema.StampStatus.COMPLETED, ""),
        ("b", schema.StampStatus.SKIPPED, ""),
    )
    out = self._status()
    self.assertIn("): COMPLETED", out)
    self.assertIn("✅ a: completed", out)
    self.assertIn("⏭️ b: skipped", out)

  def test_paused_run_reprints_gate_schema(self) -> None:
    self._save(("a", schema.StampStatus.PAUSED, ""))
    out = self._status(lib.LightflowAlreadyPausedError)
    self.assertIn("⏸️ a: paused", out)
    self.assertIn("Instructions: Check a.", out)
    self.assertIn('"required": ["approved"]', out)
    self.assertIn("Resume with: lightflow resume --stage=a", out)
    self.assertIn("⬜ b: not started", out)
    self.assertNotIn("LIGHTFLOW STATUS DIAGRAM:", out)

  def test_verbose_adds_payload_stamp_log_and_diagram(self) -> None:
    self._save(("a", schema.StampStatus.PAUSED, ""))
    out = self._status(lib.LightflowAlreadyPausedError, verbose=True)
    self.assertIn("PASSPORT PAYLOAD (JSON):", out)
    self.assertIn("STAGE STAMPS LOG:", out)
    self.assertIn("```mermaid", out)

  def test_failed_run_prints_summarised_error(self) -> None:
    self._save(("a", schema.StampStatus.FAILED, "\nDisk full\ntraceback"))
    out = self._status(lib.LightflowRunFailedError)
    self.assertIn("❌ a: failed (Disk full...)", out)
    self.assertNotIn("traceback", out)

  def test_rejected_gate_is_reported_as_rejected_not_failed(self) -> None:
    self._save(
        ("a", schema.StampStatus.FAILED, "Rejected by alice. Reason: no"),
        ("b", schema.StampStatus.FAILED, self._UPSTREAM_FAILED),
    )
    with self.assertRaisesRegex(lib.LightflowRunFailedError, "rejected"):
      lib.LightflowRunnerCLI().status(
          lightflow=self.manifest, log_id="status_run"
      )
    out = self._status(lib.LightflowRunFailedError)
    self.assertIn("): REJECTED", out)
    self.assertIn("🛑 a: rejected (Rejected by alice.", out)
    self.assertIn("⛔ b: blocked (upstream failed or was rejected)", out)

  def test_downstream_cascade_is_blocked_not_a_root_failure(self) -> None:
    self._save(
        ("a", schema.StampStatus.FAILED, "Disk full"),
        ("b", schema.StampStatus.FAILED, self._UPSTREAM_FAILED),
    )
    with self.assertRaisesRegex(
        lib.LightflowRunFailedError, r"Root failure\(s\): a\."
    ):
      lib.LightflowRunnerCLI().status(
          lightflow=self.manifest, log_id="status_run"
      )
    out = self._status(lib.LightflowRunFailedError)
    self.assertIn("): FAILED", out)
    self.assertIn("⛔ b: blocked", out)

  def _write_all_done_manifest(self) -> None:
    self._write_manifest("""
name: status_workflow
stages:
  - name: a
    operator_action:
      instructions: "'Check a.'"
  - name: b
    run_after: [a]
    trigger_rule: ALL_DONE
    operator_action:
      instructions: "'Check b.'"
""")

  def test_all_done_stage_failing_after_a_failure_is_a_root_failure(
      self,
  ) -> None:
    self._write_all_done_manifest()
    self._save(
        ("a", schema.StampStatus.FAILED, "Disk full"),
        ("b", schema.StampStatus.FAILED, "Cleanup raised: EPERM"),
    )
    with self.assertRaisesRegex(
        lib.LightflowRunFailedError, r"Root failure\(s\): a, b\."
    ):
      lib.LightflowRunnerCLI().status(
          lightflow=self.manifest, log_id="status_run"
      )
    out = self._status(lib.LightflowRunFailedError)
    self.assertIn("❌ b: failed (Cleanup raised: EPERM)", out)

  def test_all_done_stage_failing_after_a_rejection_needs_a_fix(self) -> None:
    self._write_all_done_manifest()
    self._save(
        ("a", schema.StampStatus.FAILED, "Rejected by bob."),
        ("b", schema.StampStatus.FAILED, "Cleanup raised: EPERM"),
    )
    with self.assertRaisesRegex(lib.LightflowRunFailedError, "Fix the root"):
      lib.LightflowRunnerCLI().status(
          lightflow=self.manifest, log_id="status_run"
      )
    out = self._status(lib.LightflowRunFailedError)
    self.assertIn("): FAILED", out)
    self.assertIn("🛑 a: rejected", out)
    self.assertIn("❌ b: failed", out)

    wf = schema.load_lightflow(self.manifest)
    runner_eng = engine.LightflowEngine(wf)
    ordered = runner_eng.compile()
    passport = lib.PassportManager("status_run").load_passport()
    assert passport is not None
    rearmed = lib.LightflowRunnerCLI()._rearm_failed_stages(
        runner_eng, ordered, passport, operator="bob"
    )
    self.assertEqual(rearmed, ["b"])

  def test_interrupted_run_raises_incomplete(self) -> None:
    self._save(("a", schema.StampStatus.COMPLETED, ""))
    out = self._status(lib.LightflowRunFailedError)
    self.assertIn("): INCOMPLETE", out)

  def test_upstream_failed_message_matches_saved_passports(self) -> None:
    self.assertEqual(engine.UPSTREAM_FAILED_MESSAGE, self._UPSTREAM_FAILED)

  def test_run_held_by_another_process_reports_running(self) -> None:
    self._save(("a", schema.StampStatus.PENDING, ""))
    with lib.PassportManager("status_run").lock(exclusive=True):
      out = self._status(lib.LightflowRunningError)
    self.assertIn("): RUNNING", out)

  def test_status_probe_creates_no_lock_file(self) -> None:
    self._save(("a", schema.StampStatus.COMPLETED, ""))
    lock_path = lib.PassportManager("status_run").resolved_path + ".lock"
    self.assertFalse(os.path.exists(lock_path))
    self._status(lib.LightflowRunFailedError)
    self.assertFalse(os.path.exists(lock_path))

  def test_lock_waits_out_a_momentary_holder(self) -> None:
    """A `status` probe polling at the wrong moment must not fail a run."""
    pm = lib.PassportManager("status_run")
    held = threading.Event()

    def hold_briefly() -> None:
      with pm.lock(exclusive=False):
        held.set()
        time.sleep(0.05)

    # A generous window keeps the test deterministic on a loaded machine.
    retry = mock.patch.object(lib.state_lib, "_LOCK_RETRY_SECONDS", 5.0)
    retry.start()
    self.addCleanup(retry.stop)
    holder = threading.Thread(target=hold_briefly)
    holder.start()
    self.addCleanup(holder.join)
    self.assertTrue(held.wait(timeout=5))
    with pm.lock(exclusive=True):  # Raises without the retry.
      pass

  def test_lock_errors_other_than_contention_propagate(self) -> None:
    """E.g. ENOLCK on a filesystem without locks must not read as running."""
    pm = lib.PassportManager("status_run")
    with pm.lock():
      pass  # Creates the lock file so `is_locked` gets as far as locking.

    def no_locks_flock(fd: Any, flags: int) -> None:
      del fd, flags
      raise OSError(errno.ENOLCK, "No locks available")

    no_locks_fcntl = types.SimpleNamespace(
        LOCK_EX=2, LOCK_SH=1, LOCK_NB=4, LOCK_UN=8, flock=no_locks_flock
    )

    with mock.patch.object(lib.state_lib, "fcntl", no_locks_fcntl):
      with self.assertRaises(OSError) as ctx:
        with pm.lock():
          pass
      self.assertNotIsInstance(ctx.exception, RuntimeError)
      with self.assertRaises(OSError):
        pm.is_locked()

  def test_missing_state_raises_not_found(self) -> None:
    self._status(lib.LightflowStateNotFoundError)

  def test_stamps_for_removed_stages_are_ignored(self) -> None:
    self._save(
        ("a", schema.StampStatus.COMPLETED, ""),
        ("b", schema.StampStatus.COMPLETED, ""),
        ("removed_gate", schema.StampStatus.PAUSED, ""),
    )
    out = self._status()
    self.assertIn("): COMPLETED", out)
    self.assertNotIn("removed_gate", out)

  def test_manifest_that_no_longer_compiles_falls_back(self) -> None:
    self._save(("a", schema.StampStatus.COMPLETED, ""))
    self._write_manifest("""
name: status_workflow
stages:
  - name: a
    operator_action:
      instructions: "'Check a.'"
  - name: b
    run_after: [a, missing_stage]
    operator_action:
      instructions: "'Check b.'"
""")
    err = io.StringIO()
    with contextlib.redirect_stderr(err):
      out = self._status(lib.LightflowRunFailedError)
    self.assertIn("Manifest does not compile", err.getvalue())
    self.assertIn("✅ a: completed", out)
    self.assertIn("⬜ b: not started", out)

  def test_mixed_case_log_id_notes_shared_state_on_stderr(self) -> None:
    buf = io.StringIO()
    with contextlib.redirect_stderr(buf):
      upper = lib.PassportManager("Run_A")
      lower = lib.PassportManager("run_a")
    self.assertEqual(upper.resolved_path, lower.resolved_path)
    self.assertEqual(buf.getvalue().count("case-insensitive"), 1)

  def test_lowercase_and_path_log_ids_print_nothing(self) -> None:
    buf = io.StringIO()
    with contextlib.redirect_stderr(buf):
      lib.PassportManager("run_a")
      lib.PassportManager(os.path.join(self.temp_dir, "lightflow_state_Mixed"))
    self.assertEqual(buf.getvalue(), "")

  def test_dry_run_defers_unpopulated_upstream_output_conditions_and_shows_timeout(
      self,
  ) -> None:
    module_name = __name__
    self._write_manifest(f"""
name: deferred_dry_run_wf
actions:
  - id: compute
    python_import: {module_name}.act_step_1
stages:
  - name: scan
    timeout_seconds: 300
    python_action:
      action_id: compute
  - name: triage
    run_after: [scan]
    run_if: "payload.outputs.scan.n_high + payload.outputs.scan.n_med > 0"
    python_action:
      action_id: compute
  - name: approve
    run_after: [triage]
    operator_action:
      instructions: "'Review ' + string(payload.outputs.triage.n_high)"
      json_schema: '{{"type": "object", "required": ["approved"]}}'
  - name: top_level_unsimulated
    run_after: [scan]
    run_if: "payload.n_high + 1 > 0"
    python_action:
      action_id: compute
  - name: has_method_syntax
    run_after: [scan]
    run_if: "payload.outputs.has('scan')"
    python_action:
      action_id: compute
  - name: unbounded
    run_after: [scan]
    timeout_seconds: 0
    python_action:
      action_id: compute
  - name: skipped_branch
    run_if: "false"
    python_action:
      action_id: compute
  - name: all_done_after_skipped
    run_after: [skipped_branch]
    trigger_rule: ALL_DONE
    run_if: "has(payload.outputs.skipped_branch.n_high)"
    python_action:
      action_id: compute
""")
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
      lib.LightflowRunnerCLI().dry_run(lightflow=self.manifest)
    rendered = buf.getvalue()
    self.assertIn("Timeout: 300s", rendered)
    self.assertIn(
        "[NOT SIMULATED (action does not accept dry_run, or its simulation"
        " raised)]",
        rendered,
    )
    self.assertIn("[NOT SIMULATED (awaits outputs of: scan)]", rendered)
    self.assertIn("Timeout: disabled (timeout_seconds: 0)", rendered)
    self.assertNotIn("Timeout: 0s", rendered)
    self.assertIn(
        "[DEFERRED TO RUNTIME (awaits outputs of: scan)]",
        rendered,
    )
    self.assertIn(
        "Condition 'payload.n_high + 1 > 0' -> [DEFERRED TO RUNTIME",
        rendered,
    )
    self.assertIn(
        "Condition 'payload.outputs.has('scan')' -> [DEFERRED TO RUNTIME"
        " (awaits outputs of: scan)]",
        rendered,
    )
    self.assertIn("Stage: approve", rendered)
    self.assertIn(
        "Instructions: [DEFERRED TO RUNTIME (awaits outputs of: triage)]",
        rendered,
    )
    self.assertIn(
        "Condition 'has(payload.outputs.skipped_branch.n_high)' -> False",
        rendered,
    )

  def test_polling_persists_tick_message_and_status_displays_stage_messages(
      self,
  ) -> None:
    module_name = __name__
    self._write_manifest(f"""
name: poll_status_wf
actions:
  - id: compute
    python_import: {module_name}.act_step_1
stages:
  - name: triage_findings
    polling_policy:
      poll_tick_action:
        action_id: compute
      condition: "payload.step_1_done == true"
      interval_seconds: 0
      max_attempts: 2
""")
    pm = lib.PassportManager("status_run")
    mid_poll_status_output: list[str] = []
    orig_save = pm.save_passport

    def intercept_save(passport: schema.Passport) -> None:
      orig_save(passport)
      if passport.stamps and passport.stamps[-1].message:
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
          with self.assertRaises(lib.LightflowRunFailedError):
            lib.LightflowRunnerCLI().status(
                lightflow=self.manifest, log_id="status_run"
            )
        mid_poll_status_output.append(buf.getvalue())

    wf = schema.load_lightflow(self.manifest)
    runner_eng = engine.LightflowEngine(wf, lightflow_path=self.manifest)
    passport = schema.Passport()
    with (
        contextlib.redirect_stdout(io.StringIO()),
        mock.patch.object(engine, "send_notification"),
    ):
      runner_eng.execute_stage(
          "triage_findings", passport, save_callback=intercept_save
      )
    pm.save_passport(passport)

    self.assertTrue(mid_poll_status_output)
    self.assertIn(f"Passport: {pm.passport_file}", mid_poll_status_output[0])
    self.assertIn(
        "🔄 triage_findings: pending (Step 1 done)",
        mid_poll_status_output[0],
    )

    done_buf = io.StringIO()
    with contextlib.redirect_stdout(done_buf):
      lib.LightflowRunnerCLI().status(
          lightflow=self.manifest, log_id="status_run"
      )
    # Compact status stays terse for completed stages; the final polling
    # message remains available via --verbose (stamp log).
    self.assertIn("✅ triage_findings: completed\n", done_buf.getvalue())
    self.assertNotIn("Polling condition met", done_buf.getvalue())
    verbose_buf = io.StringIO()
    with contextlib.redirect_stdout(verbose_buf):
      lib.LightflowRunnerCLI().status(
          lightflow=self.manifest, log_id="status_run", verbose=True
      )
    self.assertIn(
        "Polling condition met after 1 attempts: Step 1 done",
        verbose_buf.getvalue(),
    )

    clean_buf = io.StringIO()
    with contextlib.redirect_stdout(clean_buf):
      lib.LightflowRunnerCLI().cleanup(log_id="status_run")
    self.assertIn(
        "external workflow artifacts on disk are untouched",
        clean_buf.getvalue(),
    )


class RunnerExitCodeTest(unittest.TestCase):
  """The exit code is the contract for any agent or script wrapping the CLI.

  Asserted as literals to pin the externally visible numbers.
  """

  def setUp(self) -> None:
    super().setUp()
    try:
      from ..lightflow import runner  # pylint: disable=g-import-not-at-top
    except ImportError:
      from lightflow import runner  # type: ignore[no-redef]  # pylint: disable=g-import-not-at-top
    self.runner = runner
    patcher = mock.patch.object(engine, "wait_for_notifications")
    patcher.start()
    self.addCleanup(patcher.stop)

  def _exit_code_for(self, raised: BaseException) -> Any:
    with mock.patch.object(self.runner, "_dispatch_argv", side_effect=raised):
      with self.assertRaises(SystemExit) as ctx:
        self.runner.run_cli()
    return ctx.exception.code

  def test_run_states_map_to_exit_codes(self) -> None:
    cases = (
        (lib.LightflowAlreadyCompletedError("done"), 0),
        (lib.LightflowRunFailedError("stage failed"), 1),
        (ValueError("bad log_id"), 1),
        (lib.LightflowAlreadyPausedError("paused"), 2),
        (engine.OperatorActionSuspended("suspended"), 2),
        (lib.LightflowStateNotFoundError("no state"), 3),
        (lib.LightflowRunningError("running"), 4),
        (lib.LightflowWaitingError("waiting"), 5),
    )
    for raised, code in cases:
      with self.subTest(exception=type(raised).__name__):
        with contextlib.redirect_stdout(io.StringIO()):
          with contextlib.redirect_stderr(io.StringIO()):
            self.assertEqual(self._exit_code_for(raised), code)

  def test_already_reported_outcomes_are_not_printed_twice(self) -> None:
    for raised in (
        lib.LightflowRunFailedError("footer printed", already_reported=True),
        lib.LightflowWaitingError("footer printed", already_reported=True),
    ):
      with self.subTest(exception=type(raised).__name__):
        out = io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(out):
          self._exit_code_for(raised)
        self.assertNotIn("footer printed", out.getvalue())


def act_emit_count_42(
    payload: dict[str, Any], dry_run: bool = False, **static_kwargs: Any
) -> tuple[dict[str, Any], str]:
  del payload, dry_run, static_kwargs
  return {"count": 42, "status": "ok"}, "scan emitted 42"


_TICKS: list[int] = []


def act_tick_counter(
    payload: dict[str, Any], **static_kwargs: Any
) -> tuple[dict[str, Any], str]:
  del payload, static_kwargs
  _TICKS.append(1)
  return {"n": len(_TICKS)}, f"tick {len(_TICKS)}"


def act_emit_count_5(
    payload: dict[str, Any], dry_run: bool = False, **static_kwargs: Any
) -> tuple[dict[str, Any], str]:
  del payload, dry_run, static_kwargs
  return {"count": 5, "status": "ok"}, "triage emitted 5"


class _SandboxTestCase(unittest.TestCase):
  """An isolated state directory and environment for CLI-level tests.

  `LIGHTFLOW_STATE_DIR` points at the sandbox (or `state_subdir` under it) and
  is restored afterwards together with any other env edits a test makes.
  Classes that drive whole runs set `silence_notifications` so agent pushes
  are not attempted.
  """

  state_subdir = ""
  silence_notifications = False

  def setUp(self) -> None:
    super().setUp()
    self.temp_dir = tempfile.mkdtemp(prefix="lightflow_test_")
    self.addCleanup(shutil.rmtree, self.temp_dir, True)
    self.state_dir = (
        os.path.join(self.temp_dir, self.state_subdir)
        if self.state_subdir
        else self.temp_dir
    )
    env = mock.patch.dict(os.environ, {"LIGHTFLOW_STATE_DIR": self.state_dir})
    env.start()
    self.addCleanup(env.stop)
    if self.silence_notifications:
      quiet = mock.patch.object(engine, "send_notification")
      quiet.start()
      self.addCleanup(quiet.stop)

  def _write(self, rel: str, text: str) -> str:
    """Writes `text` under the sandbox and returns the path."""
    full = os.path.join(self.temp_dir, rel)
    os.makedirs(os.path.dirname(full), exist_ok=True)
    with open(full, "w", encoding="utf-8") as f:
      f.write(text)
    return full


class StrictExpressionsTest(_SandboxTestCase):
  """`strict_expressions: true` makes absent keys fail loudly."""

  def test_evaluator_strict_vs_lenient(self) -> None:
    payload = {"outputs": {"confirm": {"approved": True}}, "n": 3}
    typo = "payload.outputs.confirm.aproved != true"
    # Lenient: the typo silently evaluates to True (null != true).
    self.assertTrue(engine.evaluate_cel(typo, payload))
    # Strict: it raises, naming the key.
    with self.assertRaises(engine.EngineError) as ctx:
      engine.evaluate_cel(typo, payload, strict=True)
    self.assertIsInstance(ctx.exception.__cause__, engine.MissingFieldError)
    self.assertIn("no such key 'aproved'", str(ctx.exception))
    # The key comes first so it survives `status`'s 140-char summary.
    self.assertTrue(str(ctx.exception).startswith("no such key 'aproved'"))
    self.assertIn("(in expression 'payload.outputs", str(ctx.exception))
    # Strict also covers subscripts and missing upstream stages.
    with self.assertRaises(engine.EngineError):
      engine.evaluate_cel("payload['q'] == 1", payload, strict=True)
    with self.assertRaises(engine.EngineError) as ctx2:
      engine.evaluate_cel(
          "payload.outputs.scan.n_high > 0", payload, strict=True
      )
    self.assertIn("no such key 'scan'", str(ctx2.exception))
    # has() remains the sanctioned guard and never raises.
    self.assertFalse(
        engine.evaluate_cel(
            "has(payload.outputs.confirm.aproved)", payload, strict=True
        )
    )
    self.assertTrue(
        engine.evaluate_cel(
            "has(payload.outputs.confirm.approved) &&"
            " payload.outputs.confirm.approved",
            payload,
            strict=True,
        )
    )
    # Present keys behave identically in both modes.
    self.assertTrue(engine.evaluate_cel("payload.n > 2", payload, strict=True))

  def test_strict_manifest_fails_stage_on_typo_lenient_runs_it(self) -> None:
    module_name = __name__
    for strict, expect_failed in ((True, True), (False, False)):
      with self.subTest(strict=strict):
        manifest = os.path.join(self.temp_dir, f"wf_{strict}.yaml")
        with open(manifest, "w", encoding="utf-8") as f:
          f.write(f"""
name: strict_wf_{strict}
strict_expressions: {str(strict).lower()}
actions:
  - id: compute
    python_import: {module_name}.act_step_1
stages:
  - name: first
    python_action:
      action_id: compute
  - name: gated
    run_after: [first]
    run_if: "payload.outputs.first.step_1_dne != true"
    python_action:
      action_id: compute
""")
        wf = schema.load_lightflow(manifest)
        self.assertEqual(wf.strict_expressions, strict)
        cli = lib.LightflowRunnerCLI()
        buf = io.StringIO()
        with (
            contextlib.redirect_stdout(buf),
            mock.patch.object(engine, "send_notification"),
        ):
          if expect_failed:
            with self.assertRaisesRegex(
                lib.LightflowRunFailedError,
                r"Root failure\(s\): gated",
            ):
              cli.start(lightflow=manifest, log_id=f"strict_{strict}")
          else:
            cli.start(lightflow=manifest, log_id=f"strict_{strict}")
        out = buf.getvalue()
        if expect_failed:
          self.assertIn("no such key 'step_1_dne'", out)
          self.assertIn("guard optional keys with has(...)", out)
          with self.assertRaisesRegex(
              lib.LightflowRunFailedError,
              r"Fix the root cause then run: lightflow resume"
              r" --log_id=strict_True$",
          ):
            with contextlib.redirect_stdout(io.StringIO()):
              cli.status(log_id="strict_True")
        else:
          self.assertNotIn("no such key", out)
          self.assertIn("Lightflow completed successfully", out)

  def test_strict_errors_suggest_close_keys(self) -> None:
    payload = {"outputs": {"confirm": {"approved": True, "note": "x"}}}
    with self.assertRaisesRegex(
        engine.EngineError, r"no such key 'aproved'; did you mean 'approved'\?"
    ):
      engine.evaluate_cel(
          "payload.outputs.confirm.aproved", payload, strict=True
      )
    with self.assertRaisesRegex(
        engine.EngineError, r"no such key 'confrm'; did you mean 'confirm'\?"
    ):
      engine.evaluate_cel(
          "payload.outputs.confrm.approved", payload, strict=True
      )
    with self.assertRaisesRegex(
        engine.EngineError, r"no such key 'zzz'; available: outputs"
    ):
      engine.evaluate_cel("payload.zzz", payload, strict=True)

  def test_keyword_or_invalid_stage_names_are_rejected_at_compile(
      self,
  ) -> None:
    for bad in ("await", "my-stage", "1st"):
      wf = schema.Lightflow(
          name="bad_names",
          stages=[
              schema.Stage(
                  name=bad, python_action=schema.PythonAction(action_id="x")
              )
          ],
          actions=[schema.ActionDefinition(id="x", python_import="m.f")],
      )
      with self.assertRaisesRegex(
          engine.EngineError, rf"Stage name '{re.escape(bad)}' is "
      ):
        engine.LightflowEngine(wf).compile()

  def test_dry_run_seed_hints_and_summary(self) -> None:
    module_name = __name__
    manifest = os.path.join(self.temp_dir, "wf_seed.yaml")
    with open(manifest, "w", encoding="utf-8") as f:
      f.write(f"""
name: seed_wf
strict_expressions: true
actions:
  - id: compute
    python_import: {module_name}.act_emit_count_5
stages:
  - name: prod_only
    run_if: "payload.environment == 'PROD' && !has(payload.skip)"
    python_action:
      action_id: compute
  - name: after
    run_after: [prod_only]
    python_action:
      action_id: compute
""")
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
      lib.LightflowRunnerCLI().dry_run(lightflow=manifest)
    out = buf.getvalue()
    # Unseeded top-level keys defer (and keep tracing) instead of pruning.
    self.assertIn(
        "[DEFERRED TO RUNTIME (awaits --payload keys: environment)]", out
    )
    self.assertNotIn('"skip"', out)  # has() refs are not seed candidates
    self.assertIn("Stage: after", out)
    self.assertIn(
        "Summary: 2 stages · 2 would run (2 simulated, 0 not simulated)"
        " · 0 operator gate(s) · 1 deferred to runtime · 0 skipped",
        out,
    )
    self.assertIn(
        "💡 Seed these keys to trace the deferred branches (skip any that an"
        ' upstream stage sets at runtime): --payload=\'{"environment":'
        ' "<value>"}\'  (needed by: prod_only)',
        out,
    )
    seeded = io.StringIO()
    with contextlib.redirect_stdout(seeded):
      lib.LightflowRunnerCLI().dry_run(
          lightflow=manifest, payload='{"environment": "DEV"}'
      )
    self.assertIn("-> False", seeded.getvalue())
    self.assertIn("· 0 deferred to runtime · 2 skipped", seeded.getvalue())
    self.assertNotIn("💡", seeded.getvalue())

  def test_dry_run_strict_defers_unsimulated_upstream(self) -> None:
    module_name = __name__
    manifest = os.path.join(self.temp_dir, "wf_dry.yaml")
    with open(manifest, "w", encoding="utf-8") as f:
      f.write(f"""
name: strict_dry
strict_expressions: true
actions:
  - id: compute
    python_import: {module_name}.act_silent
stages:
  - name: scan
    python_action:
      action_id: compute
  - name: triage
    run_after: [scan]
    run_if: "payload.n_high > 0"
    python_action:
      action_id: compute
""")
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
      lib.LightflowRunnerCLI().dry_run(lightflow=manifest)
    # `scan` is unsimulated (act_silent has no dry_run); under strict mode the
    # missing top-level key is a MissingFieldError, which must still defer.
    self.assertIn(
        "Condition 'payload.n_high > 0' -> [DEFERRED TO RUNTIME",
        buf.getvalue(),
    )


class PayloadCollisionTest(_SandboxTestCase):
  """Top-level key overwrites are surfaced at run, dry-run, and status time."""

  def setUp(self) -> None:
    super().setUp()
    self.manifest = self._write(
        "wf.yaml",
        f"""
name: collisions_wf
actions:
  - id: a
    python_import: {__name__}.act_emit_count_42
  - id: b
    python_import: {__name__}.act_emit_count_5
stages:
  - name: scan
    python_action:
      action_id: a
  - name: triage
    run_after: [scan]
    run_if: "payload.count > 0"
    python_action:
      action_id: b
""",
    )
    # Same two stages, but nothing reads the top-level mirror: the normal
    # reusable-action pattern, which must not warn.
    self.quiet_manifest = self._write(
        "quiet.yaml",
        f"""
name: quiet_wf
actions:
  - id: a
    python_import: {__name__}.act_emit_count_42
  - id: b
    python_import: {__name__}.act_emit_count_5
stages:
  - name: scan
    python_action:
      action_id: a
  - name: triage
    run_after: [scan]
    run_if: "payload.outputs.scan.count > 0"
    python_action:
      action_id: b
""",
    )

  def test_overwrite_nobody_reads_at_top_level_is_quiet(self) -> None:
    cli = lib.LightflowRunnerCLI()
    dry = io.StringIO()
    with contextlib.redirect_stdout(dry):
      cli.dry_run(lightflow=self.quiet_manifest, payload='{"count": 1}')
    self.assertNotIn("Overwrites top-level", dry.getvalue())
    run = io.StringIO()
    with (
        contextlib.redirect_stdout(run),
        mock.patch.object(engine, "send_notification"),
    ):
      cli.start(
          lightflow=self.quiet_manifest, log_id="quiet", payload='{"count": 1}'
      )
    self.assertNotIn("overwrote top-level", run.getvalue())
    self.assertIn("Lightflow completed successfully", run.getvalue())
    # Provenance still shows every writer, so the information is not lost.
    status = io.StringIO()
    with contextlib.redirect_stdout(status):
      cli.status(lightflow=self.quiet_manifest, log_id="quiet", verbose=True)
    self.assertIn("count ← triage  (also set by: scan)", status.getvalue())

  def test_merge_reports_collisions_and_provenance(self) -> None:
    payload: dict[str, Any] = {"count": 1}
    self.assertEqual(
        engine.merge_stage_payload(payload, "scan", {"count": 42, "s": "ok"}),
        [("count", "initial payload")],
    )
    self.assertEqual(
        engine.merge_stage_payload(payload, "triage", {"count": 5, "s": "ok"}),
        [("count", "scan")],  # identical `s` is not a collision
    )
    self.assertEqual(
        engine.top_level_key_provenance(payload),
        {"count": ("triage", ["scan"]), "s": ("scan", ["triage"])},
    )
    # A stage re-writing its own key (poll ticks, retries) is not a collision,
    # even when the new value happens to differ.
    self.assertEqual(
        engine.merge_stage_payload(payload, "triage", {"count": 6}), []
    )
    self.assertEqual(payload["count"], 6)

  def test_polling_ticks_do_not_warn_about_their_own_progress(self) -> None:
    module_name = __name__
    manifest = os.path.join(self.temp_dir, "poll.yaml")
    with open(manifest, "w", encoding="utf-8") as f:
      f.write(f"""
name: poll_wf
actions:
  - id: tick
    python_import: {module_name}.act_tick_counter
stages:
  - name: await_job
    polling_policy:
      condition: "payload.outputs.await_job.n >= 3"
      interval_seconds: 1
      max_attempts: 5
      poll_tick_action:
        action_id: tick
""")
    _TICKS.clear()
    buf = io.StringIO()
    with (
        contextlib.redirect_stdout(buf),
        mock.patch.object(engine, "send_notification"),
        mock.patch.object(engine.time, "sleep"),
    ):
      lib.LightflowRunnerCLI().start(lightflow=manifest, log_id="poll")
    self.assertNotIn("overwrote top-level", buf.getvalue())
    self.assertIn("Lightflow completed successfully", buf.getvalue())

  def test_run_dry_run_and_verbose_status_surface_collisions(self) -> None:
    cli = lib.LightflowRunnerCLI()
    dry = io.StringIO()
    with contextlib.redirect_stdout(dry):
      cli.dry_run(lightflow=self.manifest, payload='{"count": 1}')
    self.assertIn(
        "⚠️ Overwrites top-level payload key 'count' (previously set by"
        " initial payload)",
        dry.getvalue(),
    )
    self.assertIn(
        "⚠️ Overwrites top-level payload key 'count' (previously set by scan)",
        dry.getvalue(),
    )

    run = io.StringIO()
    with (
        contextlib.redirect_stdout(run),
        mock.patch.object(engine, "send_notification"),
    ):
      cli.start(lightflow=self.manifest, log_id="coll", payload='{"count": 1}')
    self.assertIn(
        "⚠️ [triage] overwrote top-level payload.count (previously set by"
        " scan), which an expression reads",
        run.getvalue(),
    )

    status = io.StringIO()
    with contextlib.redirect_stdout(status):
      cli.status(lightflow=self.manifest, log_id="coll", verbose=True)
    self.assertIn("TOP-LEVEL KEY PROVENANCE", status.getvalue())
    self.assertIn("count ← triage  (also set by: scan)", status.getvalue())
    self.assertIn("status ← scan  (also set by: triage)", status.getvalue())


class ListAndManifestTrackingTest(_SandboxTestCase):
  """`list`, Passport manifest metadata, and the same-log-id guard."""

  def setUp(self) -> None:
    super().setUp()
    self.manifest_a = self._manifest("alpha_wf", "a.yaml")
    self.manifest_b = self._manifest("beta_wf", "b.yaml")

  def _manifest(self, name: str, filename: str) -> str:
    path = os.path.join(self.temp_dir, filename)
    with open(path, "w", encoding="utf-8") as f:
      f.write(f"""
name: {name}
actions:
  - id: compute
    python_import: {__name__}.act_step_1
stages:
  - name: first
    python_action:
      action_id: compute
  - name: gate
    run_after: [first]
    operator_action:
      instructions: "'Approve?'"
""")
    return path

  def test_start_records_manifest_and_status_infers_it(self) -> None:
    cli = lib.LightflowRunnerCLI()
    with (
        contextlib.redirect_stdout(io.StringIO()),
        mock.patch.object(engine, "send_notification"),
        self.assertRaises(engine.OperatorActionSuspended),
    ):
      cli.start(lightflow=self.manifest_a, log_id="run1")
    passport = lib.PassportManager("run1").load_passport()
    assert passport is not None
    self.assertEqual(passport.lightflow_name, "alpha_wf")
    self.assertEqual(passport.lightflow_path, os.path.abspath(self.manifest_a))

    # `status` no longer needs --lightflow for runs that recorded it.
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
      with self.assertRaises(lib.LightflowAlreadyPausedError):
        cli.status(log_id="run1")
    self.assertIn("Lightflow 'alpha_wf' (log ID: run1): PAUSED", buf.getvalue())

    # Reusing the log ID with a different lightflow is refused up front.
    with self.assertRaisesRegex(
        ValueError, r"already holds state for lightflow 'alpha_wf'"
    ):
      with contextlib.redirect_stdout(io.StringIO()):
        cli.start(lightflow=self.manifest_b, log_id="run1")

  def test_resume_infers_manifest_and_reports_moved_manifest(self) -> None:
    cli = lib.LightflowRunnerCLI()
    with (
        contextlib.redirect_stdout(io.StringIO()),
        mock.patch.object(engine, "send_notification"),
        self.assertRaises(engine.OperatorActionSuspended),
    ):
      cli.start(lightflow=self.manifest_a, log_id="r2")
    moved = self.manifest_a + ".moved"
    os.rename(self.manifest_a, moved)
    try:
      with self.assertRaisesRegex(ValueError, "no longer exists at"):
        cli.status(log_id="r2")
      with self.assertRaisesRegex(ValueError, "no longer exists at"):
        cli.resume(log_id="r2", stage="gate", resolution="APPROVE")
    finally:
      os.rename(moved, self.manifest_a)
    buf = io.StringIO()
    with (
        contextlib.redirect_stdout(buf),
        mock.patch.object(engine, "send_notification"),
    ):
      cli.resume(log_id="r2", stage="gate", resolution="APPROVE")
    self.assertIn("Lightflow completed successfully", buf.getvalue())

  def test_status_without_lightflow_on_legacy_passport_is_explicit(
      self,
  ) -> None:
    legacy = schema.Passport()
    stamp = legacy.stamps.add()
    stamp.stage_name = "first"
    stamp.status = schema.StampStatus.COMPLETED
    lib.PassportManager("legacy").save_passport(legacy)
    with self.assertRaisesRegex(ValueError, "predates manifest tracking"):
      lib.LightflowRunnerCLI().status(log_id="legacy")
    # Legacy passports are backfilled the next time `start` sees them.
    cli = lib.LightflowRunnerCLI()
    with (
        contextlib.redirect_stdout(io.StringIO()),
        mock.patch.object(engine, "send_notification"),
        self.assertRaises(engine.OperatorActionSuspended),
    ):
      cli.start(lightflow=self.manifest_a, log_id="legacy")
    reloaded = lib.PassportManager("legacy").load_passport()
    assert reloaded is not None
    self.assertEqual(reloaded.lightflow_name, "alpha_wf")

  def test_list_shows_runs_states_and_orphan_locks(self) -> None:
    cli = lib.LightflowRunnerCLI()
    with (
        contextlib.redirect_stdout(io.StringIO()),
        mock.patch.object(engine, "send_notification"),
        self.assertRaises(engine.OperatorActionSuspended),
    ):
      cli.start(lightflow=self.manifest_a, log_id="paused_run")
    done = schema.Passport()
    done.lightflow_name = "beta_wf"
    done.lightflow_path = self.manifest_b
    for name in ("first", "gate"):
      st = done.stamps.add()
      st.stage_name = name
      st.status = schema.StampStatus.COMPLETED
      st.timestamp.GetCurrentTime()
    lib.PassportManager("done_run").save_passport(done)
    # An interrupted `start` can leave a lock with no state directory behind.
    with open(
        os.path.join(self.temp_dir, "lightflow_state_ghost.lock"), "w"
    ) as f:
      f.write("")
    with self.assertRaises(lib.LightflowStateNotFoundError):
      cli.resume(log_id="missing_run")

    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
      cli.list()
    out = buf.getvalue()
    self.assertIn(f"Runs under {self.temp_dir}", out)
    self.assertRegex(
        out,
        r"paused_run\s+alpha_wf\s+⏸️ PAUSED\s+\d+s ago\s+"
        + re.escape(os.path.abspath(self.manifest_a)),
    )
    self.assertRegex(out, r"done_run\s+beta_wf\s+✅ COMPLETED\s+\d+s ago")
    self.assertIn(
        "⚠️ orphan lock for 'ghost' (no state directory); remove with"
        " 'cleanup --log_id=ghost'",
        out,
    )
    self.assertNotIn("missing_run", out)
    # Newest first: done_run was written after paused_run.
    self.assertLess(out.index("done_run"), out.index("paused_run"))

    empty = io.StringIO()
    with contextlib.redirect_stdout(empty):
      cli.list(state_dir=os.path.join(self.temp_dir, "nope"))
    self.assertIn("No runs found", empty.getvalue())


class DryRunDiagnosticsTest(_SandboxTestCase):
  """Behaviours added after multi-tier dogfooding of the CLI."""

  def test_none_failed_joins_after_complementary_branches(self) -> None:
    manifest = self._write(
        "join.yaml",
        f"""
name: join_wf
strict_expressions: true
actions:
  - id: compute
    python_import: {__name__}.act_emit_count_42
stages:
  - name: scan
    python_action: {{"action_id": "compute"}}
  - name: big
    run_after: [scan]
    run_if: "payload.outputs.scan.count > 10"
    python_action: {{"action_id": "compute"}}
  - name: small
    run_after: [scan]
    run_if: "payload.outputs.scan.count <= 10"
    python_action: {{"action_id": "compute"}}
  - name: join_default
    run_after: [big, small]
    python_action: {{"action_id": "compute"}}
  - name: join_none_failed
    run_after: [big, small]
    trigger_rule: NONE_FAILED
    python_action: {{"action_id": "compute"}}
""",
    )
    dry = io.StringIO()
    with contextlib.redirect_stdout(dry):
      lib.LightflowRunnerCLI().dry_run(lightflow=manifest)
    out = dry.getvalue()
    # ALL_SUCCESS join is skipped and the trace says how to fix it.
    self.assertIn("set `trigger_rule: NONE_FAILED` on 'join_default'", out)
    self.assertRegex(
        out,
        r"Stage: join_none_failed\n  Trigger: NONE_FAILED\n  Action: \[RUN\]",
    )
    run = io.StringIO()
    with (
        contextlib.redirect_stdout(run),
        mock.patch.object(engine, "send_notification"),
    ):
      lib.LightflowRunnerCLI().start(lightflow=manifest, log_id="join")
    status = io.StringIO()
    with contextlib.redirect_stdout(status):
      lib.LightflowRunnerCLI().status(log_id="join")
    self.assertIn("⏭️ join_default: skipped", status.getvalue())
    self.assertIn("✅ join_none_failed: completed", status.getvalue())

  def test_none_failed_still_cascades_failures(self) -> None:
    wf = schema.Lightflow.from_dict({
        "name": "j",
        "actions": [{"id": "x", "python_import": "m.f"}],
        "stages": [
            {"name": "a", "python_action": {"action_id": "x"}},
            {"name": "b", "python_action": {"action_id": "x"}},
            {
                "name": "join",
                "run_after": ["a", "b"],
                "trigger_rule": "NONE_FAILED",
                "python_action": {"action_id": "x"},
            },
        ],
    })
    eng = engine.LightflowEngine(wf)
    eng.compile()
    pp = schema.Passport()
    for name, st in (
        ("a", schema.StampStatus.SKIPPED),
        ("b", schema.StampStatus.FAILED),
    ):
      stamp = pp.stamps.add()
      stamp.stage_name = name
      stamp.status = st
    ok, status, reason = eng.evaluate_trigger_rule(wf.stages[2], pp)
    self.assertFalse(ok)
    self.assertEqual(status, schema.StampStatus.FAILED)
    self.assertEqual(reason, engine.UPSTREAM_FAILED_MESSAGE)

  def test_dry_run_marks_would_fail_and_lists_unused_actions(self) -> None:
    manifest = self._write(
        "fail.yaml",
        f"""
name: fail_wf
strict_expressions: true
actions:
  - id: compute
    python_import: {__name__}.act_emit_count_42
  - id: never_used
    python_import: {__name__}.act_emit_count_5
stages:
  - name: scan
    python_action: {{"action_id": "compute"}}
  - name: gated
    run_after: [scan]
    run_if: "payload.outputs.scan.cuont > 1"
    python_action: {{"action_id": "compute"}}
  - name: after
    run_after: [gated]
    python_action: {{"action_id": "compute"}}
""",
    )
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
      with self.assertRaisesRegex(
          lib.LightflowRunFailedError,
          r"Dry run: 1 stage\(s\) would fail at runtime: gated\.",
      ):
        lib.LightflowRunnerCLI().dry_run(lightflow=manifest)
    out = buf.getvalue()
    self.assertIn("did you mean 'count'?", out)
    self.assertIn("Status: [WOULD FAIL] ❌", out)
    self.assertNotIn("Status: [SKIPPED]\n\nStage: after", out)
    self.assertIn("· ❌ 1 would fail", out)
    self.assertIn("❌ Would fail at runtime: gated.", out)
    self.assertIn("ℹ️ Unused action id(s) in `actions:`: never_used", out)

  def test_dry_run_seed_hint_survives_upstream_deferral(self) -> None:
    manifest = self._write(
        "seed.yaml",
        f"""
name: seed_wf
strict_expressions: true
actions:
  - id: nosim
    python_import: {__name__}.act_step_1
stages:
  - name: scan
    python_action: {{"action_id": "nosim"}}
  - name: gate
    run_after: [scan]
    operator_action:
      instructions: "'Seed ' + string(payload.seed) + ' / ' + string(payload.outputs.scan.summary)"
""",
    )
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
      lib.LightflowRunnerCLI().dry_run(lightflow=manifest)
    out = buf.getvalue()
    self.assertIn(
        "[DEFERRED TO RUNTIME (awaits outputs of: scan; --payload keys: seed)]",
        out,
    )
    self.assertIn('--payload=\'{"seed": "<value>"}\'', out)
    self.assertIn("💡 Not simulated: scan (action 'nosim')", out)

  def test_runtime_failure_on_unseeded_key_suggests_payload(self) -> None:
    manifest = self._write(
        "unseeded.yaml",
        f"""
name: unseeded_wf
strict_expressions: true
actions:
  - id: compute
    python_import: {__name__}.act_emit_count_42
stages:
  - name: scan
    python_action: {{"action_id": "compute"}}
  - name: gate
    run_after: [scan]
    operator_action:
      instructions: "'Release ' + payload.release_label"
""",
    )
    buf = io.StringIO()
    with (
        contextlib.redirect_stdout(buf),
        mock.patch.object(engine, "send_notification"),
        self.assertRaises(lib.LightflowRunFailedError),
    ):
      lib.LightflowRunnerCLI().start(lightflow=manifest, log_id="unseeded")
    out = buf.getvalue()
    self.assertIn(
        "💡 Stage(s) failed on top-level payload key(s) release_label", out
    )
    self.assertIn(
        'lightflow resume --log_id=unseeded --payload=\'{"release_label":'
        ' "<value>"}\'',
        out,
    )
    # Following the suggestion renders the gate.
    with (
        contextlib.redirect_stdout(io.StringIO()),
        mock.patch.object(engine, "send_notification"),
        self.assertRaises(engine.OperatorActionSuspended) as ctx,
    ):
      lib.LightflowRunnerCLI().resume(
          log_id="unseeded", payload='{"release_label": "v1"}'
      )
    self.assertIn("Release v1", str(ctx.exception))

  def test_notifications_default_to_milestones(self) -> None:
    sent: list[str] = []

    def fake_run(cmd, **kwargs):
      del kwargs
      sent.append(cmd[2].removeprefix("--title="))

    with (
        mock.patch.dict(
            os.environ, {"ANTIGRAVITY_CONVERSATION_ID": "conv"}, clear=False
        ),
        mock.patch.object(engine.shutil, "which", return_value="/bin/agentapi"),
        mock.patch.object(engine.subprocess, "run", side_effect=fake_run),
    ):
      os.environ.pop("LIGHTFLOW_NOTIFY", None)
      engine.send_notification("Stage Failed: b", "❌ b: failed")
      engine.wait_for_notifications(timeout=5)
      default_sent = list(sent)
      sent.clear()
      os.environ["LIGHTFLOW_NOTIFY"] = "milestones"
      engine.send_notification("Stage Entered: a", "🎬 a: entered")
      engine.send_notification("Stage Completed: a", "✅ a: completed")
      engine.send_notification("Stage Failed: b", "❌ b: failed")
      engine.send_notification("Operator Action Suspended: g", "⏸️ g")
      engine.send_notification("Lightflow completed", "done")
      engine.wait_for_notifications(timeout=5)
      milestones = list(sent)
      sent.clear()
      os.environ["LIGHTFLOW_NOTIFY"] = "all"
      engine.send_notification("Stage Entered: a", "🎬 a: entered")
      engine.wait_for_notifications(timeout=5)
      everything = list(sent)
      sent.clear()
      os.environ["LIGHTFLOW_NOTIFY"] = "off"
      engine.send_notification("Stage Failed: b", "❌ b: failed")
      engine.wait_for_notifications(timeout=5)
      nothing = list(sent)
    self.assertEqual(default_sent, [])
    self.assertEqual(
        milestones,
        [
            "Stage Failed: b",
            "Operator Action Suspended: g",
            "Lightflow completed",
        ],
    )
    self.assertEqual(everything, ["Stage Entered: a"])
    self.assertEqual(nothing, [])

  def test_verbose_status_reports_stage_durations(self) -> None:
    manifest = self._write(
        "dur.yaml",
        f"""
name: dur_wf
actions:
  - id: compute
    python_import: {__name__}.act_emit_count_42
stages:
  - name: scan
    python_action: {{"action_id": "compute"}}
""",
    )
    with (
        contextlib.redirect_stdout(io.StringIO()),
        mock.patch.object(engine, "send_notification"),
    ):
      lib.LightflowRunnerCLI().start(lightflow=manifest, log_id="dur")
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
      lib.LightflowRunnerCLI().status(log_id="dur", verbose=True)
    self.assertRegex(
        buf.getvalue(), r"Stage 'scan' -> Completed: .* \(took \d+\.\ds\)"
    )


def act_never_ready(
    payload: dict[str, Any], **static_kwargs: Any
) -> tuple[dict[str, Any], str]:
  """Polling body whose condition never becomes true."""
  del payload, static_kwargs
  return (
      {"ready": False, "progress": "still provisioning"},
      "status=RUNNING (75%).",
  )


def act_sleepy(
    payload: dict[str, Any], **static_kwargs: Any
) -> tuple[dict[str, Any], str]:
  del payload, static_kwargs
  time.sleep(1.5)
  return {}, "done"


class FailurePathsTest(_SandboxTestCase):
  """Behaviours added after the second multi-tier dogfood round."""

  silence_notifications = True

  def _latest(self, log_id: str) -> dict[str, schema.StampStatus]:
    pp = lib.PassportManager(log_id).load_passport()
    assert pp is not None
    return {s.stage_name: s.status for s in pp.stamps}

  def test_polling_yield_is_waiting_exit_5_without_collateral(self) -> None:
    manifest = self._write(
        "wait.yaml",
        f"""
name: wait_wf
actions:
  - id: never
    python_import: {__name__}.act_never_ready
  - id: compute
    python_import: {__name__}.act_emit_count_42
stages:
  - name: wait_for_it
    python_action: {{"action_id": "never"}}
    polling_policy:
      condition: "payload.outputs.wait_for_it.ready == true"
      interval_seconds: 1
      max_attempts: 2
  - name: after
    run_after: [wait_for_it]
    python_action: {{"action_id": "compute"}}
""",
    )
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
      with self.assertRaises(lib.LightflowWaitingError) as ctx:
        lib.LightflowRunnerCLI().start(lightflow=manifest, log_id="wait_run")
    out = buf.getvalue()
    self.assertTrue(getattr(ctx.exception, "already_reported", False))
    self.assertIn("⏳ Lightflow is waiting: polling stage 'wait_for_it'", out)
    # The whole tick message survives, even though it contains ").".
    self.assertIn("Last progress: status=RUNNING (75%).", out)
    self.assertIn("lightflow resume --log_id=wait_run", out)
    # Downstream is untouched: no collateral FAILED stamps.
    latest = self._latest("wait_run")
    self.assertEqual(latest["wait_for_it"], schema.StampStatus.FAILED)
    self.assertNotIn("after", latest)
    # The runner maps it to exit 5, and status reports WAITING (exit 5 too).
    self.assertEqual(lib.EXIT_WAITING, 5)
    status_buf = io.StringIO()
    with contextlib.redirect_stdout(status_buf):
      with self.assertRaises(lib.LightflowWaitingError):
        lib.LightflowRunnerCLI().status(log_id="wait_run")
    self.assertIn(
        "⏳ wait_for_it: waiting (polling exhausted max attempts; last"
        " progress: status=RUNNING (75%).)",
        status_buf.getvalue(),
    )
    self.assertIn("⬜ after: not started", status_buf.getvalue())

  def test_rejected_gate_footer_and_one_call_reopen(self) -> None:
    manifest = self._write(
        "reject.yaml",
        f"""
name: reject_wf
actions:
  - id: compute
    python_import: {__name__}.act_emit_count_42
stages:
  - name: gate
    operator_action:
      instructions: "'Ship it?'"
      json_schema: '{{"type": "object", "required": ["approved"]}}'
  - name: ship
    run_after: [gate]
    python_action: {{"action_id": "compute"}}
""",
    )
    cli = lib.LightflowRunnerCLI()
    with self.assertRaises(engine.OperatorActionSuspended):
      cli.start(lightflow=manifest, log_id="rej_run")
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
      with self.assertRaisesRegex(
          lib.LightflowRunFailedError, r"🛑 Lightflow stopped: operator gate"
      ):
        cli.resume(
            log_id="rej_run",
            stage="gate",
            resolution="REJECT",
            comment="not today",
        )
    out = buf.getvalue()
    self.assertIn("Nothing to fix. To re-open the gate", out)
    self.assertIn(
        "lightflow resume --log_id=rej_run --stage=gate --resolution=APPROVE",
        out,
    )
    self.assertNotIn("Root failure(s)", out)
    # status: 🛑 rejected, not ❌ failed.
    sbuf = io.StringIO()
    with contextlib.redirect_stdout(sbuf):
      with self.assertRaises(lib.LightflowRunFailedError) as sctx:
        cli.status(log_id="rej_run")
    self.assertIn("🛑 gate: rejected", sbuf.getvalue())
    self.assertIn("⛔ ship: blocked", sbuf.getvalue())
    self.assertIn(
        "--stage=gate --resolution=APPROVE|REJECT", str(sctx.exception)
    )

    # One call: re-open the rejected gate and approve it; stale comment gone.
    cli.resume(log_id="rej_run", stage="gate", resolution="APPROVE")
    latest = self._latest("rej_run")
    self.assertEqual(latest["gate"], schema.StampStatus.COMPLETED)
    self.assertEqual(latest["ship"], schema.StampStatus.COMPLETED)
    pp = lib.PassportManager("rej_run").load_passport()
    assert pp is not None
    gate_out = pp.payload.get("outputs", {}).get("gate", {})
    self.assertEqual(gate_out.get("approved"), True)
    self.assertNotIn("rejection_comment", gate_out)

  def test_bare_resume_on_rejected_gate_only_reopens(self) -> None:
    manifest = self._write(
        "reject2.yaml",
        """
name: reject_wf
stages:
  - name: gate
    operator_action:
      instructions: "'Ship it?'"
""",
    )
    cli = lib.LightflowRunnerCLI()
    with self.assertRaises(engine.OperatorActionSuspended):
      cli.start(lightflow=manifest, log_id="rej2")
    with self.assertRaises(lib.LightflowRunFailedError):
      cli.resume(log_id="rej2", stage="gate", resolution="REJECT")
    # No --resolution/--payload/--comment: re-open and pause, don't decide.
    with self.assertRaises(engine.OperatorActionSuspended):
      cli.resume(log_id="rej2", stage="gate")
    self.assertEqual(self._latest("rej2")["gate"], schema.StampStatus.PAUSED)

  def test_second_start_while_locked_is_running_error(self) -> None:
    manifest = self._write(
        "lock.yaml",
        f"""
name: lock_wf
actions:
  - id: compute
    python_import: {__name__}.act_emit_count_42
stages:
  - name: a
    python_action: {{"action_id": "compute"}}
""",
    )
    pm = lib.PassportManager("lock_run")
    with pm.lock():
      with self.assertRaises(lib.LightflowRunningError):
        lib.LightflowRunnerCLI().start(lightflow=manifest, log_id="lock_run")
    # Lock released: the run proceeds normally afterwards.
    lib.LightflowRunnerCLI().start(lightflow=manifest, log_id="lock_run")
    self.assertEqual(
        self._latest("lock_run")["a"], schema.StampStatus.COMPLETED
    )

  def test_start_with_broken_manifest_leaves_no_orphan_lock(self) -> None:
    broken = self._write(
        "broken.yaml",
        "name: broken\nstages:\n  - name: x\n    run_after: [nope]\n",
    )
    with self.assertRaisesRegex(engine.EngineError, "Compilation Error"):
      lib.LightflowRunnerCLI().start(lightflow=broken, log_id="orphan_run")
    self.assertFalse(
        os.path.isdir(os.path.join(self.temp_dir, "lightflow_state_orphan_run"))
    )
    self.assertFalse(
        os.path.exists(
            os.path.join(self.temp_dir, "lightflow_state_orphan_run.lock")
        )
    )

  def test_dry_run_prints_policies_and_json_payload(self) -> None:
    manifest = self._write(
        "policies.yaml",
        f"""
name: policy_wf
actions:
  - id: compute
    python_import: {__name__}.act_emit_count_42
  - id: never
    python_import: {__name__}.act_never_ready
stages:
  - name: flaky
    python_action: {{"action_id": "compute"}}
    retry_policy: {{"max_attempts": 4, "initial_backoff_seconds": 2}}
    rollback_action: {{"action_id": "compute"}}
  - name: wait
    run_after: [flaky]
    python_action: {{"action_id": "never"}}
    polling_policy:
      condition: "payload.outputs.wait.ready == true"
      interval_seconds: 7
      timeout_seconds: 60
  - name: done
    run_after: [flaky, wait]
    trigger_rule: ALL_DONE
    python_action: {{"action_id": "compute"}}
""",
    )
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
      lib.LightflowRunnerCLI().dry_run(
          lightflow=manifest, payload={"env": "prod"}
      )
    out = buf.getvalue()
    self.assertIn('Initial Payload: {"env": "prod"}', out)
    self.assertIn("Retry: up to 4 attempts, backoff 2s ×2.0", out)
    self.assertIn("Rollback: 'compute' runs if this stage fails", out)
    self.assertIn("Poll: every 7s, timeout 60s (yields with exit 5", out)
    self.assertIn("Trigger: ALL_DONE", out)

  def test_fractional_backoff_is_rejected_with_raw_value(self) -> None:
    with self.assertRaisesRegex(
        ValueError,
        r"RetryPolicy\.initial_backoff_seconds must be a whole"
        r" number.*got 0\.2",
    ):
      schema.RetryPolicy.from_dict({"initial_backoff_seconds": 0.2})
    # Integral floats and numeric strings are tolerated.
    self.assertEqual(
        schema.RetryPolicy.from_dict({"max_attempts": 3.0}).max_attempts, 3
    )
    self.assertEqual(
        schema.PollingPolicy.from_dict(
            {"condition": "true", "interval_seconds": "15"}
        ).interval_seconds,
        15,
    )
    with self.assertRaisesRegex(ValueError, r"PollingPolicy\.timeout_seconds"):
      schema.PollingPolicy.from_dict(
          {"condition": "true", "timeout_seconds": 1.5}
      )

  def test_timeout_message_warns_about_lingering_thread(self) -> None:
    wf = schema.Lightflow.from_dict({
        "name": "slow_wf",
        "actions": [{"id": "slow", "python_import": f"{__name__}.act_sleepy"}],
        "stages": [{
            "name": "slow",
            "python_action": {"action_id": "slow"},
            "timeout_seconds": 1,
        }],
    })
    eng = engine.LightflowEngine(wf)
    pp = schema.Passport()
    with self.assertRaises(engine.StageTimeoutError):
      eng.execute_stage("slow", pp)
    msg = pp.stamps[-1].message
    self.assertEqual(pp.stamps[-1].status, schema.StampStatus.FAILED)
    self.assertIn("exceeded timeout_seconds=1", msg)
    self.assertIn("worker thread keeps running", msg)
    self.assertNotIn("Engine error:", msg)


def act_cleanup_marker(
    payload: dict[str, Any], **static_kwargs: Any
) -> tuple[dict[str, Any], str]:
  del static_kwargs
  n = int(payload.get("outputs", {}).get("cleanup", {}).get("runs", 0)) + 1
  return {"runs": n}, f"cleanup run {n}"


def act_break_then_fail_rollback(
    payload: dict[str, Any], **static_kwargs: Any
) -> tuple[dict[str, Any], str]:
  del payload, static_kwargs
  raise RuntimeError("promotion API returned 503")


def act_failing_rollback(
    payload: dict[str, Any], **static_kwargs: Any
) -> tuple[dict[str, Any], str]:
  del payload, static_kwargs
  raise RuntimeError("lock held by another deployer")


class DayTwoOperationsTest(_SandboxTestCase):
  """Day-two behaviours: moved manifests, --force, re-opened gates."""

  state_subdir = "state"
  silence_notifications = True

  def _gate_wf(self, rel: str = "work/lf.yaml") -> str:
    return self._write(
        rel,
        f"""
name: day2_wf
strict_expressions: true
actions:
  - id: compute
    python_import: {__name__}.act_emit_count_42
  - id: cleanup
    python_import: {__name__}.act_cleanup_marker
stages:
  - name: prep
    python_action: {{"action_id": "compute"}}
  - name: gate
    run_after: [prep]
    operator_action:
      instructions: "'Promote?'"
      json_schema: '{{"type": "object", "required": ["approved", "ticket"], "properties": {{"ticket": {{"type": "string"}}}}}}'
  - name: promote
    run_after: [gate]
    run_if: "payload.outputs.gate.approved == true"
    python_action: {{"action_id": "compute"}}
  - name: cleanup
    run_after: [promote]
    trigger_rule: ALL_DONE
    python_action: {{"action_id": "cleanup"}}
""",
    )

  def _stamps(self, log_id: str) -> list[tuple[str, str]]:
    pp = lib.PassportManager(log_id).load_passport()
    assert pp is not None
    return [
        (s.stage_name, schema.StampStatus.Name(s.status)) for s in pp.stamps
    ]

  def test_resume_command_is_cwd_independent_and_pasteable(self) -> None:
    manifest = self._gate_wf()
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
      with self.assertRaises(engine.OperatorActionSuspended) as ctx:
        lib.LightflowRunnerCLI().start(lightflow=manifest, log_id="r1")
    cmd = "lightflow resume --log_id=r1 --stage=gate --resolution=APPROVE"
    self.assertIn(
        cmd + ' --payload=\'{"ticket": "<string>"}\'', str(ctx.exception)
    )
    self.assertNotIn("--lightflow", str(ctx.exception).split("Resume with:")[1])
    # `status` from any cwd reprints the same command.
    sbuf = io.StringIO()
    with contextlib.redirect_stdout(sbuf):
      with self.assertRaises(lib.LightflowAlreadyPausedError):
        lib.LightflowRunnerCLI().status(log_id="r1")
    self.assertIn("Resume with: " + cmd, sbuf.getvalue())

  def test_explicit_lightflow_on_moved_manifest_is_re_recorded(self) -> None:
    manifest = self._gate_wf()
    cli = lib.LightflowRunnerCLI()
    with self.assertRaises(engine.OperatorActionSuspended):
      cli.start(lightflow=manifest, log_id="mv")
    new_dir = os.path.join(self.temp_dir, "reorg")
    os.makedirs(new_dir)
    new_path = os.path.join(new_dir, "lf.yaml")
    shutil.move(manifest, new_path)
    # Without the flag: a clear pointer, not an errno.
    with self.assertRaisesRegex(ValueError, "no longer exists at"):
      cli.status(log_id="mv")
    # A wrong explicit path says so and does not crash with [Errno 2].
    with self.assertRaisesRegex(ValueError, r"No such manifest: 'nope\.yaml'"):
      cli.status(lightflow="nope.yaml", log_id="mv")
    # Passing the new path once re-records it ...
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
      with self.assertRaises(lib.LightflowRunFailedError):
        cli.resume(
            lightflow=new_path, log_id="mv", stage="gate", resolution="REJECT"
        )
    self.assertIn("ℹ️ Manifest path updated for 'mv'", buf.getvalue())
    pp = lib.PassportManager("mv").load_passport()
    assert pp is not None
    self.assertEqual(pp.lightflow_path, os.path.abspath(new_path))
    # ... so the printed re-open hint works verbatim without --lightflow.
    cli.resume(
        log_id="mv",
        stage="gate",
        resolution="APPROVE",
        payload={"ticket": "CHG-1"},
    )
    latest = dict(self._stamps("mv"))
    self.assertEqual(latest["promote"], "COMPLETED")

  def test_explicit_lightflow_of_other_workflow_is_not_adopted(self) -> None:
    manifest = self._gate_wf()
    cli = lib.LightflowRunnerCLI()
    with self.assertRaises(engine.OperatorActionSuspended):
      cli.start(lightflow=manifest, log_id="keep")
    other = self._write(
        "other/lf.yaml",
        "name: other_wf\nactions:\n  - id: c\n    python_import:"
        f" {__name__}.act_emit_count_42\nstages:\n  - name: prep\n   "
        ' python_action: {"action_id": "c"}\n  - name: gate\n    run_after:'
        ' [prep]\n    operator_action: {"instructions": "\'x\'"}\n',
    )
    # Neither `status` nor `resume` will drive this run with another
    # lightflow's manifest, and the recorded path is left alone.
    with contextlib.redirect_stdout(io.StringIO()):
      with self.assertRaisesRegex(ValueError, "already holds state for"):
        cli.status(lightflow=other, log_id="keep")
      with self.assertRaisesRegex(ValueError, "already holds state for"):
        cli.resume(lightflow=other, log_id="keep")
    pp = lib.PassportManager("keep").load_passport()
    assert pp is not None
    self.assertEqual(pp.lightflow_path, os.path.abspath(manifest))
    self.assertEqual(
        [s.stage_name for s in pp.stamps if s.stage_name == "prep"][:1],
        ["prep"],
    )

  def test_all_done_cleanup_reruns_after_gate_reopen(self) -> None:
    manifest = self._gate_wf()
    cli = lib.LightflowRunnerCLI()
    with self.assertRaises(engine.OperatorActionSuspended):
      cli.start(lightflow=manifest, log_id="ad")
    with contextlib.redirect_stdout(io.StringIO()):
      with self.assertRaises(lib.LightflowRunFailedError):
        cli.resume(log_id="ad", stage="gate", resolution="REJECT")
    # cleanup ran on the rejection path.
    self.assertIn(("cleanup", "COMPLETED"), self._stamps("ad"))
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
      cli.resume(
          log_id="ad",
          stage="gate",
          resolution="APPROVE",
          payload={"ticket": "CHG-2"},
      )
    self.assertIn("↻ cleanup re-armed", buf.getvalue())
    stamps = self._stamps("ad")
    # cleanup ran again, and after promote this time.
    cleanup_runs = [
        i for i, s in enumerate(stamps) if s == ("cleanup", "COMPLETED")
    ]
    promote_done = stamps.index(("promote", "COMPLETED"))
    self.assertEqual(len(cleanup_runs), 2)
    self.assertGreater(cleanup_runs[-1], promote_done)
    pp = lib.PassportManager("ad").load_passport()
    assert pp is not None
    self.assertEqual(pp.payload["outputs"]["cleanup"]["runs"], 2)

  def test_all_done_cleanup_reruns_after_failed_stage_is_rearmed(self) -> None:
    global _STAGE_2_SHOULD_FAIL
    manifest = self._write(
        "work/fail.yaml",
        f"""
name: fail_wf
actions:
  - id: flaky
    python_import: {__name__}.act_step_2_repairable
  - id: cleanup
    python_import: {__name__}.act_cleanup_marker
stages:
  - name: work
    python_action: {{"action_id": "flaky"}}
  - name: cleanup
    run_after: [work]
    trigger_rule: ALL_DONE
    python_action: {{"action_id": "cleanup"}}
""",
    )
    cli = lib.LightflowRunnerCLI()
    _STAGE_2_SHOULD_FAIL = True
    with contextlib.redirect_stdout(io.StringIO()):
      with self.assertRaises(lib.LightflowRunFailedError):
        cli.start(lightflow=manifest, log_id="fa")
    _STAGE_2_SHOULD_FAIL = False
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
      cli.resume(log_id="fa")
    self.assertIn("↻ cleanup re-armed", buf.getvalue())
    pp = lib.PassportManager("fa").load_passport()
    assert pp is not None
    self.assertEqual(pp.payload["outputs"]["cleanup"]["runs"], 2)

  def test_force_prints_what_it_discards(self) -> None:
    manifest = self._gate_wf()
    cli = lib.LightflowRunnerCLI()
    with self.assertRaises(engine.OperatorActionSuspended):
      cli.start(lightflow=manifest, log_id="fo")
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
      with self.assertRaises(engine.OperatorActionSuspended):
        cli.start(lightflow=manifest, log_id="fo", force=True)
    out = buf.getvalue()
    self.assertIn("⚠️ --force discarded the previous state of 'fo'", out)
    self.assertIn("1 completed stage(s) (prep)", out)
    self.assertIn("paused gate 'gate'", out)
    self.assertIn("`resume` would have kept", out)

  def test_rollback_failure_is_called_out_in_footer(self) -> None:
    manifest = self._write(
        "work/rb.yaml",
        f"""
name: rb_wf
actions:
  - id: boom
    python_import: {__name__}.act_break_then_fail_rollback
  - id: rb
    python_import: {__name__}.act_failing_rollback
stages:
  - name: promote
    python_action: {{"action_id": "boom"}}
    rollback_action: {{"action_id": "rb"}}
""",
    )
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
      with self.assertRaises(lib.LightflowRunFailedError):
        lib.LightflowRunnerCLI().start(lightflow=manifest, log_id="rb")
    out = buf.getvalue()
    self.assertIn("- promote: Failed after 1 attempt.", out)
    self.assertIn(
        "⚠️ rollback 'rb' ALSO FAILED: lock held by another deployer", out
    )
    self.assertIn("verify by hand before resuming", out)

  def test_status_not_found_names_the_state_dir(self) -> None:
    os.environ["LIGHTFLOW_STATE_DIR"] = os.path.join(self.temp_dir, "nope")
    with self.assertRaisesRegex(
        lib.LightflowStateNotFoundError,
        r"No execution state found for log ID 'zz' under .*nope"
        r" \(that directory does not exist\)",
    ):
      lib.LightflowRunnerCLI().status(log_id="zz")

  def test_dry_run_may_seed_outputs_but_start_may_not(self) -> None:
    manifest = self._gate_wf()
    # A dry run may pretend a gate was approved to trace the branch behind it.
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
      lib.LightflowRunnerCLI().dry_run(
          lightflow=manifest,
          payload={"outputs": {"gate": {"approved": True, "ticket": "T"}}},
      )
    self.assertRegex(buf.getvalue(), r"Stage: promote\n  Condition .* -> True")
    # `start` seeds real state, where `outputs` is the engine's namespace.
    with contextlib.redirect_stdout(io.StringIO()):
      with self.assertRaisesRegex(ValueError, "reserved key 'outputs'"):
        lib.LightflowRunnerCLI().start(
            lightflow=manifest, log_id="res", payload={"outputs": {}}
        )
    # And the rejected `start` left no orphan lock behind.
    self.assertFalse(
        os.path.exists(
            os.path.join(self.temp_dir, "state", "lightflow_state_res.lock")
        )
    )

  def test_did_you_mean_for_stage_and_action_typos(self) -> None:
    bad_dep = self._write(
        "work/dep.yaml",
        "name: w\nactions:\n  - id: c\n    python_import:"
        f" {__name__}.act_emit_count_42\nstages:\n  - name: fast_track\n   "
        ' python_action: {"action_id": "c"}\n  - name: join\n    run_after:'
        ' [fast_trak]\n    python_action: {"action_id": "c"}\n',
    )
    with self.assertRaisesRegex(
        engine.EngineError,
        r"non-existent stage 'fast_trak'\. Did you mean 'fast_track'\?",
    ):
      lib.LightflowRunnerCLI().dry_run(lightflow=bad_dep)
    bad_act = self._write(
        "work/act.yaml",
        "name: w\nactions:\n  - id: promote\n    python_import:"
        f" {__name__}.act_emit_count_42\nstages:\n  - name: s\n   "
        ' python_action: {"action_id": "promot"}\n',
    )
    with self.assertRaisesRegex(
        engine.EngineError,
        r"unregistered action ID 'promot'\. Did you mean 'promote'\?",
    ):
      lib.LightflowRunnerCLI().dry_run(lightflow=bad_act)
    manifest = self._gate_wf()
    with self.assertRaises(engine.OperatorActionSuspended):
      lib.LightflowRunnerCLI().start(lightflow=manifest, log_id="dym")
    with self.assertRaisesRegex(
        ValueError, r"Unknown stage 'gat'\. Did you mean 'gate'\?"
    ):
      lib.LightflowRunnerCLI().resume(log_id="dym", stage="gat")

  def test_python_import_error_explains_manifest_relative_resolution(
      self,
  ) -> None:
    manifest = self._write(
        "work/imp.yaml",
        "name: w\nactions:\n  - id: c\n    python_import: no_such_mod_xyz.fn\n"
        'stages:\n  - name: s\n    python_action: {"action_id": "c"}\n',
    )
    with contextlib.redirect_stdout(io.StringIO()):
      with self.assertRaises(lib.LightflowRunFailedError):
        lib.LightflowRunnerCLI().start(lightflow=manifest, log_id="imp")
    pp = lib.PassportManager("imp").load_passport()
    assert pp is not None
    msg = pp.stamps[-1].message
    self.assertIn("Cannot import 'no_such_mod_xyz.fn' for action 'c'", msg)
    self.assertIn("relative to the manifest's directory", msg)
    self.assertIn(os.path.dirname(manifest), msg)

  def test_list_marks_missing_manifest(self) -> None:
    manifest = self._gate_wf()
    with self.assertRaises(engine.OperatorActionSuspended):
      lib.LightflowRunnerCLI().start(lightflow=manifest, log_id="ls")
    os.remove(manifest)
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
      lib.LightflowRunnerCLI().list()
    self.assertIn(
        "(missing — pass --lightflow=<new path> once to re-record)",
        buf.getvalue(),
    )


class RunStateContractTest(_SandboxTestCase):
  """Behaviours pinned down by the CL review: yields, re-opens, list, exits."""

  state_subdir = "state"
  silence_notifications = True

  def _latest(self, log_id: str) -> dict[str, str]:
    pp = lib.PassportManager(log_id).load_passport()
    assert pp is not None
    out: dict[str, str] = {}
    for st in pp.stamps:
      out[st.stage_name] = lib.effective_status_name(st)
    return out

  def _yield_and_gate_wf(self) -> str:
    # Two independent branches: a poll that yields, and a gate.
    return self._write(
        "work/par.yaml",
        f"""
name: par_wf
actions:
  - id: never
    python_import: {__name__}.act_never_ready
  - id: compute
    python_import: {__name__}.act_emit_count_42
stages:
  - name: wait_for_it
    python_action: {{"action_id": "never"}}
    rollback_action: {{"action_id": "compute"}}
    polling_policy:
      condition: "payload.outputs.wait_for_it.ready == true"
      interval_seconds: 1
      max_attempts: 1
  - name: after_wait
    run_after: [wait_for_it]
    python_action: {{"action_id": "compute"}}
  - name: side_branch
    python_action: {{"action_id": "compute"}}
  - name: gate
    run_after: [side_branch]
    operator_action:
      instructions: "'Go?'"
  - name: after_gate
    run_after: [gate]
    python_action: {{"action_id": "compute"}}
""",
    )

  def test_yield_parks_its_branch_but_independent_branches_continue(self):
    manifest = self._yield_and_gate_wf()
    cli = lib.LightflowRunnerCLI()
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
      # The poll yields first; the side branch still runs up to its gate.
      with self.assertRaises(engine.OperatorActionSuspended):
        cli.start(lightflow=manifest, log_id="par")
    latest = self._latest("par")
    self.assertEqual(latest["wait_for_it"], "WAITING")
    self.assertNotIn("after_wait", latest)  # parked, not blocked
    self.assertEqual(latest["side_branch"], "COMPLETED")
    self.assertEqual(latest["gate"], "PAUSED")
    # No rollback on a yield: the external job is still running.
    self.assertNotIn("Executing rollback action", buf.getvalue())
    pp = lib.PassportManager("par").load_passport()
    assert pp is not None
    self.assertNotIn("Rollback executed", pp.stamps[-1].message)

  def test_answering_gate_rearms_yielded_poll_without_collateral(self):
    manifest = self._yield_and_gate_wf()
    cli = lib.LightflowRunnerCLI()
    with contextlib.redirect_stdout(io.StringIO()):
      with self.assertRaises(engine.OperatorActionSuspended):
        cli.start(lightflow=manifest, log_id="par2")
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
      with self.assertRaises(lib.LightflowWaitingError):
        cli.resume(log_id="par2", stage="gate", resolution="APPROVE")
    out = buf.getvalue()
    self.assertIn("↻ wait_for_it re-armed: polling again.", out)
    latest = self._latest("par2")
    self.assertEqual(latest["after_gate"], "COMPLETED")
    self.assertEqual(latest["wait_for_it"], "WAITING")  # polled again, yielded
    self.assertNotIn("after_wait", latest)  # still no collateral stamp

  def test_list_distinguishes_waiting_and_rejected_from_failed(self):
    manifest = self._yield_and_gate_wf()
    cli = lib.LightflowRunnerCLI()
    with contextlib.redirect_stdout(io.StringIO()):
      with self.assertRaises(engine.OperatorActionSuspended):
        cli.start(lightflow=manifest, log_id="lw")
      with self.assertRaises(lib.LightflowRunFailedError):
        cli.resume(log_id="lw", stage="gate", resolution="REJECT")
    gate_only = self._write(
        "work/g.yaml",
        "name: g_wf\nstages:\n  - name: gate\n    operator_action:"
        ' {"instructions": "\'Go?\'"}\n',
    )
    with contextlib.redirect_stdout(io.StringIO()):
      with self.assertRaises(engine.OperatorActionSuspended):
        cli.start(lightflow=gate_only, log_id="lr")
      with self.assertRaises(lib.LightflowRunFailedError):
        cli.resume(log_id="lr", stage="gate", resolution="REJECT")
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
      cli.list()
    out = buf.getvalue()
    self.assertRegex(out, r"lr\s+g_wf\s+🛑 REJECTED")
    # A rejected gate outranks the parked poll in the coarse state.
    self.assertRegex(out, r"lw\s+par_wf\s+🛑 REJECTED")
    self.assertNotIn("❌ FAILED", out)

  def test_reopen_drops_top_level_keys_the_gate_mirrored(self):
    manifest = self._write(
        "work/keys.yaml",
        f"""
name: keys_wf
actions:
  - id: compute
    python_import: {__name__}.act_emit_count_42
stages:
  - name: gate
    operator_action:
      instructions: "'Ticket?'"
  - name: ship
    run_after: [gate]
    python_action: {{"action_id": "compute"}}
""",
    )
    cli = lib.LightflowRunnerCLI()
    with contextlib.redirect_stdout(io.StringIO()):
      with self.assertRaises(engine.OperatorActionSuspended):
        cli.start(lightflow=manifest, log_id="keys")
      with self.assertRaises(lib.LightflowRunFailedError):
        cli.resume(
            log_id="keys",
            stage="gate",
            resolution="REJECT",
            payload={"ticket": "OLD-1", "reviewer": "alice"},
        )
      cli.resume(
          log_id="keys",
          stage="gate",
          resolution="APPROVE",
          payload={"ticket": "NEW-2"},
      )
    pp = lib.PassportManager("keys").load_passport()
    assert pp is not None
    payload = pp.payload.to_dict()
    self.assertEqual(payload["ticket"], "NEW-2")
    self.assertNotIn("reviewer", payload)  # mirrored by the rejection only
    self.assertEqual(payload["outputs"]["gate"].get("ticket"), "NEW-2")
    self.assertNotIn("reviewer", payload["outputs"]["gate"])

  def test_resume_unknown_run_is_not_found_and_cleanup_lock_is_running(self):
    with self.assertRaises(lib.LightflowStateNotFoundError):
      lib.LightflowRunnerCLI().resume(log_id="ghost")
    pm = lib.PassportManager("busy")
    pm.save_passport(schema.Passport())
    with pm.lock():
      with self.assertRaises(lib.LightflowRunningError):
        lib.LightflowRunnerCLI().cleanup(log_id="busy")

  def test_dry_run_retry_line_matches_engine_defaults(self):
    manifest = self._write(
        "work/retry.yaml",
        f"""
name: retry_wf
actions:
  - id: compute
    python_import: {__name__}.act_emit_count_42
stages:
  - name: flaky
    python_action: {{"action_id": "compute"}}
    retry_policy: {{"initial_backoff_seconds": 2}}
""",
    )
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
      lib.LightflowRunnerCLI().dry_run(lightflow=manifest)
    self.assertIn("Retry: up to 1 attempts, backoff 2s ×2.0", buf.getvalue())
    self.assertEqual(engine.effective_retry_policy(None), (1, 5, 2.0))

  def test_missing_key_hint_handles_subscript_syntax(self):
    msgs = {
        "gate": (
            "no such key/index 'release' (strict) (in expression"
            " 'payload['release'] == 1')"
        )
    }
    self.assertEqual(
        lib._missing_top_level_keys_from_failures(msgs, ["gate"]),  # pylint: disable=protected-access
        ["release"],
    )

  def test_start_with_missing_manifest_path_is_a_clear_error(self):
    with self.assertRaisesRegex(ValueError, r"No such manifest: 'nope\.yaml'"):
      lib.LightflowRunnerCLI().start(lightflow="nope.yaml", log_id="x")
    with self.assertRaisesRegex(ValueError, r"No such manifest: 'nope\.yaml'"):
      lib.LightflowRunnerCLI().dry_run(lightflow="nope.yaml")

  def test_visualizer_defines_every_utility_class_it_uses(self):
    # github.com/google/lightflow/issues/19: visualizer.html ships its own
    # utility CSS subset; a class the markup uses but the subset lacks fails
    # silently (invisible status dots, no stage rings). Keep them in sync.
    html = visualizer._load_template()  # pylint: disable=protected-access
    style_match = re.search(r"<style>(.*?)</style>", html, re.S)
    assert style_match is not None
    defined = {
        m.group(1).replace("\\", "")
        for m in re.finditer(
            r"\.([A-Za-z](?:\\.|[^\s,{:>.])*)", style_match.group(1)
        )
    }
    # Every place a class name is spelled: class attributes (HTML and
    # template literals), className assignments, classList calls, and the
    # class-string variables the renderer composes (`dotColor = "..."`).
    sites = (
        r'class(?:Name)?\s*=\s*"([^"]*)"',
        r"classList\.(?:add|remove|toggle)\(([^)]*)\)",
        r'\w*(?:Color|Border|Ring|Badge|Bg)\s*=\s*"([^"]*)"',
    )
    used = set()
    for pattern in sites:
      for m in re.finditer(pattern, html):
        literal = re.sub(r"\$\{[^}]*\}", " ", m.group(1))
        used.update(re.findall(r"[A-Za-z][\w:\[\]()/.,#%-]*", literal))
    # JS hooks (no styling), substituted placeholders, and gradient stops that
    # are baked into .bg-gradient-to-tr.
    hooks = re.compile(
        r"^(?:tab|view|inspect|inspector|dag|wf|timeline|payload|raw|search"
        r"|toggle|type|zoom|edges|stage|orient|arrow|auto|marker|custom)[-\w]*$"
    )
    skip = {"from-sky-500", "to-indigo-500", "lightflow"}
    missing = sorted(
        t
        for t in used
        if t not in defined
        and t not in skip
        and not hooks.match(t)
        and "__" not in t
    )
    self.assertEqual(
        missing, [], f"visualizer.html uses undefined CSS: {missing}"
    )

  def test_visualize_infers_manifest_from_log_id(self):
    manifest = self._write(
        "work/viz.yaml",
        f"""
name: viz_wf
actions:
  - id: compute
    python_import: {__name__}.act_emit_count_42
stages:
  - name: scan
    python_action: {{"action_id": "compute"}}
""",
    )
    cli = lib.LightflowRunnerCLI()
    cli.start(lightflow=manifest, log_id="viz_run")
    html = cli.visualize(log_id="viz_run")
    self.assertIn("viz_wf", html)
    with self.assertRaises(lib.LightflowStateNotFoundError):
      cli.visualize(log_id="no_such_run")

  def test_list_accepts_explicit_state_dir_outside_env_sandbox(self):
    manifest = self._write(
        "work/list_ext.yaml",
        f"""
name: list_ext_wf
actions:
  - id: compute
    python_import: {__name__}.act_emit_count_42
stages:
  - name: scan
    python_action: {{"action_id": "compute"}}
""",
    )
    parent = os.environ.get("TEST_TMPDIR") or os.path.expanduser("~")
    with tempfile.TemporaryDirectory(
        dir=parent, prefix=".lf_test_state_"
    ) as custom_dir:
      with mock.patch.dict(
          os.environ, {"LIGHTFLOW_STATE_DIR": custom_dir}, clear=False
      ):
        lib.LightflowRunnerCLI().start(lightflow=manifest, log_id="ext_run")
      with mock.patch.dict(os.environ, {}, clear=False):
        os.environ.pop("LIGHTFLOW_STATE_DIR", None)
        runs, _, _ = lib.collect_runs(state_dir=custom_dir)
    self.assertEqual(len(runs), 1)
    self.assertEqual(runs[0].log_id, "ext_run")
    self.assertEqual(runs[0].state, "COMPLETED")


if __name__ == "__main__":
  unittest.main()
