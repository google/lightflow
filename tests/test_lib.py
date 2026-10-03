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
import os
import shutil
import sys
import tempfile
import threading
import time
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
  from ..lightflow import schema
except (ImportError, ValueError):
  from lightflow import engine  # pyrefly: ignore[missing-import]
  from lightflow import lib  # pyrefly: ignore[missing-import]
  from lightflow import schema  # pyrefly: ignore[missing-import]


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


_VERIFY_ATTEMPT = 0


def act_verify_adjustable(
    payload: dict[str, Any], **static_kwargs: Any
) -> tuple[dict[str, Any], str]:
  del payload, static_kwargs
  global _VERIFY_ATTEMPT
  _VERIFY_ATTEMPT += 1
  _EXEC_TRACE.append(f"verify_{_VERIFY_ATTEMPT}")
  if _VERIFY_ATTEMPT == 1:
    return {
        "verdict": "FAIL",
        "stale_detail": "missing_entry",
    }, "Verification attempt 1"
  return {"verdict": "PASS"}, "Verification attempt 2"


class PassportAndLightflowRunnerTest(unittest.TestCase):
  """Tests state management, operator gates, subgraph re-arming, and HTML output."""

  def setUp(self) -> None:
    super().setUp()
    os.environ.pop("ANTIGRAVITY_CONVERSATION_ID", None)
    _EXEC_TRACE.clear()
    global _STAGE_2_SHOULD_FAIL, _VERIFY_ATTEMPT
    _STAGE_2_SHOULD_FAIL = True
    _VERIFY_ATTEMPT = 0
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
    # First run fails at s2 (s1 completes, s3 fails as collateral)
    with self.assertRaises(engine.EngineError):
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

  def test_rerun_completed_stage_cascades_and_replaces_stage_outputs(
      self,
  ) -> None:
    module_name = __name__
    manifest_path = os.path.join(self.temp_dir, "rerun.yaml")
    with open(manifest_path, "w", encoding="utf-8") as f:
      f.write(f"""
name: rerun_pipeline
actions:
  - id: step_1
    python_import: {module_name}.act_step_1
  - id: verify
    python_import: {module_name}.act_verify_adjustable
  - id: step_3
    python_import: {module_name}.act_step_3
stages:
  - name: s1
    python_action:
      action_id: step_1
  - name: verify_spec
    run_after: [s1]
    python_action:
      action_id: verify
  - name: review_gate
    run_after: [verify_spec]
    operator_action:
      instructions: "'Verdict: ' + payload.outputs.verify_spec.verdict"
  - name: s3
    run_after: [review_gate]
    python_action:
      action_id: step_3
""")

    cli = lib.LightflowRunnerCLI()
    # 1. Initial start runs s1 + verify_spec (verdict=FAIL,
    # stale_detail=missing_entry) and pauses at review_gate.
    with self.assertRaises(engine.OperatorActionSuspended) as ctx:
      cli.start(lightflow=manifest_path, log_id="rerun_run")
    self.assertIn("Verdict: FAIL", str(ctx.exception))
    self.assertEqual(_EXEC_TRACE, ["step_1", "verify_1"])

    pm = lib.PassportManager("rerun_run")
    passport = pm.load_passport()
    assert passport is not None
    self.assertEqual(
        dict(passport.payload)["outputs"]["verify_spec"],
        {"verdict": "FAIL", "stale_detail": "missing_entry"},
    )

    # 2. Re-run `verify_spec` while paused at downstream `review_gate` using
    # `--rerun=verify_spec`. `s1` stays COMPLETED, `verify_spec` re-runs and
    # replaces `payload.outputs.verify_spec` (dropping `stale_detail`), and
    # `review_gate` re-suspends with updated instructions ("Verdict: PASS").
    with self.assertRaises(engine.OperatorActionSuspended) as ctx2:
      cli.resume(
          lightflow=manifest_path,
          log_id="rerun_run",
          rerun="verify_spec",
      )
    self.assertIn("Verdict: PASS", str(ctx2.exception))
    self.assertEqual(_EXEC_TRACE, ["step_1", "verify_1", "verify_2"])

    passport = pm.load_passport()
    assert passport is not None
    self.assertEqual(
        dict(passport.payload)["outputs"]["verify_spec"],
        {"verdict": "PASS"},
    )

    # 2a. Re-running `verify_spec` with `cascade=False` while paused at
    # downstream `review_gate` raises LightflowAlreadyPausedError because
    # `review_gate` is not re-armed when cascade is False.
    with self.assertRaises(lib.LightflowAlreadyPausedError):
      cli.resume(
          lightflow=manifest_path,
          log_id="rerun_run",
          rerun="verify_spec",
          cascade=False,
      )

    # 2b. Combining `--rerun` with `--resolution=REJECT` raises ValueError.
    with self.assertRaisesRegex(ValueError, "Cannot combine '--rerun'"):
      cli.resume(
          lightflow=manifest_path,
          log_id="rerun_run",
          rerun="verify_spec",
          resolution="REJECT",
      )

    # 3. Approve `review_gate` -> workflow completes (`s3` runs).
    cli.resume(
        lightflow=manifest_path,
        log_id="rerun_run",
        stage="review_gate",
        resolution="APPROVE",
    )
    self.assertEqual(_EXEC_TRACE, ["step_1", "verify_1", "verify_2", "step_3"])

    # 4. Re-run `s3` after workflow completion via `--stage=s3 --rerun`
    # (`rerun=True`).
    cli.resume(
        lightflow=manifest_path,
        log_id="rerun_run",
        stage="s3",
        rerun=True,
    )
    self.assertEqual(
        _EXEC_TRACE, ["step_1", "verify_1", "verify_2", "step_3", "step_3"]
    )

    # 5. Conflicting `--stage` and `--rerun` raises ValueError.
    with self.assertRaises(ValueError):
      cli.resume(
          lightflow=manifest_path,
          log_id="rerun_run",
          stage="s1",
          rerun="s3",
      )

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
      with self.assertRaises(engine.EngineError):
        cli._execute_engine_loop(  # pylint: disable=protected-access
            runner=runner,
            ordered_stages=["gate"],
            passport=passport,
            pm=pm,
        )
    out = buf.getvalue()
    self.assertIn("Notice: A polling gate timed out", out)
    self.assertIn("my-custom-runner resume", out)

  def test_windows_msvcrt_lock_fallback_and_cross_drive_commonpath(
      self,
  ) -> None:
    calls: list[tuple[int, int]] = []

    class FakeMsvcrt:
      LK_NBLCK = 2
      LK_UNLCK = 0

      @staticmethod
      def locking(fd: int, mode: int, nbytes: int) -> None:
        del fd
        calls.append((mode, nbytes))

    pm = lib.PassportManager("win_lock_run")
    with (
        mock.patch.object(lib, "fcntl", None),
        mock.patch.object(lib, "msvcrt", FakeMsvcrt),
    ):
      with pm.lock():
        pass
    self.assertEqual(calls, [(2, 1), (0, 1)])

    # Simulate Windows lock contention (PermissionError -> RuntimeError)
    calls.clear()

    class BusyMsvcrt:
      LK_NBLCK = 2
      LK_UNLCK = 0

      @staticmethod
      def locking(fd: int, mode: int, nbytes: int) -> None:
        del fd
        calls.append((mode, nbytes))
        raise PermissionError(13, "Permission denied")

    with (
        mock.patch.object(lib, "fcntl", None),
        mock.patch.object(lib, "msvcrt", BusyMsvcrt),
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

    self.assertEqual(runner._VERSION, "0.2.0")  # pylint: disable=protected-access
    examples_dir = os.path.join(_OSS_ROOT, "examples")
    cli = lib.LightflowRunnerCLI()

    # 1. Verify all 8 example manifests compile and dry-run cleanly end-to-end
    dry_run_buf = io.StringIO()
    with contextlib.redirect_stdout(dry_run_buf):
      for name in (
          "async_job_watcher",
          "blue_green_release",
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
    self.assertIn("USGS reported 2 M4.5+ event(s)", dry_run_out)
    self.assertIn(
        "Output Namespace: payload.outputs.editorial_gate", dry_run_out
    )
    self.assertIn("/tmp/usgs_bulletin.md", dry_run_out)
    self.assertIn("--log_id=custom_preview_id", dry_run_out)
    self.assertIsNone(lib.PassportManager("custom_preview_id").load_passport())

    branch_dry_run_buf = io.StringIO()
    with contextlib.redirect_stdout(branch_dry_run_buf):
      for name in ("incident_db_failover", "tenant_gitops_onboarding"):
        wf_dir = os.path.join(examples_dir, name)
        wf_path = os.path.join(wf_dir, "lightflow.yaml")
        self.assertTrue(os.path.isfile(wf_path), f"Missing {wf_path}")
        self.assertEqual(schema.resolve_lightflow_path(wf_dir), wf_path)
        runner._dispatch_argv(  # pylint: disable=protected-access
            runner.CliWrapper(cli),
            ["dry_run", f"--lightflow={wf_path}"],
        )
    branch_dry_run_out = branch_dry_run_buf.getvalue()
    self.assertIn("Stage: publish_failover_ledger", branch_dry_run_out)
    self.assertIn("Stage: wait_argocd_mesh_sync", branch_dry_run_out)
    self.assertIn("Status: [SKIPPED]", branch_dry_run_out)

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
      with self.assertRaises(engine.OperatorActionSuspended):
        cli.start(
            lightflow=hn_wf,
            log_id="hn_test_run",
            payload={"offline": True, "limit": 2},
        )
    self.assertIn("via offline_fixture", hn_buf.getvalue())
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
      with self.assertRaises(engine.EngineError):
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
      with self.assertRaises(engine.EngineError):
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
      with self.assertRaises(engine.EngineError):
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
          engine.EngineError, "Failed stages: \\['timed_out_stage'\\]"
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
    self.assertIn("PASSPORT PAYLOAD:", out)
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
    self.assertIn("❌ a: rejected (Rejected by alice.", out)
    self.assertIn("⛔ b: blocked (upstream failed or was rejected)", out)

  def test_downstream_cascade_is_blocked_not_a_root_failure(self) -> None:
    self._save(
        ("a", schema.StampStatus.FAILED, "Disk full"),
        ("b", schema.StampStatus.FAILED, self._UPSTREAM_FAILED),
    )
    with self.assertRaisesRegex(lib.LightflowRunFailedError, r"\['a'\]"):
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
    with self.assertRaisesRegex(lib.LightflowRunFailedError, r"\['a', 'b'\]"):
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
    self.assertIn("❌ a: rejected", out)
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
    retry = mock.patch.object(lib, "_LOCK_RETRY_SECONDS", 5.0)
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

    class NoLocksFcntl:
      LOCK_EX, LOCK_SH, LOCK_NB, LOCK_UN = 2, 1, 4, 8

      @staticmethod
      def flock(fd: Any, flags: int) -> None:
        del fd, flags
        raise OSError(errno.ENOLCK, "No locks available")

    with mock.patch.object(lib, "fcntl", NoLocksFcntl):
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
    )
    for raised, code in cases:
      with self.subTest(exception=type(raised).__name__):
        with contextlib.redirect_stdout(io.StringIO()):
          with contextlib.redirect_stderr(io.StringIO()):
            self.assertEqual(self._exit_code_for(raised), code)


if __name__ == "__main__":
  unittest.main()
