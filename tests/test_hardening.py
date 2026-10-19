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

"""Regression tests for the v0.1 hardening pass (timeouts, CEL caps, CLI input validation)."""

from __future__ import annotations

# pylint: disable=missing-function-docstring,protected-access,unused-argument

import contextlib
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
  from ..lightflow import runner
  from ..lightflow import schema
except (ImportError, ValueError):
  from lightflow import engine  # pyrefly: ignore[missing-import]
  from lightflow import lib  # pyrefly: ignore[missing-import]
  from lightflow import runner  # pyrefly: ignore[missing-import]
  from lightflow import schema  # pyrefly: ignore[missing-import]
# pylint: enable=g-import-not-at-top,g-bad-import-order


_CALLS: list[str] = []
_STAGE_2_SHOULD_FAIL = True


def act_ok(payload: dict[str, Any], **_: Any) -> tuple[dict[str, Any], str]:
  _CALLS.append("ok")
  return {"ok": True}, "ok done"


def act_step_2(payload: dict[str, Any], **_: Any) -> tuple[dict[str, Any], str]:
  _CALLS.append("step_2")
  if _STAGE_2_SHOULD_FAIL:
    raise RuntimeError("step 2 broken")
  return {"step_2_done": True}, "step 2 ok"


def act_step_3(payload: dict[str, Any], **_: Any) -> tuple[dict[str, Any], str]:
  _CALLS.append("step_3")
  return {"step_3_done": True}, "step 3 ok"


def act_clobber_outputs(
    payload: dict[str, Any], **_: Any
) -> tuple[dict[str, Any], str]:
  return {"outputs": "corrupted", "real_val": 42}, "tried to clobber outputs"


def act_raise_with_marker(payload: dict[str, Any], **_: Any) -> None:
  raise RuntimeError(f"upstream said: {engine.POLL_YIELD_MARKER}")


_EVENTS: list[str] = []
_RELEASE = threading.Event()  # tests set it once they have asserted


def act_slow_deploy(payload: dict[str, Any], **_: Any) -> dict[str, Any]:
  _EVENTS.append("deploy:start")
  _RELEASE.wait(timeout=30)  # outlives any timeout/backoff the test configures
  _EVENTS.append("deploy:side_effect")
  return {"deployed": True}


def act_undo(payload: dict[str, Any], **_: Any) -> dict[str, Any]:
  _EVENTS.append("rollback")
  return {"rolled_back": True}


def act_boom(payload: dict[str, Any], **_: Any) -> None:
  raise RuntimeError(f"real failure ({payload.get('db_password', 'no-pw')})")


def act_mint_token(
    payload: dict[str, Any], dry_run: bool = False, **_: Any
) -> tuple[dict[str, Any], str]:
  del dry_run
  pw = payload.get("db_password", "")
  return (
      {"api_token": "tok-SECRET-123", "session": f"sess-for-{pw}", "rows": 3},
      f"logged in with {pw}",
  )


def act_slow_rollback(
    payload: dict[str, Any], **_: Any
) -> tuple[dict[str, Any], str]:
  time.sleep(2.0)
  return {"rolled_back": True}, "too slow"


class HardeningTest(unittest.TestCase):
  """Covers behaviours added after the first external review."""

  def setUp(self) -> None:
    super().setUp()
    global _STAGE_2_SHOULD_FAIL
    _CALLS.clear()
    _STAGE_2_SHOULD_FAIL = True
    os.environ.pop("ANTIGRAVITY_CONVERSATION_ID", None)
    self.temp_dir = tempfile.mkdtemp(prefix="lightflow_hardening_")
    self.old_env = os.environ.get("LIGHTFLOW_STATE_DIR")
    os.environ["LIGHTFLOW_STATE_DIR"] = self.temp_dir

  def tearDown(self) -> None:
    if self.old_env is None:
      os.environ.pop("LIGHTFLOW_STATE_DIR", None)
    else:
      os.environ["LIGHTFLOW_STATE_DIR"] = self.old_env
    shutil.rmtree(self.temp_dir, ignore_errors=True)
    super().tearDown()

  def _write(self, name: str, text: str) -> str:
    path = os.path.join(self.temp_dir, name)
    with open(path, "w", encoding="utf-8") as f:
      f.write(text)
    return path

  # --- engine: CEL evaluator caps ----------------------------------------

  def test_cel_modulo_is_numeric_only_and_results_are_capped(self) -> None:
    self.assertEqual(engine.evaluate_cel("7 % 3", {}), 1)
    with self.assertRaisesRegex(engine.EngineError, "'%' is only supported"):
      engine.evaluate_cel("'%s' % 3", {})
    with self.assertRaises(engine.EngineError):
      engine.evaluate_cel("'ab' * 100000", {})
    with self.assertRaises(engine.EngineError):
      engine.evaluate_cel(
          "payload.s.replace('x', payload.big)",
          {"s": "x", "big": "y" * 200_000},
      )
    with self.assertRaises(engine.EngineError):
      engine.evaluate_cel("string(payload.big)", {"big": "y" * 200_000})
    self.assertEqual(engine.evaluate_cel("'ab' * 3", {}), "ababab")

  # --- schema: strict enums ----------------------------------------------

  def test_unknown_enum_values_are_rejected_not_defaulted(self) -> None:
    self.assertEqual(
        schema.TriggerRule.from_value(None), schema.TriggerRule.ALL_SUCCESS
    )
    self.assertEqual(
        schema.TriggerRule.from_value("ALL_DONE"), schema.TriggerRule.ALL_DONE
    )
    with self.assertRaisesRegex(
        ValueError, "Unknown trigger_rule 'BOGUS_RULE'"
    ):
      schema.TriggerRule.from_value("BOGUS_RULE")
    with self.assertRaises(ValueError):
      schema.TriggerRule.from_value(True)
    with self.assertRaises(ValueError):
      schema.Resolution.from_value("MAYBE")
    with self.assertRaises(ValueError):
      schema.Resolution.from_value(False)

  # --- lib: gates and re-arming ------------------------------------------

  def _gate_manifest(self) -> str:
    return self._write(
        "gate.yaml",
        f"""
name: gate_wf
actions:
  - id: ok
    python_import: {__name__}.act_ok
stages:
  - name: first
    python_action:
      action_id: ok
  - name: human_gate
    run_after: [first]
    operator_action:
      instructions: "'approve?'"
      json_schema: '{{"type": "object", "required": ["approved"]}}'
  - name: after
    run_after: [human_gate]
    python_action:
      action_id: ok
""",
    )

  def test_approve_with_approved_false_is_rejected_as_contradictory(
      self,
  ) -> None:
    manifest = self._gate_manifest()
    cli = lib.LightflowRunnerCLI()
    with self.assertRaises(engine.OperatorActionSuspended):
      cli.start(lightflow=manifest, log_id="gate_run")
    with self.assertRaisesRegex(ValueError, "approved"):
      cli.resume(
          lightflow=manifest,
          log_id="gate_run",
          stage="human_gate",
          resolution="APPROVE",
          payload={"approved": False},
      )
    # The gate is still open: a plain APPROVE (which implies approved=true)
    # completes the run.
    cli.resume(
        lightflow=manifest,
        log_id="gate_run",
        stage="human_gate",
        resolution="APPROVE",
    )
    self.assertEqual(_CALLS, ["ok", "ok"])

  def test_explicit_operator_keeps_session_provenance_in_stamps(self) -> None:
    # Issue #17: `--operator` names who decided; the stamp must still show
    # which session made the call. The payload keeps the plain operator.
    manifest = self._gate_manifest()
    cli = lib.LightflowRunnerCLI()
    os.environ["LIGHTFLOW_OPERATOR"] = "agent-session"
    try:
      with self.assertRaises(engine.OperatorActionSuspended):
        cli.start(lightflow=manifest, log_id="prov_run")
      cli.resume(
          lightflow=manifest,
          log_id="prov_run",
          stage="human_gate",
          resolution="APPROVE",
          operator="cfo@example.com",
      )
    finally:
      os.environ.pop("LIGHTFLOW_OPERATOR", None)
    passport = lib.PassportManager("prov_run").load_passport()
    assert passport is not None
    gate = [s for s in passport.stamps if s.stage_name == "human_gate"][-1]
    self.assertIn(
        "approved by cfo@example.com (via agent-session)", gate.message
    )
    self.assertEqual(passport.payload.to_dict()["operator"], "cfo@example.com")

  def test_failure_message_containing_yield_marker_is_still_a_failure(
      self,
  ) -> None:
    # Issue #14: status comes from `Stamp.reason`, not from message text.
    manifest = self._write(
        "marker.yaml",
        f"""
name: marker_wf
actions:
  - id: boom
    python_import: {__name__}.act_raise_with_marker
stages:
  - name: a
    python_action:
      action_id: boom
""",
    )
    with self.assertRaises(lib.LightflowRunFailedError):
      lib.LightflowRunnerCLI().start(lightflow=manifest, log_id="marker_run")
    passport = lib.PassportManager("marker_run").load_passport()
    assert passport is not None
    self.assertEqual(lib.effective_status_name(passport.stamps[-1]), "FAILED")

  def test_effective_status_prefers_reason_and_anchors_legacy_fallback(
      self,
  ) -> None:
    def stamp(message: str, reason=schema.StampReason.REASON_UNSPECIFIED):
      return schema.Stamp(
          stage_name="s",
          status=schema.StampStatus.FAILED,
          message=message,
          reason=reason,
      )

    marker = engine.POLL_YIELD_MARKER
    self.assertEqual(
        lib.effective_status_name(stamp("x", schema.StampReason.POLL_YIELD)),
        "WAITING",
    )
    self.assertEqual(
        lib.effective_status_name(stamp("x", schema.StampReason.REJECTED)),
        "REJECTED",
    )
    # Legacy passports (no reason): the engine's own yield message still
    # reads as WAITING; an action error quoting the marker does not.
    self.assertEqual(
        lib.effective_status_name(stamp(f"Stage 's' polling ... {marker}")),
        "WAITING",
    )
    self.assertEqual(
        lib.effective_status_name(stamp(f"Failed after 1 attempt: {marker}")),
        "FAILED",
    )
    # Round-trips through passport.json; absent when unset.
    self.assertNotIn("reason", stamp("x").to_dict())
    self.assertEqual(
        schema.Stamp.from_dict(
            stamp("x", schema.StampReason.POLL_YIELD).to_dict()
        ).reason,
        schema.StampReason.POLL_YIELD,
    )

  # --- engine/lib: failures are announced; gates park like polls (#15) -----

  def test_failure_is_printed_and_gate_does_not_halt_independent_stages(
      self,
  ) -> None:
    manifest = self._write(
        "probe.yaml",
        f"""
name: probe
actions:
  - id: boom
    python_import: {__name__}.act_boom
  - id: ok
    python_import: {__name__}.act_ok
stages:
  - name: a
    python_action: {{action_id: boom}}
  - name: gate
    operator_action:
      instructions: "'approve?'"
  - name: independent
    python_action: {{action_id: ok}}
""",
    )
    out = io.StringIO()
    with contextlib.redirect_stdout(out):
      with self.assertRaises(lib.LightflowRunFailedError):
        lib.LightflowRunnerCLI().start(lightflow=manifest, log_id="probe")
    text = out.getvalue()
    self.assertIn(
        "❌ [a] Failed after 1 attempt. Last error: real failure", text
    )
    self.assertIn("Open gate(s), still awaiting a decision: gate", text)
    self.assertEqual(_CALLS, ["ok"])  # `independent` ran despite the gate
    passport = lib.PassportManager("probe").load_passport()
    assert passport is not None
    latest = {st.stage_name: st.status for st in passport.stamps}
    self.assertEqual(latest["gate"], schema.StampStatus.PAUSED)
    self.assertEqual(latest["independent"], schema.StampStatus.COMPLETED)
    # The parked gate does not block re-arming the failed sibling.
    with self.assertRaises(lib.LightflowRunFailedError):
      lib.LightflowRunnerCLI().resume(lightflow=manifest, log_id="probe")

  # --- engine: a timed-out attempt is never overlapped (#11) ---------------

  def test_timed_out_attempt_is_not_retried_or_rolled_back_while_alive(
      self,
  ) -> None:
    _EVENTS.clear()
    _RELEASE.clear()
    self.addCleanup(_RELEASE.set)
    manifest = self._write(
        "race.yaml",
        f"""
name: race
actions:
  - id: deploy
    python_import: {__name__}.act_slow_deploy
  - id: undo
    python_import: {__name__}.act_undo
stages:
  - name: deploy
    python_action: {{action_id: deploy}}
    rollback_action: {{action_id: undo}}
    timeout_seconds: 1
    retry_policy: {{max_attempts: 2, initial_backoff_seconds: 1}}
""",
    )
    with self.assertRaises(lib.LightflowRunFailedError):
      lib.LightflowRunnerCLI().start(lightflow=manifest, log_id="race")
    passport = lib.PassportManager("race").load_passport()
    assert passport is not None
    msg = passport.stamps[-1].message
    self.assertIn("Not retried", msg)
    self.assertIn("Rollback 'undo' skipped", msg)
    self.assertEqual(_EVENTS, ["deploy:start"])  # one attempt, no rollback
    _RELEASE.set()

  def test_polling_tick_timeout_does_not_race_rollback(self) -> None:
    # Issue #11 also applies to poll ticks: a tick that times out keeps
    # running; rollback must not overlap it.
    _EVENTS.clear()
    _RELEASE.clear()
    self.addCleanup(_RELEASE.set)
    manifest = self._write(
        "tick.yaml",
        f"""
name: tick_race
actions:
  - id: tick
    python_import: {__name__}.act_slow_deploy
  - id: undo
    python_import: {__name__}.act_undo
stages:
  - name: watch
    timeout_seconds: 1
    rollback_action: {{action_id: undo}}
    polling_policy:
      poll_tick_action: {{action_id: tick}}
      condition: "payload.outputs.watch.deployed == true"
      interval_seconds: 1
      max_attempts: 2
""",
    )
    with self.assertRaises(lib.LightflowRunFailedError):
      lib.LightflowRunnerCLI().start(lightflow=manifest, log_id="tick_race")
    passport = lib.PassportManager("tick_race").load_passport()
    assert passport is not None
    self.assertIn("Rollback 'undo' skipped", passport.stamps[-1].message)
    self.assertNotIn("rollback", _EVENTS)
    _RELEASE.set()

  def test_two_gates_park_independently_and_list_agrees_with_status(
      self,
  ) -> None:
    manifest = self._write(
        "two_gates.yaml",
        f"""
name: two_gates
actions:
  - id: ok
    python_import: {__name__}.act_ok
  - id: boom
    python_import: {__name__}.act_boom
stages:
  - name: gate_a
    operator_action: {{instructions: "'a?'"}}
  - name: gate_b
    operator_action: {{instructions: "'b?'"}}
  - name: after_a
    run_after: [gate_a]
    python_action: {{action_id: ok}}
  - name: boom
    python_action: {{action_id: boom}}
""",
    )
    cli = lib.LightflowRunnerCLI()
    # Failure outranks both open gates: exit 1, footer names the gates.
    with self.assertRaises(lib.LightflowRunFailedError):
      cli.start(lightflow=manifest, log_id="two_gates")
    out = io.StringIO()
    with contextlib.redirect_stdout(out):
      with self.assertRaises(lib.LightflowRunFailedError):
        cli.status(log_id="two_gates")
    self.assertIn("(log ID: two_gates): FAILED", out.getvalue())
    self.assertIn("Paused at operator action 'gate_a'", out.getvalue())
    self.assertIn("Paused at operator action 'gate_b'", out.getvalue())
    out = io.StringIO()
    with contextlib.redirect_stdout(out):
      cli.list()
    self.assertIn("FAILED", out.getvalue())  # `list` agrees with `status`
    # Answering one gate runs its dependents and leaves the other parked;
    # with the failure still there the run stays exit 1.
    with self.assertRaises(lib.LightflowRunFailedError):
      cli.resume(log_id="two_gates", stage="gate_a", resolution="APPROVE")
    passport = lib.PassportManager("two_gates").load_passport()
    assert passport is not None
    latest = {st.stage_name: st.status for st in passport.stamps}
    self.assertEqual(latest["after_a"], schema.StampStatus.COMPLETED)
    self.assertEqual(latest["gate_b"], schema.StampStatus.PAUSED)

  # --- secret_keys: never in shared output, always in passport.json ------

  def test_secret_keys_are_redacted_in_shared_output_only(self) -> None:
    # github.com/google/lightflow/issues/21 (item 6), #22, and #23.
    manifest = self._write(
        "secrets.yaml",
        f"""
name: secrets_wf
secret_keys: [db_password, api_token]
actions:
  - id: mint
    python_import: {__name__}.act_mint_token
  - id: boom
    python_import: {__name__}.act_boom
  - id: ok
    python_import: {__name__}.act_ok
stages:
  - name: mint
    python_action: {{action_id: mint}}
  - name: gate
    run_after: [mint]
    operator_action:
      instructions: "'Token in use: ' + payload.api_token"
  - name: boom
    python_action: {{action_id: boom}}
  - name: tail
    run_after: [mint]
    python_action: {{action_id: ok}}
""",
    )
    cli = lib.LightflowRunnerCLI()
    seed = {"db_password": "hunter2", "region": "eu"}
    dry_out = io.StringIO()
    with contextlib.redirect_stdout(dry_out):
      cli.dry_run(lightflow=manifest, payload=seed)
    start_out = io.StringIO()
    with contextlib.redirect_stdout(start_out):
      with self.assertRaises(lib.LightflowRunFailedError):
        cli.start(lightflow=manifest, log_id="secrets_run", payload=seed)
    status_out = io.StringIO()
    with contextlib.redirect_stdout(status_out):
      with self.assertRaises(lib.LightflowRunFailedError):
        cli.status(log_id="secrets_run", verbose=True)
      html_path = cli.visualize(
          log_id="secrets_run", output=os.path.join(self.temp_dir, "v.html")
      )
    dry_text = dry_out.getvalue()
    status_text = status_out.getvalue()
    with open(html_path, encoding="utf-8") as f:
      html = f.read()
    pm = lib.PassportManager("secrets_run")
    with open(pm.passport_file, encoding="utf-8") as f:
      passport_json = f.read()

    for secret in ("hunter2", "tok-SECRET-123"):
      self.assertNotIn(secret, dry_text)  # dry_run echo, sim output & gate
      self.assertNotIn(secret, status_text)  # status & --verbose
      self.assertNotIn(secret, html)  # visualizer payload, stamps & gate
      self.assertIn(secret, passport_json)  # resume still has the values
    self.assertIn('"db_password": "<redacted>"', dry_text)
    self.assertIn('"session": "sess-for-<redacted>"', status_text)
    self.assertIn("Instructions: Token in use: <redacted>", status_text)
    self.assertIn("logged in with <redacted>", status_text)
    # Issue #22: `tail` finished last (COMPLETED), yet the run failed (`boom`),
    # so the visualizer header state must agree with `status`: FAILED.
    self.assertIn('"run_state": "FAILED"', html)
    self.assertIn("eu", html)  # non-secret keys untouched
    self.assertEqual(oct(os.stat(pm.resolved_path).st_mode & 0o777), "0o700")

  # --- engine: import-time errors are configuration errors, not transient ---

  def _import_error_manifest(self, module_text: str) -> str:
    self._write("broken_acts.py", module_text)
    return self._write(
        "broken.yaml",
        """
name: broken_wf
actions:
  - id: go
    python_import: broken_acts.go
stages:
  - name: only
    python_action:
      action_id: go
    retry_policy: {max_attempts: 3, initial_backoff_seconds: 1}
""",
    )

  def test_syntax_error_in_action_module_fails_without_retry(self) -> None:
    # Issue #18a: a SyntaxError can never succeed on retry; with 3 attempts
    # and 1s backoff a retry would take >= 2s.
    manifest = self._import_error_manifest("def go(payload)\n  return {}\n")
    cli = lib.LightflowRunnerCLI()
    sys.modules.pop("broken_acts", None)
    t0 = time.monotonic()
    with self.assertRaises(lib.LightflowRunFailedError):
      cli.start(lightflow=manifest, log_id="syntax_run")
    self.assertLess(time.monotonic() - t0, 1.5)
    passport = lib.PassportManager("syntax_run").load_passport()
    assert passport is not None
    stamp = passport.stamps[-1]
    self.assertIn("SyntaxError", stamp.message)
    self.assertIn("not retried", stamp.message)

  def test_missing_dependency_is_named_not_blamed_on_manifest_path(
      self,
  ) -> None:
    # Issue #18b: the action module exists but imports an uninstalled package.
    manifest = self._import_error_manifest(
        "import requestz_not_installed\ndef go(payload):\n  return {}\n"
    )
    cli = lib.LightflowRunnerCLI()
    sys.modules.pop("broken_acts", None)
    with self.assertRaises(lib.LightflowRunFailedError):
      cli.start(lightflow=manifest, log_id="dep_run")
    passport = lib.PassportManager("dep_run").load_passport()
    assert passport is not None
    msg = passport.stamps[-1].message
    self.assertIn("imports 'requestz_not_installed'", msg)
    self.assertNotIn("if the manifest moved", msg)

  def test_cascade_false_rearms_only_target_while_default_cascades(
      self,
  ) -> None:
    global _STAGE_2_SHOULD_FAIL
    manifest = self._write(
        "chain.yaml",
        f"""
name: chain_wf
actions:
  - id: s2
    python_import: {__name__}.act_step_2
  - id: s3
    python_import: {__name__}.act_step_3
stages:
  - name: two
    python_action:
      action_id: s2
  - name: three
    run_after: [two]
    python_action:
      action_id: s3
""",
    )
    cli = lib.LightflowRunnerCLI()
    with self.assertRaises(lib.LightflowRunFailedError):
      cli.start(lightflow=manifest, log_id="chain_run")
    _STAGE_2_SHOULD_FAIL = False
    # cascade=False surgically re-runs only 'two'; 'three' stays FAILED until
    # resumed.
    with self.assertRaises(lib.LightflowRunFailedError):
      cli.resume(lightflow=manifest, log_id="chain_run", cascade=False)
    self.assertEqual(_CALLS, ["step_2", "step_2"])
    cli.resume(lightflow=manifest, log_id="chain_run")
    self.assertEqual(_CALLS, ["step_2", "step_2", "step_3"])
    passport = lib.PassportManager("chain_run").load_passport()
    assert passport is not None
    self.assertEqual(
        {
            s.stage_name
            for s in passport.stamps
            if s.status == schema.StampStatus.COMPLETED
        },
        {"two", "three"},
    )

  # --- schema: the stdlib YAML fallback fails loudly on unsupported syntax ---

  def test_fallback_yaml_rejects_unsupported_syntax_instead_of_guessing(
      self,
  ) -> None:
    parse = schema._parse_simple_yaml  # pylint: disable=protected-access
    bad = {
        "a: {foo: 1}\n": "flow mapping",
        "a: &x 1\nb: *x\n": "anchors, aliases and tags",
        "a: !!str 5\n": "anchors, aliases and tags",
        "a: [unclosed\n": "unterminated flow collection",
        "- just\n- a list\n": "mapping at top level",
        "a: 1\n  bogus\n": "unexpected indentation",
        "a:\n  b: 1\n  nokey\n": "expected 'key: value'",
        "---\na: 1\n---\nb: 2\n": "multi-document",
        "---\na: 1\n...\nb: 2\n": "multi-document",
    }
    for src, msg in bad.items():
      with self.subTest(src=src):
        with self.assertRaisesRegex(ValueError, msg):
          parse(src)

  def test_fallback_yaml_parses_the_supported_subset(self) -> None:
    parse = schema._parse_simple_yaml  # pylint: disable=protected-access
    self.assertEqual(parse("---\na: 1\n...\n"), {"a": 1})
    self.assertEqual(
        parse('a: {"foo": 1}\nb: [x, 2]\n'), {"a": {"foo": 1}, "b": ["x", 2]}
    )
    self.assertEqual(
        parse("c: |\n  # heading\n  ---\n  l2\nd: >-\n  x\n  y\n"),
        {"c": "# heading\n---\nl2", "d": "x y"},
    )
    examples = os.path.join(_OSS_ROOT, "examples")
    for name in sorted(os.listdir(examples)):
      manifest = os.path.join(examples, name, "lightflow.yaml")
      if not os.path.exists(manifest):
        continue
      with self.subTest(example=name), open(manifest, encoding="utf-8") as f:
        wf = schema.Lightflow.from_dict(parse(f.read()))
        self.assertTrue(
            wf.stages, f"{name}: fallback parser produced no stages"
        )

  def test_allowed_import_prefixes_blocks_manifest_local_module_shadowing(
      self,
  ) -> None:
    """When allowed_import_prefixes is set, manifest-local files cannot shadow allowlisted packages."""
    with tempfile.TemporaryDirectory() as tmp:
      trusted_root = os.path.join(tmp, "trusted_site_pkgs")
      trusted_pkg = os.path.join(trusted_root, "trusted_allowlisted_pkg")
      os.makedirs(trusted_pkg)
      with open(
          os.path.join(trusted_pkg, "__init__.py"), "w", encoding="utf-8"
      ) as f:
        f.write("")
      with open(
          os.path.join(trusted_pkg, "actions.py"), "w", encoding="utf-8"
      ) as f:
        f.write(
            "def safe_action(payload, **kw):\n"
            "  return {'trusted': True}, 'TRUSTED_EXECUTED'\n"
        )

      untrusted_dir = os.path.join(tmp, "untrusted_manifest_dir")
      evil_pkg = os.path.join(untrusted_dir, "trusted_allowlisted_pkg")
      os.makedirs(evil_pkg)
      with open(
          os.path.join(evil_pkg, "__init__.py"), "w", encoding="utf-8"
      ) as f:
        f.write("")
      with open(
          os.path.join(evil_pkg, "actions.py"), "w", encoding="utf-8"
      ) as f:
        f.write(
            "def safe_action(payload, **kw):\n"
            "  return {'shadowed': True}, 'EVIL_SHADOW_EXECUTED'\n"
        )
      manifest_path = os.path.join(untrusted_dir, "lightflow.yaml")
      with open(manifest_path, "w", encoding="utf-8") as f:
        f.write(
            "name: shadow_test\n"
            "actions:\n"
            "  - id: safe_action\n"
            "    python_import: trusted_allowlisted_pkg.actions.safe_action\n"
            "stages:\n"
            "  - name: s1\n"
            "    python_action:\n"
            "      action_id: safe_action\n"
        )

      sys.path.insert(0, trusted_root)
      try:
        wf = schema.load_lightflow(manifest_path)
        eng = engine.LightflowEngine(
            wf,
            workflow_path=manifest_path,
            allowed_import_prefixes=["trusted_allowlisted_pkg"],
        )
        passport = schema.Passport(payload=schema.StructDict({}))
        eng.execute_stage("s1", passport)
        self.assertNotIn("shadowed", passport.payload)
        self.assertTrue(passport.payload.get("trusted"))
      finally:
        if trusted_root in sys.path:
          sys.path.remove(trusted_root)
        for mod_name in list(sys.modules):
          if mod_name == "trusted_allowlisted_pkg" or mod_name.startswith(
              "trusted_allowlisted_pkg."
          ):
            sys.modules.pop(mod_name, None)

  def test_manifest_rejects_unknown_fields_with_did_you_mean_hint(self) -> None:
    with self.assertRaisesRegex(
        ValueError, r"Unknown field 'run_aftr'.*Did you mean 'run_after'\?"
    ):
      schema.Stage.from_dict({"name": "s1", "run_aftr": ["s0"]})

    with self.assertRaisesRegex(
        ValueError,
        r"Unknown field 'max_attemps'.*Did you mean 'max_attempts'\?",
    ):
      schema.RetryPolicy.from_dict({"max_attemps": 3})

    with self.assertRaisesRegex(
        ValueError,
        r"Unknown field 'python_imports'.*Did you mean 'python_import'\?",
    ):
      schema.ActionDefinition.from_dict(
          {"id": "a1", "python_imports": "pkg.mod.fn"}
      )

    with self.assertRaisesRegex(
        ValueError, r"Unknown field 'stags'.*Did you mean 'stages'\?"
    ):
      schema.Lightflow.from_dict({"name": "wf", "stags": []})

  def test_current_operator_records_env_override_and_agent_conversation(
      self,
  ) -> None:
    orig_op = os.environ.get("LIGHTFLOW_OPERATOR")
    orig_conv = os.environ.get("ANTIGRAVITY_CONVERSATION_ID")
    try:
      os.environ.pop("LIGHTFLOW_OPERATOR", None)
      os.environ["ANTIGRAVITY_CONVERSATION_ID"] = "conv-uuid-123"
      op_with_conv = lib._current_operator()  # pylint: disable=protected-access
      self.assertIn("(agent:conv-uuid-123)", op_with_conv)

      os.environ["LIGHTFLOW_OPERATOR"] = "ci-bot"
      self.assertEqual(
          lib._current_operator(), "ci-bot"  # pylint: disable=protected-access
      )
    finally:
      if orig_op is None:
        os.environ.pop("LIGHTFLOW_OPERATOR", None)
      else:
        os.environ["LIGHTFLOW_OPERATOR"] = orig_op
      if orig_conv is None:
        os.environ.pop("ANTIGRAVITY_CONVERSATION_ID", None)
      else:
        os.environ["ANTIGRAVITY_CONVERSATION_ID"] = orig_conv

  def test_utf8_stdio_reconfigure_and_payload_file_and_split_json_argv(
      self,
  ) -> None:
    reconfigured: list[tuple[str, str]] = []

    class FakeCp1252Stream:
      encoding = "cp1252"

      def reconfigure(self, *, encoding: str = "", errors: str = "") -> None:
        reconfigured.append((encoding, errors))

    with (
        mock.patch.object(sys, "stdout", FakeCp1252Stream()),
        mock.patch.object(sys, "stderr", FakeCp1252Stream()),
    ):
      lib._ensure_utf8_stdio()
    self.assertEqual(reconfigured, [("utf-8", "replace"), ("utf-8", "replace")])

    wf = self._write(
        "simple.yaml",
        f"""
name: simple_wf
actions:
  - id: ok
    python_import: {__name__}.act_ok
stages:
  - name: s1
    python_action:
      action_id: ok
""",
    )
    payload_file = os.path.join(self.temp_dir, "payload.json")
    # Write with UTF-8 BOM to simulate Windows PowerShell Out-File / Set-Content
    with open(payload_file, "wb") as f:
      f.write(b'\xef\xbb\xbf{"greeting": "hello world", "count": 3}')

    wrapper = runner.CliWrapper(lib.LightflowRunnerCLI())
    runner._dispatch_argv(
        wrapper,
        [
            "start",
            f"--lightflow={wf}",
            "--log_id=run_at_file",
            f"--payload=@{payload_file}",
        ],
    )
    loaded = lib.PassportManager("run_at_file").load_passport()
    assert loaded is not None
    self.assertEqual(loaded.payload.to_dict()["greeting"], "hello world")
    self.assertEqual(loaded.payload.to_dict()["count"], 3)

    # Also test --payload_file=<path> and PowerShell split-JSON token reassembly
    runner._dispatch_argv(
        wrapper,
        [
            "start",
            f"--lightflow={wf}",
            "--log_id=run_split_tokens",
            '--payload={"msg":',
            '"split',
            'across",',
            '"n":',
            "7}",
        ],
    )
    loaded_split = lib.PassportManager("run_split_tokens").load_passport()
    assert loaded_split is not None
    self.assertEqual(loaded_split.payload.to_dict()["msg"], "split across")
    self.assertEqual(loaded_split.payload.to_dict()["n"], 7)

    # Unexpected positional arguments when --lightflow is already set raise a
    # clear ValueError rather than TypeError: got multiple values for
    # 'lightflow'.
    with self.assertRaisesRegex(
        ValueError, "Unexpected positional argument.*--payload=@payload.json"
    ):
      runner._dispatch_argv(
          wrapper,
          ["start", f"--lightflow={wf}", "--log_id=r_bad", "stray_arg"],
      )

  def test_reserved_outputs_key_rejected_in_payload_and_stripped_from_actions(
      self,
  ) -> None:
    wf = self._gate_manifest()
    cli = lib.LightflowRunnerCLI()
    with self.assertRaisesRegex(
        ValueError, "cannot contain reserved key 'outputs'"
    ):
      cli.start(
          lightflow=wf,
          log_id="forged_start",
          payload='{"outputs": {"s1": {"ok": true}}}',
      )

    with self.assertRaises(engine.OperatorActionSuspended):
      cli.start(lightflow=wf, log_id="forged_resume")

    with self.assertRaisesRegex(
        ValueError, "cannot contain reserved key 'outputs'"
    ):
      cli.resume(
          lightflow=wf,
          log_id="forged_resume",
          stage="human_gate",
          payload='{"note": "hi", "outputs": {"s1": {"forged": true}}}',
      )

    # Action returning {"outputs": "corrupted"} cannot clobber payload.outputs
    clobber_wf = self._write(
        "clobber.yaml",
        f"""
name: clobber_wf
actions:
  - id: ok
    python_import: {__name__}.act_ok
  - id: clobber
    python_import: {__name__}.act_clobber_outputs
stages:
  - name: s1
    python_action:
      action_id: ok
  - name: s2
    run_after: [s1]
    python_action:
      action_id: clobber
""",
    )
    cli.start(lightflow=clobber_wf, log_id="clobber_run")
    passport = lib.PassportManager("clobber_run").load_passport()
    assert passport is not None
    p_dict = passport.payload.to_dict()
    self.assertEqual(p_dict["outputs"]["s1"], {"ok": True})
    self.assertEqual(p_dict["outputs"]["s2"], {"real_val": 42})

  def test_start_force_preserves_held_lockfile_inode(self) -> None:
    wf = self._write(
        "force_lock.yaml",
        f"""
name: force_lock_wf
actions:
  - id: ok
    python_import: {__name__}.act_ok
stages:
  - name: s1
    python_action:
      action_id: ok
""",
    )
    cli = lib.LightflowRunnerCLI()
    cli.start(lightflow=wf, log_id="force_lock_run")
    pm = lib.PassportManager("force_lock_run")
    lock_path = pm.resolved_path + ".lock"
    self.assertTrue(os.path.exists(lock_path))
    inode_before = os.stat(lock_path).st_ino

    cli.start(lightflow=wf, log_id="force_lock_run", force=True)
    self.assertTrue(os.path.exists(lock_path))
    inode_after = os.stat(lock_path).st_ino
    self.assertEqual(inode_before, inode_after)

  def test_multi_gate_required_fields_do_not_leak_from_earlier_gates(
      self,
  ) -> None:
    wf = self._write(
        "two_gates.yaml",
        """
name: two_gates_wf
stages:
  - name: gate_1
    operator_action:
      instructions: "'First gate'"
      json_schema: >
        {"type": "object",
         "properties": {"project": {"type": "string"}, "reviewer": {"type": "string"}},
         "required": ["project", "reviewer"]}
  - name: gate_2
    run_after: [gate_1]
    operator_action:
      instructions: "'Second gate'"
      json_schema: >
        {"type": "object",
         "properties": {"project": {"type": "string"}, "reviewer": {"type": "string"}},
         "required": ["project", "reviewer"]}
""",
    )
    cli = lib.LightflowRunnerCLI()
    # Start with initial payload containing "project"
    with self.assertRaises(engine.OperatorActionSuspended):
      cli.start(
          lightflow=wf,
          log_id="two_gates_run",
          payload='{"project": "apollo"}',
      )

    # gate_1 inherits "project" from start --payload and receives "reviewer"
    with self.assertRaises(engine.OperatorActionSuspended):
      cli.resume(
          lightflow=wf,
          log_id="two_gates_run",
          stage="gate_1",
          payload='{"reviewer": "alice"}',
      )

    # gate_2 inherits "project" from start --payload, but must NOT inherit
    # "reviewer" from gate_1's output
    with self.assertRaisesRegex(
        ValueError, "failed JSON schema validation.*'reviewer'"
    ):
      cli.resume(
          lightflow=wf,
          log_id="two_gates_run",
          stage="gate_2",
          payload="{}",
      )

    # Supplying "reviewer" explicitly for gate_2 succeeds (while "project"
    # is still back-filled from initial start --payload)
    cli.resume(
        lightflow=wf,
        log_id="two_gates_run",
        stage="gate_2",
        payload='{"reviewer": "bob"}',
    )
    passport = lib.PassportManager("two_gates_run").load_passport()
    assert passport is not None
    p_dict = passport.payload.to_dict()
    self.assertEqual(p_dict["outputs"]["gate_1"]["reviewer"], "alice")
    self.assertNotIn("project", p_dict["outputs"]["gate_1"])
    self.assertEqual(p_dict["outputs"]["gate_2"]["reviewer"], "bob")
    self.assertNotIn("project", p_dict["outputs"]["gate_2"])

  def test_local_action_module_reloads_when_edited_on_disk(self) -> None:
    wf_dir = os.path.join(self.temp_dir, "hot_reload_wf")
    os.makedirs(wf_dir, exist_ok=True)
    actions_py = os.path.join(wf_dir, "hot_actions.py")
    with open(actions_py, "w", encoding="utf-8") as f:
      f.write(
          "def run_step(payload, **kwargs):\n  raise RuntimeError('buggy v1')\n"
      )
    wf_path = os.path.join(wf_dir, "lightflow.yaml")
    with open(wf_path, "w", encoding="utf-8") as f:
      f.write(
          "name: hot_reload\n"
          "actions:\n"
          "  - id: step\n"
          "    python_import: hot_actions.run_step\n"
          "stages:\n"
          "  - name: s1\n"
          "    python_action:\n"
          "      action_id: step\n"
      )

    cli = lib.LightflowRunnerCLI()
    with self.assertRaisesRegex(lib.LightflowRunFailedError, "s1"):
      cli.start(lightflow=wf_path, log_id="hot_run")

    # Edit hot_actions.py in place and resume within the same process
    with open(actions_py, "w", encoding="utf-8") as f:
      f.write(
          "def run_step(payload, **kwargs):\n"
          "  return {'reloaded': True}, 'fixed v2'\n"
      )

    cli.resume(lightflow=wf_path, log_id="hot_run")
    passport = lib.PassportManager("hot_run").load_passport()
    assert passport is not None
    self.assertTrue(passport.payload.to_dict().get("reloaded"))

  def test_mermaid_collision_free_ids_and_empty_rollback_msg_and_timeout(
      self,
  ) -> None:
    wf = schema.Lightflow.from_dict({
        "name": "mermaid_wf",
        "actions": [
            {"id": "fail_act", "python_import": f"{__name__}.act_step_2"},
            {"id": "slow_rb", "python_import": f"{__name__}.act_slow_rollback"},
        ],
        "stages": [
            {
                "name": "build-api",
                "python_action": {"action_id": "fail_act"},
                "rollback_action": {"action_id": "slow_rb"},
                "timeout_seconds": 1,
            },
            {
                "name": "build_api",
                "run_after": ["build-api"],
                "python_action": {"action_id": "fail_act"},
            },
        ],
    })
    passport = schema.Passport()
    stamp = passport.stamps.add()
    stamp.stage_name = "build-api"
    stamp.status = schema.StampStatus.FAILED
    stamp.message = "Failed.\nRollback executed successfully."
    mermaid = lib.generate_status_mermaid(wf, passport)
    self.assertIn("build_api[", mermaid)
    self.assertIn("build_api_2[", mermaid)
    self.assertIn("build_api --> build_api_2", mermaid)
    self.assertIn("[rolled back]", mermaid)

    # Verify rollback_action honors stage.timeout_seconds
    global _STAGE_2_SHOULD_FAIL
    _STAGE_2_SHOULD_FAIL = True
    runner_eng = engine.LightflowEngine(wf)
    p_timeout = runner_eng.execute_stage("build-api", schema.Passport())
    self.assertIn("exceeded timeout_seconds=1", p_timeout.stamps[-1].message)
    self.assertIn("worker thread keeps running", p_timeout.stamps[-1].message)


if __name__ == "__main__":
  unittest.main()
