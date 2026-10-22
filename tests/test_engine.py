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

"""Unit tests for Lightflow OSS schema, AST evaluator, and LightflowEngine."""

from __future__ import annotations

# pylint: disable=missing-function-docstring,protected-access,unused-argument

import os
import sys
import tempfile
import time
from typing import Any
import unittest

# Ensure standalone execution (`python3 -m unittest`) resolves `lightflow`
_OSS_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _OSS_ROOT not in sys.path:
  sys.path.insert(0, _OSS_ROOT)

# pylint: disable=g-import-not-at-top,g-bad-import-order
try:
  from ..lightflow import engine
  from ..lightflow import schema
except (ImportError, ValueError):
  from lightflow import engine  # pyrefly: ignore[missing-import]
  from lightflow import schema  # pyrefly: ignore[missing-import]
# pylint: enable=g-import-not-at-top,g-bad-import-order


# Sample test actions registered in this module
_CALL_LOG: list[str] = []
_FLAKY_COUNTER = 0
_POLL_COUNTER = 0


def sample_fetch(
    payload: dict[str, Any], **static_kwargs: Any
) -> tuple[dict[str, Any], str]:
  _CALL_LOG.append("sample_fetch")
  limit = static_kwargs.get("limit", 10)
  return {
      "row_count": limit,
      "summary": f"{limit} rows",
  }, f"Fetched {limit} rows"


def sample_publish(
    payload: dict[str, Any], **static_kwargs: Any
) -> tuple[dict[str, Any], str]:
  del static_kwargs
  _CALL_LOG.append("sample_publish")
  return {
      "published": True,
      "source_rows": payload.get("row_count"),
  }, "Published"


def sample_fail(
    payload: dict[str, Any], **static_kwargs: Any
) -> tuple[dict[str, Any], str]:
  del payload, static_kwargs
  _CALL_LOG.append("sample_fail")
  raise RuntimeError("Simulated database outage")


def sample_rollback(
    payload: dict[str, Any], **static_kwargs: Any
) -> tuple[dict[str, Any], str]:
  del payload, static_kwargs
  _CALL_LOG.append("sample_rollback")
  return {"rolled_back": True}, "Cleaned up temp table"


def sample_cleanup_done(
    payload: dict[str, Any], **static_kwargs: Any
) -> tuple[dict[str, Any], str]:
  del payload, static_kwargs
  _CALL_LOG.append("sample_cleanup_done")
  return {"cleanup_ran": True}, "ALL_DONE cleanup finished"


def sample_flaky(
    payload: dict[str, Any], **static_kwargs: Any
) -> tuple[dict[str, Any], str]:
  del payload, static_kwargs
  global _FLAKY_COUNTER
  _FLAKY_COUNTER += 1
  if _FLAKY_COUNTER < 2:
    raise ValueError("Transient network glitch")
  return {"flaky_ok": True}, "Recovered on retry"


def sample_poll_tick(
    payload: dict[str, Any], **static_kwargs: Any
) -> tuple[dict[str, Any], str]:
  del payload, static_kwargs
  global _POLL_COUNTER
  _POLL_COUNTER += 1
  status_val = "READY" if _POLL_COUNTER >= 2 else "RUNNING"
  return {"job_status": status_val}, f"Tick {_POLL_COUNTER}: {status_val}"


def sample_hung_tick(
    payload: dict[str, Any], **static_kwargs: Any
) -> tuple[dict[str, Any], str]:
  """Poll tick that outlives a short budget (sleep_seconds is a static kwarg)."""
  del payload
  time.sleep(float(static_kwargs.get("sleep_seconds", 2)))
  return {"job_status": "RUNNING"}, "still running"


def sample_rollback_marker(
    payload: dict[str, Any], **static_kwargs: Any
) -> tuple[dict[str, Any], str]:
  del payload, static_kwargs
  _CALL_LOG.append("sample_rollback_marker")
  return {}, "rolled back"


class SchemaAndManifestParserTest(unittest.TestCase):
  """Tests YAML, JSON, and Textproto manifest parsing in schema.py."""

  def test_parse_yaml_manifest(self) -> None:
    yaml_text = """
name: revenue_workflow
description: YAML test workflow
actions:
  - id: fetch_rows
    python_import: test_engine.sample_fetch
stages:
  - name: fetch
    python_action:
      action_id: fetch_rows
      static_kwargs:
        limit: 250
  - name: approve
    run_after: [fetch]
    operator_action:
      instructions: "'Approve ' + payload.outputs.fetch.summary"
      json_schema: '{"type": "object", "required": ["approved"]}'
"""
    wf = schema.parse_lightflow_text(yaml_text, fmt="yaml")
    self.assertEqual(wf.name, "revenue_workflow")
    self.assertEqual(len(wf.actions), 1)
    self.assertEqual(wf.actions[0].id, "fetch_rows")
    self.assertEqual(len(wf.stages), 2)
    assert wf.stages[0].python_action is not None
    self.assertEqual(wf.stages[0].python_action.static_kwargs["limit"], 250)
    self.assertTrue(wf.stages[1].HasField("operator_action"))
    assert wf.stages[1].operator_action is not None
    self.assertIn("Approve", wf.stages[1].operator_action.instructions)

  def test_parse_legacy_textproto_manifest(self) -> None:
    textproto = """
name: "legacy_textproto_workflow"
actions {
  id: "fetch_rows"
  python_import: "my_pkg.actions.fetch_rows"
}
stages {
  name: "fetch"
  python_action {
    action_id: "fetch_rows"
    static_kwargs {
      fields {
        key: "limit"
        value { number_value: 500 }
      }
    }
  }
  retry_policy { max_attempts: 3 }
}
stages {
  name: "approve"
  operator_action {
    instructions:
      "'Ready to publish: ' + payload.outputs.fetch.summary + "
      "'. Compare row_count before approving.'"
  }
  run_after: "fetch"
}
"""
    wf = schema.parse_lightflow_text(textproto, fmt="textproto")
    self.assertEqual(wf.name, "legacy_textproto_workflow")
    assert wf.stages[0].python_action is not None
    self.assertEqual(wf.stages[0].python_action.static_kwargs["limit"], 500)
    assert wf.stages[0].retry_policy is not None
    self.assertEqual(wf.stages[0].retry_policy.max_attempts, 3)
    assert wf.stages[1].operator_action is not None
    self.assertIn(
        "Ready to publish:", wf.stages[1].operator_action.instructions
    )

  def test_parse_yaml_raises_on_malformed_yaml_or_non_dict(self) -> None:
    with self.assertRaises(Exception):
      schema.parse_lightflow_text("stages:\n  - name: [unclosed", fmt="yaml")
    with self.assertRaises(ValueError):
      schema.parse_lightflow_text("- item1\n- item2\n", fmt="yaml")

  def test_simple_yaml_preserves_hash_in_quotes_and_unescapes_proto(
      self,
  ) -> None:
    yaml_text = """
name: hash_test # trailing comment stripped
description: "Step #1: Extract & Verify #2"
stages:
  - name: gate_1
    operator_action:
      instructions: "'Check item #1 and #2'"
"""
    parsed = schema._parse_simple_yaml(yaml_text)
    self.assertEqual(parsed["name"], "hash_test")
    self.assertEqual(parsed["description"], "Step #1: Extract & Verify #2")
    self.assertEqual(
        parsed["stages"][0]["operator_action"]["instructions"],
        "'Check item #1 and #2'",
    )
    self.assertEqual(schema._unescape_proto_string(r"\\n"), r"\n")
    self.assertEqual(schema._unescape_proto_string(r"\n"), "\n")

  def test_single_secret_key_in_textproto_and_from_dict(self) -> None:
    parsed = schema.parse_lightflow_text(
        'name: "wf"\nsecret_keys: "my_secret_token"\n',
        fmt="textproto",
    )
    self.assertEqual(list(parsed.secret_keys), ["my_secret_token"])
    from_dict = schema.Lightflow.from_dict(
        {"name": "wf", "secret_keys": "my_secret_token"}
    )
    self.assertEqual(list(from_dict.secret_keys), ["my_secret_token"])

  def test_passport_and_stamp_from_dict_accept_camelcase_proto_json(
      self,
  ) -> None:
    passport = schema.Passport.from_dict({
        "lightflowName": "g3_wf",
        "lightflowPath": "/abs/lightflow.textproto",
        "passportVersion": 1,
        "stamps": [{
            "stageName": "gate",
            "status": "PAUSED",
            "resumeCommand": "lightflow resume --stage=gate",
        }],
    })
    self.assertEqual(passport.lightflow_name, "g3_wf")
    self.assertEqual(passport.lightflow_path, "/abs/lightflow.textproto")
    self.assertEqual(len(passport.stamps), 1)
    self.assertEqual(passport.stamps[0].stage_name, "gate")
    self.assertEqual(passport.stamps[0].status, schema.StampStatus.PAUSED)
    self.assertEqual(
        passport.stamps[0].resume_command, "lightflow resume --stage=gate"
    )


class SafeCelEvaluatorTest(unittest.TestCase):
  """Tests the AST-based SafeCelEvaluator."""

  def test_boolean_and_null_safe_methods(self) -> None:
    payload = {
        "env": "production",
        "items": ["a", "b", "c"],
        "outputs": {"fetch": {"row_count": 500}},
        "null_field": None,
    }
    self.assertTrue(
        engine.evaluate_cel(
            "payload.env.startsWith('prod') && payload.items.size() == 3",
            payload,
        )
    )
    self.assertTrue(
        engine.evaluate_cel(
            "has(payload.outputs.fetch.row_count) && !has(payload.missing.key)",
            payload,
        )
    )
    self.assertFalse(
        engine.evaluate_cel("payload.null_field.contains('x')", payload)
    )
    self.assertEqual(
        engine.evaluate_cel(
            "'Rows: ' + string(payload.outputs.fetch.row_count) +"
            " string(payload.null_field)",
            payload,
        ),
        "Rows: 500",
    )
    self.assertEqual(
        engine.evaluate_cel(
            "'Rows: ' + payload.outputs.fetch.row_count",
            payload,
        ),
        "Rows: 500",
    )
    self.assertEqual(engine.evaluate_cel("int('42') + int(null)", payload), 42)
    self.assertEqual(engine.evaluate_cel("double('3.5')", payload), 3.5)
    self.assertTrue(engine.evaluate_cel("bool(1) && !bool(0)", payload))
    self.assertFalse(engine.evaluate_cel("!payload.env == true", payload))
    self.assertTrue(engine.evaluate_cel("false == !payload.env", payload))
    self.assertEqual(
        engine.evaluate_cel(
            "payload.outputs.fetch.row_count + ' rows'", payload
        ),
        "500 rows",
    )
    self.assertFalse(
        engine.evaluate_cel("payload.outputs.missing_stage.count > 0", payload)
    )

  def test_blocks_private_attributes_and_subscripts_and_oom_mul(self) -> None:
    with self.assertRaises(engine.EngineError):
      engine.evaluate_cel("payload.__class__", {"a": 1})
    with self.assertRaises(engine.EngineError):
      engine.evaluate_cel("payload['_secret']", {"_secret": "123"})
    self.assertFalse(
        engine.evaluate_cel("has(payload['_secret'])", {"_secret": "123"})
    )
    with self.assertRaises(engine.EngineError):
      engine.evaluate_cel("'a' * 1000000", {})
    with self.assertRaises(engine.EngineError):
      engine.evaluate_cel("payload.env.upper", {"env": "prod"})

  def test_negated_membership_operator_in_cel(self) -> None:
    self.assertTrue(
        engine.evaluate_cel(
            "!payload.env in ['dev', 'staging']", {"env": "prod"}
        )
    )
    self.assertFalse(
        engine.evaluate_cel(
            "!payload.env in ['dev', 'staging']", {"env": "dev"}
        )
    )
    self.assertTrue(
        engine.evaluate_cel(
            "!!payload.env in ['dev', 'staging']", {"env": "dev"}
        )
    )
    self.assertFalse(
        engine.evaluate_cel(
            "!!payload.env in ['dev', 'staging']", {"env": "prod"}
        )
    )
    self.assertTrue(
        engine.evaluate_cel(
            "!payload.env not in ['dev', 'staging']", {"env": "dev"}
        )
    )
    self.assertFalse(
        engine.evaluate_cel(
            "!payload.env not in ['dev', 'staging']", {"env": "prod"}
        )
    )

  def test_import_prefix_boundary_and_duplicate_stage_guard(self) -> None:
    dup_wf = schema.Lightflow(
        name="dup",
        stages=[schema.Stage(name="s1"), schema.Stage(name="s1")],
    )
    with self.assertRaises(engine.EngineError):
      engine.LightflowEngine(dup_wf).compile()

    both_actions_wf = schema.Lightflow(
        name="both_actions",
        actions=[schema.ActionDefinition(id="fn", python_import="mod.fn")],
        stages=[
            schema.Stage(
                name="s1",
                python_action=schema.PythonAction(action_id="fn"),
                operator_action=schema.OperatorAction(instructions="'Approve'"),
            )
        ],
    )
    with self.assertRaises(engine.EngineError):
      engine.LightflowEngine(both_actions_wf).compile()

    no_action_wf = schema.Lightflow(
        name="no_action",
        stages=[schema.Stage(name="empty_stage")],
    )
    with self.assertRaisesRegex(engine.EngineError, "has no defined action"):
      engine.LightflowEngine(no_action_wf).compile()

    poll_retry_wf = schema.Lightflow(
        name="poll_retry",
        actions=[schema.ActionDefinition(id="fn", python_import="mod.fn")],
        stages=[
            schema.Stage(
                name="s1",
                polling_policy=schema.PollingPolicy(
                    condition="true",
                    poll_tick_action=schema.PythonAction(action_id="fn"),
                ),
                retry_policy=schema.RetryPolicy(max_attempts=2),
            )
        ],
    )
    with self.assertRaisesRegex(
        engine.EngineError, "combines polling_policy with retry_policy"
    ):
      engine.LightflowEngine(poll_retry_wf).compile()

    both_tick_wf = schema.Lightflow(
        name="both_tick",
        actions=[schema.ActionDefinition(id="fn", python_import="mod.fn")],
        stages=[
            schema.Stage(
                name="s1",
                python_action=schema.PythonAction(action_id="fn"),
                polling_policy=schema.PollingPolicy(
                    condition="true",
                    poll_tick_action=schema.PythonAction(action_id="fn"),
                ),
            )
        ],
    )
    with self.assertRaisesRegex(
        engine.EngineError,
        "defines both python_action and polling_policy.poll_tick_action",
    ):
      engine.LightflowEngine(both_tick_wf).compile()

    wf = schema.Lightflow(
        name="prefix_guard",
        actions=[
            schema.ActionDefinition(
                id="evil", python_import="my_app_evil.mod.fn"
            ),
            schema.ActionDefinition(
                id="priv", python_import="my_app.mod._private_fn"
            ),
        ],
        stages=[
            schema.Stage(
                name="s1", python_action=schema.PythonAction(action_id="evil")
            )
        ],
    )
    eng = engine.LightflowEngine(wf, allowed_import_prefixes=["my_app"])
    with self.assertRaises(engine.EngineError):
      eng._execute_action("s1", schema.PythonAction(action_id="evil"), {})
    with self.assertRaises(engine.EngineError):
      eng._execute_action("s1", schema.PythonAction(action_id="priv"), {})


class LightflowEngineExecutionTest(unittest.TestCase):
  """Tests compilation, execution, retry, polling, rollback, and gates."""

  def setUp(self) -> None:
    super().setUp()
    os.environ.pop("ANTIGRAVITY_CONVERSATION_ID", None)
    _CALL_LOG.clear()
    global _FLAKY_COUNTER, _POLL_COUNTER
    _FLAKY_COUNTER = 0
    _POLL_COUNTER = 0

  def test_execute_action_and_suspend_at_operator_gate(self) -> None:
    module_name = __name__
    yaml_manifest = f"""
name: gate_test_wf
actions:
  - id: fetch
    python_import: {module_name}.sample_fetch
  - id: publish
    python_import: {module_name}.sample_publish
stages:
  - name: fetch_stage
    python_action:
      action_id: fetch
      static_kwargs:
        limit: 42.0
  - name: review_gate
    run_after: [fetch_stage]
    operator_action:
      instructions: "'Please approve ' + payload.outputs.fetch_stage.summary"
  - name: publish_stage
    run_after: [review_gate]
    run_if: "payload.outputs.review_gate.approved == true"
    python_action:
      action_id: publish
"""
    wf = schema.parse_lightflow_text(yaml_manifest)
    eng = engine.LightflowEngine(
        wf, lightflow_path="workflow.yaml", log_id="run_1"
    )
    order = eng.compile()
    self.assertEqual(order, ["fetch_stage", "review_gate", "publish_stage"])

    passport = schema.Passport()
    passport = eng.execute_stage("fetch_stage", passport)
    self.assertEqual(passport.payload["row_count"], 42)
    self.assertEqual(
        passport.payload["outputs"]["fetch_stage"]["summary"], "42 rows"
    )

    with self.assertRaises(engine.OperatorActionSuspended) as ctx:
      eng.execute_stage("review_gate", passport)
    self.assertIn("Please approve 42 rows", str(ctx.exception))
    self.assertIn(
        "Operator action: follow the Instructions", str(ctx.exception)
    )
    self.assertEqual(passport.stamps[-1].status, schema.StampStatus.PAUSED)
    self.assertIn("resume", passport.stamps[-1].resume_command)
    # The schema lists `approved` under `required` but the resume command
    # omits it; the briefing must say why, right under the command.
    self.assertIn(
        "--resolution=APPROVE records approved=true", str(ctx.exception)
    )

  def test_resume_note_names_remaining_payload_keys(self) -> None:
    note = engine.resume_note(
        '{"type": "object", "required": ["approved", "ticket", "tier"]}'
    )
    self.assertIn("records approved=true for you", note)
    self.assertIn("--payload only needs: ticket, tier.", note)
    self.assertIn('--resolution=REJECT --comment="<reason>"', note)
    # Only `approved` required: no payload at all.
    self.assertIn(
        "no --payload is needed.",
        engine.resume_note('{"type": "object", "required": ["approved"]}'),
    )
    self.assertIn("no --payload is needed.", engine.resume_note(""))
    # Unparseable schema: still explains APPROVE, defers on the key list.
    self.assertIn("the keys the schema requires", engine.resume_note("{nope"))
    # A malformed `required` (not a list) must not be iterated char by char.
    self.assertIn(
        "the keys the schema requires",
        engine.resume_note('{"type": "object", "required": "approved"}'),
    )

  def test_retry_polling_rollback_and_all_done_trigger(self) -> None:
    module_name = __name__
    yaml_manifest = f"""
name: resilience_wf
actions:
  - id: flaky
    python_import: {module_name}.sample_flaky
  - id: poll_tick
    python_import: {module_name}.sample_poll_tick
  - id: fail_act
    python_import: {module_name}.sample_fail
  - id: rollback_act
    python_import: {module_name}.sample_rollback
  - id: cleanup_act
    python_import: {module_name}.sample_cleanup_done
stages:
  - name: flaky_stage
    python_action:
      action_id: flaky
    retry_policy:
      max_attempts: 3
      initial_backoff_seconds: 1
  - name: poll_stage
    run_after: [flaky_stage]
    polling_policy:
      condition: "payload.job_status == 'READY'"
      interval_seconds: 1
      max_attempts: 5
      poll_tick_action:
        action_id: poll_tick
  - name: failing_stage
    run_after: [poll_stage]
    python_action:
      action_id: fail_act
    rollback_action:
      action_id: rollback_act
  - name: skipped_downstream
    run_after: [failing_stage]
    python_action:
      action_id: flaky
  - name: always_cleanup
    run_after: [failing_stage]
    trigger_rule: ALL_DONE
    python_action:
      action_id: cleanup_act
"""
    wf = schema.parse_lightflow_text(yaml_manifest)
    eng = engine.LightflowEngine(wf)
    order = eng.compile()
    passport = schema.Passport()
    for st_name in order:
      passport = eng.execute_stage(st_name, passport)

    self.assertTrue(passport.payload.get("flaky_ok"))
    self.assertEqual(passport.payload.get("job_status"), "READY")
    self.assertTrue(passport.payload.get("rolled_back"))
    self.assertTrue(
        passport.payload.get("outputs", {})
        .get("failing_stage", {})
        .get("rolled_back")
    )
    self.assertTrue(passport.payload.get("cleanup_ran"))

    latest = {s.stage_name: s.status for s in passport.stamps}
    self.assertEqual(latest["flaky_stage"], schema.StampStatus.COMPLETED)
    self.assertEqual(latest["poll_stage"], schema.StampStatus.COMPLETED)
    self.assertEqual(latest["failing_stage"], schema.StampStatus.FAILED)
    self.assertEqual(latest["skipped_downstream"], schema.StampStatus.FAILED)
    self.assertEqual(latest["always_cleanup"], schema.StampStatus.COMPLETED)

  def test_poll_tick_budget_picks_the_tighter_bound(self) -> None:
    # No polling budget: only the stage timeout applies (or nothing).
    self.assertEqual(engine.poll_tick_budget(None, None), (None, False))
    self.assertEqual(engine.poll_tick_budget(0, None), (None, False))
    self.assertEqual(engine.poll_tick_budget(7, None), (7, False))
    # Polling budget only: rounded up, never below one second.
    self.assertEqual(engine.poll_tick_budget(None, 59.2), (60, True))
    self.assertEqual(engine.poll_tick_budget(None, 0.01), (1, True))
    self.assertEqual(engine.poll_tick_budget(None, -3.0), (1, True))
    # Both: the tighter one wins and says which it was.
    self.assertEqual(engine.poll_tick_budget(2, 60.0), (2, False))
    self.assertEqual(engine.poll_tick_budget(60, 1.5), (2, True))
    self.assertEqual(engine.poll_tick_budget(2, 2.0), (2, True))

  def test_polling_budget_bounds_a_hung_tick_and_yields(self) -> None:
    # No stage timeout: before the fix the tick ran inline and unbounded.
    yaml_manifest = f"""
name: poll_budget_wf
actions:
  - id: hung_tick
    python_import: {__name__}.sample_hung_tick
  - id: rb
    python_import: {__name__}.sample_rollback_marker
stages:
  - name: poll_stage
    rollback_action: {{"action_id": "rb"}}
    polling_policy:
      condition: "payload.job_status == 'READY'"
      interval_seconds: 1
      timeout_seconds: 1
      poll_tick_action:
        action_id: hung_tick
        static_kwargs: {{"sleep_seconds": 2}}
"""
    wf = schema.parse_lightflow_text(yaml_manifest)
    start = time.monotonic()
    passport = engine.LightflowEngine(wf).execute_stage(
        "poll_stage", schema.Passport()
    )

    # Bounded by the 1s polling budget, not the 2s tick.
    self.assertLess(time.monotonic() - start, 2.5)
    latest = passport.stamps[-1]
    self.assertEqual(latest.status, schema.StampStatus.FAILED)
    # Running out of budget mid-tick is a yield, exactly like running out
    # between ticks: resumable, and no rollback.
    self.assertEqual(latest.reason, schema.Stamp.POLL_YIELD)
    self.assertIn("polling timed out", latest.message)
    self.assertIn("poll tick was still running", latest.message)
    self.assertIn("Yielding control back to operator/agent", latest.message)
    self.assertNotIn("sample_rollback_marker", _CALL_LOG)

  def test_polling_stage_timeout_tighter_than_budget_is_a_hard_failure(
      self,
  ) -> None:
    # 1.5s: past the 1s stage timeout, but done within the rollback grace.
    yaml_manifest = f"""
name: poll_stage_timeout_wf
actions:
  - id: hung_tick
    python_import: {__name__}.sample_hung_tick
  - id: rb
    python_import: {__name__}.sample_rollback_marker
stages:
  - name: poll_stage
    timeout_seconds: 1
    rollback_action: {{"action_id": "rb"}}
    polling_policy:
      condition: "payload.job_status == 'READY'"
      interval_seconds: 1
      timeout_seconds: 60
      poll_tick_action:
        action_id: hung_tick
        static_kwargs: {{"sleep_seconds": 1.5}}
"""
    wf = schema.parse_lightflow_text(yaml_manifest)
    passport = engine.LightflowEngine(wf).execute_stage(
        "poll_stage", schema.Passport()
    )

    latest = passport.stamps[-1]
    self.assertEqual(latest.status, schema.StampStatus.FAILED)
    self.assertNotEqual(latest.reason, schema.Stamp.POLL_YIELD)
    self.assertIn("exceeded timeout_seconds=1", latest.message)
    # The stage timeout keeps its failure semantics: rollback ran (once the
    # abandoned tick finished).
    self.assertIn("sample_rollback_marker", _CALL_LOG)

  def test_build_resume_command_quotes_args_with_spaces(self) -> None:
    wf = schema.Lightflow(
        name="wf",
        stages=[
            schema.Stage(
                name="stage with space",
                operator_action=schema.OperatorAction(instructions="Approve?"),
            )
        ],
    )
    eng = engine.LightflowEngine(
        wf, lightflow_path="dir with space/lf.yaml", log_id="<log_id>"
    )
    op = wf.stages[0].operator_action
    assert op is not None
    # No --lightflow: the run records its manifest, so the command is valid
    # from any cwd and after the manifest moves.
    self.assertEqual(
        eng.build_resume_command("stage with space", op),
        "lightflow resume"
        " --log_id=<log_id>"
        " --stage='stage with space'"
        " --resolution=APPROVE",
    )

  def test_build_resume_command_payload_placeholder_from_schema(self) -> None:
    def _cmd(json_schema: str) -> str:
      wf = schema.Lightflow(
          name="wf",
          stages=[
              schema.Stage(
                  name="gate",
                  operator_action=schema.OperatorAction(
                      instructions="'Approve?'", json_schema=json_schema
                  ),
              )
          ],
      )
      eng = engine.LightflowEngine(wf, lightflow_path="lf.yaml", log_id="r1")
      op = wf.stages[0].operator_action
      assert op is not None
      return eng.build_resume_command("gate", op)

    # Only `approved` required: --resolution covers it, so no --payload.
    self.assertEqual(
        _cmd('{"type": "object", "required": ["approved"]}'),
        "lightflow resume --log_id=r1 --stage=gate --resolution=APPROVE",
    )
    # Extra required keys get typed, editable placeholders.
    self.assertEqual(
        _cmd(
            '{"type": "object", "required": ["approved", "ticket", "count",'
            ' "tier"], "properties": {"ticket": {"type": "string"},'
            ' "count": {"type": "integer"},'
            ' "tier": {"type": "string", "enum": ["low", "high"]}}}'
        ),
        "lightflow resume --log_id=r1 --stage=gate --resolution=APPROVE"
        ' --payload=\'{"ticket": "<string>", "count": <integer>,'
        ' "tier": "<low|high>"}\'',
    )
    # A single legal value (`const`, or a one-item `enum`) is written out, so
    # the command pastes as-is; non-string choices are not quoted.
    self.assertEqual(
        _cmd(
            '{"type": "object", "required": ["approved", "ack", "seen", "n"],'
            ' "properties": {"ack": {"type": "boolean", "const": true},'
            ' "seen": {"type": "boolean", "enum": [true]},'
            ' "n": {"type": "integer", "enum": [1, 2]}}}'
        ),
        "lightflow resume --log_id=r1 --stage=gate --resolution=APPROVE"
        ' --payload=\'{"ack": true, "seen": true, "n": <1|2>}\'',
    )
    # Unparseable schema falls back to the generic placeholder.
    self.assertIn(
        "--payload='<JSON matching the stage json_schema>'", _cmd("{not json")
    )
    with self.assertRaisesRegex(
        engine.EngineError, "Keyword arguments are not supported"
    ):
      engine.evaluate_cel('int("10", base=16) == 16', {})

  def test_cel_ternary_and_dict_get_and_compile_ast_validation(self) -> None:
    p = {"prod": True, "n": 0, "env": "staging", "null_map": None, "_s": "x"}
    for expr, expected in (
        ("payload.prod ? 'live' : 'dry'", "live"),
        ("'Mode: ' + (payload.n > 0 ? 'non-empty' : 'empty')", "Mode: empty"),
        ("payload.n > 10 ? 'hi' : payload.prod ? 'prod-0' : 'lo'", "prod-0"),
        ("has(payload.missing) ? payload.missing : 'fallback'", "fallback"),
        ("'yes' if payload.prod else 'no'", "yes"),
        ("payload.get('env', 'dev')", "staging"),
        ("payload.get('missing', 'dev')", "dev"),
        ("payload.get('missing')", None),
        ("payload.null_map.get('k', 'def')", "def"),
        ("payload.prod &&\n  payload.env == 'staging'", True),
    ):
      self.assertEqual(engine.evaluate_cel(expr, p, strict=True), expected)
    with self.assertRaisesRegex(engine.EngineError, "private key '_s'"):
      engine.evaluate_cel("payload.get('_s', 'x')", p)
    for bad in ("[x for x in payload.a]", "(lambda x: x)(1)", "bad_fn(1)"):
      with self.assertRaisesRegex(engine.EngineError, "not a parseable"):
        engine.LightflowEngine(
            schema.parse_lightflow_text(
                f'name: w\nstages:\n  - name: s\n    run_if: "{bad}"\n'
                "    operator_action:\n      instructions: \"'ok'\"\n"
            )
        ).compile()

  def test_compile_validates_payload_outputs_and_isolates_workflow_modules(
      self,
  ) -> None:
    wf_bad = schema.parse_lightflow_text(
        "name: bad_outputs\n"
        "stages:\n"
        "  - name: s1\n"
        "    operator_action:\n"
        "      instructions: \"'Step 1'\"\n"
        "  - name: s2\n"
        "    run_if: 'payload.outputs.s1.approved == true'\n"
        "    operator_action:\n"
        "      instructions: \"'Step 2'\"\n"
    )
    with self.assertRaisesRegex(
        engine.EngineError, "not an upstream dependency in `run_after`"
    ):
      engine.LightflowEngine(wf_bad).compile()

    with tempfile.TemporaryDirectory() as tmpdir:
      dir_a = os.path.join(tmpdir, "wf_a")
      dir_b = os.path.join(tmpdir, "wf_b")
      os.makedirs(dir_a, exist_ok=True)
      os.makedirs(dir_b, exist_ok=True)
      for workflow_dir, tag in ((dir_a, "from_a"), (dir_b, "from_b")):
        with open(
            os.path.join(workflow_dir, "actions.py"), "w", encoding="utf-8"
        ) as f:
          f.write(
              "def step(payload, **kwargs):\n"
              f"  return {{'origin': '{tag}'}}, '{tag}'\n"
          )
        with open(
            os.path.join(workflow_dir, "lightflow.yaml"), "w", encoding="utf-8"
        ) as f:
          f.write(
              "name: wf\n"
              "actions:\n"
              "  - id: step\n"
              "    python_import: actions.step\n"
              "stages:\n"
              "  - name: s1\n"
              "    python_action:\n"
              "      action_id: step\n"
          )

      wf_a = schema.load_lightflow(dir_a)
      eng_a = engine.LightflowEngine(wf_a, lightflow_path=dir_a)
      eng_a.compile()
      p_a = eng_a.execute_stage("s1", schema.Passport())
      self.assertEqual(p_a.payload["origin"], "from_a")

      wf_b = schema.load_lightflow(dir_b)
      eng_b = engine.LightflowEngine(wf_b, lightflow_path=dir_b)
      eng_b.compile()
      p_b = eng_b.execute_stage("s1", schema.Passport())
      self.assertEqual(p_b.payload["origin"], "from_b")


class NoneFailedTriggerRuleTest(unittest.TestCase):
  """NONE_FAILED: runs after skips, waits for stragglers, cascades failures."""

  def _engine_and_join(self):
    wf = schema.Lightflow.from_dict({
        "name": "nf",
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
    return eng, wf.stages[2]

  def _passport(self, **statuses: schema.StampStatus) -> schema.Passport:
    pp = schema.Passport()
    for name, st in statuses.items():
      stamp = pp.stamps.add()
      stamp.stage_name = name
      stamp.status = st
    return pp

  def test_completed_plus_skipped_runs(self) -> None:
    eng, join = self._engine_and_join()
    ok, _, _ = eng.evaluate_trigger_rule(
        join,
        self._passport(
            a=schema.StampStatus.COMPLETED, b=schema.StampStatus.SKIPPED
        ),
    )
    self.assertTrue(ok)

  def test_completed_plus_failed_is_blocked(self) -> None:
    eng, join = self._engine_and_join()
    ok, status, reason = eng.evaluate_trigger_rule(
        join,
        self._passport(
            a=schema.StampStatus.COMPLETED, b=schema.StampStatus.FAILED
        ),
    )
    self.assertFalse(ok)
    self.assertEqual(status, schema.StampStatus.FAILED)
    self.assertEqual(reason, engine.UPSTREAM_FAILED_MESSAGE)

  def test_failed_plus_unstarted_waits_for_the_straggler(self) -> None:
    eng, join = self._engine_and_join()
    ok, status, _ = eng.evaluate_trigger_rule(
        join, self._passport(a=schema.StampStatus.FAILED)
    )
    self.assertFalse(ok)
    self.assertNotEqual(status, schema.StampStatus.FAILED)


if __name__ == "__main__":
  unittest.main()
