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
    self.assertIn("Operator Action Required:", str(ctx.exception))
    self.assertIn("never decide for them", str(ctx.exception))
    self.assertEqual(passport.stamps[-1].status, schema.StampStatus.PAUSED)
    self.assertIn("resume", passport.stamps[-1].resume_command)

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
    self.assertEqual(
        eng.build_resume_command("stage with space", op),
        "lightflow resume"
        " --lightflow='dir with space/lf.yaml'"
        " --log_id=<log_id>"
        " --stage='stage with space'"
        " --resolution=APPROVE",
    )
    with self.assertRaisesRegex(
        engine.EngineError, "Keyword arguments are not supported"
    ):
      engine.evaluate_cel('int("10", base=16) == 16', {})

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


if __name__ == "__main__":
  unittest.main()
