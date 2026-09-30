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

"""Regression tests for the v0.1 hardening pass (timeouts, CEL caps, MCP input validation)."""

from __future__ import annotations

# pylint: disable=missing-function-docstring,protected-access,unused-argument

import os
import shutil
import sys
import tempfile
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
  from ..lightflow import mcp_server
  from ..lightflow import schema
except (ImportError, ValueError):
  from lightflow import engine  # pyrefly: ignore[missing-import]
  from lightflow import lib  # pyrefly: ignore[missing-import]
  from lightflow import mcp_server  # pyrefly: ignore[missing-import]
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
    with self.assertRaises(engine.EngineError):
      cli.start(lightflow=manifest, log_id="chain_run")
    _STAGE_2_SHOULD_FAIL = False
    # cascade=False surgically re-runs only 'two'; 'three' stays FAILED until
    # resumed.
    with self.assertRaises(engine.EngineError):
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

  # --- mcp: input validation never crashes the loop -----------------------

  def test_tools_call_validates_params_before_dispatch(self) -> None:
    def call(params: Any) -> dict[str, Any]:
      resp = mcp_server.handle_jsonrpc_message(
          {"jsonrpc": "2.0", "id": 9, "method": "tools/call", "params": params}
      )
      assert resp is not None
      return resp

    self.assertEqual(call("garbage")["error"]["code"], -32602)
    self.assertEqual(call({"name": 42})["error"]["code"], -32602)
    unknown = call({"name": "nope", "arguments": {}})["error"]
    self.assertEqual(unknown["code"], -32602)
    self.assertIn("run_lightflow", unknown["message"])
    missing = call({"name": "run_lightflow", "arguments": {"log_id": "x"}})
    self.assertEqual(missing["error"]["code"], -32602)
    self.assertIn("lightflow", missing["error"]["message"])
    empty_req = call({
        "name": "run_lightflow",
        "arguments": {"lightflow": "  ", "log_id": "x"},
    })
    self.assertEqual(empty_req["error"]["code"], -32602)
    self.assertEqual(
        call({"name": "run_lightflow", "arguments": "nope"})["error"]["code"],
        -32602,
    )

  def test_tools_call_handler_exception_is_an_internal_error(self) -> None:
    with mock.patch.object(
        mcp_server, "call_tool", side_effect=RuntimeError("unexpected crash")
    ):
      resp = mcp_server.handle_jsonrpc_message({
          "jsonrpc": "2.0",
          "id": 10,
          "method": "tools/call",
          "params": {
              "name": "dry_run_lightflow",
              "arguments": {"lightflow": "examples/hn_digest"},
          },
      })
    assert resp is not None
    self.assertEqual(resp["error"]["code"], -32603)
    self.assertIn("RuntimeError: unexpected crash", resp["error"]["message"])

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


if __name__ == "__main__":
  unittest.main()
