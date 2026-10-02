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
  from ..lightflow import mcp_server
  from ..lightflow import runner
  from ..lightflow import schema
except (ImportError, ValueError):
  from lightflow import engine  # pyrefly: ignore[missing-import]
  from lightflow import lib  # pyrefly: ignore[missing-import]
  from lightflow import mcp_server  # pyrefly: ignore[missing-import]
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

      def reconfigure(
          self, *, encoding: str = "", errors: str = ""
      ) -> None:
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
    # clear ValueError rather than TypeError: got multiple values for 'lightflow'
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
         "properties": {"reviewer": {"type": "string"}},
         "required": ["reviewer"]}
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

    # gate_1 inherits "project" from initial start --payload, and receives "reviewer"
    with self.assertRaises(engine.OperatorActionSuspended):
      cli.resume(
          lightflow=wf,
          log_id="two_gates_run",
          stage="gate_1",
          payload='{"reviewer": "alice"}',
      )

    # gate_2 must NOT inherit "reviewer" from gate_1's output
    with self.assertRaisesRegex(
        ValueError, "failed JSON schema validation.*'reviewer'"
    ):
      cli.resume(
          lightflow=wf,
          log_id="two_gates_run",
          stage="gate_2",
          payload="{}",
      )

    # Supplying "reviewer" explicitly for gate_2 succeeds
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
    self.assertEqual(p_dict["outputs"]["gate_2"]["reviewer"], "bob")

  def test_local_action_module_reloads_when_edited_on_disk(self) -> None:
    wf_dir = os.path.join(self.temp_dir, "hot_reload_wf")
    os.makedirs(wf_dir, exist_ok=True)
    actions_py = os.path.join(wf_dir, "hot_actions.py")
    with open(actions_py, "w", encoding="utf-8") as f:
      f.write(
          "def run_step(payload, **kwargs):\n"
          "  raise RuntimeError('buggy v1')\n"
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
    with self.assertRaisesRegex(engine.EngineError, "s1"):
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
    self.assertIn("timed out after 1s", p_timeout.stamps[-1].message)


if __name__ == "__main__":
  unittest.main()
