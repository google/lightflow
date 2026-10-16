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

"""Unit tests for the Lightflow JSON-RPC 2.0 Stdio MCP Server (Claude & Antigravity)."""

from __future__ import annotations

# pylint: disable=g-import-not-at-top,g-bad-import-order,missing-function-docstring,protected-access

import io
import json
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
  from ..lightflow import mcp_server
except (ImportError, ValueError):
  from lightflow import mcp_server  # pyrefly: ignore[missing-import]


def mcp_test_action(
    payload: dict[str, Any], **static_kwargs: Any
) -> tuple[dict[str, Any], str]:
  del payload, static_kwargs
  print("Action stdout message that must not break JSON-RPC")
  return {"computed": 99, "summary": "99 items"}, "Computed 99 items"


class McpServerJsonRpcTest(unittest.TestCase):
  """Tests JSON-RPC 2.0 protocol and all 5 MCP tools."""

  def setUp(self) -> None:
    super().setUp()
    os.environ.pop("ANTIGRAVITY_CONVERSATION_ID", None)
    self.temp_dir = tempfile.mkdtemp(prefix="lightflow_mcp_test_")
    self.old_env = os.environ.get("LIGHTFLOW_STATE_DIR")
    os.environ["LIGHTFLOW_STATE_DIR"] = self.temp_dir

  def tearDown(self) -> None:
    if self.old_env is None:
      os.environ.pop("LIGHTFLOW_STATE_DIR", None)
    else:
      os.environ["LIGHTFLOW_STATE_DIR"] = self.old_env
    shutil.rmtree(self.temp_dir, ignore_errors=True)
    super().tearDown()

  def test_jsonrpc_initialize_and_tools_list(self) -> None:
    init_resp = mcp_server.handle_jsonrpc_message(
        {"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {}}
    )
    self.assertIsNotNone(init_resp)
    self.assertEqual(init_resp["result"]["serverInfo"]["name"], "lightflow-mcp")

    list_resp = mcp_server.handle_jsonrpc_message(
        {"jsonrpc": "2.0", "id": 2, "method": "tools/list"}
    )
    self.assertIsNotNone(list_resp)
    tool_names = [t["name"] for t in list_resp["result"]["tools"]]
    self.assertEqual(
        tool_names,
        [
            "run_lightflow",
            "resume_lightflow",
            "get_lightflow_status",
            "list_lightflow_runs",
            "dry_run_lightflow",
            "visualize_lightflow",
        ],
    )

  def test_mcp_lightflow_lifecycle_run_suspend_status_resume(self) -> None:
    module_name = __name__
    manifest_path = os.path.join(self.temp_dir, "mcp_wf.yaml")
    with open(manifest_path, "w", encoding="utf-8") as f:
      f.write(f"""
name: mcp_test_pipeline
actions:
  - id: compute
    python_import: {module_name}.mcp_test_action
stages:
  - name: compute_stage
    python_action:
      action_id: compute
  - name: confirm_gate
    run_after: [compute_stage]
    operator_action:
      instructions: "'Confirm ' + payload.outputs.compute_stage.summary"
      json_schema: '{{"type": "object", "required": ["confirmed"]}}'
  - name: final_stage
    run_after: [confirm_gate]
    run_if: "payload.outputs.confirm_gate.confirmed == true"
    python_action:
      action_id: compute
""")

    # 1. Dry Run
    dry_resp = mcp_server.handle_jsonrpc_message({
        "jsonrpc": "2.0",
        "id": 10,
        "method": "tools/call",
        "params": {
            "name": "dry_run_lightflow",
            "arguments": {"lightflow": manifest_path},
        },
    })
    assert dry_resp is not None
    dry_data = json.loads(dry_resp["result"]["content"][0]["text"])
    self.assertTrue(dry_data["valid"])

    # 2. Run Lightflow -> Suspends at confirm_gate (exit_code=2, isError=False)
    run_resp = mcp_server.handle_jsonrpc_message({
        "jsonrpc": "2.0",
        "id": 11,
        "method": "tools/call",
        "params": {
            "name": "run_lightflow",
            "arguments": {
                "lightflow": manifest_path,
                "log_id": "mcp_run_01",
            },
        },
    })
    assert run_resp is not None
    self.assertFalse(run_resp["result"]["isError"])
    run_data = json.loads(run_resp["result"]["content"][0]["text"])
    self.assertEqual(run_data["status"], "SUSPENDED")
    self.assertEqual(run_data["exit_code"], 2)
    self.assertEqual(run_data["paused_stage"], "confirm_gate")
    self.assertIn("Confirm 99 items", run_data["instructions"])
    self.assertEqual(
        run_data["json_schema"],
        {"type": "object", "required": ["confirmed"]},
    )
    # Compact: per-stage list + stdout, no diagram (ask status verbose=true).
    self.assertNotIn("mermaid_diagram", run_data)
    self.assertEqual(run_data["state"], "SUSPENDED")
    run_stage_status = {s["name"]: s["status"] for s in run_data["stages"]}
    self.assertEqual(run_stage_status["confirm_gate"], "PAUSED")
    self.assertIn("[compute_stage] Computed 99 items", run_data["stdout"])

    # 3. Get Lightflow Status
    status_resp = mcp_server.handle_jsonrpc_message({
        "jsonrpc": "2.0",
        "id": 12,
        "method": "tools/call",
        "params": {
            "name": "get_lightflow_status",
            "arguments": {
                "lightflow": manifest_path,
                "log_id": "mcp_run_01",
            },
        },
    })
    assert status_resp is not None
    self.assertFalse(status_resp["result"]["isError"])
    status_data = json.loads(status_resp["result"]["content"][0]["text"])
    self.assertTrue(status_data["exists"])
    self.assertEqual(status_data["paused_stage"], "confirm_gate")
    self.assertEqual(status_data["exit_code"], 2)
    self.assertEqual(status_data["state"], "SUSPENDED")
    # Compact by default: a per-stage list, no stamp log / diagram.
    self.assertEqual(
        [(s["name"], s["status"]) for s in status_data["stages"]],
        [
            ("compute_stage", "COMPLETED"),
            ("confirm_gate", "PAUSED"),
            ("final_stage", "NOT_STARTED"),
        ],
    )
    self.assertNotIn("mermaid_diagram", status_data)
    self.assertNotIn("stamps", status_data)
    self.assertEqual(status_resp["result"]["structuredContent"], status_data)

    # 3a. verbose=true adds the diagram, the stamp log and the CLI text.
    verbose_resp = mcp_server.handle_jsonrpc_message({
        "jsonrpc": "2.0",
        "id": 121,
        "method": "tools/call",
        "params": {
            "name": "get_lightflow_status",
            "arguments": {
                "lightflow": manifest_path,
                "log_id": "mcp_run_01",
                "verbose": True,
            },
        },
    })
    assert verbose_resp is not None
    verbose_data = json.loads(verbose_resp["result"]["content"][0]["text"])
    self.assertIn("```mermaid", verbose_data["mermaid_diagram"])
    self.assertIn("stamps", verbose_data["passport"])

    # 3b. Status on an unknown log ID is a result, not a tool error.
    missing_resp = mcp_server.handle_jsonrpc_message({
        "jsonrpc": "2.0",
        "id": 121,
        "method": "tools/call",
        "params": {
            "name": "get_lightflow_status",
            "arguments": {
                "lightflow": manifest_path,
                "log_id": "mcp_run_missing",
            },
        },
    })
    assert missing_resp is not None
    self.assertFalse(missing_resp["result"]["isError"])
    missing_data = json.loads(missing_resp["result"]["content"][0]["text"])
    self.assertFalse(missing_data["exists"])
    self.assertEqual(missing_data["exit_code"], 3)

    # 4. Resume Lightflow -> Completes (exit_code=0)
    resume_resp = mcp_server.handle_jsonrpc_message({
        "jsonrpc": "2.0",
        "id": 13,
        "method": "tools/call",
        "params": {
            "name": "resume_lightflow",
            "arguments": {
                "lightflow": manifest_path,
                "log_id": "mcp_run_01",
                "stage": "confirm_gate",
                "resolution": "APPROVE",
                "payload": {"confirmed": True},
            },
        },
    })
    assert resume_resp is not None
    self.assertFalse(resume_resp["result"]["isError"])
    resume_data = json.loads(resume_resp["result"]["content"][0]["text"])
    self.assertEqual(resume_data["status"], "COMPLETED")
    self.assertEqual(resume_data["exit_code"], 0)

    # 5. Visualize without output file -> returns inline HTML (and infers
    # `lightflow` from `log_id` when omitted).
    viz_resp = mcp_server.handle_jsonrpc_message({
        "jsonrpc": "2.0",
        "id": 14,
        "method": "tools/call",
        "params": {
            "name": "visualize_lightflow",
            "arguments": {
                "log_id": "mcp_run_01",
            },
        },
    })
    assert viz_resp is not None
    self.assertFalse(viz_resp["result"]["isError"])
    viz_data = json.loads(viz_resp["result"]["content"][0]["text"])
    self.assertIsNotNone(viz_data["html"])
    self.assertIn("mcp_test_pipeline", viz_data["html"])

  def test_content_length_and_newline_stdio_framing(self) -> None:
    req = {"jsonrpc": "2.0", "id": 42, "method": "ping"}
    req_bytes = json.dumps(req).encode("utf-8")
    framed_input = (
        f"Content-Length: {len(req_bytes)}\r\n\r\n".encode("ascii")
        + req_bytes
        + b"\n"
        + json.dumps(
            {"jsonrpc": "2.0", "id": 43, "method": "resources/list"}
        ).encode("utf-8")
        + b"\n"
    )
    in_buf = io.BytesIO(framed_input)
    out_buf = io.BytesIO()
    mcp_server.process_stdio_stream(in_buf, out_buf)
    out_text = out_buf.getvalue().decode("utf-8")
    self.assertIn("Content-Length:", out_text)
    self.assertIn('"id": 42', out_text)
    self.assertIn('"resources": []', out_text)

  def test_main_redirects_sys_stdout_to_stderr_during_event_loop(self) -> None:
    req = (
        json.dumps({"jsonrpc": "2.0", "id": 99, "method": "ping"}).encode(
            "utf-8"
        )
        + b"\n"
    )
    fake_stdin = io.TextIOWrapper(io.BytesIO(req), encoding="utf-8")
    out_raw = io.BytesIO()
    fake_stdout = io.TextIOWrapper(out_raw, encoding="utf-8")
    fake_stderr = io.StringIO()

    orig_handle = mcp_server.handle_jsonrpc_message

    def noisy_handle(msg):
      print(" stray print from background thread ")
      return orig_handle(msg)

    with (
        mock.patch.object(mcp_server.sys, "stdin", fake_stdin),
        mock.patch.object(mcp_server.sys, "stdout", fake_stdout),
        mock.patch.object(mcp_server.sys, "stderr", fake_stderr),
        mock.patch.object(
            mcp_server, "handle_jsonrpc_message", side_effect=noisy_handle
        ),
    ):
      mcp_server.main()
      self.assertIs(mcp_server.sys.stdout, fake_stdout)

    self.assertIn("stray print from background thread", fake_stderr.getvalue())
    out_line = out_raw.getvalue().decode("utf-8").strip()
    parsed = json.loads(out_line)
    self.assertEqual(parsed["id"], 99)

  def test_mcp_list_runs_and_status_without_manifest(self) -> None:
    module_name = __name__
    manifest_path = os.path.join(self.temp_dir, "mcp_list_wf.yaml")
    with open(manifest_path, "w", encoding="utf-8") as f:
      f.write(f"""
name: mcp_list_pipeline
actions:
  - id: compute
    python_import: {module_name}.mcp_test_action
stages:
  - name: compute_stage
    python_action:
      action_id: compute
  - name: confirm_gate
    run_after: [compute_stage]
    operator_action:
      instructions: "'Confirm?'"
""")
    run_resp = mcp_server.handle_jsonrpc_message({
        "jsonrpc": "2.0",
        "id": 301,
        "method": "tools/call",
        "params": {
            "name": "run_lightflow",
            "arguments": {"lightflow": manifest_path, "log_id": "mcp_list_run"},
        },
    })
    assert run_resp is not None
    self.assertEqual(
        json.loads(run_resp["result"]["content"][0]["text"])["status"],
        "SUSPENDED",
    )

    list_resp = mcp_server.handle_jsonrpc_message({
        "jsonrpc": "2.0",
        "id": 302,
        "method": "tools/call",
        "params": {"name": "list_lightflow_runs", "arguments": {}},
    })
    assert list_resp is not None
    listed = json.loads(list_resp["result"]["content"][0]["text"])
    by_id = {r["log_id"]: r for r in listed["runs"]}
    self.assertIn("mcp_list_run", by_id)
    self.assertEqual(
        by_id["mcp_list_run"]["lightflow_name"], "mcp_list_pipeline"
    )
    self.assertEqual(by_id["mcp_list_run"]["state"], "PAUSED")
    self.assertEqual(
        by_id["mcp_list_run"]["lightflow_path"], os.path.abspath(manifest_path)
    )

    # `lightflow` is optional on get_lightflow_status for recorded runs.
    status_resp = mcp_server.handle_jsonrpc_message({
        "jsonrpc": "2.0",
        "id": 303,
        "method": "tools/call",
        "params": {
            "name": "get_lightflow_status",
            "arguments": {"log_id": "mcp_list_run"},
        },
    })
    assert status_resp is not None
    status_data = json.loads(status_resp["result"]["content"][0]["text"])
    self.assertFalse(status_resp["result"]["isError"])
    self.assertEqual(status_data["exit_code"], 2)
    self.assertEqual(status_data["paused_stage"], "confirm_gate")
    self.assertEqual(status_data["state"], "SUSPENDED")
    self.assertNotIn("mermaid_diagram", status_data)

    # Resume without `lightflow` either: the run remembers its manifest.
    resume_resp = mcp_server.handle_jsonrpc_message({
        "jsonrpc": "2.0",
        "id": 33,
        "method": "tools/call",
        "params": {
            "name": "resume_lightflow",
            "arguments": {
                "log_id": "mcp_list_run",
                "stage": "confirm_gate",
                "resolution": "APPROVE",
            },
        },
    })
    assert resume_resp is not None
    self.assertFalse(resume_resp["result"]["isError"])
    resume_data = json.loads(resume_resp["result"]["content"][0]["text"])
    self.assertEqual(resume_data["status"], "COMPLETED")
    self.assertEqual(resume_data["exit_code"], 0)

  def test_mcp_payload_file_support(self) -> None:
    module_name = __name__
    manifest_path = os.path.join(self.temp_dir, "mcp_pf_wf.yaml")
    with open(manifest_path, "w", encoding="utf-8") as f:
      f.write(f"""
name: mcp_test_pipeline
actions:
  - id: compute
    python_import: {module_name}.mcp_test_action
stages:
  - name: compute_stage
    python_action:
      action_id: compute
  - name: confirm_gate
    run_after: [compute_stage]
    operator_action:
      instructions: "'Confirm ' + payload.outputs.compute_stage.summary"
      json_schema: '{{"type": "object", "required": ["confirmed"]}}'
  - name: final_stage
    run_after: [confirm_gate]
    run_if: "payload.outputs.confirm_gate.confirmed == true"
    python_action:
      action_id: compute
""")
    init_file = os.path.join(self.temp_dir, "init_payload.json")
    with open(init_file, "w", encoding="utf-8") as f:
      json.dump({"seed_key": "from_file"}, f)

    dry_resp = mcp_server.handle_jsonrpc_message({
        "jsonrpc": "2.0",
        "id": 201,
        "method": "tools/call",
        "params": {
            "name": "dry_run_lightflow",
            "arguments": {
                "lightflow": manifest_path,
                "payload_file": init_file,
            },
        },
    })
    assert dry_resp is not None
    dry_data = json.loads(dry_resp["result"]["content"][0]["text"])
    self.assertTrue(dry_data["valid"])
    self.assertIn("from_file", dry_data["trace"])

    run_resp = mcp_server.handle_jsonrpc_message({
        "jsonrpc": "2.0",
        "id": 202,
        "method": "tools/call",
        "params": {
            "name": "run_lightflow",
            "arguments": {
                "lightflow": manifest_path,
                "log_id": "mcp_pf_run",
                "payload_file": init_file,
            },
        },
    })
    assert run_resp is not None
    run_data = json.loads(run_resp["result"]["content"][0]["text"])
    self.assertEqual(run_data["status"], "SUSPENDED")
    self.assertEqual(run_data["passport"]["payload"]["seed_key"], "from_file")

    resume_file = os.path.join(self.temp_dir, "resume_payload.json")
    with open(resume_file, "w", encoding="utf-8") as f:
      json.dump({"confirmed": True}, f)

    resume_resp = mcp_server.handle_jsonrpc_message({
        "jsonrpc": "2.0",
        "id": 203,
        "method": "tools/call",
        "params": {
            "name": "resume_lightflow",
            "arguments": {
                "lightflow": manifest_path,
                "log_id": "mcp_pf_run",
                "stage": "confirm_gate",
                "resolution": "APPROVE",
                "payload_file": resume_file,
            },
        },
    })
    assert resume_resp is not None
    resume_data = json.loads(resume_resp["result"]["content"][0]["text"])
    self.assertEqual(resume_data["status"], "COMPLETED")
    self.assertEqual(resume_data["exit_code"], 0)


class McpContractTest(unittest.TestCase):
  """One `state`, honest `isError`, structured `next_action`."""

  def setUp(self) -> None:
    super().setUp()
    self.temp_dir = tempfile.mkdtemp(prefix="lightflow_mcp_contract_")
    self.addCleanup(shutil.rmtree, self.temp_dir, True)
    self.old_state_dir = os.environ.get("LIGHTFLOW_STATE_DIR")
    os.environ["LIGHTFLOW_STATE_DIR"] = self.temp_dir
    notify = mock.patch.object(mcp_server.engine, "send_notification")
    notify.start()
    self.addCleanup(notify.stop)
    self.manifest = os.path.join(self.temp_dir, "gate.yaml")
    with open(self.manifest, "w", encoding="utf-8") as f:
      f.write(f"""
name: contract_wf
actions:
  - id: compute
    python_import: {__name__}.mcp_test_action
stages:
  - name: prep
    python_action: {{"action_id": "compute"}}
  - name: gate
    run_after: [prep]
    operator_action:
      instructions: "'Ship?'"
      json_schema: '{{"type": "object", "required": ["approved", "ticket"], "properties": {{"ticket": {{"type": "string"}}}}}}'
  - name: ship
    run_after: [gate]
    run_if: "payload.outputs.gate.approved == true"
    python_action: {{"action_id": "compute"}}
""")

  def tearDown(self) -> None:
    if self.old_state_dir is None:
      os.environ.pop("LIGHTFLOW_STATE_DIR", None)
    else:
      os.environ["LIGHTFLOW_STATE_DIR"] = self.old_state_dir
    super().tearDown()

  def _call(self, name: str, **arguments: Any) -> tuple[bool, dict[str, Any]]:
    resp = mcp_server.handle_jsonrpc_message({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "tools/call",
        "params": {"name": name, "arguments": arguments},
    })
    assert resp is not None
    result = resp["result"]
    data = json.loads(result["content"][0]["text"])
    self.assertEqual(result["structuredContent"], data)
    return bool(result["isError"]), data

  def test_reject_is_an_outcome_with_next_action_not_an_error(self) -> None:
    err, run = self._call("run_lightflow", lightflow=self.manifest, log_id="c1")
    self.assertFalse(err)
    self.assertEqual((run["state"], run["status"]), ("SUSPENDED", "SUSPENDED"))
    self.assertEqual(run["next_action"]["tool"], "resume_lightflow")
    self.assertEqual(run["next_action"]["required_payload_keys"], ["ticket"])
    self.assertNotIn("Payload Schema:", run["message"])  # trimmed
    self.assertIn("do not self-approve", run["message"])

    err, rej = self._call(
        "resume_lightflow", log_id="c1", stage="gate", resolution="REJECT"
    )
    self.assertFalse(err, "a deliberate rejection is not a tool error")
    self.assertEqual((rej["state"], rej["status"]), ("REJECTED", "REJECTED"))
    self.assertEqual(rej["exit_code"], 1)
    self.assertEqual(rej["rejected_stage"], "gate")
    self.assertIsNone(rej["paused_stage"])
    self.assertEqual(rej["json_schema"]["required"], ["approved", "ticket"])
    self.assertEqual(
        rej["next_action"]["arguments"],
        {"log_id": "c1", "stage": "gate", "resolution": "APPROVE"},
    )
    self.assertEqual(rej["next_action"]["required_payload_keys"], ["ticket"])

    # status agrees: REJECTED (not FAILED), and still not an error.
    err, st = self._call("get_lightflow_status", log_id="c1")
    self.assertFalse(err)
    self.assertEqual(st["state"], "REJECTED")
    self.assertEqual(st["rejected_stage"], "gate")
    self.assertEqual(st["next_action"]["required_payload_keys"], ["ticket"])

    # Forgetting the ticket IS a call error: the schema is returned with it.
    err, bad = self._call(
        "resume_lightflow", log_id="c1", stage="gate", resolution="APPROVE"
    )
    self.assertTrue(err)
    self.assertIn("'ticket' is a required property", bad["error"])
    self.assertEqual(bad["state"], "REJECTED")
    self.assertEqual(bad["json_schema"]["required"], ["approved", "ticket"])

    err, ok = self._call(
        "resume_lightflow",
        log_id="c1",
        stage="gate",
        resolution="APPROVE",
        payload={"ticket": "CHG-1"},
    )
    self.assertFalse(err)
    self.assertEqual(ok["state"], "COMPLETED")
    self.assertIsNone(ok["next_action"])

  def test_genuine_failure_is_error_and_verbose_has_no_stdout(self) -> None:
    bad = os.path.join(self.temp_dir, "bad.yaml")
    with open(bad, "w", encoding="utf-8") as f:
      f.write(
          "name: bad_wf\nactions:\n  - id: c\n    python_import: nope_mod.fn\n"
          'stages:\n  - name: s\n    python_action: {"action_id": "c"}\n'
      )
    err, data = self._call("run_lightflow", lightflow=bad, log_id="c2")
    self.assertTrue(err)
    self.assertEqual((data["state"], data["status"]), ("FAILED", "FAILED"))
    self.assertEqual(data["next_action"]["arguments"], {"log_id": "c2"})
    err, verbose = self._call("get_lightflow_status", log_id="c2", verbose=True)
    self.assertTrue(err)
    self.assertIn("mermaid_diagram", verbose)
    self.assertIn("stamps", verbose["passport"])
    self.assertNotIn("stdout", verbose)

  def test_not_found_and_list(self) -> None:
    err, data = self._call("get_lightflow_status", log_id="ghost")
    self.assertFalse(err)
    self.assertEqual((data["state"], data["exit_code"]), ("NOT_FOUND", 3))
    self.assertIn(os.path.realpath(self.temp_dir), data["message"])
    self.assertIsNone(data["next_action"])
    self._call("run_lightflow", lightflow=self.manifest, log_id="c3")
    _, listing = self._call("list_lightflow_runs")
    run = next(r for r in listing["runs"] if r["log_id"] == "c3")
    self.assertEqual(run["state"], "PAUSED")


if __name__ == "__main__":
  unittest.main()
