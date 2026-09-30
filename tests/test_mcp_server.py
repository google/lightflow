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
    self.assertIn("```mermaid", run_data["mermaid_diagram"])

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
    self.assertIn("```mermaid", status_data["mermaid_diagram"])

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

    # 5. Visualize without output file -> returns inline HTML
    viz_resp = mcp_server.handle_jsonrpc_message({
        "jsonrpc": "2.0",
        "id": 14,
        "method": "tools/call",
        "params": {
            "name": "visualize_lightflow",
            "arguments": {
                "lightflow": manifest_path,
                "log_id": "mcp_run_01",
            },
        },
    })
    assert viz_resp is not None
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


if __name__ == "__main__":
  unittest.main()
