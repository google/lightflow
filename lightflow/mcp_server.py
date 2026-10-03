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

"""Zero-dependency Model Context Protocol (MCP) JSON-RPC 2.0 Stdio Server for Lightflow.

Exposes Lightflow workflow compilation, execution, operator gate resumption,
status inspection, and Generative UI visualization to Claude Code, Claude
Desktop,
Antigravity, and any MCP-compliant agent host.
"""

from __future__ import annotations

import contextlib
import importlib
import io
import json
import os
import sys
from typing import Any, Optional

# pylint: disable=g-import-not-at-top,g-bad-import-order
try:
  from . import engine
  from . import lib
  from . import schema
except ImportError:
  from lightflow import engine  # pyrefly: ignore[missing-import]
  from lightflow import lib  # pyrefly: ignore[missing-import]
  from lightflow import schema  # pyrefly: ignore[missing-import]
# pylint: enable=g-import-not-at-top,g-bad-import-order


SERVER_INFO = {
    "name": "lightflow-mcp",
    "version": "0.2.0",
}

MCP_TOOLS: list[dict[str, Any]] = [
    {
        "name": "run_lightflow",
        "description": (
            "Starts or continues a Lightflow deterministic DAG workflow. If the"
            " run reaches an operator_action checkpoint, execution suspends"
            " (status='SUSPENDED', exit_code=2) and returns the gate"
            " instructions, JSON schema, and resume command. Follow the"
            " returned instructions: if they ask for a person's decision,"
            " present them to that person and call resume_lightflow only with"
            " their answer; never decide for them."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "lightflow": {
                    "type": "string",
                    "description": (
                        "Path to the workflow directory (or manifest file)."
                    ),
                },
                "log_id": {
                    "type": "string",
                    "description": "Unique state identifier for this run.",
                },
                "payload": {
                    "type": ["object", "string"],
                    "description": (
                        "Optional initial payload dictionary or JSON string."
                    ),
                },
                "force": {
                    "type": "boolean",
                    "description": (
                        "If true, discards existing state and restarts from"
                        " scratch."
                    ),
                },
                "verbose": {
                    "type": "boolean",
                    "description": (
                        "If true, includes the full passport and"
                        " mermaid_diagram in the response."
                    ),
                },
                "fields": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": (
                        "Optional list of dotted field paths (e.g."
                        " ['payload.outputs.verify_spec']) to extract from the"
                        " passport without returning the full passport."
                    ),
                },
            },
            "required": ["lightflow", "log_id"],
        },
    },
    {
        "name": "resume_lightflow",
        "description": (
            "Resumes a paused or failed Lightflow run, or re-runs a specific"
            " stage (`rerun`). When answering a paused operator_action gate,"
            " pass `stage`, `resolution` ('APPROVE' or 'REJECT'), and `payload`"
            " matching the stage's json_schema."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "lightflow": {
                    "type": "string",
                    "description": (
                        "Path to the workflow directory (or manifest file)."
                    ),
                },
                "log_id": {
                    "type": "string",
                    "description": "Unique state identifier for this run.",
                },
                "stage": {
                    "type": "string",
                    "description": (
                        "Operator action stage name to resolve, or failed stage"
                        " to re-arm."
                    ),
                },
                "rerun": {
                    "type": ["string", "boolean"],
                    "description": (
                        "Stage name to re-arm and re-run (even if already"
                        " COMPLETED or SKIPPED), preserving upstream completed"
                        " work."
                    ),
                },
                "resolution": {
                    "type": "string",
                    "enum": ["APPROVE", "REJECT"],
                    "description": "Gate decision ('APPROVE' or 'REJECT').",
                },
                "payload": {
                    "type": ["object", "string"],
                    "description": (
                        "Payload dictionary or JSON string to merge into the"
                        " Passport."
                    ),
                },
                "comment": {
                    "type": "string",
                    "description": (
                        "Optional approval note or rejection reason."
                    ),
                },
                "operator": {
                    "type": "string",
                    "description": "Optional operator ID or agent identifier.",
                },
                "cascade": {
                    "type": "boolean",
                    "description": (
                        "When re-arming a failed or `rerun` stage, also re-arm"
                        " downstream dependents (default: true)."
                    ),
                },
                "verbose": {
                    "type": "boolean",
                    "description": (
                        "If true, includes the full passport and"
                        " mermaid_diagram in the response."
                    ),
                },
                "fields": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": (
                        "Optional list of dotted field paths (e.g."
                        " ['payload.outputs.verify_spec']) to extract from the"
                        " passport without returning the full passport."
                    ),
                },
            },
            "required": ["lightflow", "log_id"],
        },
    },
    {
        "name": "get_lightflow_status",
        "description": (
            "Inspects the compact run status, non-completed stages, and active"
            " operator_action instructions for a lightflow run. Pass"
            " `verbose=true` to include the full Passport payload, stamp log,"
            " and Mermaid diagram, or `fields=[...]` for targeted reads."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "lightflow": {
                    "type": "string",
                    "description": (
                        "Path to the workflow directory (or manifest file)."
                    ),
                },
                "log_id": {
                    "type": "string",
                    "description": "Unique state identifier for the run.",
                },
                "verbose": {
                    "type": "boolean",
                    "description": (
                        "If true, includes the full passport,"
                        " mermaid_diagram, and verbose CLI stdout."
                    ),
                },
                "fields": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": (
                        "Optional list of dotted field paths (e.g."
                        " ['payload.outputs.verify_spec']) to extract from the"
                        " passport without returning the full passport."
                    ),
                },
            },
            "required": ["lightflow", "log_id"],
        },
    },
    {
        "name": "dry_run_lightflow",
        "description": (
            "Compiles and validates a Lightflow workflow (DAG topology, action"
            " references, CEL-like expression syntax, and JSON schemas) and"
            " simulates its execution trace without side effects."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "lightflow": {
                    "type": "string",
                    "description": (
                        "Path to the workflow directory (or manifest file)."
                    ),
                },
                "payload": {
                    "type": ["object", "string"],
                    "description": (
                        "Optional seed payload for simulating CEL-like"
                        " expressions."
                    ),
                },
            },
            "required": ["lightflow"],
        },
    },
    {
        "name": "visualize_lightflow",
        "description": (
            "Generates a standalone, zero-dependency HTML5 Generative UI"
            " visualizer containing the interactive DAG canvas, stage"
            " inspector, and passport timeline playback."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "lightflow": {
                    "type": "string",
                    "description": (
                        "Path to the workflow directory (or manifest file)."
                    ),
                },
                "log_id": {
                    "type": "string",
                    "description": (
                        "Optional run log_id to bundle execution history."
                    ),
                },
                "output": {
                    "type": "string",
                    "description": (
                        "Optional file path to write the standalone HTML file."
                    ),
                },
                "title": {
                    "type": "string",
                    "description": "Optional custom display title.",
                },
            },
            "required": ["lightflow"],
        },
    },
]


def _resolve_dotted_parts(cur: Any, parts: list[str]) -> tuple[bool, Any]:
  """Walks `parts` into `cur`, supporting literal dots in dictionary keys."""
  if not parts:
    return True, cur
  if isinstance(cur, dict):
    for end in range(1, len(parts) + 1):
      candidate = ".".join(parts[:end])
      if candidate in cur:
        found, val = _resolve_dotted_parts(cur[candidate], parts[end:])
        if found:
          return True, val
    return False, None
  if isinstance(cur, list) and parts[0].isdigit():
    list_idx = int(parts[0])
    if 0 <= list_idx < len(cur):
      return _resolve_dotted_parts(cur[list_idx], parts[1:])
  return False, None


def _extract_passport_fields(
    passport_dict: Optional[dict[str, Any]],
    field_paths: Optional[list[str]],
) -> dict[str, Any]:
  """Extracts targeted dotted field paths from a passport dictionary."""
  if not passport_dict or not field_paths:
    return {}
  extracted: dict[str, Any] = {}
  payload_dict = passport_dict.get("payload", {})
  for raw_path in field_paths:
    if not isinstance(raw_path, str) or not raw_path.strip():
      continue
    parts = [p for p in raw_path.strip().split(".") if p]
    if not parts:
      continue
    found, val = _resolve_dotted_parts(passport_dict, parts)
    if not found and isinstance(payload_dict, dict):
      found, val = _resolve_dotted_parts(payload_dict, parts)
    extracted[raw_path] = val if found else None
  return extracted


def _inspect_run_state(
    log_id: str,
    lightflow_path: Optional[str] = None,
    *,
    verbose: bool = False,
    fields: Optional[list[str]] = None,
) -> dict[str, Any]:
  """Reads current Passport state and optional Mermaid graph for a run."""
  pm = lib.PassportManager(log_id)
  passport = pm.load_passport()
  if not passport:
    res: dict[str, Any] = {
        "exists": False,
        "log_id": log_id,
        "status": "NO_STATE",
    }
    if verbose:
      res["passport"] = None
    return res

  passport_dict = passport.to_dict()
  paused_stage = None
  instructions = None
  resume_command = None
  json_schema: Any = None

  wf = None
  mermaid = None
  if lightflow_path and os.path.exists(os.path.expanduser(lightflow_path)):
    try:
      wf = schema.load_lightflow(lightflow_path)
      if verbose:
        mermaid = lib.generate_status_mermaid(wf, passport)
    except Exception:  # pylint: disable=broad-exception-caught
      pass

  ordered_stages: list[str] = []
  if wf is not None:
    try:
      ordered_stages = engine.LightflowEngine(
          wf, lightflow_path=lightflow_path, log_id=pm.workflow_id
      ).compile()
    except Exception:  # pylint: disable=broad-exception-caught
      ordered_stages = [s.name for s in wf.stages]

  derive_order_from_stamps = not ordered_stages
  known_stages = {s.name: s for s in wf.stages} if wf else None
  latest_by_stage: dict[str, schema.Stamp] = {}
  for stamp in passport.stamps:
    if known_stages is not None and stamp.stage_name not in known_stages:
      continue
    latest_by_stage[stamp.stage_name] = stamp
    if derive_order_from_stamps and stamp.stage_name not in ordered_stages:
      ordered_stages.append(stamp.stage_name)

  effective: dict[str, str] = {}
  for st_name in ordered_stages:
    stamp = latest_by_stage.get(st_name)
    if stamp is None:
      effective[st_name] = "NOT_STARTED"
      continue
    name = schema.StampStatus.Name(stamp.status)
    if name == "FAILED":
      if (stamp.message or "").startswith(
          lib._REJECTED_PREFIX  # pylint: disable=protected-access
      ):
        name = "REJECTED"
      elif stamp.message == engine.UPSTREAM_FAILED_MESSAGE:
        name = "BLOCKED"
    effective[st_name] = name

  non_completed_stages: list[dict[str, Any]] = []
  completed_count = 0
  not_started_count = 0
  for st_name in ordered_stages:
    eff = effective.get(st_name, "NOT_STARTED")
    if eff == "COMPLETED":
      completed_count += 1
      continue
    if eff == "NOT_STARTED":
      not_started_count += 1
      continue
    entry: dict[str, Any] = {
        "name": st_name,
        "status": eff.lower(),
    }
    stamp = latest_by_stage.get(st_name)
    if stamp and stamp.message and eff != "PAUSED":
      if eff == "BLOCKED":
        entry["message"] = "upstream failed or was rejected"
      else:
        summary = engine.summarise(stamp.message)
        if summary:
          entry["message"] = summary
    non_completed_stages.append(entry)

  for st_name in ordered_stages:
    stamp = latest_by_stage.get(st_name)
    if stamp and stamp.status == schema.StampStatus.PAUSED:
      paused_stage = st_name
      instructions = stamp.instructions
      resume_command = stamp.resume_command
      if known_stages and st_name in known_stages:
        stage_obj = known_stages[st_name]
        if stage_obj.operator_action and stage_obj.operator_action.json_schema:
          raw_schema = stage_obj.operator_action.json_schema
          try:
            json_schema = json.loads(raw_schema)
          except json.JSONDecodeError:
            json_schema = raw_schema
      break

  res = {
      "exists": True,
      "log_id": log_id,
      "paused_stage": paused_stage,
      "instructions": instructions,
      "json_schema": json_schema,
      "resume_command": resume_command,
      "completed_count": completed_count,
      "not_started_count": not_started_count,
      "stages": non_completed_stages,
  }
  if fields:
    res["fields"] = _extract_passport_fields(passport_dict, fields)
  if verbose:
    res["passport"] = passport_dict
    res["mermaid_diagram"] = mermaid
  return res


def call_tool(name: str, arguments: dict[str, Any]) -> dict[str, Any]:
  """Executes an MCP tool call with stdout redirection and structured output."""
  cli = lib.LightflowRunnerCLI()
  buf = io.StringIO()

  if name == "run_lightflow":
    lightflow = arguments["lightflow"]
    log_id = arguments["log_id"]
    payload = arguments.get("payload", "{}")
    force = bool(arguments.get("force", False))
    verbose = bool(arguments.get("verbose", False))
    fields = arguments.get("fields")

    status = "COMPLETED"
    exit_code = 0
    error_message = None

    with contextlib.redirect_stdout(buf):
      try:
        cli.start(
            lightflow=lightflow,
            log_id=log_id,
            force=force,
            payload=payload,
        )
      except (
          engine.OperatorActionSuspended,
          lib.LightflowAlreadyPausedError,
      ) as e:
        status = "SUSPENDED"
        exit_code = lib.EXIT_SUSPENDED
        error_message = str(e)
      except lib.LightflowAlreadyCompletedError as e:
        status = "COMPLETED"
        exit_code = 0
        error_message = str(e)
      except Exception as e:  # pylint: disable=broad-exception-caught
        status = "FAILED"
        exit_code = 1
        error_message = str(e)

    state_info = _inspect_run_state(
        log_id, lightflow, verbose=verbose, fields=fields
    )
    if status == "SUSPENDED" and error_message and not verbose:
      concise_msg = error_message.splitlines()[0]
    else:
      concise_msg = error_message or "Lightflow completed successfully."
    state_info.update({
        "status": status,
        "exit_code": exit_code,
        "message": concise_msg,
        "stdout": buf.getvalue(),
    })
    return {
        "content": [{"type": "text", "text": json.dumps(state_info, indent=2)}],
        "isError": status == "FAILED",
    }

  elif name == "resume_lightflow":
    lightflow = arguments["lightflow"]
    log_id = arguments["log_id"]
    stage = arguments.get("stage")
    rerun = arguments.get("rerun")
    resolution = arguments.get("resolution", "APPROVE")
    payload = arguments.get("payload", "{}")
    comment = arguments.get("comment")
    operator = arguments.get("operator")
    cascade = bool(arguments.get("cascade", True))
    verbose = bool(arguments.get("verbose", False))
    fields = arguments.get("fields")

    status = "COMPLETED"
    exit_code = 0
    error_message = None

    with contextlib.redirect_stdout(buf):
      try:
        cli.resume(
            lightflow=lightflow,
            log_id=log_id,
            stage=stage,
            payload=payload,
            resolution=resolution,
            comment=comment,
            operator=operator,
            cascade=cascade,
            rerun=rerun,
        )
      except (
          engine.OperatorActionSuspended,
          lib.LightflowAlreadyPausedError,
      ) as e:
        status = "SUSPENDED"
        exit_code = lib.EXIT_SUSPENDED
        error_message = str(e)
      except lib.LightflowAlreadyCompletedError as e:
        status = "COMPLETED"
        exit_code = 0
        error_message = str(e)
      except Exception as e:  # pylint: disable=broad-exception-caught
        status = "FAILED"
        exit_code = 1
        error_message = str(e)

    state_info = _inspect_run_state(
        log_id, lightflow, verbose=verbose, fields=fields
    )
    if status == "SUSPENDED" and error_message and not verbose:
      concise_msg = error_message.splitlines()[0]
    else:
      concise_msg = error_message or "Lightflow resumed and completed."
    state_info.update({
        "status": status,
        "exit_code": exit_code,
        "message": concise_msg,
        "stdout": buf.getvalue(),
    })
    return {
        "content": [{"type": "text", "text": json.dumps(state_info, indent=2)}],
        "isError": status == "FAILED",
    }

  elif name == "get_lightflow_status":
    log_id = arguments["log_id"]
    lightflow = arguments["lightflow"]
    verbose = bool(arguments.get("verbose", False))
    fields = arguments.get("fields")
    is_err = False
    err_msg = None
    status_label = "COMPLETED"
    # Run states are results, not tool errors; report them as the CLI's
    # `status` exit code.
    exit_code = 0
    with contextlib.redirect_stdout(buf):
      try:
        cli.status(lightflow=lightflow, log_id=log_id, verbose=verbose)
      except lib.LightflowAlreadyPausedError as e:
        status_label = "SUSPENDED"
        exit_code = lib.EXIT_SUSPENDED
        err_msg = str(e)
      except lib.LightflowRunFailedError as e:
        status_label = "FAILED"
        exit_code = lib.EXIT_FAILED
        err_msg = str(e)
      except lib.LightflowStateNotFoundError as e:
        status_label = "NO_STATE"
        exit_code = lib.EXIT_NO_STATE
        err_msg = str(e)
      except lib.LightflowRunningError as e:
        status_label = "RUNNING"
        exit_code = lib.EXIT_RUNNING
        err_msg = str(e)
      except Exception as e:  # pylint: disable=broad-exception-caught
        is_err = True
        status_label = "FAILED"
        err_msg = str(e)
        exit_code = lib.EXIT_FAILED
    state_info = _inspect_run_state(
        log_id, lightflow, verbose=verbose, fields=fields
    )
    stdout_text = buf.getvalue()
    header_line = stdout_text.splitlines()[0] if stdout_text.strip() else ""
    state_info["status"] = status_label
    state_info["exit_code"] = exit_code
    state_info["message"] = (
        header_line or err_msg or "Lightflow completed successfully."
    )
    if verbose:
      if "```mermaid" in stdout_text:
        idx = stdout_text.find("```mermaid")
        state_info["mermaid_diagram"] = stdout_text[idx:].strip()
      state_info["stdout"] = stdout_text
    if is_err and err_msg:
      state_info["error"] = err_msg
    return {
        "content": [{"type": "text", "text": json.dumps(state_info, indent=2)}],
        "isError": is_err,
    }

  elif name == "dry_run_lightflow":
    lightflow = arguments["lightflow"]
    payload = arguments.get("payload", "{}")
    with contextlib.redirect_stdout(buf):
      try:
        cli.dry_run(lightflow=lightflow, payload=payload)
        is_err = False
        err_msg = None
      except Exception as e:  # pylint: disable=broad-exception-caught
        is_err = True
        err_msg = str(e)

    result = {
        "valid": not is_err,
        "error": err_msg,
        "trace": buf.getvalue(),
    }
    return {
        "content": [{"type": "text", "text": json.dumps(result, indent=2)}],
        "isError": is_err,
    }

  elif name == "visualize_lightflow":
    lightflow = arguments["lightflow"]
    log_id = arguments.get("log_id")
    output = arguments.get("output")
    title = arguments.get("title")
    is_err = False
    err_msg = None
    out = ""
    with contextlib.redirect_stdout(buf):
      try:
        out = cli.visualize(
            lightflow=lightflow,
            log_id=log_id,
            output=output,
            title=title,
        )
      except Exception as e:  # pylint: disable=broad-exception-caught
        is_err = True
        err_msg = str(e)
    result = {
        "output": out if (output and not is_err) else None,
        "html": out if (not output and not is_err) else None,
        "html_length": (
            0 if is_err else (len(out) if not output else os.path.getsize(out))
        ),
        "error": err_msg,
        "stdout": buf.getvalue(),
    }
    return {
        "content": [{"type": "text", "text": json.dumps(result, indent=2)}],
        "isError": is_err,
    }

  raise ValueError(f"Unknown MCP tool: {name}")


def handle_jsonrpc_message(msg: dict[str, Any]) -> Optional[dict[str, Any]]:
  """Processes a single JSON-RPC 2.0 message and returns response (if any)."""
  if not isinstance(msg, dict):
    return {
        "jsonrpc": "2.0",
        "id": None,
        "error": {
            "code": -32600,
            "message": "Invalid Request: expected JSON object.",
        },
    }

  method = msg.get("method", "")
  req_id = msg.get("id")

  # Notifications (no id) do not receive a response
  if req_id is None:
    return None

  if method == "initialize":
    return {
        "jsonrpc": "2.0",
        "id": req_id,
        "result": {
            "protocolVersion": "2024-11-05",
            "capabilities": {
                "tools": {"listChanged": False},
            },
            "serverInfo": SERVER_INFO,
        },
    }

  if method == "ping":
    return {
        "jsonrpc": "2.0",
        "id": req_id,
        "result": {},
    }

  if method == "tools/list":
    return {
        "jsonrpc": "2.0",
        "id": req_id,
        "result": {
            "tools": MCP_TOOLS,
        },
    }

  if method == "resources/list":
    return {
        "jsonrpc": "2.0",
        "id": req_id,
        "result": {
            "resources": [],
        },
    }

  if method == "prompts/list":
    return {
        "jsonrpc": "2.0",
        "id": req_id,
        "result": {
            "prompts": [],
        },
    }

  if method == "tools/call":
    params = msg.get("params")
    if params is None:
      params = {}
    if not isinstance(params, dict):
      return {
          "jsonrpc": "2.0",
          "id": req_id,
          "error": {
              "code": -32602,
              "message": "Invalid params: expected a JSON object.",
          },
      }
    tool_name = params.get("name", "")
    arguments = params.get("arguments")
    if arguments is None:
      arguments = {}
    if not isinstance(tool_name, str) or not isinstance(arguments, dict):
      return {
          "jsonrpc": "2.0",
          "id": req_id,
          "error": {
              "code": -32602,
              "message": (
                  "Invalid params: 'name' must be a string and 'arguments'"
                  " a JSON object."
              ),
          },
      }
    tool_spec = next((t for t in MCP_TOOLS if t["name"] == tool_name), None)
    if tool_spec is None:
      return {
          "jsonrpc": "2.0",
          "id": req_id,
          "error": {
              "code": -32602,
              "message": (
                  f"Unknown tool '{tool_name}'. Available:"
                  f" {', '.join(t['name'] for t in MCP_TOOLS)}."
              ),
          },
      }
    required = tool_spec.get("inputSchema", {}).get("required", [])
    missing = [
        k
        for k in required
        if k not in arguments
        or arguments[k] is None
        or (isinstance(arguments[k], str) and not arguments[k].strip())
    ]
    if missing:
      return {
          "jsonrpc": "2.0",
          "id": req_id,
          "error": {
              "code": -32602,
              "message": (
                  f"Tool '{tool_name}' is missing required argument(s):"
                  f" {', '.join(missing)}."
              ),
          },
      }
    try:
      tool_res = call_tool(tool_name, arguments)
      return {
          "jsonrpc": "2.0",
          "id": req_id,
          "result": tool_res,
      }
    except Exception as e:  # pylint: disable=broad-exception-caught
      return {
          "jsonrpc": "2.0",
          "id": req_id,
          "error": {
              "code": -32603,
              "message": f"{type(e).__name__}: {e}",
          },
      }

  return {
      "jsonrpc": "2.0",
      "id": req_id,
      "error": {
          "code": -32601,
          "message": f"Method not found: {method}",
      },
  }


def _write_response(
    out_stream: Any,
    response: dict[str, Any],
    use_content_length: bool,
    is_binary: bool,
) -> None:
  """Writes a JSON-RPC 2.0 response in either Content-Length or newline framing."""
  resp_json = json.dumps(response)
  if use_content_length:
    resp_bytes = resp_json.encode("utf-8")
    header = f"Content-Length: {len(resp_bytes)}\r\n\r\n"
    if is_binary:
      out_stream.write(header.encode("ascii") + resp_bytes)
    else:
      out_stream.write(header + resp_json)
  else:
    line = resp_json + "\n"
    if is_binary:
      out_stream.write(line.encode("utf-8"))
    else:
      out_stream.write(line)
  out_stream.flush()


def process_stdio_stream(in_stream: Any, out_stream: Any) -> None:
  """Processes stdio JSON-RPC messages supporting both newline and Content-Length framing."""
  while True:
    raw_line = in_stream.readline()
    if not raw_line:
      break
    is_binary = isinstance(raw_line, (bytes, bytearray))
    line_str = (
        raw_line.decode("utf-8", errors="replace")
        if is_binary
        else str(raw_line)
    )
    stripped = line_str.strip()
    if not stripped:
      continue

    if stripped.lower().startswith("content-length:"):
      try:
        content_length = int(stripped.split(":", 1)[1].strip())
      except ValueError:
        continue

      # Consume remaining HTTP-style headers up to the blank line
      while True:
        hdr_line = in_stream.readline()
        if not hdr_line:
          break
        hdr_str = (
            hdr_line.decode("utf-8", errors="replace")
            if isinstance(hdr_line, (bytes, bytearray))
            else str(hdr_line)
        )
        if not hdr_str.strip():
          break

      chunks = []
      remaining = content_length
      while remaining > 0:
        chunk = in_stream.read(remaining)
        if not chunk:
          break
        chunks.append(chunk)
        remaining -= len(chunk)
      raw_body = b"".join(chunks) if is_binary else "".join(chunks)
      body_str = (
          raw_body.decode("utf-8", errors="replace")
          if isinstance(raw_body, (bytes, bytearray))
          else str(raw_body)
      )
      try:
        msg = json.loads(body_str)
      except json.JSONDecodeError as e:
        err_resp = {
            "jsonrpc": "2.0",
            "id": None,
            "error": {"code": -32700, "message": f"Parse error: {e}"},
        }
        _write_response(
            out_stream, err_resp, use_content_length=True, is_binary=is_binary
        )
        continue

      response = handle_jsonrpc_message(msg)
      if response is not None:
        _write_response(
            out_stream, response, use_content_length=True, is_binary=is_binary
        )
    else:
      try:
        msg = json.loads(stripped)
      except json.JSONDecodeError as e:
        err_resp = {
            "jsonrpc": "2.0",
            "id": None,
            "error": {"code": -32700, "message": f"Parse error: {e}"},
        }
        _write_response(
            out_stream, err_resp, use_content_length=False, is_binary=is_binary
        )
        continue

      response = handle_jsonrpc_message(msg)
      if response is not None:
        _write_response(
            out_stream, response, use_content_length=False, is_binary=is_binary
        )


def main() -> None:
  """Runs the stdio JSON-RPC 2.0 event loop."""
  if os.name == "nt":
    try:
      win_msvcrt: Any = importlib.import_module("msvcrt")
      o_binary = getattr(os, "O_BINARY", 0)
      for stream in (sys.stdin, sys.stdout):
        win_msvcrt.setmode(stream.fileno(), o_binary)
    except Exception:  # pylint: disable=broad-exception-caught
      pass
  orig_stdout = sys.stdout
  in_stream = getattr(sys.stdin, "buffer", sys.stdin)
  # The real stdout is reserved for JSON-RPC framing. Redirecting `sys.stdout`
  # only covers Python-level prints; subprocesses spawned by actions inherit
  # fd 1 directly. So keep a private duplicate of fd 1 for the protocol
  # writer and point fd 1 itself at stderr for everyone else.
  out_stream: Any
  proto_fd: Optional[int] = None
  orig_stdout_fd: Optional[int] = None
  owns_out_stream = False
  try:
    orig_stdout.flush()
    orig_stdout_fd = orig_stdout.fileno()
    proto_fd = os.dup(orig_stdout_fd)
    os.dup2(sys.stderr.fileno(), orig_stdout_fd)
    out_stream = os.fdopen(proto_fd, "wb", buffering=0)
    owns_out_stream = True
  except (OSError, ValueError, AttributeError):
    if proto_fd is not None and not owns_out_stream:
      try:
        os.close(proto_fd)
      except OSError:
        pass
    proto_fd = None
    out_stream = getattr(orig_stdout, "buffer", orig_stdout)
  sys.stdout = sys.stderr
  try:
    process_stdio_stream(in_stream, out_stream)
  finally:
    sys.stdout = orig_stdout
    if owns_out_stream and proto_fd is not None and orig_stdout_fd is not None:
      try:
        out_stream.flush()
        os.dup2(proto_fd, orig_stdout_fd)
      except OSError:
        pass
      try:
        out_stream.close()
      except OSError:
        pass


if __name__ == "__main__":
  main()
