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
import re
import sys
from typing import Any, Optional

# pylint: disable=g-import-not-at-top,g-bad-import-order
try:
  from . import engine
  from . import lib
  from . import schema
except ImportError:
  # Fallback for running this file directly from a source checkout (e.g.
  # ``python lightflow/runner.py``) without installing the package: put the
  # repository root on sys.path so ``lightflow.*`` resolves as a top-level
  # package. No-op when installed via pip (the relative import above succeeds).
  _OSS_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
  if _OSS_ROOT not in sys.path:
    sys.path.insert(0, _OSS_ROOT)
  from lightflow import engine  # pyrefly: ignore[missing-import]
  from lightflow import lib  # pyrefly: ignore[missing-import]
  from lightflow import schema  # pyrefly: ignore[missing-import]
# pylint: enable=g-import-not-at-top,g-bad-import-order


SERVER_INFO = {
    "name": "lightflow-mcp",
    "version": "0.3.1",
}

MCP_TOOLS: list[dict[str, Any]] = [
    {
        "name": "run_lightflow",
        "description": (
            "Starts or continues a Lightflow deterministic DAG from a"
            " YAML, JSON, or Textproto manifest. If the lightflow reaches an"
            " operator_action human-in-the-loop checkpoint, execution safely"
            " suspends (status='SUSPENDED', exit_code=2) and returns the gate"
            " instructions, JSON schema, and resume command. When status is"
            " 'SUSPENDED', STOP and present the instructions and schema to the"
            " human operator; do NOT self-approve or call resume_lightflow"
            " until the operator explicitly responds. Every result carries one"
            " `state` (COMPLETED, SUSPENDED, WAITING, REJECTED, FAILED,"
            " RUNNING, NOT_FOUND), `exit_code`, and a `next_action` with the"
            " follow-up call. `isError` is true only when the call could not"
            " do what was asked (bad arguments, payload failed the gate schema,"
            " lock held) or a stage genuinely failed; a rejection, a suspension"
            " or a polling yield are outcomes, not errors. With `force`, the"
            " previous state is discarded and summarised in `stdout`."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "lightflow": {
                    "type": "string",
                    "description": (
                        "Path to the lightflow manifest (.yaml, .json, or"
                        " .textproto)."
                    ),
                },
                "log_id": {
                    "type": "string",
                    "description": "Unique state identifier for this run.",
                },
                "payload": {
                    "type": ["object", "string"],
                    "description": (
                        "Optional initial payload dictionary, JSON string, or"
                        " @file.json reference."
                    ),
                },
                "payload_file": {
                    "type": "string",
                    "description": (
                        "Optional path to a JSON file containing the initial"
                        " payload."
                    ),
                },
                "force": {
                    "type": "boolean",
                    "description": (
                        "If true, discards existing state (completed stages,"
                        " gate decisions, history) and restarts from scratch."
                        " Never use it to recover a failed run; call"
                        " resume_lightflow instead."
                    ),
                },
            },
            "required": ["lightflow", "log_id"],
        },
    },
    {
        "name": "resume_lightflow",
        "description": (
            "Resumes a paused, waiting, failed, or rejected Lightflow run."
            " When answering a paused operator_action gate, pass `stage`,"
            " `resolution` ('APPROVE' or 'REJECT'), and `payload` matching the"
            " stage's json_schema (see `next_action.required_payload_keys`)."
            " On a rejected gate, an explicit `resolution` re-opens it and"
            " applies the decision in one call. Without `stage`, failed stages"
            " are re-armed and completed ones are kept. `lightflow` is"
            " optional: runs remember their manifest; pass it once if the file"
            " moved and the new path is recorded."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "lightflow": {
                    "type": "string",
                    "description": (
                        "Path to the lightflow manifest. Optional; only needed"
                        " if the run predates manifest tracking or the file"
                        " moved (then the new path is recorded for later"
                        " calls)."
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
                "resolution": {
                    "type": "string",
                    "enum": ["APPROVE", "REJECT"],
                    "description": "Gate decision ('APPROVE' or 'REJECT').",
                },
                "payload": {
                    "type": ["object", "string"],
                    "description": (
                        "Payload dictionary, JSON string, or @file.json"
                        " reference to merge into the Passport."
                    ),
                },
                "payload_file": {
                    "type": "string",
                    "description": (
                        "Optional path to a JSON file containing the payload to"
                        " merge into the Passport."
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
                        "When re-arming a failed stage, also re-arm downstream"
                        " dependents."
                    ),
                },
            },
            "required": ["log_id"],
        },
    },
    {
        "name": "get_lightflow_status",
        "description": (
            "Inspects a lightflow run: `state`, `exit_code`, one entry per"
            " stage, the paused or rejected gate's `instructions` and"
            " `json_schema`, the Passport payload, and a `next_action` with the"
            " follow-up call (`status` is an alias of `state` kept for older"
            " clients). Compact by default; `verbose` adds the stamp log"
            " and Mermaid diagram. An unknown log_id is an answer"
            " (state NOT_FOUND, exit_code 3), not an error."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "lightflow": {
                    "type": "string",
                    "description": (
                        "Path to the lightflow manifest. Optional: runs record"
                        " their manifest on start, so omit it unless the run"
                        " predates that or the file has moved."
                    ),
                },
                "log_id": {
                    "type": "string",
                    "description": "Unique state identifier for the run.",
                },
                "verbose": {
                    "type": "boolean",
                    "description": (
                        "Also include the full stamp log and the Mermaid"
                        " diagram (large). Default false."
                    ),
                },
            },
            "required": ["log_id"],
        },
    },
    {
        "name": "list_lightflow_runs",
        "description": (
            "Lists every saved lightflow run under the state directory"
            " ($LIGHTFLOW_STATE_DIR or ~/.lightflow), newest first, with its"
            " log_id, lightflow name, coarse state (RUNNING, PAUSED, FAILED,"
            " REJECTED, WAITING, COMPLETED, STARTED), last activity, and"
            " recorded manifest path;"
            " also reports orphan lock files. Use this to rediscover a run"
            " when the log_id is not known, then get_lightflow_status for the"
            " authoritative per-stage view."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "state_dir": {
                    "type": "string",
                    "description": "Optional directory to scan instead.",
                },
            },
            "required": [],
        },
    },
    {
        "name": "dry_run_lightflow",
        "description": (
            "Compiles and validates a Lightflow manifest (DAG topology, action"
            " references, CEL-like expression syntax, and JSON schemas) and"
            " simulates its execution trace without side effects. Returns"
            " `valid`, `would_fail` (stages whose expressions would error at"
            " runtime; also sets isError), `error`, and the full `trace` text"
            " (read its `Summary:` line and 💡 hints)."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "lightflow": {
                    "type": "string",
                    "description": "Path to the lightflow manifest.",
                },
                "payload": {
                    "type": ["object", "string"],
                    "description": (
                        "Optional seed payload (dict, JSON string, or"
                        " @file.json) for simulating CEL-like expressions."
                    ),
                },
                "payload_file": {
                    "type": "string",
                    "description": (
                        "Optional path to a JSON file containing the seed"
                        " payload for dry_run."
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
            " inspector, and passport timeline playback. Pass `lightflow`"
            " and/or the `log_id` of an existing run."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "lightflow": {
                    "type": "string",
                    "description": (
                        "Path to the lightflow manifest. Optional when"
                        " `log_id` is provided: the path recorded in the"
                        " run's Passport is used."
                    ),
                },
                "log_id": {
                    "type": "string",
                    "description": (
                        "Optional run log_id to bundle execution history"
                        " (also supplies the manifest path when `lightflow`"
                        " is omitted)."
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
            "required": [],
        },
    },
]


def _required_payload_keys(json_schema: Any) -> list[str]:
  """Required keys of a gate schema other than `approved` (set by resolution).

  Args:
    json_schema: The gate's parsed JSON schema, or None.

  Returns:
    The key names, in schema order.
  """
  if not isinstance(json_schema, dict):
    return []
  return [k for k in json_schema.get("required") or [] if k != "approved"]


def _next_action(
    log_id: str, state: str, gate: Optional[str], json_schema: Any
) -> Optional[dict[str, Any]]:
  """The structured follow-up call for the run's current state, if any.

  Args:
    log_id: Run identifier.
    state: Canonical run state (SUSPENDED, REJECTED, FAILED, WAITING, ...).
    gate: The paused or rejected gate, if any.
    json_schema: That gate's parsed JSON schema, if any.

  Returns:
    A dict with `tool`, `arguments`, optional `required_payload_keys` and a
    `note`, or None when nothing follows (e.g. COMPLETED).
  """
  if state == "SUSPENDED" and gate:
    return {
        "tool": "resume_lightflow",
        "arguments": {"log_id": log_id, "stage": gate, "resolution": "APPROVE"},
        "required_payload_keys": _required_payload_keys(json_schema),
        "note": (
            "Present `instructions` and `json_schema` to the human operator"
            " first; pass their decision as `resolution` (APPROVE|REJECT) and"
            " any required keys as `payload`. Do not self-approve."
        ),
    }
  if state == "REJECTED" and gate:
    return {
        "tool": "resume_lightflow",
        "arguments": {"log_id": log_id, "stage": gate, "resolution": "APPROVE"},
        "required_payload_keys": _required_payload_keys(json_schema),
        "note": (
            "Terminal unless deliberately re-opened: calling this with an"
            " explicit `resolution` re-opens the rejected gate and applies the"
            " decision in one call; omit `resolution`/`payload` to only"
            " re-open it and pause there."
        ),
    }
  if state == "FAILED":
    return {
        "tool": "resume_lightflow",
        "arguments": {"log_id": log_id},
        "note": (
            "Fix the root cause first; `resume` re-arms failed stages and keeps"
            " completed ones. Never `run_lightflow` with force to recover."
        ),
    }
  if state == "WAITING":
    return {
        "tool": "resume_lightflow",
        "arguments": {"log_id": log_id},
        "note": (
            "A polling stage yielded before its condition was met; nothing is"
            " broken. Resume once the external condition holds."
        ),
    }
  if state == "INCOMPLETE":
    return {
        "tool": "resume_lightflow",
        "arguments": {"log_id": log_id},
        "note": (
            "The run stopped mid-execution with unfinished stages; `resume`"
            " continues from where it left off and keeps completed ones."
        ),
    }
  return None


def _inspect_run_state(
    log_id: str, lightflow_path: Optional[str] = None, *, verbose: bool = False
) -> dict[str, Any]:
  """Reads current Passport state and optional Mermaid graph for a run.

  Args:
    log_id: Run identifier.
    lightflow_path: Manifest path, used for stage order, gate schemas and the
      diagram.
    verbose: Include the full stamp log and Mermaid diagram (large).

  Returns:
    The structured run view (`exists`, `state`, `stages`, gate fields,
    `next_action`, `passport`, and with `verbose` the `mermaid_diagram`).
  """
  pm = lib.PassportManager(log_id)
  passport = pm.load_passport()
  if not passport:
    return {
        "exists": False,
        "log_id": log_id,
        "state": "NOT_FOUND",
        "passport": None,
    }

  passport_dict = passport.to_dict()
  wf = None
  mermaid = None
  if lightflow_path and os.path.exists(os.path.expanduser(lightflow_path)):
    try:
      wf = schema.load_lightflow(lightflow_path)
      mermaid = lib.generate_status_mermaid(wf, passport)
    except Exception:  # pylint: disable=broad-exception-caught
      pass

  known_stages = {s.name: s for s in wf.stages} if wf else None
  latest_by_stage: dict[str, schema.Stamp] = {}
  last_paused_by_stage: dict[str, schema.Stamp] = {}
  for stamp in passport.stamps:
    if known_stages is not None and stamp.stage_name not in known_stages:
      continue
    latest_by_stage[stamp.stage_name] = stamp
    if stamp.status == schema.StampStatus.PAUSED:
      last_paused_by_stage[stamp.stage_name] = stamp

  def _schema_for(stage_name: str) -> Any:
    if not known_stages or stage_name not in known_stages:
      return None
    stage_obj = known_stages[stage_name]
    op = stage_obj.operator_action
    if op is None or not op.json_schema:
      return None
    raw_schema = op.json_schema
    try:
      return json.loads(raw_schema)
    except (TypeError, json.JSONDecodeError):
      return raw_schema

  paused_stage = None
  rejected_stage = None
  instructions = None
  resume_command = None
  json_schema: Any = None
  for st_name, stamp in latest_by_stage.items():
    if stamp.status == schema.StampStatus.PAUSED:
      paused_stage = st_name
      instructions = stamp.instructions
      resume_command = stamp.resume_command
      json_schema = _schema_for(st_name)
      break
  if paused_stage is None:
    for st_name, stamp in latest_by_stage.items():
      if lib.effective_status_name(stamp) == "REJECTED":
        rejected_stage = st_name
        prior = last_paused_by_stage.get(st_name)
        instructions = prior.instructions if prior is not None else None
        json_schema = _schema_for(st_name)
        break

  ordered = [s.name for s in wf.stages] if wf else []
  stages = _stage_table(passport, ordered)
  by_status = {row["status"] for row in stages}
  if paused_stage:
    state = "SUSPENDED"
  elif "FAILED" in by_status:
    state = "FAILED"
  elif "REJECTED" in by_status:
    state = "REJECTED"
  elif "WAITING" in by_status:
    state = "WAITING"
  elif "PENDING" in by_status or "NOT_STARTED" in by_status:
    state = "INCOMPLETE"
  else:
    state = "COMPLETED"
  compact_passport = dict(passport_dict)
  if not verbose:
    compact_passport.pop("stamps", None)
  gate = paused_stage or rejected_stage
  result: dict[str, Any] = {
      "exists": True,
      "log_id": log_id,
      "state": state,
      "stages": stages,
      "paused_stage": paused_stage,
      "rejected_stage": rejected_stage,
      "instructions": instructions,
      "json_schema": json_schema,
      "resume_command": resume_command,
      "next_action": _next_action(log_id, state, gate, json_schema),
      "passport": compact_passport,
  }
  if verbose:
    result["mermaid_diagram"] = mermaid
  return result


_WOULD_FAIL_RE = re.compile(r"would fail at runtime: (.*?)\.$")


def _would_fail_stages(error: Optional[str]) -> list[str]:
  """Stage names from a dry run's `LightflowRunFailedError` message, if any.

  Args:
    error: The exception text, e.g. "Dry run: 1 stage(s) would fail at
      runtime: gated.", or None.

  Returns:
    The stage names, or an empty list.
  """
  m = _WOULD_FAIL_RE.search(error or "")
  return [x.strip() for x in m.group(1).split(",")] if m else []


def _resolve_lightflow_arg(arguments: dict[str, Any], log_id: str) -> str:
  """Returns the manifest path: the `lightflow` argument, else the Passport's."""
  lightflow = arguments.get("lightflow") or ""
  if lightflow:
    return str(lightflow)
  try:
    recorded = lib.PassportManager(log_id).load_passport()
  except Exception:  # pylint: disable=broad-exception-caught
    return ""
  if recorded is not None and recorded.lightflow_path:
    return recorded.lightflow_path
  return ""


_EXIT_TO_STATE = {
    0: "COMPLETED",
    lib.EXIT_FAILED: "FAILED",
    lib.EXIT_SUSPENDED: "SUSPENDED",
    lib.EXIT_NO_STATE: "NOT_FOUND",
    lib.EXIT_RUNNING: "RUNNING",
    lib.EXIT_WAITING: "WAITING",
}


def _stage_table(passport: Any, ordered: list[str]) -> list[dict[str, Any]]:
  """Compact per-stage view: name, effective status, one-line message."""
  latest: dict[str, Any] = {}
  for stamp in passport.stamps:
    latest[stamp.stage_name] = stamp
  rows: list[dict[str, Any]] = []
  names = ordered or list(latest)
  for name in names:
    stamp = latest.get(name)
    if stamp is None:
      rows.append({"name": name, "status": "NOT_STARTED", "message": ""})
      continue
    status = lib.effective_status_name(stamp)
    message = stamp.message or ""
    row: dict[str, Any] = {
        "name": name,
        "status": status,
        "message": engine.summarise(message),
    }
    if "Rollback executed successfully" in message:
      row["rolled_back"] = True
    elif "Rollback action" in message and "failed" in message:
      row["rolled_back"] = False
    rows.append(row)
  return rows


def _tool_result(obj: dict[str, Any], is_error: bool) -> dict[str, Any]:
  """Renders a tool result as text plus `structuredContent`.

  The text block is the serialized JSON on purpose: the MCP spec asks tools
  that return `structuredContent` to also return it serialized in a text
  block, for clients that do not surface structured content. Responses are
  kept compact instead (stamp log and diagram only with `verbose`), and the
  text is not pretty-printed: its reader is a model, and `indent=2` cost a
  quarter of the characters on a 22-stage run.

  Args:
    obj: JSON-serialisable result.
    is_error: Whether the call could not do what was asked.

  Returns:
    The MCP tool result object.
  """
  return {
      "content": [{"type": "text", "text": json.dumps(obj)}],
      "structuredContent": obj,
      "isError": is_error,
  }


# Exceptions that describe a workflow *outcome*. Anything else raised by the
# CLI (bad arguments, payload schema validation, compile errors, a held lock)
# means the call itself could not do what was asked.
_OUTCOME_EXCEPTIONS = (
    engine.OperatorActionSuspended,
    lib.LightflowAlreadyPausedError,
    lib.LightflowAlreadyCompletedError,
    lib.LightflowWaitingError,
    lib.LightflowRunFailedError,
)


def _run_outcome(
    exc: Optional[BaseException], observed_state: Optional[str]
) -> tuple[str, int, bool]:
  """Maps a CLI exception (or None) to `(state, exit_code, call_error)`.

  `observed_state` is what the Passport says afterwards; it refines a generic
  failure into REJECTED (a deliberate operator decision, not an error).

  Args:
    exc: The exception the CLI raised, or None on success.
    observed_state: The run state read back from the Passport.

  Returns:
    The canonical state, the CLI exit code, and whether the call itself
    failed (as opposed to the workflow reaching an outcome).
  """
  if exc is None:
    return "COMPLETED", 0, False
  if isinstance(
      exc, (engine.OperatorActionSuspended, lib.LightflowAlreadyPausedError)
  ):
    return "SUSPENDED", lib.EXIT_SUSPENDED, False
  if isinstance(exc, lib.LightflowAlreadyCompletedError):
    return "COMPLETED", 0, False
  if isinstance(exc, lib.LightflowWaitingError):
    return "WAITING", lib.EXIT_WAITING, False
  if isinstance(exc, lib.LightflowRunFailedError):
    if observed_state == "REJECTED":
      return "REJECTED", lib.EXIT_FAILED, False
    return "FAILED", lib.EXIT_FAILED, False
  if isinstance(exc, lib.LightflowRunningError):
    return "RUNNING", lib.EXIT_RUNNING, True
  if isinstance(exc, lib.LightflowStateNotFoundError):
    return "NOT_FOUND", lib.EXIT_NO_STATE, True
  # The run is unchanged; report its state and flag the call.
  return observed_state or "UNKNOWN", lib.EXIT_FAILED, True


def _outcome_message(exc: Optional[BaseException], state: str) -> str:
  """Short `message`; the structured fields carry the details.

  Args:
    exc: The exception the CLI raised, or None on success.
    state: The canonical state derived from it.

  Returns:
    A one-line message (plus the governance reminder when suspended).
  """
  if exc is None:
    return "Lightflow completed successfully."
  text = str(exc)
  if state == "SUSPENDED":
    first = text.strip().splitlines()[0] if text.strip() else text
    return (
        f"{first} Present `instructions` and `json_schema` to the human"
        " operator and wait for their decision (see `next_action`); do not"
        " self-approve."
    )
  return text


def _finish_run_call(
    exc: Optional[BaseException],
    log_id: str,
    lightflow: str,
    stdout: str,
) -> dict[str, Any]:
  """Shared tail of run_lightflow / resume_lightflow.

  Args:
    exc: The exception the CLI raised, or None on success.
    log_id: Run identifier.
    lightflow: Manifest path (may be empty when the Passport records it).
    stdout: Captured CLI output.

  Returns:
    The MCP tool result with one canonical `state`, `exit_code`, `message`,
    `stdout`, and `isError` set only for call errors or a genuine failure.
  """
  state_info = _inspect_run_state(log_id, lightflow)
  state, exit_code, call_error = _run_outcome(exc, state_info.get("state"))
  state_info["state"] = state
  state_info["status"] = state  # alias kept for older clients; same value.
  state_info["exit_code"] = exit_code
  state_info["message"] = _outcome_message(exc, state)
  if call_error:
    state_info["error"] = str(exc)
  state_info["stdout"] = stdout
  # `isError` means the call could not do what was asked, or the workflow hit
  # a genuine failure. A rejected gate, a suspension and a polling yield are
  # outcomes, not errors.
  return _tool_result(state_info, call_error or state == "FAILED")


def call_tool(name: str, arguments: dict[str, Any]) -> dict[str, Any]:
  """Executes an MCP tool call with stdout redirection and structured output."""
  cli = lib.LightflowRunnerCLI()
  buf = io.StringIO()

  if name == "run_lightflow":
    lightflow = arguments["lightflow"]
    log_id = arguments["log_id"]
    payload = arguments.get("payload", "{}")
    payload_file = arguments.get("payload_file")
    force = bool(arguments.get("force", False))

    exc: Optional[BaseException] = None
    with contextlib.redirect_stdout(buf):
      try:
        cli.start(
            lightflow=lightflow,
            log_id=log_id,
            force=force,
            payload=payload,
            payload_file=payload_file,
        )
      except Exception as e:  # pylint: disable=broad-exception-caught
        exc = e
    return _finish_run_call(exc, log_id, lightflow, buf.getvalue())

  elif name == "resume_lightflow":
    log_id = arguments["log_id"]
    lightflow = _resolve_lightflow_arg(arguments, log_id)
    stage = arguments.get("stage")
    resolution = arguments.get("resolution")
    payload = arguments.get("payload", "{}")
    payload_file = arguments.get("payload_file")
    comment = arguments.get("comment")
    operator = arguments.get("operator")
    cascade = bool(arguments.get("cascade", True))

    exc: Optional[BaseException] = None
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
            payload_file=payload_file,
        )
      except Exception as e:  # pylint: disable=broad-exception-caught
        exc = e
    return _finish_run_call(exc, log_id, lightflow, buf.getvalue())

  elif name == "get_lightflow_status":
    log_id = arguments["log_id"]
    lightflow = _resolve_lightflow_arg(arguments, log_id)
    verbose = bool(arguments.get("verbose", False))
    err_msg = None
    exit_code = 0
    tool_failed = False
    with contextlib.redirect_stdout(buf):
      try:
        cli.status(lightflow=lightflow, log_id=log_id, verbose=verbose)
      except lib.LightflowAlreadyPausedError:
        exit_code = lib.EXIT_SUSPENDED
      except lib.LightflowRunFailedError as e:
        exit_code = lib.EXIT_FAILED
        err_msg = str(e)
      except lib.LightflowWaitingError as e:
        exit_code = lib.EXIT_WAITING
        err_msg = str(e)
      except lib.LightflowStateNotFoundError as e:
        exit_code = lib.EXIT_NO_STATE
        err_msg = str(e)
      except lib.LightflowRunningError:
        exit_code = lib.EXIT_RUNNING
      except Exception as e:  # pylint: disable=broad-exception-caught
        tool_failed = True
        err_msg = str(e)
        exit_code = lib.EXIT_FAILED
    state_info = _inspect_run_state(log_id, lightflow, verbose=verbose)
    state_info["exit_code"] = exit_code
    observed = state_info.get("state")
    # On exit 1 the Passport is more precise than the exit code: it tells a
    # rejection or interrupted run from a failure.
    if not (
        exit_code == lib.EXIT_FAILED
        and observed in ("FAILED", "REJECTED", "INCOMPLETE")
    ):
      state_info["state"] = _EXIT_TO_STATE.get(exit_code, observed)
    state_info["status"] = state_info["state"]  # alias, see run_lightflow.
    state_info["next_action"] = _next_action(
        log_id,
        str(state_info.get("state")),
        state_info.get("paused_stage") or state_info.get("rejected_stage"),
        state_info.get("json_schema"),
    )
    if err_msg:
      state_info["message"] = err_msg
    if tool_failed:
      state_info["error"] = err_msg
    # `isError` only when the call itself failed or the run hit a genuine
    # failure; rejected / waiting / suspended / not-found are answers.
    is_error = tool_failed or state_info.get("state") == "FAILED"
    return _tool_result(state_info, is_error)

  elif name == "list_lightflow_runs":
    state_dir = arguments.get("state_dir") or ""
    runs, orphan_locks, root = lib.collect_runs(state_dir)
    return _tool_result(
        {
            "state_dir": root,
            "runs": [
                {
                    "log_id": r.log_id,
                    "lightflow_name": r.lightflow_name,
                    "state": r.state,
                    "last_activity_epoch": r.last_activity,
                    "lightflow_path": r.lightflow_path,
                    "error": r.error,
                }
                for r in runs
            ],
            "orphan_locks": orphan_locks,
        },
        False,
    )

  elif name == "dry_run_lightflow":
    lightflow = arguments["lightflow"]
    payload = arguments.get("payload", "{}")
    payload_file = arguments.get("payload_file")
    compiled = True
    err_msg = None
    with contextlib.redirect_stdout(buf):
      try:
        cli.dry_run(
            lightflow=lightflow, payload=payload, payload_file=payload_file
        )
      except lib.LightflowRunFailedError as e:
        # Manifest compiled and was traced; some expressions would fail.
        err_msg = str(e)
      except Exception as e:  # pylint: disable=broad-exception-caught
        compiled = False
        err_msg = str(e)
    result = {
        "valid": compiled,
        "would_fail": _would_fail_stages(err_msg) if compiled else [],
        "error": err_msg,
        "trace": buf.getvalue(),
    }
    return _tool_result(result, (not compiled) or bool(result["would_fail"]))

  elif name == "visualize_lightflow":
    log_id = arguments.get("log_id")
    lightflow = (
        _resolve_lightflow_arg(arguments, log_id)
        if log_id
        else (arguments.get("lightflow") or "")
    )
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
    return _tool_result(result, is_err)

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
