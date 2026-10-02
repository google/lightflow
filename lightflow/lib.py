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

"""Library for managing Lightflow OSS execution loop and Passport state."""

from __future__ import annotations

import contextlib
import getpass
import html
import importlib
import json
import os
import pprint
import re
import shlex
import shutil
import sys
import tempfile
import time
from typing import Any, Optional

# pylint: disable=g-import-not-at-top,g-bad-import-order
fcntl: Any = None
msvcrt: Any = None
try:
  import fcntl as _fcntl

  fcntl = _fcntl
except ImportError:
  pass

if os.name == "nt":
  try:
    msvcrt = importlib.import_module("msvcrt")
  except ImportError:
    pass

try:
  from . import engine
  from . import schema
  from . import visualizer
except ImportError:
  from lightflow import engine  # pyrefly: ignore[missing-import]
  from lightflow import schema  # pyrefly: ignore[missing-import]
  from lightflow import visualizer  # pyrefly: ignore[missing-import]
# pylint: enable=g-import-not-at-top,g-bad-import-order


class StateCorruptedError(Exception):
  """Raised when workflow state file is present but corrupted."""


class LightflowAlreadyPausedError(Exception):
  """Raised when starting a lightflow that is already paused."""


class LightflowAlreadyCompletedError(Exception):
  """Raised when starting a lightflow that has already completed."""


class LightflowRunFailedError(Exception):
  """Raised by `status` when a stage failed or stages remain unfinished."""


class LightflowStateNotFoundError(Exception):
  """Raised by `status` when no Passport state exists for the log ID."""


class LightflowRunningError(Exception):
  """Raised by `status` when another process holds the run's lock."""


# Process exit code for a run that stopped at an operator action.
EXIT_SUSPENDED = 2

# Process exit code for a failed run, compile error, or other error.
EXIT_FAILED = 1

# Process exit code for `status` on a log ID with no saved state.
EXIT_NO_STATE = 3

# Process exit code for `status` while another process is executing the run.
EXIT_RUNNING = 4

_STAMP_ICONS = {
    "PENDING": "⏳",
    "COMPLETED": "✅",
    "SKIPPED": "⏭️",
    "FAILED": "❌",
    "REJECTED": "❌",
    "BLOCKED": "⛔",
    "PAUSED": "⏸️",
}

# Prefix of the FAILED stamp message `resume --resolution=REJECT` writes.
_REJECTED_PREFIX = "Rejected by "

# How long a contended lock is retried before giving up. Covers a `status`
# probe's momentary lock; a real run holds its lock far longer.
_LOCK_RETRY_SECONDS = 0.25
_LOCK_RETRY_INTERVAL_SECONDS = 0.025


def _ensure_utf8_stdio() -> None:
  """Reconfigures stdout and stderr to UTF-8 on non-UTF-8 console encodings (e.g.

  Windows cp1252).
  """
  for stream in (sys.stdout, sys.stderr):
    enc = getattr(stream, "encoding", None)
    if isinstance(enc, str) and enc.lower().replace("-", "") != "utf8":
      reconfigure = getattr(stream, "reconfigure", None)
      if callable(reconfigure):
        try:
          reconfigure(encoding="utf-8", errors="replace")
        except Exception:  # pylint: disable=broad-exception-caught
          pass


_ensure_utf8_stdio()


def _try_lock(fd: Any, exclusive: bool) -> bool:
  """Takes a non-blocking lock; returns False if another process holds it."""
  try:
    if fcntl is not None:
      flags = fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH
      fcntl.flock(fd, flags | fcntl.LOCK_NB)
    elif msvcrt is not None:
      # msvcrt has no shared mode; every lock is exclusive.
      fd.seek(0)
      msvcrt.locking(fd.fileno(), msvcrt.LK_NBLCK, 1)
  except (BlockingIOError, PermissionError):
    # flock reports contention as EWOULDBLOCK, msvcrt as EACCES. Other
    # OSErrors (e.g. ENOLCK) are real failures and propagate.
    return False
  return True


def _lock_with_retry(fd: Any, exclusive: bool, retry_seconds: float) -> bool:
  """Calls `_try_lock`, retrying for `retry_seconds` on contention."""
  deadline = time.monotonic() + retry_seconds
  while not _try_lock(fd, exclusive):
    if time.monotonic() >= deadline:
      return False
    time.sleep(_LOCK_RETRY_INTERVAL_SECONDS)
  return True


def _unlock(fd: Any) -> None:
  """Releases a lock taken by `_try_lock`, ignoring errors."""
  try:
    if fcntl is not None:
      fcntl.flock(fd, fcntl.LOCK_UN)
    elif msvcrt is not None:
      fd.seek(0)
      msvcrt.locking(fd.fileno(), msvcrt.LK_UNLCK, 1)
  except OSError:
    pass


def _current_operator() -> str:
  """Returns the invoking operator identity (with agent session provenance if set)."""
  explicit = os.environ.get("LIGHTFLOW_OPERATOR", "").strip()
  if explicit:
    return explicit
  try:
    user = getpass.getuser()
  except Exception:  # pylint: disable=broad-except
    user = "unknown"
  conv_id = os.environ.get("ANTIGRAVITY_CONVERSATION_ID", "").strip()
  if conv_id:
    return f"{user} (agent:{conv_id})"
  return user


_SAFE_WORKFLOW_ID_RE = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,127}$")


class PassportManager:
  """Manages reading, writing, and locking workflow Passport state on disk."""

  def __init__(self, workflow_id: str):
    self.workflow_id = str(workflow_id).strip()
    resolved_path = self._resolve_workflow_id(self.workflow_id)
    self._validate_path_safety(resolved_path)
    self.resolved_path = os.path.realpath(os.path.expanduser(resolved_path))

  @contextlib.contextmanager
  def lock(self, exclusive: bool = True):
    """Context manager to acquire an advisory file lock on the state directory."""
    parent_dir = os.path.dirname(self.resolved_path)
    os.makedirs(parent_dir, exist_ok=True)

    lock_path = self.resolved_path + ".lock"
    self._validate_path_safety(lock_path)

    open_flags = (
        os.O_CREAT
        | os.O_RDWR
        | getattr(os, "O_NOFOLLOW", 0)
        | getattr(os, "O_BINARY", 0)
    )
    raw_fd = os.open(lock_path, open_flags, 0o600)
    fd = os.fdopen(raw_fd, "r+b")
    try:
      acquired = _lock_with_retry(fd, exclusive, _LOCK_RETRY_SECONDS)
    except OSError:
      fd.close()
      raise
    if not acquired:
      fd.close()
      raise RuntimeError(
          f"Could not acquire lock on workflow '{self.workflow_id}'."
          " Another process may be running this workflow."
      )

    try:
      yield
    finally:
      _unlock(fd)
      fd.close()

  def is_locked(self) -> bool:
    """Returns whether another process holds this run's lock.

    Read-only probe for `status`: never creates the lock file or its
    directory, and holds a lock only momentarily. Where locks are exclusive
    only (Windows), it retries briefly so two concurrent probes do not mistake
    each other for a running process.
    """
    lock_path = self.resolved_path + ".lock"
    self._validate_path_safety(lock_path)
    open_flags = (
        os.O_RDWR | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_BINARY", 0)
    )
    try:
      raw_fd = os.open(lock_path, open_flags)
    except FileNotFoundError:
      return False
    retry_seconds = 0.0 if fcntl is not None else _LOCK_RETRY_SECONDS
    with os.fdopen(raw_fd, "r+b") as fd:
      if not _lock_with_retry(fd, False, retry_seconds):
        return True
      _unlock(fd)
      return False

  def _resolve_workflow_id(self, workflow_id: str) -> str:
    """Resolves a logical workflow_id or path to a local state directory."""
    expanded = os.path.expanduser(workflow_id)
    if (
        workflow_id.startswith("/")
        or workflow_id.startswith("~")
        or os.path.isabs(expanded)
    ):
      return expanded
    if not _SAFE_WORKFLOW_ID_RE.match(workflow_id):
      raise ValueError(
          f"Invalid workflow_id '{workflow_id}'. Logical IDs must match"
          " ^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,127}$ without path separators."
      )
    cleaned_id = workflow_id.lower().strip()
    if cleaned_id != workflow_id.strip():
      # stderr, so stdout stays clean for callers that parse it (and for the
      # MCP server's JSON-RPC stream).
      print(
          f"ℹ️ Log IDs are case-insensitive: '{workflow_id.strip()}' shares"
          f" state with '{cleaned_id}'.",
          file=sys.stderr,
      )
    env_state_dir = os.environ.get("LIGHTFLOW_STATE_DIR")
    if env_state_dir:
      return os.path.join(
          os.path.expanduser(env_state_dir), f"lightflow_state_{cleaned_id}"
      )
    return os.path.expanduser(f"~/.lightflow/lightflow_state_{cleaned_id}")

  def _validate_path_safety(self, path: str) -> None:
    """Validates that the path resides within allowed safe directories."""
    normalized = os.path.realpath(os.path.expanduser(path))
    gemini_sandbox = os.path.realpath(os.path.expanduser("~/.gemini/"))
    lightflow_sandbox = os.path.realpath(os.path.expanduser("~/.lightflow/"))
    sys_tmp = os.path.realpath(tempfile.gettempdir())
    real_slash_tmp = os.path.realpath("/tmp")

    allowed_roots = [
        gemini_sandbox,
        lightflow_sandbox,
        sys_tmp,
        real_slash_tmp,
    ]
    env_state_dir = os.environ.get("LIGHTFLOW_STATE_DIR")
    if env_state_dir:
      env_root = os.path.realpath(os.path.expanduser(env_state_dir))
      allowed_roots.append(env_root)

    norm_target = os.path.normcase(normalized)
    if any(
        norm_target == os.path.normcase(r) for r in (*allowed_roots, "/tmp")
    ):
      raise ValueError("Cannot operate on the sandbox root directory itself.")

    def _is_within(root: str, target: str) -> bool:
      try:
        nc_root = os.path.normcase(root)
        nc_target = os.path.normcase(target)
        return (
            nc_target != nc_root
            and os.path.commonpath([nc_root, nc_target]) == nc_root
        )
      except ValueError:
        return False

    is_in_sandbox = any(_is_within(root, normalized) for root in allowed_roots)
    is_in_tmp = normalized.startswith("/tmp/")

    if is_in_tmp and len(normalized.split("/")) < 3:
      raise ValueError(f"Unsafe tmp path blocked: {path}")

    if not (is_in_sandbox or is_in_tmp):
      raise ValueError(f"Unsafe directory path blocked: {path}")

    base_name = os.path.basename(normalized)
    if base_name.endswith(".lock"):
      base_name = base_name[: -len(".lock")]
    if not base_name.startswith("lightflow_state_"):
      raise ValueError(
          f"Unsafe state directory basename '{base_name}': must start with"
          " 'lightflow_state_'."
      )

    if os.path.exists(normalized) and hasattr(os, "getuid"):
      try:
        st = os.stat(normalized)
        if st.st_uid != os.getuid():
          raise ValueError(
              f"Directory ownership conflict: '{path}' is owned by UID"
              f" {st.st_uid}, but the current process is running as UID"
              f" {os.getuid()}."
          )
      except OSError as e:
        raise ValueError(
            f"Failed to verify directory ownership of '{path}': {e}"
        ) from e

  def load_passport(self) -> Optional[schema.Passport]:
    """Loads the passport state from disk.

    Safe to call without acquiring `lock(exclusive=False)` during active runs
    because `save_passport` writes to a temporary file and commits via atomic
    `os.replace`, preventing partial reads.

    Returns:
      The parsed `Passport` object, or `None` if no passport exists on disk.

    Raises:
      StateCorruptedError: If the passport file exists but cannot be parsed.
    """
    passport_file = os.path.join(self.resolved_path, "passport.json")
    if not os.path.exists(passport_file):
      return None
    try:
      with open(passport_file, "r", encoding="utf-8") as f:
        data = json.loads(f.read())
      return schema.Passport.from_dict(data)
    except Exception as e:
      raise StateCorruptedError(
          "Failed to load or parse workflow passport state from disk at"
          f" '{passport_file}': {e}"
      ) from e

  def save_passport(self, passport: schema.Passport) -> None:
    """Saves the passport state to disk atomically."""
    os.makedirs(self.resolved_path, mode=0o700, exist_ok=True)
    self._validate_path_safety(self.resolved_path)
    passport_file = os.path.join(self.resolved_path, "passport.json")
    json_str = json.dumps(passport.to_dict(), indent=2)
    temp_file = None
    try:
      with tempfile.NamedTemporaryFile(
          mode="w",
          dir=self.resolved_path,
          prefix="passport.json.tmp-",
          delete=False,
          encoding="utf-8",
      ) as f:
        temp_file = f.name
        f.write(json_str)
      os.replace(temp_file, passport_file)
      temp_file = None
    except Exception as e:
      if temp_file and os.path.exists(temp_file):
        try:
          os.remove(temp_file)
        except OSError:
          pass
      raise StateCorruptedError(
          f"Failed to save passport state to disk: {e}"
      ) from e

  def delete_state(self, remove_lock: bool = True) -> None:
    """Deletes the workflow state directory and optionally the advisory lock file."""
    if os.path.islink(self.resolved_path):
      raise ValueError(
          f"Refusing to delete symlinked state path: '{self.resolved_path}'."
      )
    self._validate_path_safety(self.resolved_path)
    if os.path.exists(self.resolved_path):
      shutil.rmtree(self.resolved_path)
    if remove_lock:
      lock_path = self.resolved_path + ".lock"
      if os.path.exists(lock_path) and not os.path.islink(lock_path):
        try:
          os.remove(lock_path)
        except OSError:
          pass


def generate_status_mermaid(
    workflow: schema.Lightflow, passport: Optional[schema.Passport] = None
) -> str:
  """Generates a color-coded Mermaid diagram representing the workflow DAG."""
  stage_statuses = {}
  stage_messages = {}
  if passport is not None:
    for stamp in passport.stamps:
      stage_statuses[stamp.stage_name] = stamp.status
      stage_messages[stamp.stage_name] = stamp.message

  node_ids: dict[str, str] = {}
  used_ids: set[str] = set()
  for stage in workflow.stages:
    base_id = re.sub(r"[^a-zA-Z0-9_]", "_", stage.name) or "stage"
    candidate = base_id
    suffix_idx = 2
    while candidate in used_ids:
      candidate = f"{base_id}_{suffix_idx}"
      suffix_idx += 1
    used_ids.add(candidate)
    node_ids[stage.name] = candidate

  lines = ["```mermaid", "graph TD"]

  for stage in workflow.stages:
    name = stage.name
    node_id = node_ids[name]
    desc = html.escape(stage.description or name).replace('"', '\\"')
    status = stage_statuses.get(name, schema.StampStatus.STATUS_UNSPECIFIED)
    msg = stage_messages.get(name, "")
    rollback_suffix = ""
    if passport is not None and status == schema.StampStatus.FAILED:
      if "Rollback executed successfully" in msg:
        rollback_suffix = " [rolled back]"
      elif "Rollback action " in msg and " failed:" in msg:
        rollback_suffix = " [rollback failed]"

    if stage.HasField("python_action") and stage.python_action:
      node_str = (
          f'  {node_id}["{desc} (python_action:'
          f' {stage.python_action.action_id}){rollback_suffix}"]'
      )
    elif stage.HasField("operator_action"):
      node_str = f'  {node_id}{{"{desc} (operator_action){rollback_suffix}"}}'
    else:
      node_str = f'  {node_id}["{desc}{rollback_suffix}"]'
    lines.append(node_str)

    if passport is None:
      if stage.HasField("operator_action"):
        lines.append(
            f"  style {node_id} fill:#fff3cd,stroke:#ffc107,stroke-width:2px"
        )
      else:
        lines.append(
            f"  style {node_id} fill:#e2f0d9,stroke:#385723,stroke-width:1px"
        )
      continue

    if status == schema.StampStatus.COMPLETED:
      lines.append(
          f"  style {node_id} fill:#e2f0d9,stroke:#385723,stroke-width:2px"
      )
    elif status == schema.StampStatus.SKIPPED:
      lines.append(
          f"  style {node_id}"
          " fill:#f2f2f2,stroke:#7f7f7f,stroke-width:1px,stroke-dasharray: 5 5"
      )
    elif status == schema.StampStatus.FAILED:
      lines.append(
          f"  style {node_id} fill:#f8cbad,stroke:#c00000,stroke-width:2px"
      )
    elif status == schema.StampStatus.PAUSED:
      lines.append(
          f"  style {node_id} fill:#fff3cd,stroke:#ffc107,stroke-width:2px"
      )
    else:
      lines.append(
          f"  style {node_id} fill:#ffffff,stroke:#bfbfbf,stroke-width:1px"
      )

  for stage in workflow.stages:
    node_id = node_ids[stage.name]
    for parent in stage.run_after:
      parent_id = node_ids.get(parent, re.sub(r"[^a-zA-Z0-9_]", "_", parent))
      if stage.run_if:
        sanitized_cond = stage.run_if.replace('"', "'")
        lines.append(f'  {parent_id} -. "if: {sanitized_cond}" .-> {node_id}')
      else:
        lines.append(f"  {parent_id} --> {node_id}")

  lines.append("```")
  return "\n".join(lines)


_generate_status_mermaid = generate_status_mermaid


def _synthesize_mock_gate_payload(
    operator_action: schema.OperatorAction,
) -> dict[str, Any]:
  """Synthesizes a mock approval dictionary from an operator_action schema for dry_run."""
  mock_data: dict[str, Any] = {"approved": True, "resolution": "APPROVE"}
  if not operator_action.json_schema:
    return mock_data
  try:
    schema_dict = json.loads(operator_action.json_schema)
  except Exception:  # pylint: disable=broad-exception-caught
    return mock_data
  if not isinstance(schema_dict, dict):
    return mock_data
  props = schema_dict.get("properties")
  if not isinstance(props, dict):
    return mock_data

  required_raw = schema_dict.get("required")
  required_fields = (
      set(required_raw) if isinstance(required_raw, list) else None
  )

  for prop_name, prop_schema in props.items():
    if not isinstance(prop_schema, dict):
      continue
    if "default" in prop_schema:
      mock_data[prop_name] = prop_schema["default"]
    elif required_fields and prop_name not in required_fields:
      continue
    elif "enum" in prop_schema and prop_schema["enum"]:
      mock_data[prop_name] = prop_schema["enum"][0]
    else:
      prop_type = prop_schema.get("type")
      if prop_type == "boolean":
        mock_data[prop_name] = True
      elif prop_type == "integer":
        mock_data[prop_name] = 1
      elif prop_type == "number":
        mock_data[prop_name] = 1.0
      elif prop_type == "string":
        mock_data[prop_name] = f"<mock_{prop_name}>"
      elif prop_type == "array":
        items = prop_schema.get("items") or {}
        item_type = items.get("type") if isinstance(items, dict) else None
        if item_type == "integer":
          mock_data[prop_name] = [1]
        elif item_type == "string":
          mock_data[prop_name] = [f"<mock_{prop_name}>"]
        else:
          mock_data[prop_name] = []
      elif prop_type == "object":
        mock_data[prop_name] = {}
  mock_data["approved"] = True
  mock_data["resolution"] = "APPROVE"
  return mock_data


def _raise_for_run_state(
    state: str,
    paused: list[str],
    failed: list[str],
    rejected: list[str],
    unfinished: list[str],
) -> None:
  """Raises the exception the runner maps to `state`'s exit code."""
  if state == "RUNNING":
    raise LightflowRunningError(
        "Lightflow is running in another process. Wait for it to finish and"
        " check 'status' again; do not run 'resume' or 'start' meanwhile."
    )
  if state == "PAUSED":
    raise LightflowAlreadyPausedError(
        f"Lightflow is paused at operator action '{paused[0]}'. Present the"
        " gate to the operator, then run 'resume' with their decision."
    )
  if state == "FAILED":
    suffix = f" Rejected gates: {rejected}." if rejected else ""
    raise LightflowRunFailedError(
        f"Lightflow failed. Failed stages: {failed}.{suffix} Fix the root"
        " cause and run 'resume'."
    )
  if state == "REJECTED":
    raise LightflowRunFailedError(
        f"Lightflow stopped: operator action(s) {rejected} were rejected."
        " Nothing to fix; run 'resume' only if the operator wants to re-open"
        " the gate for a new decision."
    )
  if state == "INCOMPLETE":
    raise LightflowRunFailedError(
        f"Lightflow has unfinished stages: {unfinished}. Run 'resume' to"
        " continue."
    )


def _parse_payload_arg(
    payload: str | dict[str, Any],
    *,
    context_label: str = "--payload",
    allow_outputs: bool = False,
) -> dict[str, Any]:
  """Parses a payload dict, JSON string, or `@<file>` / `@-` file reference."""
  if isinstance(payload, dict):
    parsed: Any = dict(payload)
  elif isinstance(payload, str):
    stripped = payload.strip()
    if stripped.startswith("@") and len(stripped) > 1:
      file_ref = stripped[1:]
      if file_ref == "-":
        raw_json = sys.stdin.read().lstrip("\ufeff")
      else:
        file_path = os.path.expanduser(file_ref)
        with open(file_path, "r", encoding="utf-8-sig") as f:
          raw_json = f.read()
      try:
        parsed = json.loads(raw_json)
      except json.JSONDecodeError as e:
        raise ValueError(
            f"Failed to parse {context_label} JSON from '{stripped}': {e}"
        ) from e
    else:
      try:
        parsed = json.loads(payload)
      except json.JSONDecodeError as e:
        raise ValueError(
            f"Failed to parse {context_label} JSON: {e}. Tip: you can pass a"
            " JSON file via --payload=@payload.json (or --payload=@- for"
            " stdin)."
        ) from e
  else:
    parsed = payload

  if not isinstance(parsed, dict):
    raise ValueError(
        f"{context_label} must be a JSON object (dictionary), got:"
        f" {type(parsed).__name__}"
    )

  if not allow_outputs and engine.STAGE_OUTPUTS_KEY in parsed:
    raise ValueError(
        f"{context_label} cannot contain reserved key"
        f" '{engine.STAGE_OUTPUTS_KEY}'; stage outputs are managed by the"
        " engine."
    )
  return dict(parsed)


class LightflowRunnerCLI:
  """CLI interface for executing, inspecting, and resuming Lightflow DAGs.

  Core Lifecycle & Exit Codes:
    - start --lightflow=<path> --log_id=<id> [--payload='{...}' |
      --payload=@file.json] [--dry_run]:
      Starts a new run (or previews the DAG trace with --dry_run).
      * Exit 0: Completed all stages.
      * Exit 2 (SUSPENDED): Paused at an `operator_action` human-in-the-loop
        checkpoint. STOP and present the printed Instructions and Payload Schema
        to the human operator/user. Do NOT self-approve or run `resume` until
        the operator provides their explicit decision.
      * Exit 1 (FAILED): A stage failed. Fix the root cause and run `resume`
        (never `start --force`, which wipes the Passport and repeats completed
        upstream side effects).
    - resume --lightflow=<path> --log_id=<id> [--stage=<gate>
      --resolution=APPROVE|REJECT --payload='{...}' | --payload=@file.json]:
      Resolves a paused `operator_action` gate or re-arms failed stages and
      their downstream dependents without re-running completed upstream stages.
    - status --lightflow=<path> --log_id=<id> [--verbose]:
      Prints each stage's latest status and any paused gate's Instructions,
      Payload Schema, and resume command. Exits like the run: 0 completed,
      1 failed, rejected, or interrupted, 2 paused, 3 no saved state,
      4 running in another process. `--verbose` adds the Passport payload,
      full stamp log, and Mermaid progress diagram.
    - visualize --lightflow=<path> [--log_id=<id>] [--out=<file.html>]:
      Generates a standalone interactive HTML5 DAG & Passport visualizer.
    - cleanup --log_id=<id> [--lightflow=<path>]:
      Deletes the saved Passport state directory for `--log_id`.
  """

  def start(
      self,
      lightflow: str = "",
      log_id: str = "",
      force: bool = False,
      payload: str | dict[str, Any] = "{}",
      dry_run: bool = False,
      *,
      workflow: Optional[str] = None,
  ) -> None:
    """Starts a new lightflow execution run."""
    if dry_run:
      self.dry_run(
          lightflow=lightflow,
          payload=payload,
          log_id=log_id,
          workflow=workflow,
      )
      return
    resolved_lightflow = lightflow or workflow
    if not resolved_lightflow:
      raise ValueError("Missing required '--lightflow' manifest path.")
    workflow_path = os.path.expanduser(resolved_lightflow)
    pm = PassportManager(log_id)

    with pm.lock(exclusive=True):
      workflow_proto = schema.load_lightflow(workflow_path)
      runner = engine.LightflowEngine(
          workflow_proto, lightflow_path=workflow_path, log_id=pm.workflow_id
      )
      try:
        ordered_stages = runner.compile()
      except Exception as e:
        raise engine.EngineError(f"Compilation Error: {e}") from e

      initial_payload = _parse_payload_arg(
          payload, context_label="--payload", allow_outputs=False
      )

      if force:
        pm.delete_state(remove_lock=False)

      passport = pm.load_passport()
      if passport:
        latest_status = {}
        for stamp in passport.stamps:
          latest_status[stamp.stage_name] = stamp.status

        for stage_name, status in latest_status.items():
          if status == schema.StampStatus.PAUSED:
            raise LightflowAlreadyPausedError(
                f"Lightflow is paused at operator action '{stage_name}'."
                " Please run 'resume'."
            )

        if ordered_stages and all(
            latest_status.get(stage)
            in (
                schema.StampStatus.COMPLETED,
                schema.StampStatus.SKIPPED,
            )
            for stage in ordered_stages
        ):
          raise LightflowAlreadyCompletedError(
              "Lightflow has already completed successfully. Use --force to"
              " restart."
          )

        payload_updated = False
        if initial_payload:
          payload_dict = passport.payload.to_dict()
          payload_dict.update(initial_payload)
          passport.payload.CopyFrom(payload_dict)
          payload_updated = True

        rearmed = self._rearm_failed_stages(
            runner, ordered_stages, passport, operator=_current_operator()
        )
        if rearmed or payload_updated:
          pm.save_passport(passport)
        if rearmed:
          print(f"Re-armed {len(rearmed)} failed stage(s): {rearmed}")

      if not passport:
        passport = schema.Passport()
        if "operator" not in initial_payload:
          initial_payload["operator"] = _current_operator()

        passport.payload.CopyFrom(initial_payload)
        pm.save_passport(passport)
        print(f"Starting execution of lightflow '{workflow_proto.name}'...")
      else:
        print(f"Resuming execution of lightflow '{workflow_proto.name}'...")
      self._execute_engine_loop(runner, ordered_stages, passport, pm)

  def resume(
      self,
      lightflow: str = "",
      log_id: str = "",
      stage: Optional[str] = None,
      payload: str | dict[str, Any] = "{}",
      resolution: str = "APPROVE",
      comment: Optional[str] = None,
      operator: Optional[str] = None,
      cascade: bool = True,
      *,
      workflow: Optional[str] = None,
  ) -> None:
    """Continues an existing lightflow run or resolves an operator action."""
    resolved_lightflow = lightflow or workflow
    if not resolved_lightflow:
      raise ValueError("Missing required '--lightflow' manifest path.")
    workflow_path = os.path.expanduser(resolved_lightflow)
    pm = PassportManager(log_id)

    if not operator:
      operator = _current_operator()

    norm_resolution = str(resolution).upper().strip()
    if norm_resolution not in ("APPROVE", "REJECT", "1", "2"):
      raise ValueError(
          f"Invalid resolution '{resolution}'. Must be 'APPROVE' or 'REJECT'."
      )
    is_reject = norm_resolution in ("REJECT", "2")

    with pm.lock(exclusive=True):
      workflow_proto = schema.load_lightflow(workflow_path)
      passport = pm.load_passport()
      if not passport:
        raise ValueError(
            f"No workflow state found for log ID: {log_id}. Nothing to"
            " resume; use 'start' to begin a new run."
        )

      latest_status = {}
      for stamp in passport.stamps:
        latest_status[stamp.stage_name] = stamp.status

      if stage is None or (
          latest_status.get(stage) != schema.StampStatus.PAUSED
      ):
        self._resume_execution(
            workflow_proto,
            workflow_path,
            log_id,
            pm,
            passport,
            latest_status,
            stage,
            cascade,
            operator,
            payload=payload,
        )
        return

      action_stage = None
      for s in workflow_proto.stages:
        if s.name == stage and s.HasField("operator_action"):
          action_stage = s.operator_action
          break

      if not action_stage:
        raise ValueError(
            f"Operator action stage '{stage}' not found in configuration."
        )

      runner = engine.LightflowEngine(
          workflow_proto, lightflow_path=workflow_path, log_id=pm.workflow_id
      )
      try:
        ordered_stages = runner.compile()
      except Exception as e:
        raise engine.EngineError(f"Compilation Error: {e}") from e

      payload_dict = passport.payload.to_dict()
      payload_dict["operator"] = operator

      if is_reject:
        stage_output: dict[str, Any] = {
            "approved": False,
            "resolution": "REJECT",
        }
        if comment:
          payload_dict["rejection_comment"] = comment
          stage_output["comment"] = comment
        payload_dict["resolution"] = "REJECT"

        if payload and payload not in ("{}", ""):
          input_dict = _parse_payload_arg(
              payload,
              context_label=f"Operator action stage '{stage}' resume payload",
              allow_outputs=False,
          )
          payload_dict.update(input_dict)
          stage_output.update(input_dict)

        payload_dict["operator"] = operator
        payload_dict["approved"] = False
        payload_dict["resolution"] = "REJECT"
        stage_output["approved"] = False
        stage_output["resolution"] = "REJECT"
        engine.record_stage_outputs(payload_dict, stage, stage_output)
        passport.payload.CopyFrom(payload_dict)

        failed_stamp = passport.stamps.add()
        failed_stamp.stage_name = stage
        failed_stamp.status = schema.StampStatus.FAILED
        failed_stamp.timestamp.CopyFrom(engine.get_timestamp())
        reject_msg = f"{_REJECTED_PREFIX}{operator}."
        if comment:
          reject_msg += f" Reason: {comment}"
        failed_stamp.message = reject_msg

        stage_proto = runner.get_stage(stage)
        if stage_proto:
          runner.run_rollback_if_defined(
              stage, stage_proto, passport, payload_dict, failed_stamp
          )

        pm.save_passport(passport)
        engine.send_notification(
            f"Stage Rejected: {stage}", f"❌ {stage}: rejected"
        )

        print(f"Resuming execution after rejection of stage '{stage}'...")
        self._execute_engine_loop(runner, ordered_stages, passport, pm)
        return

      input_dict = _parse_payload_arg(
          payload,
          context_label=f"Operator action stage '{stage}' resume payload",
          allow_outputs=False,
      )

      approved_value = input_dict.get("approved", True)
      if not (isinstance(approved_value, bool) and approved_value):
        raise ValueError(
            f"Operator action stage '{stage}' resume payload sets"
            f" approved={input_dict['approved']!r} but --resolution is APPROVE"
            " (the default). To reject the gate, pass --resolution=REJECT; to"
            " approve it, drop 'approved' from the payload (it is set to true"
            " automatically)."
        )

      if action_stage.json_schema:
        try:
          schema_dict = json.loads(action_stage.json_schema)
        except json.JSONDecodeError as e:
          raise ValueError(
              "Failed to parse stage operator action json_schema"
              f" configuration: {e}"
          ) from e
        validation_dict = dict(input_dict)
        if isinstance(schema_dict, dict):
          props = schema_dict.get("properties")
          req_keys = schema_dict.get("required")
          if (isinstance(props, dict) and "approved" in props) or (
              isinstance(req_keys, list) and "approved" in req_keys
          ):
            input_dict.setdefault("approved", True)
            validation_dict.setdefault("approved", True)
          if isinstance(req_keys, list):
            stage_output_keys: set[str] = set()
            raw_outputs = payload_dict.get(engine.STAGE_OUTPUTS_KEY)
            if isinstance(raw_outputs, dict):
              for stage_out in raw_outputs.values():
                if isinstance(stage_out, dict):
                  stage_output_keys.update(stage_out.keys())
            for req_key in req_keys:
              if (
                  isinstance(req_key, str)
                  and req_key not in validation_dict
                  and req_key in payload_dict
                  and req_key not in stage_output_keys
                  and req_key
                  not in (
                      "outputs",
                      "operator",
                      "resolution",
                      "approved",
                      "approval_note",
                      "rejection_comment",
                  )
              ):
                validation_dict[req_key] = payload_dict[req_key]
        try:
          schema.validate_json_schema(
              instance=validation_dict, schema_dict=schema_dict
          )
        except Exception as e:
          raise ValueError(
              "Operator action resume payload failed JSON schema"
              f" validation: {e}"
          ) from e

      payload_dict.update(input_dict)
      stage_output = dict(input_dict)
      if comment:
        payload_dict["approval_note"] = comment
        stage_output["comment"] = comment
      payload_dict["operator"] = operator
      payload_dict["approved"] = True
      payload_dict["resolution"] = "APPROVE"
      stage_output["approved"] = True
      stage_output["resolution"] = "APPROVE"
      engine.record_stage_outputs(payload_dict, stage, stage_output)

      passport.payload.CopyFrom(payload_dict)

      stamp = passport.stamps.add()
      stamp.stage_name = stage
      stamp.status = schema.StampStatus.COMPLETED
      stamp.timestamp.CopyFrom(engine.get_timestamp())
      stamp_msg = f"Resumed and approved by {operator}."
      if comment:
        stamp_msg += f" Note: {comment}"
      stamp.message = stamp_msg

      pm.save_passport(passport)
      engine.send_notification(
          f"Stage Completed: {stage}", f"✅ {stage}: completed"
      )

      print(f"Resuming execution of lightflow starting at stage '{stage}'...")
      self._execute_engine_loop(runner, ordered_stages, passport, pm)

  def _resume_execution(
      self,
      workflow_proto: schema.Lightflow,
      workflow_path: str,
      log_id: str,
      pm: PassportManager,
      passport: schema.Passport,
      latest_status: dict[str, schema.StampStatus],
      stage: Optional[str],
      cascade: bool,
      operator: str,
      payload: str | dict[str, Any] = "{}",
  ) -> None:
    """Re-arms failed stages, then runs whatever is left of the graph."""
    del log_id
    runner = engine.LightflowEngine(
        workflow_proto, lightflow_path=workflow_path, log_id=pm.workflow_id
    )
    try:
      ordered_stages = runner.compile()
    except Exception as e:
      raise engine.EngineError(f"Compilation Error: {e}") from e

    if stage and stage not in ordered_stages:
      raise ValueError(
          f"Unknown stage '{stage}'. Stages in this lightflow:"
          f" {ordered_stages}."
      )

    paused = sorted(
        name
        for name, status in latest_status.items()
        if status == schema.StampStatus.PAUSED
    )
    if paused:
      raise LightflowAlreadyPausedError(
          f"Lightflow is paused at operator action '{paused[0]}'. Answer it"
          f" with 'resume --stage={paused[0]} --resolution=APPROVE|REJECT'."
      )

    if ordered_stages and all(
        latest_status.get(name)
        in (
            schema.StampStatus.COMPLETED,
            schema.StampStatus.SKIPPED,
        )
        for name in ordered_stages
    ):
      raise LightflowAlreadyCompletedError(
          "Lightflow has already completed successfully. Use 'start --force'"
          " to restart."
      )

    extra_payload: dict[str, Any] = {}
    if payload and payload not in ("{}", ""):
      extra_payload = _parse_payload_arg(
          payload, context_label="Resume payload", allow_outputs=False
      )

    rearmed = self._rearm_failed_stages(
        runner, ordered_stages, passport, stage, cascade, operator
    )
    if rearmed or extra_payload:
      payload_dict = passport.payload.to_dict()
      if extra_payload:
        payload_dict.update(extra_payload)
      payload_dict["operator"] = operator
      passport.payload.CopyFrom(payload_dict)
      pm.save_passport(passport)
    if rearmed:
      print(f"Re-armed {len(rearmed)} failed stage(s): {rearmed}")

    print(f"Resuming execution of lightflow '{workflow_proto.name}'...")
    self._execute_engine_loop(runner, ordered_stages, passport, pm)

  def _rearm_failed_stages(
      self,
      runner: engine.LightflowEngine,
      ordered_stages: list[str],
      passport: schema.Passport,
      stage: Optional[str] = None,
      cascade: bool = True,
      operator: Optional[str] = None,
  ) -> list[str]:
    """Marks failed stages eligible to run again, preserving completed work."""
    latest_status = {}
    latest_stamp = {}
    for stamp in passport.stamps:
      latest_status[stamp.stage_name] = stamp.status
      latest_stamp[stamp.stage_name] = stamp

    failed_stages = sorted(
        name
        for name, status in latest_status.items()
        if status == schema.StampStatus.FAILED
    )

    if stage:
      current = latest_status.get(stage)
      if current != schema.StampStatus.FAILED:
        status_name = (
            schema.StampStatus.Name(current).title()
            if current is not None
            else "Unstarted"
        )
        raise ValueError(
            f"Stage '{stage}' is not failed. Current status: {status_name}."
            f" Failed stages: {failed_stages if failed_stages else 'none'}."
        )
      targets = [stage]
    else:
      if not failed_stages:
        return []
      # Only root failures are re-armed implicitly: REJECTED gates need an
      # explicit --stage, and BLOCKED collateral whose upstream is still FAILED
      # is picked up below when cascade is True.
      blocked_by_failed_upstream = set()
      for failed_name in failed_stages:
        blocked_by_failed_upstream.update(
            runner.get_downstream_dependents(failed_name)
        )
      targets = [
          name
          for name in failed_stages
          if not (latest_stamp[name].message or "").startswith(_REJECTED_PREFIX)
          and (
              latest_stamp[name].message != engine.UPSTREAM_FAILED_MESSAGE
              or name not in blocked_by_failed_upstream
          )
      ]

    rearm = set(targets)
    if cascade:
      for target in targets:
        for dep in runner.get_downstream_dependents(target):
          if latest_status.get(dep) in (
              schema.StampStatus.FAILED,
              schema.StampStatus.SKIPPED,
          ):
            rearm.add(dep)

    for stage_name in ordered_stages:
      if stage_name not in rearm:
        continue
      previous = schema.StampStatus.Name(latest_status[stage_name]).title()
      stamp = passport.stamps.add()
      stamp.stage_name = stage_name
      stamp.status = schema.StampStatus.PENDING
      stamp.timestamp.CopyFrom(engine.get_timestamp())
      stamp.message = (
          f"Re-armed for retry by {operator} (previous status: {previous})."
      )

    return sorted(rearm)

  def status(
      self,
      lightflow: str = "",
      log_id: str = "",
      verbose: bool = False,
      *,
      workflow: Optional[str] = None,
  ) -> None:
    """Prints a compact run summary and signals the run's state.

    Args:
      lightflow: Path to the Lightflow manifest.
      log_id: Run identifier whose Passport to inspect.
      verbose: Also print the Passport payload, the full stamp log, and the
        Mermaid progress diagram.
      workflow: Deprecated alias for `lightflow`.

    Raises:
      LightflowStateNotFoundError: No state exists for `log_id` (exit 3).
      LightflowRunningError: Another process is executing the run (exit 4).
      LightflowAlreadyPausedError: The run is paused at a gate (exit 2).
      LightflowRunFailedError: A stage failed or a gate was rejected, or the
        run was interrupted with stages unfinished (exit 1).
    """
    resolved_lightflow = lightflow or workflow
    if not resolved_lightflow:
      raise ValueError("Missing required '--lightflow' manifest path.")
    if not log_id:
      raise ValueError("Missing required '--log_id' run identifier.")
    lightflow_path = os.path.expanduser(resolved_lightflow)
    lightflow_proto = schema.load_lightflow(lightflow_path)
    pm = PassportManager(log_id)
    passport = pm.load_passport()
    if not passport:
      raise LightflowStateNotFoundError(
          f"No execution state found under log ID: {log_id}"
      )
    running = pm.is_locked()

    try:
      ordered_stages = engine.LightflowEngine(
          lightflow_proto, lightflow_path=lightflow_path, log_id=pm.workflow_id
      ).compile()
    except Exception as e:  # pylint: disable=broad-exception-caught
      # `status` only needs an order; still show the run for a manifest that
      # was edited or broke after it started.
      print(
          f"⚠️ Manifest does not compile ({engine.summarise(str(e))});"
          " listing stages in manifest order.",
          file=sys.stderr,
      )
      ordered_stages = [s.name for s in lightflow_proto.stages]

    latest: dict[str, schema.Stamp] = {}
    for stamp in passport.stamps:
      latest[stamp.stage_name] = stamp

    # Effective status per stage. A rejected gate is stamped FAILED, and so is
    # every stage the engine never ran because an upstream stage failed; tell
    # those apart from stages whose own work failed (even ones that ran after a
    # failure, like ALL_DONE cleanups), which are the only ones needing a fix.
    stages_by_name = {s.name: s for s in lightflow_proto.stages}
    effective: dict[str, str] = {}
    for stage_name in ordered_stages:
      stamp = latest.get(stage_name)
      if stamp is None:
        effective[stage_name] = ""
        continue
      name = schema.StampStatus.Name(stamp.status)
      if name == "FAILED":
        if stamp.message.startswith(_REJECTED_PREFIX):
          name = "REJECTED"
        elif stamp.message == engine.UPSTREAM_FAILED_MESSAGE:
          name = "BLOCKED"
      effective[stage_name] = name

    paused = [s for s in ordered_stages if effective[s] == "PAUSED"]
    failed = [s for s in ordered_stages if effective[s] == "FAILED"]
    rejected = [s for s in ordered_stages if effective[s] == "REJECTED"]
    unfinished = [
        s
        for s in ordered_stages
        if effective[s] not in ("COMPLETED", "SKIPPED")
    ]
    if running:
      state = "RUNNING"
    elif paused:
      state = "PAUSED"
    elif failed:
      state = "FAILED"
    elif rejected:
      state = "REJECTED"
    elif unfinished:
      state = "INCOMPLETE"
    else:
      state = "COMPLETED"

    shown_id = pm.workflow_id
    if not os.path.isabs(os.path.expanduser(shown_id)):
      shown_id = shown_id.lower()
    print(f"Lightflow '{lightflow_proto.name}' (log ID: {shown_id}): {state}")
    for stage_name in ordered_stages:
      name = effective[stage_name]
      if not name:
        print(f"  ⬜ {stage_name}: not started")
        continue
      detail = ""
      if name in ("FAILED", "REJECTED"):
        summary = engine.summarise(latest[stage_name].message)
        detail = f" ({summary})" if summary else ""
      elif name == "BLOCKED":
        detail = " (upstream failed or was rejected)"
      icon = _STAMP_ICONS.get(name, "•")
      print(f"  {icon} {stage_name}: {name.lower()}{detail}")

    for stage_name in paused:
      stamp = latest[stage_name]
      action = stages_by_name[stage_name].operator_action
      json_schema = action.json_schema if action else ""
      print(f"\nPaused at operator action '{stage_name}':")
      if stamp.instructions:
        print(f"  Instructions: {stamp.instructions}")
      if json_schema:
        print(f"  Payload Schema: {json_schema}")
      if stamp.resume_command:
        print(f"  Resume with: {stamp.resume_command}")

    if verbose:
      print("\nPASSPORT PAYLOAD:")
      pprint.pprint(passport.payload.to_dict())

      print("\nSTAGE STAMPS LOG:")
      for stamp in passport.stamps:
        status_name = schema.StampStatus.Name(stamp.status).title()
        time_str = stamp.timestamp.ToJsonString()
        msg = f": {stamp.message}" if stamp.message else ""
        print(
            f"  [{time_str}] Stage '{stamp.stage_name}' -> {status_name}{msg}"
        )

      print("\nLIGHTFLOW STATUS DIAGRAM:")
      print(generate_status_mermaid(lightflow_proto, passport))

    _raise_for_run_state(state, paused, failed, rejected, unfinished)

  def dry_run(
      self,
      lightflow: str = "",
      payload: str | dict[str, Any] = "{}",
      log_id: str = "",
      *,
      workflow: Optional[str] = None,
  ) -> None:
    """Simulates lightflow execution paths without triggering side-effects."""
    resolved_lightflow = lightflow or workflow
    if not resolved_lightflow:
      raise ValueError("Missing required '--lightflow' manifest path.")
    workflow_path = os.path.expanduser(resolved_lightflow)
    workflow_proto = schema.load_lightflow(workflow_path)

    current_payload = _parse_payload_arg(
        payload, context_label="--payload", allow_outputs=True
    )

    print(f"=== DRY RUN: {workflow_proto.name} ===")
    print(f"Initial Payload: {current_payload}\n")

    preview_log_id = str(log_id).strip() if log_id else "<log_id>"
    runner = engine.LightflowEngine(
        workflow_proto,
        lightflow_path=workflow_path,
        log_id=preview_log_id or "<log_id>",
    )
    try:
      ordered_stages = runner.compile()
    except Exception as e:
      raise engine.EngineError(f"Compilation Error: {e}") from e

    print("Simulated Execution Trace:")
    sim_passport = schema.Passport()
    for stage_name in ordered_stages:
      stage_proto = runner.get_stage(stage_name)
      if not stage_proto:
        continue

      print(f"Stage: {stage_name}")
      if stage_proto.description:
        print(f"  Description: {stage_proto.description}")

      if stage_proto.run_after:
        trigger_res = runner.evaluate_trigger_rule(stage_proto, sim_passport)
        if (
            isinstance(trigger_res, tuple)
            and len(trigger_res) == 3
            and not trigger_res[0]
        ):
          _, target_status, reason = trigger_res
          print(f"  Trigger Rule: {reason}")
          print("  Status: [SKIPPED]\n")
          sim_stamp = sim_passport.stamps.add()
          sim_stamp.stage_name = stage_name
          sim_stamp.status = target_status
          continue

      should_run = True
      if stage_proto.run_if:
        try:
          should_run = engine.evaluate_cel(stage_proto.run_if, current_payload)
          print(f"  Condition '{stage_proto.run_if}' -> {should_run}")
        except engine.EngineError as e:
          print(f"  Condition evaluation failed: {e}")
          should_run = False

      if not should_run:
        print("  Status: [SKIPPED]\n")
        sim_stamp = sim_passport.stamps.add()
        sim_stamp.stage_name = stage_name
        sim_stamp.status = schema.StampStatus.SKIPPED
        continue

      if stage_proto.HasField("polling_policy") and stage_proto.polling_policy:
        policy = stage_proto.polling_policy
        tick_action = (
            policy.poll_tick_action
            if policy.HasField("poll_tick_action") and policy.poll_tick_action
            else stage_proto.python_action
        )
        if tick_action is not None:
          print(
              "  Action: [POLL] executes"
              f" '{tick_action.action_id}' until '{policy.condition}'"
          )
          if tick_action.static_kwargs:
            print(f"    Static Args: {dict(tick_action.static_kwargs)}")
          simulated = runner.simulate_dry_run_action(
              stage_name, tick_action, current_payload
          )
          if simulated is not None:
            sim_payload, sim_msg = simulated
            engine.merge_stage_payload(current_payload, stage_name, sim_payload)
            print(f"    Dry-Run Output: {sim_msg}")
      elif stage_proto.HasField("python_action") and stage_proto.python_action:
        print(
            "  Action: [RUN] executes python_action"
            f" '{stage_proto.python_action.action_id}'"
        )
        if stage_proto.python_action.static_kwargs:
          print(
              "    Static Args:"
              f" {dict(stage_proto.python_action.static_kwargs)}"
          )
        simulated = runner.simulate_dry_run_action(
            stage_name, stage_proto.python_action, current_payload
        )
        if simulated is not None:
          sim_payload, sim_msg = simulated
          engine.merge_stage_payload(current_payload, stage_name, sim_payload)
          print(f"    Dry-Run Output: {sim_msg}")
      elif (
          stage_proto.HasField("operator_action")
          and stage_proto.operator_action
      ):
        operator_action = stage_proto.operator_action
        try:
          instructions = engine.evaluate_cel(
              operator_action.instructions, current_payload
          )
          print("  Action: [SUSPEND] waits at operator_action")
          print(f"    Instructions: {instructions}")
        except Exception as e:  # pylint: disable=broad-exception-caught
          print(
              "  Action: [SUSPEND] waits at operator_action (instructions"
              f" eval failed: {e})"
          )
        print(
            f"    Output Namespace: payload.outputs.{stage_name}"
            " (and top-level payload)"
        )
        if operator_action.json_schema:
          print(f"    Payload Schema: {operator_action.json_schema}")
        resume_command = runner.build_resume_command(
            stage_name, operator_action
        )
        if resume_command:
          print(f"    Resume With: {resume_command}")
        mock_gate = _synthesize_mock_gate_payload(operator_action)
        existing_outputs = current_payload.get("outputs")
        if isinstance(existing_outputs, dict) and isinstance(
            existing_outputs.get(stage_name), dict
        ):
          mock_gate.update(existing_outputs[stage_name])
        for k, v in mock_gate.items():
          current_payload.setdefault(k, v)
        engine.record_stage_outputs(current_payload, stage_name, mock_gate)
      sim_stamp = sim_passport.stamps.add()
      sim_stamp.stage_name = stage_name
      sim_stamp.status = schema.StampStatus.COMPLETED
      print()

  def render(
      self,
      lightflow: str = "",
      *,
      workflow: Optional[str] = None,
  ) -> None:
    """Generates a Mermaid visual representation of the lightflow DAG."""
    resolved_lightflow = lightflow or workflow
    if not resolved_lightflow:
      raise ValueError("Missing required '--lightflow' manifest path.")
    workflow_path = os.path.expanduser(resolved_lightflow)
    workflow_proto = schema.load_lightflow(workflow_path)
    print(generate_status_mermaid(workflow_proto))

  def visualize(
      self,
      lightflow: str = "",
      log_id: Optional[str] = None,
      output: Optional[str] = None,
      title: Optional[str] = None,
      *,
      out: Optional[str] = None,
      workflow: Optional[str] = None,
  ) -> str:
    """Generates an interactive Generative UI HTML visualizer for the lightflow."""
    resolved_lightflow = lightflow or workflow
    if not resolved_lightflow:
      raise ValueError("Missing required '--lightflow' manifest path.")
    workflow_path = os.path.expanduser(resolved_lightflow)
    workflow_proto = schema.load_lightflow(workflow_path)

    passport_proto = None
    if log_id:
      pm = PassportManager(log_id)
      passport_proto = pm.load_passport()

    html_content = visualizer.generate_visualizer_html(
        lightflow=workflow_proto,
        passport=passport_proto,
        title=title,
        standalone=True,
    )

    resolved_output = output or out
    if resolved_output:
      out_path = os.path.expanduser(resolved_output)
      os.makedirs(os.path.dirname(os.path.abspath(out_path)), exist_ok=True)
      with open(out_path, "w", encoding="utf-8") as f:
        f.write(html_content)
      print(f"Visualizer HTML generated at: {out_path}")
      return out_path

    return html_content

  def _send_status_notification(
      self,
      runner: engine.LightflowEngine,
      passport: schema.Passport,
      message: str,
      is_failed: bool = False,
  ) -> None:
    """Sends a concise status notification to the active conversation."""
    latest_status = {st.stage_name: st.status for st in passport.stamps}
    if is_failed:
      title = "Lightflow Failed"
    elif schema.StampStatus.PAUSED in latest_status.values():
      title = "Lightflow Suspended"
    elif all(
        latest_status.get(stage.name)
        in (
            schema.StampStatus.COMPLETED,
            schema.StampStatus.SKIPPED,
        )
        for stage in runner.lightflow.stages
    ):
      title = "Lightflow Completed"
    else:
      title = "Lightflow Status Update"

    engine.send_notification(title, message)

  def _execute_engine_loop(
      self,
      runner: engine.LightflowEngine,
      ordered_stages: list[str],
      passport: schema.Passport,
      pm: PassportManager,
  ) -> None:
    """Iterates through topological stages, running executable steps."""
    for stage_name in ordered_stages:
      latest_status = {
          stamp.stage_name: stamp.status for stamp in passport.stamps
      }

      if latest_status.get(stage_name) in (
          schema.StampStatus.COMPLETED,
          schema.StampStatus.SKIPPED,
          schema.StampStatus.FAILED,
      ):
        continue

      try:
        passport = runner.execute_stage(
            stage_name, passport, save_callback=pm.save_passport
        )
        pm.save_passport(passport)
      except engine.OperatorActionSuspended:
        pm.save_passport(passport)
        raise
      except (engine.StageTimeoutError, engine.EngineError):
        # Stage failure or timeout has already been stamped as FAILED (and
        # rolled back if configured) inside execute_stage. Persist the passport
        # and continue walking the DAG so independent branches and ALL_DONE
        # cleanup stages can still execute before failing the overall workflow
        # at the end.
        pm.save_passport(passport)
      except Exception as e:
        pm.save_passport(passport)
        self._send_status_notification(
            runner, passport, f"Execution Failed: {e}", is_failed=True
        )
        print(f"\nExecution Failed: {e}")
        raise

    latest_status = {}
    for stamp in passport.stamps:
      latest_status[stamp.stage_name] = stamp.status

    failed_stages = []
    for s_name in ordered_stages:
      if latest_status.get(s_name) == schema.StampStatus.FAILED:
        failed_stages.append(s_name)

    if failed_stages:
      err_msg = f"Lightflow failed. Failed stages: {failed_stages}"
      self._send_status_notification(runner, passport, err_msg, is_failed=True)
      print(f"\n{err_msg}")
      latest_stamp_by_stage = {
          stamp.stage_name: stamp for stamp in passport.stamps
      }
      for s_name in failed_stages:
        stamp = latest_stamp_by_stage.get(s_name)
        msg_lines = (
            stamp.message.splitlines() if stamp and stamp.message else []
        )
        if msg_lines:
          print(f"  - {s_name}: {msg_lines[0]}")
      has_polling_timeout = any(
          "Yielding control back to operator/agent"
          in latest_stamp_by_stage[s_name].message
          for s_name in failed_stages
          if s_name in latest_stamp_by_stage
      )
      has_unrejected_failure = any(
          s_name not in latest_stamp_by_stage
          or not (
              (latest_stamp_by_stage[s_name].message or "").startswith(
                  _REJECTED_PREFIX
              )
              or latest_stamp_by_stage[s_name].message
              == engine.UPSTREAM_FAILED_MESSAGE
          )
          for s_name in failed_stages
      )
      target = runner.resume_target()
      runner_bin = target if isinstance(target, str) and target else "lightflow"
      cmd_parts = [runner_bin, "resume"]
      if isinstance(runner.lightflow_path, str) and runner.lightflow_path:
        cmd_parts.append(f"--lightflow={shlex.quote(runner.lightflow_path)}")
      cmd_parts.append(f"--log_id={shlex.quote(str(pm.workflow_id))}")
      resume_cmd = " ".join(cmd_parts)
      if has_polling_timeout:
        print(
            "\n💡 Notice: A polling gate timed out waiting for an external"
            " condition (e.g. code review or sync cycle).\nThis is an expected"
            " timeout yield to return control to the operator/agent.\nYour"
            " state is preserved in the passport. Once the external blocker is"
            " resolved, resume with:\n "
            f" {resume_cmd}\n"
        )
      elif has_unrejected_failure:
        print(
            "\nResume after fixing the root cause (preserves completed"
            f" stages):\n  {resume_cmd}"
        )
      raise engine.EngineError(err_msg)

    self._send_status_notification(
        runner, passport, "Lightflow completed successfully!"
    )
    print("\nLightflow completed successfully!")

  def cleanup(
      self,
      lightflow: str = "",
      log_id: str = "",
      *,
      workflow: Optional[str] = None,
  ) -> None:
    """Deletes the saved lightflow execution state for a given log ID."""
    del lightflow, workflow
    if not log_id:
      raise ValueError("Missing required '--log_id' run identifier.")
    pm = PassportManager(log_id)
    try:
      with pm.lock(exclusive=True):
        pm.delete_state(remove_lock=False)
      lock_path = pm.resolved_path + ".lock"
      if os.path.exists(lock_path) and not os.path.islink(lock_path):
        try:
          os.remove(lock_path)
        except OSError:
          pass
      print(f"Successfully cleaned up state for log ID: {log_id}")
    except RuntimeError as e:
      raise RuntimeError(
          f"Cannot clean up state for log ID '{log_id}' because the lightflow"
          " is currently running."
      ) from e
