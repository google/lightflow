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
import datetime
import difflib
import getpass
import html
import importlib
import json
import os
import re
import shlex
import shutil
import sys
import tempfile
import time
from typing import Any, Callable, NamedTuple, Optional

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
  """Raised when a run is paused at an operator gate (exit 2).

  `start` refuses to restart such a run; `status` and a `resume` that
  stopped at another gate use it to signal the paused state.
  """


class LightflowAlreadyCompletedError(Exception):
  """Raised by `start` and `resume` on a run that already completed."""


class LightflowRunFailedError(Exception):
  """Raised when a stage failed, a gate was rejected, or stages are unfinished.

  Attributes:
    already_reported: True when the raiser already printed a full footer, so the
      runner should only set the exit code and not print the message again.
  """

  def __init__(self, message: str, *, already_reported: bool = False):
    super().__init__(message)
    self.already_reported = already_reported


class LightflowStateNotFoundError(Exception):
  """Raised by `status` and `resume` when the log ID has no saved state."""


class LightflowRunningError(RuntimeError):
  """Raised when another process holds the run's lock (exit 4).

  Subclasses RuntimeError only so callers written against the earlier bare
  `RuntimeError` keep working.
  """


class LightflowWaitingError(Exception):
  """Raised when a polling stage yielded and the run awaits an external event.

  Nothing is broken: the run is parked (exit 5). Continue with `resume` once
  the external condition is met.

  Attributes:
    already_reported: True when the raiser already printed a full footer.
  """

  def __init__(self, message: str, *, already_reported: bool = False):
    super().__init__(message)
    self.already_reported = already_reported


# Process exit code for a failed run, compile error, or other error.
EXIT_FAILED = 1

# Process exit code for a run that stopped at an operator action.
EXIT_SUSPENDED = 2

# Process exit code for `status` on a log ID with no saved state.
EXIT_NO_STATE = 3

# Process exit code when another process holds the run's lock (`start`,
# `resume`, `cleanup`, or `status` while a run is executing).
EXIT_RUNNING = 4

# Process exit code when a polling stage yielded: the run is parked waiting for
# an external condition. Not a failure; `resume` once the condition is met.
EXIT_WAITING = 5

_STAMP_ICONS = {
    "PENDING": "🔄",
    "COMPLETED": "✅",
    "SKIPPED": "⏭️",
    "FAILED": "❌",
    "REJECTED": "🛑",
    "BLOCKED": "⛔",
    "PAUSED": "⏸️",
    "WAITING": "⏳",
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

  def __init__(self, workflow_id: str, state_root: Optional[str] = None):
    self.workflow_id = str(workflow_id).strip()
    self.state_root = state_root
    resolved_path = self._resolve_workflow_id(self.workflow_id)
    self._validate_path_safety(resolved_path)
    self.resolved_path = os.path.realpath(os.path.expanduser(resolved_path))

  @contextlib.contextmanager
  def lock(self, exclusive: bool = True):
    """Context manager to acquire an advisory file lock on the state directory."""
    parent_dir = os.path.dirname(self.resolved_path)
    os.makedirs(parent_dir, mode=0o700, exist_ok=True)

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
      raise LightflowRunningError(
          f"Could not acquire lock on workflow '{self.workflow_id}'."
          " Another process is running this workflow; wait for it to finish"
          " (check with 'status'), then retry."
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
    """Resolves a logical workflow ID or explicit path to a state directory."""
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
    return os.path.join(default_state_root(), f"lightflow_state_{cleaned_id}")

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
    if self.state_root:
      allowed_roots.append(
          os.path.realpath(os.path.expanduser(self.state_root))
      )

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

  @property
  def passport_file(self) -> str:
    """Returns the absolute path to the passport.json file for this run."""
    return os.path.join(self.resolved_path, "passport.json")

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
    passport_file = self.passport_file
    if not os.path.exists(passport_file):
      return None
    try:
      with open(passport_file, "r", encoding="utf-8") as f:
        return engine.passport_from_json(f.read())
    except Exception as e:
      raise StateCorruptedError(
          "Failed to load or parse workflow passport state from disk at"
          f" '{passport_file}': {e}"
      ) from e

  def save_passport(self, passport: schema.Passport) -> None:
    """Saves the passport state to disk atomically."""
    os.makedirs(self.resolved_path, mode=0o700, exist_ok=True)
    self._validate_path_safety(self.resolved_path)
    passport_file = self.passport_file
    json_str = engine.passport_to_json(passport)
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
      base_name = os.path.basename(self.resolved_path)
      if not base_name.startswith("lightflow_state_"):
        unexpected = [
            entry
            for entry in os.listdir(self.resolved_path)
            if entry != "passport.json"
            and not entry.startswith("passport.json.tmp-")
        ]
        if unexpected:
          raise ValueError(
              "Refusing to delete non-lightflow_state_ directory"
              f" '{self.resolved_path}' containing unexpected files:"
              f" {unexpected}"
          )
      shutil.rmtree(self.resolved_path)
    if remove_lock:
      lock_path = self.resolved_path + ".lock"
      if os.path.exists(lock_path) and not os.path.islink(lock_path):
        try:
          os.remove(lock_path)
        except OSError:
          pass


# Kept for callers that imported it from here.
resolve_lightflow_path = engine.resolve_lightflow_path


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


_TRIGGER_RULE_NAMES = {0: "ALL_SUCCESS", 1: "ALL_DONE", 2: "NONE_FAILED"}


def _trigger_rule_name(stage_proto: schema.Stage) -> str:
  """Returns the stage's trigger rule as a name (ALL_SUCCESS by default).

  Args:
    stage_proto: The stage definition.

  Returns:
    The rule name, e.g. "ALL_DONE".
  """
  rule = getattr(stage_proto, "trigger_rule", 0)
  if hasattr(rule, "name"):
    return str(rule.name)
  try:
    return _TRIGGER_RULE_NAMES.get(rule, "") or "ALL_SUCCESS"
  except TypeError:
    return "ALL_SUCCESS"


def _print_stage_policies(stage_proto: Any) -> None:
  """Prints retry/rollback/polling/trigger settings in the dry-run trace."""
  rp = getattr(stage_proto, "retry_policy", None)
  if stage_proto.HasField("retry_policy") and rp:
    attempts, backoff, mult = engine.effective_retry_policy(rp)
    print(f"  Retry: up to {attempts} attempts, backoff {backoff}s ×{mult}")
  rb = getattr(stage_proto, "rollback_action", None)
  if stage_proto.HasField("rollback_action") and rb and rb.action_id:
    print(f"  Rollback: '{rb.action_id}' runs if this stage fails")
  pp = getattr(stage_proto, "polling_policy", None)
  if stage_proto.HasField("polling_policy") and pp:
    interval = pp.interval_seconds if pp.HasField("interval_seconds") else 30
    bound = []
    if pp.HasField("timeout_seconds") and pp.timeout_seconds:
      bound.append(f"timeout {pp.timeout_seconds}s")
    if pp.HasField("max_attempts") and pp.max_attempts:
      bound.append(f"max {pp.max_attempts} attempts")
    print(
        f"  Poll: every {interval}s"
        + (f", {' / '.join(bound)}" if bound else "")
        + " (yields with exit 5 if the condition is not met in time)"
    )
  rule_name = _trigger_rule_name(stage_proto)
  if rule_name != "ALL_SUCCESS":
    print(f"  Trigger: {rule_name}")


def _replace_stage_outputs(
    payload_dict: dict[str, Any], stage_name: str, stage_output: dict[str, Any]
) -> None:
  """Sets `payload.outputs.<stage>` to exactly `stage_output` (no merge).

  Top-level keys that the stage's previous decision mirrored (and that nothing
  else has written since) are dropped too, so a re-opened gate's earlier
  `ticket`/`comment` cannot leak into the new decision.

  Args:
    payload_dict: The whole payload (mutated in place).
    stage_name: The gate.
    stage_output: Its new output.
  """
  outputs = payload_dict.get(engine.STAGE_OUTPUTS_KEY)
  if not isinstance(outputs, dict):
    outputs = {}
  previous = outputs.get(stage_name)
  if isinstance(previous, dict) and previous:
    provenance = engine.top_level_key_provenance(payload_dict)
    for key in previous:
      owner = provenance.get(key, ("", []))[0]
      if (
          owner == stage_name
          and key in payload_dict
          and key not in stage_output
      ):
        payload_dict.pop(key)
  outputs[stage_name] = dict(stage_output)
  payload_dict[engine.STAGE_OUTPUTS_KEY] = outputs


def _latest_stamp_is_poll_yield(
    passport: schema.Passport, stage_name: str
) -> bool:
  """True when `stage_name`'s latest stamp is a polling yield (timeout/attempts)."""
  for stamp in reversed(list(passport.stamps)):
    if stamp.stage_name == stage_name:
      return effective_status_name(stamp) == "WAITING"
  return False


def _rearm_yielded_polls(
    ordered_stages: list[str], passport: schema.Passport, operator: str
) -> list[str]:
  """Re-arms polling stages that yielded, so a `resume` polls them again.

  Answering a gate continues the run without the general re-arm pass; a poll
  that yielded in a parallel branch must still be retried rather than left
  looking failed.

  Args:
    ordered_stages: Topological stage order.
    passport: The run's Passport (mutated in place).
    operator: Who triggered the resume, for the stamp message.

  Returns:
    The re-armed stage names.
  """
  rearmed = [
      s for s in ordered_stages if _latest_stamp_is_poll_yield(passport, s)
  ]
  for stage_name in rearmed:
    stamp = passport.stamps.add()
    stamp.stage_name = stage_name
    stamp.status = schema.StampStatus.PENDING
    stamp.timestamp.CopyFrom(engine.get_timestamp())
    stamp.message = f"Re-armed by {operator}: polling again."
    print(f"↻ {stage_name} re-armed: polling again.")
  return rearmed


_ROLLBACK_FAILED_RE = re.compile(r"Rollback action '([^']+)' failed: (.+)")

# Pulls the last poll-tick message out of a polling yield stamp. Anchored on
# the fixed text that follows it because the tick message itself may contain
# ")." (e.g. "status=RUNNING (75%).").
_LAST_PROGRESS_RE = re.compile(
    r"\(last progress: (.*)\)\. Yielding control", re.S
)

_MISSING_KEY_RE = re.compile(
    r"no such key(?:/index)? '([^']+)'.*?\(in expression '(.*?)'\)"
)


def effective_status_name(stamp: schema.Stamp) -> str:
  """Returns the user-facing status of a stamp.

  The engine stamps three different situations as FAILED; tell them apart so
  `status`, `list`, the failure footer and the MCP server agree: REJECTED (an
  operator said no), BLOCKED (never ran because an upstream failed) and
  WAITING (a polling stage yielded). Everything else is the stamp's own
  status name.

  Args:
    stamp: Any Passport stamp.

  Returns:
    One of COMPLETED, SKIPPED, PENDING, PAUSED, FAILED, REJECTED, BLOCKED,
    WAITING.
  """
  name = schema.StampStatus.Name(stamp.status)
  if name != "FAILED":
    return name
  message = stamp.message or ""
  if message.startswith(_REJECTED_PREFIX):
    return "REJECTED"
  if message == engine.UPSTREAM_FAILED_MESSAGE:
    return "BLOCKED"
  if engine.POLL_YIELD_MARKER in message:
    return "WAITING"
  return "FAILED"


def _missing_top_level_keys_from_failures(
    messages_by_stage: dict[str, str], failed_stages: list[str]
) -> list[str]:
  """Extracts top-level payload keys named by strict-mode failures.

  Only keys referenced as `payload.<key>` / `payload['<key>']` in the failing
  expression qualify; `payload.outputs.<stage>.<key>` misses are code bugs,
  not missing seeds.

  Args:
    messages_by_stage: Stage name -> latest stamp message (str).
    failed_stages: Stage names that failed in this run.

  Returns:
    Distinct missing top-level keys, in failure order.
  """
  keys: list[str] = []
  for stage_name in failed_stages:
    message = messages_by_stage.get(stage_name) or ""
    m = _MISSING_KEY_RE.search(message)
    if not m:
      continue
    key, expr = m.group(1), m.group(2)
    if (
        f"payload.{key}" in expr or f"payload['{key}']" in expr
    ) and key not in keys:
      keys.append(key)
  return keys


def _continue_command(
    runner: Any, log_id: str, *, include_lightflow: bool
) -> str:
  """Renders the plain `resume` command that continues a failed run."""
  try:
    prefix = runner.resume_prefix()
  except Exception:  # pylint: disable=broad-exception-caught
    prefix = ""
  if not isinstance(prefix, str) or not prefix:
    prefix = "lightflow"
  args = [prefix, "resume"]
  manifest = getattr(runner, "lightflow_path", "")
  if include_lightflow and isinstance(manifest, str) and manifest:
    args.append(f"--lightflow={shlex.quote(manifest)}")
  args.append(f"--log_id={shlex.quote(log_id)}")
  return " ".join(args)


def _raise_for_run_state(
    state: str,
    paused: list[str],
    failed: list[str],
    rejected: list[str],
    unfinished: list[str],
    *,
    resume_hint: str = "",
    waiting: Optional[list[str]] = None,
) -> None:
  """Raises the exception the runner maps to `state`'s exit code.

  Args:
    state: Effective run state computed by `status`.
    paused: Stages paused at an operator gate.
    failed: Stages whose own work failed.
    rejected: Gates an operator rejected.
    unfinished: Stages not yet COMPLETED or SKIPPED.
    resume_hint: Concrete `resume` command for the message.
    waiting: Polling stages that yielded.

  Raises:
    LightflowRunningError: Another process holds the lock (exit 4).
    LightflowAlreadyPausedError: Paused at a gate (exit 2).
    LightflowRunFailedError: Failed, rejected, or unfinished (exit 1).
    LightflowWaitingError: Parked on a polling stage (exit 5).
  """
  waiting = waiting or []
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
    suffix = f" Rejected gates: {', '.join(rejected)}." if rejected else ""
    how = f" then run: {resume_hint}" if resume_hint else " and run 'resume'"
    raise LightflowRunFailedError(
        f"Lightflow failed. Root failure(s): {', '.join(failed)}.{suffix} Fix"
        " the root"
        f" cause{how}"
    )
  if state == "REJECTED":
    gate = rejected[0]
    cmd = resume_hint or "lightflow resume --log_id=<id>"
    raise LightflowRunFailedError(
        f"Lightflow stopped: operator gate(s) {', '.join(rejected)} were"
        " rejected."
        " Nothing to fix. To re-open the gate and decide again in one call:"
        f" {cmd} --stage={gate} --resolution=APPROVE|REJECT --payload='{{...}}'"
        f" (or just {cmd} --stage={gate} to re-open it and pause there)."
    )
  if state == "WAITING":
    cmd = resume_hint or "lightflow resume --log_id=<id>"
    raise LightflowWaitingError(
        f"Lightflow is waiting: polling stage(s) {', '.join(waiting)} yielded"
        " before their external condition was met. Nothing to fix; once it is"
        " met,"
        f" continue with: {cmd}"
    )
  if state == "INCOMPLETE":
    raise LightflowRunFailedError(
        f"Lightflow has unfinished stages: {', '.join(unfinished)}. Run"
        " 'resume' to continue."
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
        try:
          with open(file_path, "r", encoding="utf-8-sig") as f:
            raw_json = f.read()
        except OSError as e:
          raise ValueError(
              f"{context_label} / --payload_file: cannot read payload file"
              f" '{file_ref}': {e.strerror or e}"
          ) from e
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


def _is_deferrable_dry_run_eval_error(
    error: Exception,
    output_refs: list[str],
    has_unsimulated_ancestor: bool,
) -> bool:
  """Returns True when a dry-run expression failure should defer to runtime.

  Expressions that explicitly reference ``payload.outputs.<stage>`` are
  handled up-front by the caller (via ``extract_payload_output_stage_refs``).
  This covers the remaining case: an expression over *top-level* payload keys
  (e.g. ``payload.n_high > 0``) that an unsimulated upstream action would have
  populated at runtime. Because a missing key resolves to ``None``, the CEL
  shim surfaces it as an ``EngineError`` chained to a ``TypeError`` whose text
  mentions ``NoneType`` (e.g. ``unsupported operand type(s) ... 'NoneType'``);
  under ``strict_expressions`` it is a ``MissingFieldError`` instead.
  Any other failure (syntax errors, genuine type mismatches, keys missing from
  a seed ``--payload`` when no upstream stage could have produced them) is
  still reported as a real evaluation failure.

  Args:
    error: The exception raised by ``engine.evaluate_cel``.
    output_refs: Explicit ``payload.outputs.<stage>`` references found in the
      expression; non-empty means the caller already handled deferral.
    has_unsimulated_ancestor: Whether any transitive upstream stage could not be
      simulated in this dry run.
  """
  if output_refs or not has_unsimulated_ancestor:
    return False
  cause = error.__cause__
  # Strict mode names the missing key directly; lenient mode only surfaces it
  # once the resulting null hits an operator.
  if isinstance(cause, engine.MissingFieldError):
    return True
  return isinstance(cause, TypeError) and "NoneType" in str(error)


def _print_not_simulated_note(
    stage_name: str,
    unsimulated_stages: set[str],
    runner: Any,
) -> None:
  """Explains in a dry-run trace why a stage produced no simulated output."""
  upstream = sorted(
      u
      for u in unsimulated_stages
      if u != stage_name and stage_name in runner.get_downstream_dependents(u)
  )
  if upstream:
    note = f"awaits outputs of: {', '.join(upstream)}"
  else:
    note = "action does not accept dry_run, or its simulation raised"
  print(f"    Dry-Run Output: [NOT SIMULATED ({note})]")


def _resolve_payload_input(payload: Any, payload_file: Any = None) -> Any:
  """Normalizes `--payload` and `--payload_file` into one payload argument."""
  if payload_file is None:
    return payload
  if not isinstance(payload_file, str) or not payload_file.strip():
    raise ValueError("--payload_file requires a file path.")
  cleaned = payload_file.strip()
  if isinstance(payload, dict):
    has_explicit_payload = bool(payload)
  elif isinstance(payload, str):
    has_explicit_payload = bool(payload.strip() not in ("{}", ""))
  else:
    has_explicit_payload = payload is not None
  if has_explicit_payload:
    raise ValueError("Cannot specify both --payload and --payload_file.")
  return cleaned if cleaned.startswith("@") else f"@{cleaned}"


class RunSummary(NamedTuple):
  """One saved run as reported by `collect_runs` / `lightflow list`."""

  log_id: str
  lightflow_name: str
  state: str
  last_activity: float
  lightflow_path: str
  error: str


def default_state_root() -> str:
  """Returns the directory that logical log IDs resolve under."""
  env_state_dir = os.environ.get("LIGHTFLOW_STATE_DIR")
  if env_state_dir:
    return os.path.expanduser(env_state_dir)
  return os.path.expanduser("~/.lightflow")


def collect_runs(
    state_dir: str = "",
) -> tuple[list[RunSummary], list[str], str]:
  """Scans the state directory for saved runs and orphan lock files.

  Args:
    state_dir: Directory to scan; defaults to `default_state_root()`.

  Returns:
    `(runs, orphan_lock_log_ids, root)` with `runs` sorted newest first.
  """
  root = os.path.abspath(
      os.path.expanduser(state_dir) if state_dir else default_state_root()
  )
  prefix = "lightflow_state_"
  runs: list[RunSummary] = []
  orphan_locks: list[str] = []
  if not os.path.isdir(root):
    return runs, orphan_locks, root
  for entry in sorted(os.listdir(root)):
    if not entry.startswith(prefix):
      continue
    full = os.path.join(root, entry)
    if entry.endswith(".lock"):
      if not os.path.isdir(full[: -len(".lock")]):
        orphan_locks.append(entry[len(prefix) : -len(".lock")])
      continue
    if not os.path.isdir(full):
      continue
    log_id = entry[len(prefix) :]
    try:
      pm = PassportManager(full, state_root=root)
      passport = pm.load_passport()
    except Exception as e:  # pylint: disable=broad-exception-caught
      runs.append(RunSummary(log_id, "", "UNREADABLE", 0.0, "", str(e)[:120]))
      continue
    if passport is None:
      runs.append(RunSummary(log_id, "", "EMPTY", 0.0, "", ""))
      continue
    state, last_ts = _summarise_run(passport, pm.is_locked())
    runs.append(
        RunSummary(
            log_id,
            passport.lightflow_name or "",
            state,
            last_ts,
            passport.lightflow_path or "",
            "",
        )
    )
  runs.sort(key=lambda r: r.last_activity, reverse=True)
  return runs, orphan_locks, root


_STATE_ICONS = {
    "RUNNING": "🔄",
    "PAUSED": "⏸️",
    "FAILED": "❌",
    "REJECTED": "🛑",
    "WAITING": "⏳",
    "COMPLETED": "✅",
    "STARTED": "⬜",
}


def _require_manifest(given: str, resolver: Callable[[str], str]) -> str:
  """Resolves a user-supplied manifest path, failing clearly if it is absent.

  Args:
    given: The `--lightflow` value as typed.
    resolver: Turns it into a file path (expands `~`, default filenames).

  Returns:
    The resolved path, which exists.

  Raises:
    ValueError: The file does not exist (instead of a bare `[Errno 2]`).
  """
  resolved = resolver(given)
  if not os.path.exists(resolved):
    raise ValueError(
        f"No such manifest: {given!r} (resolved to {resolved}). Check the"
        " path, or run 'list' to see recorded manifests."
    )
  return resolved


def _manifest_for_run(
    explicit: Optional[str],
    passport: schema.Passport,
    log_id: str,
    resolver: Callable[[str], str],
) -> str:
  """Resolves the manifest for an existing run: `--lightflow`, else Passport.

  Args:
    explicit: The `--lightflow` (or deprecated `--workflow`) flag value.
    passport: The run's loaded Passport (may predate manifest tracking).
    log_id: Run identifier, for error messages.
    resolver: Callable turning a user path into a manifest file path.

  Returns:
    The manifest path to load.

  Raises:
    ValueError: No path was given and none is recorded, or the recorded path
      no longer exists.
  """
  recorded = getattr(passport, "lightflow_path", "") or ""
  if explicit:
    resolved = resolver(explicit)
    if not os.path.exists(resolved):
      hint = ""
      if recorded and os.path.exists(recorded):
        hint = (
            f" The run's recorded manifest is at {recorded}; drop --lightflow"
            " to use it."
        )
      raise ValueError(
          f"No such manifest: {explicit!r} (resolved to {resolved}).{hint}"
      )
    return resolved
  if not recorded:
    raise ValueError(
        "Missing required '--lightflow' manifest path (this run predates"
        " manifest tracking, so it cannot be inferred from the Passport)."
    )
  if not os.path.exists(recorded):
    raise ValueError(
        f"The manifest recorded for log ID '{log_id}' no longer exists at"
        f" {recorded}; pass --lightflow=<new path> explicitly."
    )
  return resolver(recorded)


def _check_same_lightflow(
    passport: schema.Passport, workflow_name: str, log_id: str
) -> None:
  """Refuses to drive a run's Passport with a differently named lightflow.

  Args:
    passport: The run's Passport (its `lightflow_name` is set on `start`).
    workflow_name: `name:` of the manifest about to be used.
    log_id: Run identifier, for the message.

  Raises:
    ValueError: The names differ.
  """
  recorded = getattr(passport, "lightflow_name", "") or ""
  if recorded and recorded != workflow_name:
    raise ValueError(
        f"Log ID '{log_id}' already holds state for lightflow '{recorded}',"
        f" but this manifest is '{workflow_name}'. Use a different --log_id,"
        f" or run 'cleanup --log_id={log_id}' first."
    )


def _record_manifest_path(
    pm: "PassportManager",
    passport: schema.Passport,
    workflow_path: str,
    workflow_name: str,
    *,
    explicit: bool,
) -> None:
  """Re-records the manifest path when the caller passed a different one.

  A run remembers where its manifest was on `start`. When the file moves and
  the operator passes `--lightflow=<new path>` once, keep that: every later
  `status`/`resume` can then omit the flag again, and printed hints stay
  correct. Only an explicit flag can re-record, and only for a manifest of
  the same lightflow name.

  Args:
    pm: The run's PassportManager (used to save).
    passport: Its loaded Passport (mutated and saved when re-recording).
    workflow_path: The manifest path the caller passed.
    workflow_name: That manifest's `name:`.
    explicit: Whether the caller passed `--lightflow` at all.
  """
  if not explicit:
    return
  recorded = getattr(passport, "lightflow_path", "") or ""
  new_path = os.path.abspath(workflow_path)
  if new_path == recorded:
    return
  recorded_name = getattr(passport, "lightflow_name", "") or ""
  if recorded_name and recorded_name != workflow_name:
    return  # `start` reports this mismatch; do not silently adopt it here.
  passport.lightflow_path = new_path
  if not recorded_name:
    passport.lightflow_name = workflow_name
  pm.save_passport(passport)
  shown_old = recorded or "<none recorded>"
  print(
      f"ℹ️ Manifest path updated for '{pm.workflow_id}': {shown_old} →"
      f" {new_path} (later commands can omit --lightflow)."
  )


def _describe_discarded_state(log_id: str, passport: schema.Passport) -> str:
  """One loud line saying what `start --force` is about to throw away.

  Args:
    log_id: Run identifier.
    passport: The Passport that is about to be deleted.

  Returns:
    The warning line to print.
  """
  latest: dict[str, schema.Stamp] = {}
  for stamp in passport.stamps:
    latest[stamp.stage_name] = stamp
  completed = [
      n for n, st in latest.items() if st.status == schema.StampStatus.COMPLETED
  ]
  paused = [
      n for n, st in latest.items() if st.status == schema.StampStatus.PAUSED
  ]
  failed = [
      n for n, st in latest.items() if st.status == schema.StampStatus.FAILED
  ]
  bits = [f"{len(completed)} completed stage(s)"]
  if completed:
    shown = ", ".join(completed[:4]) + (", …" if len(completed) > 4 else "")
    bits[-1] += f" ({shown})"
  if paused:
    bits.append(f"paused gate '{paused[0]}'")
  if failed:
    bits.append(f"{len(failed)} failed/blocked")
  bits.append(f"{len(list(passport.stamps))} stamp(s) of history")
  return (
      f"⚠️ --force discarded the previous state of '{log_id}': "
      + ", ".join(bits)
      + ". (`resume` would have kept the completed stages.)"
  )


def _ran_because_of(
    runner: engine.LightflowEngine,
    dep: str,
    latest_status: dict[str, int],
) -> bool:
  """True when `dep` is an ALL_DONE stage that completed on a failure path.

  An ALL_DONE stage downstream of a failed or rejected stage runs on the
  failure path (that is its job). When the cause is re-armed, the "end" of the
  run moves, so such a stage has to run again or the success path never gets
  its cleanup. Because `dep` depends on the cause, it can only have completed
  after the cause ended, so no timestamp comparison is needed.

  Args:
    runner: Compiled engine for the manifest.
    dep: A downstream dependent of the stage being re-armed.
    latest_status: Latest stamp status per stage.

  Returns:
    Whether `dep` must be re-armed along with its cause.
  """
  if latest_status.get(dep) != schema.StampStatus.COMPLETED:
    return False
  stage_proto = runner.get_stage(dep)
  return (
      stage_proto is not None and _trigger_rule_name(stage_proto) == "ALL_DONE"
  )


def _stamp_epoch(stamp: Any) -> float:
  """Returns a stamp's timestamp as a POSIX epoch, or 0.0 if unset/unparsable.

  Goes through the JSON (RFC 3339) form, which both the protobuf Timestamp and
  the OSS TimestampValue shim expose, rather than `ToDatetime`, which differs
  between them.

  Args:
    stamp: Any Passport stamp.
  """
  try:
    iso = stamp.timestamp.ToJsonString()
  except Exception:  # pylint: disable=broad-exception-caught
    return 0.0
  if not iso:
    return 0.0
  try:
    parsed = datetime.datetime.fromisoformat(iso.replace("Z", "+00:00"))
  except ValueError:
    return 0.0
  if parsed.tzinfo is None:
    parsed = parsed.replace(tzinfo=datetime.timezone.utc)
  return parsed.timestamp()


def _summarise_run(
    passport: schema.Passport, running: bool
) -> tuple[str, float]:
  """Derives a coarse `(state, last_activity_epoch)` from a Passport alone.

  `status` needs the manifest to tell COMPLETED from INCOMPLETE; `list` has
  only the Passport, so it reports what the stamps alone support, using the
  same FAILED/REJECTED/WAITING distinction as `status`.

  Args:
    passport: The run's Passport.
    running: Whether another process currently holds the run's lock.

  Returns:
    The coarse state name and the epoch of the most recent stamp.
  """
  latest: dict[str, schema.Stamp] = {}
  last_ts = 0.0
  for stamp in passport.stamps:
    latest[stamp.stage_name] = stamp
    last_ts = max(last_ts, _stamp_epoch(stamp))
  states = {effective_status_name(st) for st in latest.values()}
  if running:
    state = "RUNNING"
  elif "PAUSED" in states:
    state = "PAUSED"
  elif "FAILED" in states:
    state = "FAILED"
  elif "REJECTED" in states:
    state = "REJECTED"
  elif "WAITING" in states:
    state = "WAITING"
  elif "PENDING" in states or not states:
    state = "STARTED"
  else:
    state = "COMPLETED"
  return state, last_ts


def _manifest_cell(path: str) -> str:
  """Renders the `list` MANIFEST column, flagging a recorded path that is gone.

  Args:
    path: The manifest path recorded in the Passport (may be empty).

  Returns:
    The path, suffixed with a `(missing …)` note when the file is gone.
  """
  if not path or os.path.exists(path):
    return path
  return f"{path} (missing — pass --lightflow=<new path> once to re-record)"


def _format_duration(seconds: float) -> str:
  seconds = max(0.0, seconds)
  if seconds < 60:
    return f"{seconds:.1f}s"
  if seconds < 3600:
    return f"{int(seconds // 60)}m{int(seconds % 60):02d}s"
  return f"{int(seconds // 3600)}h{int((seconds % 3600) // 60):02d}m"


def _format_age(epoch: float) -> str:
  if not epoch:
    return "-"
  delta = max(0.0, time.time() - epoch)
  if delta < 90:
    return f"{int(delta)}s ago"
  if delta < 5400:
    return f"{int(delta // 60)}m ago"
  if delta < 172800:
    return f"{int(delta // 3600)}h ago"
  return f"{int(delta // 86400)}d ago"


class LightflowRunnerCLI:
  """CLI interface for executing, inspecting, and resuming Lightflow DAGs.

  Core Lifecycle & Exit Codes:
    - start --lightflow=<path> --log_id=<id> [--payload='{...}' |
      --payload=@file.json | --payload_file=<file.json>] [--dry_run]:
      Starts a new run (or previews the DAG trace with --dry_run).
      * Exit 0: Completed all stages.
      * Exit 2 (SUSPENDED): Paused at an `operator_action` human-in-the-loop
        checkpoint. STOP and present the printed Instructions and Payload Schema
        to the human operator/user. Do NOT self-approve or run `resume` until
        the operator provides their explicit decision.
      * Exit 5 (WAITING): A polling stage yielded before its external
        condition was met. Nothing to fix; run `resume` once it is.
      * Exit 1 (FAILED): A stage failed. Fix the root cause and run `resume`
        (never `start --force`, which wipes the Passport and repeats completed
        upstream side effects; it prints what it discarded).
    - resume [--lightflow=<path>] --log_id=<id> [--stage=<gate>
      --resolution=APPROVE|REJECT --payload='{...}' | --payload=@file.json |
      --payload_file=<file.json>] [--cascade=false]:
      Resolves a paused `operator_action` gate or re-arms failed stages and
      their downstream dependents without re-running completed upstream stages.
      On a rejected gate, an explicit --resolution re-opens it and applies the
      decision in one call. --lightflow is only needed if the manifest moved
      (the new path is then recorded for later commands).
    - list [--state_dir=<dir>]:
      Lists every saved run (log ID, lightflow, state, last activity,
      manifest) plus orphan lock files. Exit 0.
    - status [--lightflow=<path>] --log_id=<id> [--verbose]:
      Prints each stage's latest status and any paused gate's Instructions,
      Payload Schema, and resume command. Exits like the run: 0 completed,
      1 failed, rejected, or interrupted, 2 paused, 3 no saved state,
      4 running in another process, 5 waiting on a polling stage. `--verbose`
      adds the Passport payload, full stamp log, and Mermaid progress diagram.
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
      payload_file: Optional[str] = None,
      workflow: Optional[str] = None,
  ) -> None:
    """Starts a new run, or continues an existing one for the same log ID.

    Prints the Passport path, one progress line per stage, and on a pause the
    gate's Instructions, Payload Schema, and exact `resume` command. Exit 0
    when every stage finished, 2 when paused at an `operator_action`, 1 when
    a stage failed (the footer prints the `resume` command to continue with).

    Args:
      lightflow: Path to the manifest (YAML/JSON/textproto).
      log_id: Run identifier; state lives under `$LIGHTFLOW_STATE_DIR`.
      force: Discard any existing state for `log_id` first (re-runs completed
        stages and repeats their side effects).
      payload: Initial payload as inline JSON, `@file.json`, or `@-` (stdin).
      dry_run: Equivalent to the `dry_run` subcommand.
      payload_file: Path to a JSON file with the initial payload (alternative to
        `--payload=@file.json`).
      workflow: Deprecated alias for `lightflow`.

    Raises:
      LightflowAlreadyPausedError: The run is paused at a gate; use `resume`.
      LightflowAlreadyCompletedError: The run already completed; use `--force`
        to rerun from scratch.
      LightflowRunFailedError: A stage failed or a gate was rejected (exit 1).
      LightflowWaitingError: A polling stage yielded (exit 5).
      LightflowRunningError: Another process holds the run's lock (exit 4).
      ValueError: Bad arguments, a payload that uses a reserved key, or a
        manifest whose name differs from the run's recorded lightflow.
    """
    effective_payload = _resolve_payload_input(payload, payload_file)
    if dry_run:
      self.dry_run(
          lightflow=lightflow,
          payload=effective_payload,
          log_id=log_id,
          workflow=workflow,
      )
      return
    resolved_lightflow = lightflow or workflow
    if not resolved_lightflow:
      raise ValueError("Missing required '--lightflow' manifest path.")
    workflow_path = _require_manifest(
        resolved_lightflow, engine.resolve_lightflow_path
    )
    # Parse and compile the manifest before taking the lock so a bad path or
    # a manifest error cannot leave an orphan lock file behind.
    workflow_proto = engine.load_lightflow(workflow_path)
    pm = PassportManager(log_id)
    runner = engine.LightflowEngine(
        workflow_proto, lightflow_path=workflow_path, log_id=pm.workflow_id
    )
    try:
      ordered_stages = runner.compile()
    except Exception as e:
      raise engine.EngineError(f"Compilation Error: {e}") from e

    # Validate the payload before taking the lock, so a rejected payload
    # cannot leave an orphan lock file either.
    initial_payload = _parse_payload_arg(
        effective_payload, context_label="--payload", allow_outputs=False
    )

    with pm.lock(exclusive=True):
      if force:
        previous = pm.load_passport()
        if previous is not None:
          print(_describe_discarded_state(pm.workflow_id, previous))
        pm.delete_state(remove_lock=False)

      passport = pm.load_passport()
      if passport:
        _check_same_lightflow(passport, workflow_proto.name, pm.workflow_id)
        if not passport.lightflow_name or not passport.lightflow_path:
          # Backfill for runs started before the manifest was recorded.
          passport.lightflow_name = workflow_proto.name
          passport.lightflow_path = os.path.abspath(workflow_path)
          pm.save_passport(passport)
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
          payload_dict = engine.payload_to_dict(passport.payload)
          payload_dict.update(initial_payload)
          engine.set_payload(passport.payload, payload_dict)
          payload_updated = True

        rearmed = self._rearm_failed_stages(
            runner, ordered_stages, passport, operator=_current_operator()
        )
        if rearmed or payload_updated:
          pm.save_passport(passport)

      if not passport:
        passport = schema.Passport()
        passport.lightflow_name = workflow_proto.name
        passport.lightflow_path = os.path.abspath(workflow_path)
        if "operator" not in initial_payload:
          initial_payload["operator"] = _current_operator()

        engine.set_payload(passport.payload, initial_payload)
        pm.save_passport(passport)
        print(f"Starting execution of lightflow '{workflow_proto.name}'...")
      else:
        print(f"Resuming execution of lightflow '{workflow_proto.name}'...")
      print(f"Passport: {pm.passport_file}")
      self._execute_engine_loop(runner, ordered_stages, passport, pm)

  def resume(
      self,
      lightflow: str = "",
      log_id: str = "",
      stage: Optional[str] = None,
      payload: str | dict[str, Any] = "{}",
      resolution: Optional[str] = None,
      comment: Optional[str] = None,
      operator: Optional[str] = None,
      cascade: bool = True,
      *,
      payload_file: Optional[str] = None,
      workflow: Optional[str] = None,
  ) -> None:
    """Resolves a paused gate, or continues a failed/interrupted run.

    With `--stage` naming a paused `operator_action`: records the operator's
    decision (`--resolution=APPROVE` sets `approved=true`; REJECT sets it
    false and stops the run) merged with `--payload`, then continues. Without
    `--stage`: re-arms failed stages (and their dependents) and continues
    without re-running completed stages; `--payload` keys are merged into
    the Passport first, which is how to supply a top-level key a stage failed
    on. `--lightflow` is only needed for runs that predate manifest tracking
    or whose manifest has moved.

    Args:
      lightflow: Path to the manifest; inferred from the Passport if omitted.
      log_id: Run identifier.
      stage: Paused `operator_action` stage to resolve (omit to continue a
        failed run).
      payload: JSON object, `@file.json`, or `@-`; the gate's schema is
        validated against it (plus the injected `approved`).
      resolution: `APPROVE` (default when omitted) or `REJECT`. Passing it
        explicitly on a *rejected* gate re-opens the gate and applies the
        decision in one call; omitting it only re-opens (pauses) the gate.
      comment: Optional operator comment recorded on the stamp.
      operator: Optional operator identity; defaults to the current user.
      cascade: When re-arming a named failed stage, also re-arm its dependents
        (default true).
      payload_file: Path to a JSON file with the payload.
      workflow: Deprecated alias for `lightflow`.

    Raises:
      LightflowStateNotFoundError: No state exists for `log_id` (exit 3).
      LightflowAlreadyCompletedError: Nothing left to run.
      LightflowRunFailedError: A stage failed or a gate was rejected (exit 1).
      LightflowWaitingError: A polling stage yielded (exit 5).
      LightflowRunningError: Another process holds the run's lock (exit 4).
      ValueError: Bad arguments, a payload that fails the gate schema, an
        unknown `--stage`, or a manifest of a different lightflow.
    """
    effective_payload = _resolve_payload_input(payload, payload_file)
    if not log_id:
      raise ValueError("Missing required '--log_id' run identifier.")
    pm = PassportManager(log_id)

    if not operator:
      operator = _current_operator()

    resolution_given = resolution is not None and bool(str(resolution).strip())
    norm_resolution = (
        str(resolution).upper().strip() if resolution_given else "APPROVE"
    )
    if norm_resolution not in ("APPROVE", "REJECT", "1", "2"):
      raise ValueError(
          f"Invalid resolution '{resolution}'. Must be 'APPROVE' or 'REJECT'."
      )
    is_reject = norm_resolution in ("REJECT", "2")

    with pm.lock(exclusive=True):
      passport = pm.load_passport()
      if not passport:
        raise LightflowStateNotFoundError(
            f"No execution state found for log ID '{log_id}'. Nothing to"
            " resume; use 'start' to begin a new run, or 'list' to"
            " find the right log ID."
        )
      workflow_path = _manifest_for_run(
          lightflow or workflow, passport, log_id, engine.resolve_lightflow_path
      )
      workflow_proto = engine.load_lightflow(workflow_path)
      _check_same_lightflow(passport, workflow_proto.name, pm.workflow_id)
      _record_manifest_path(
          pm,
          passport,
          workflow_path,
          workflow_proto.name,
          explicit=bool(lightflow or workflow),
      )

      latest_status = {}
      for stamp in passport.stamps:
        latest_status[stamp.stage_name] = stamp.status

      if (
          stage is not None
          and latest_status.get(stage) == schema.StampStatus.FAILED
      ):
        latest_stamp_for = {}
        for stamp in passport.stamps:
          latest_stamp_for[stamp.stage_name] = stamp
        rejected_stamp = latest_stamp_for.get(stage)
        if rejected_stamp is not None and (
            rejected_stamp.message or ""
        ).startswith(_REJECTED_PREFIX):
          decided = (
              bool(effective_payload and effective_payload not in ("{}", ""))
              or resolution_given
              or bool(comment)
          )
          if decided:
            # Re-open in place and fall through to the normal resolution path
            # below, so the caller's --resolution/--payload are applied now.
            reopened = passport.stamps.add()
            reopened.stage_name = stage
            reopened.status = schema.StampStatus.PAUSED
            reopened.timestamp.CopyFrom(engine.get_timestamp())
            reopened.message = f"Re-opened after rejection by {operator}."
            for prior in reversed(list(passport.stamps)):
              if (
                  prior.stage_name == stage
                  and prior.status == schema.StampStatus.PAUSED
                  and prior is not reopened
              ):
                reopened.instructions = prior.instructions
                reopened.resume_command = prior.resume_command
                break
            latest_status[stage] = schema.StampStatus.PAUSED
            # Downstream stages were stamped as blocked collateral by the
            # rejection; re-arm them so the decision can flow through.
            reopen_runner = engine.LightflowEngine(
                workflow_proto,
                lightflow_path=workflow_path,
                log_id=pm.workflow_id,
            )
            try:
              reopen_runner.compile()
            except Exception as e:
              raise engine.EngineError(f"Compilation Error: {e}") from e
            for dep in reopen_runner.get_downstream_dependents(stage):
              consequential = _ran_because_of(reopen_runner, dep, latest_status)
              if consequential:
                print(
                    f"↻ {dep} re-armed: its ALL_DONE rule ran it after the"
                    " rejection; it will run again at the end."
                )
              if consequential or latest_status.get(dep) in (
                  schema.StampStatus.FAILED,
                  schema.StampStatus.SKIPPED,
              ):
                rearmed = passport.stamps.add()
                rearmed.stage_name = dep
                rearmed.status = schema.StampStatus.PENDING
                rearmed.timestamp.CopyFrom(engine.get_timestamp())
                rearmed.message = (
                    f"Re-armed by {operator} after gate '{stage}' was"
                    " re-opened."
                )
                latest_status[dep] = schema.StampStatus.PENDING
            print(
                f"Re-opening rejected gate '{stage}' to apply your decision..."
            )
          else:
            print(
                f"Re-opening rejected gate '{stage}' (no --payload/--resolution"
                " given, so it pauses there; pass them to decide in one call)."
            )

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
            payload=effective_payload,
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

      payload_dict = engine.payload_to_dict(passport.payload)
      payload_dict["operator"] = operator

      if is_reject:
        if comment:
          payload_dict["rejection_comment"] = comment
        payload_dict["resolution"] = "REJECT"
        input_dict: dict[str, Any] = {}

        if effective_payload and effective_payload not in ("{}", ""):
          input_dict = _parse_payload_arg(
              effective_payload,
              context_label=f"Operator action stage '{stage}' resume payload",
              allow_outputs=False,
          )
          payload_dict.update(input_dict)

        payload_dict["operator"] = operator
        payload_dict["approved"] = False
        payload_dict["resolution"] = "REJECT"
        stage_output: dict[str, Any] = {
            **input_dict,
            "approved": False,
            "resolution": "REJECT",
        }
        if comment:
          stage_output["comment"] = comment
        payload_dict.pop("approval_note", None)
        _replace_stage_outputs(payload_dict, stage, stage_output)
        engine.set_payload(passport.payload, payload_dict)

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
            f"Stage Rejected: {stage}", f"🛑 {stage}: rejected"
        )

        print(f"Resuming execution after rejection of stage '{stage}'...")
        print(f"Passport: {pm.passport_file}")
        self._execute_engine_loop(runner, ordered_stages, passport, pm)
        return

      input_dict = _parse_payload_arg(
          effective_payload,
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
          engine.validate_json_schema(validation_dict, schema_dict)
        except ValueError as e:
          raise ValueError(
              "Operator action resume payload failed JSON schema"
              f" validation: {e}"
          ) from e

      payload_dict.update(input_dict)
      if comment:
        payload_dict["approval_note"] = comment
      payload_dict["operator"] = operator
      payload_dict["approved"] = True
      payload_dict["resolution"] = "APPROVE"
      stage_output = {**input_dict, "approved": True, "resolution": "APPROVE"}
      if comment:
        stage_output["comment"] = comment
      payload_dict.pop("rejection_comment", None)
      # A fresh decision replaces (not merges with) any earlier rejection's
      # output, so stale `comment`/fields do not leak into the approval.
      _replace_stage_outputs(payload_dict, stage, stage_output)

      engine.set_payload(passport.payload, payload_dict)

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
      print(f"Passport: {pm.passport_file}")
      _rearm_yielded_polls(ordered_stages, passport, operator)
      self._execute_engine_loop(runner, ordered_stages, passport, pm)

  def _resume_execution(
      self,
      workflow_proto: schema.Lightflow,
      workflow_path: str,
      log_id: str,
      pm: PassportManager,
      passport: schema.Passport,
      latest_status: dict[str, int],
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
      close = difflib.get_close_matches(stage, ordered_stages, n=1, cutoff=0.6)
      hint = f" Did you mean '{close[0]}'?" if close else ""
      raise ValueError(
          f"Unknown stage '{stage}'.{hint} Stages in this lightflow:"
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
      payload_dict = engine.payload_to_dict(passport.payload)
      if extra_payload:
        payload_dict.update(extra_payload)
      payload_dict["operator"] = operator
      engine.set_payload(passport.payload, payload_dict)
      pm.save_passport(passport)

    print(f"Resuming execution of lightflow '{workflow_proto.name}'...")
    print(f"Passport: {pm.passport_file}")
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
    """Marks failed stages eligible to run again, preserving completed work.

    Prints one summary line naming the root stage(s) re-armed and, separately,
    the downstream stages (skipped or blocked because of them) that will run
    again as a consequence.

    Args:
      runner: Compiled engine, used to walk `run_after` edges.
      ordered_stages: Topologically ordered stage names.
      passport: Passport to stamp. Mutated in place; the caller saves it.
      stage: Specific failed stage to re-arm. Defaults to every root failure.
      cascade: Also re-arm stages downstream of the re-armed ones.
      operator: LDAP or agent ID recorded on the re-arm stamp.

    Returns:
      Every re-armed stage, in topological order.
    """
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
            f" Failed stages: {', '.join(failed_stages) or 'none'}."
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
    # Decide the wording before PENDING stamps hide the previous state.
    repolled = {s for s in targets if _latest_stamp_is_poll_yield(passport, s)}
    reran: list[str] = []
    if cascade:
      for target in targets:
        for dep in runner.get_downstream_dependents(target):
          if latest_status.get(dep) in (
              schema.StampStatus.FAILED,
              schema.StampStatus.SKIPPED,
          ):
            rearm.add(dep)
          elif _ran_because_of(runner, dep, latest_status):
            rearm.add(dep)
            reran.append(dep)
    for dep in sorted(set(reran)):
      print(
          f"↻ {dep} re-armed: its ALL_DONE rule ran it on the failure path;"
          " it will run again at the end."
      )

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

    rearmed = [s for s in ordered_stages if s in rearm]
    if rearmed:
      failed = [s for s in rearmed if s in targets and s not in repolled]
      polls = [s for s in rearmed if s in repolled]
      downstream = [s for s in rearmed if s not in targets]
      parts = []
      if failed:
        parts.append(f"Re-armed failed stage(s): {', '.join(failed)}")
      if polls:
        parts.append(f"Polling again: {', '.join(polls)}")
      line = "; ".join(parts)
      if downstream:
        line += f" (+{len(downstream)} downstream: {', '.join(downstream)})"
      print(line)
    return rearmed

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
      lightflow: Path to the Lightflow manifest. Optional when the run was
        started by a version that records the manifest path in the Passport.
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
      LightflowWaitingError: A polling stage yielded before its condition was
        met (exit 5).
      ValueError: `--lightflow` names a different lightflow than the run's.
    """
    if not log_id:
      raise ValueError("Missing required '--log_id' run identifier.")
    pm = PassportManager(log_id)
    passport = pm.load_passport()
    if not passport:
      state_root = os.path.dirname(pm.resolved_path)
      where = (
          f"under {state_root}"
          if os.path.isdir(state_root)
          else f"under {state_root} (that directory does not exist)"
      )
      raise LightflowStateNotFoundError(
          f"No execution state found for log ID '{log_id}' {where}."
          " Check LIGHTFLOW_STATE_DIR, or run 'list'."
      )
    lightflow_path = _manifest_for_run(
        lightflow or workflow, passport, log_id, engine.resolve_lightflow_path
    )
    lightflow_proto = engine.load_lightflow(lightflow_path)
    running = pm.is_locked()
    _check_same_lightflow(passport, lightflow_proto.name, pm.workflow_id)
    explicit_path = bool(lightflow or workflow)
    recorded_path = passport.lightflow_path or ""
    if (
        not running
        and explicit_path
        and recorded_path
        and os.path.abspath(lightflow_path) != recorded_path
    ):
      # A read verb that writes: re-recording a moved manifest is the one
      # mutation `status` performs, and it does so under the run lock on a
      # fresh copy so a concurrent `resume` cannot be overwritten.
      try:
        with pm.lock(exclusive=True):
          fresh = pm.load_passport()
          if fresh is not None:
            _record_manifest_path(
                pm, fresh, lightflow_path, lightflow_proto.name, explicit=True
            )
            passport = fresh
      except LightflowRunningError:
        running = True

    runner = engine.LightflowEngine(
        lightflow_proto, lightflow_path=lightflow_path, log_id=pm.workflow_id
    )
    try:
      ordered_stages = runner.compile()
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
      effective[stage_name] = effective_status_name(stamp)

    paused = [s for s in ordered_stages if effective[s] == "PAUSED"]
    failed = [s for s in ordered_stages if effective[s] == "FAILED"]
    rejected = [s for s in ordered_stages if effective[s] == "REJECTED"]
    waiting = [s for s in ordered_stages if effective[s] == "WAITING"]
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
    elif waiting:
      state = "WAITING"
    elif unfinished:
      state = "INCOMPLETE"
    else:
      state = "COMPLETED"

    shown_id = pm.workflow_id
    if not os.path.isabs(os.path.expanduser(shown_id)):
      shown_id = shown_id.lower()
    print(f"Lightflow '{lightflow_proto.name}' (log ID: {shown_id}): {state}")
    print(f"Passport: {pm.passport_file}")
    for stage_name in ordered_stages:
      name = effective[stage_name]
      if not name:
        print(f"  ⬜ {stage_name}: not started")
        continue
      detail = ""
      if name in ("FAILED", "REJECTED", "PENDING", "SKIPPED", "WAITING"):
        message = latest[stage_name].message or ""
        if name == "WAITING":
          # Lead with what the operator needs: the last progress seen.
          m = _LAST_PROGRESS_RE.search(message)
          progress = m.group(1) if m else ""
          why = (
              "polling timed out"
              if "timed out" in message
              else "polling exhausted max attempts"
          )
          summary = f"{why}; last progress: {progress}" if progress else why
        else:
          summary = engine.summarise(message).removeprefix("Skipped: ")
          if "Rollback executed successfully" in message:
            summary = f"{summary} · rolled back" if summary else "rolled back"
          elif "Rollback action" in message and "failed" in message:
            summary = (
                f"{summary} · rollback FAILED" if summary else "rollback FAILED"
            )
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
      print("\nPASSPORT PAYLOAD (JSON):")
      print(
          json.dumps(
              engine.payload_to_dict(passport.payload), indent=2, default=str
          )
      )

      provenance = engine.top_level_key_provenance(
          engine.payload_to_dict(passport.payload)
      )
      if provenance:
        print("\nTOP-LEVEL KEY PROVENANCE (payload.<key> ← last writer):")
        for key, (owner, others) in sorted(provenance.items()):
          also = f"  (also set by: {', '.join(others)})" if others else ""
          print(f"  {key} ← {owner}{also}")

      print("\nSTAGE STAMPS LOG:")
      pending_since: dict[str, float] = {}
      for stamp in passport.stamps:
        status_name = schema.StampStatus.Name(stamp.status).title()
        time_str = stamp.timestamp.ToJsonString()
        msg = f": {stamp.message}" if stamp.message else ""
        # Elapsed time from the stage's most recent PENDING stamp to this
        # terminal stamp, so slow stages stand out in the audit log.
        took = ""
        epoch = _stamp_epoch(stamp)
        if stamp.status == schema.StampStatus.PENDING:
          pending_since[stamp.stage_name] = epoch
        elif stamp.stage_name in pending_since and epoch:
          started = pending_since.pop(stamp.stage_name)
          if started:
            took = f" (took {_format_duration(epoch - started)})"
        print(
            f"  [{time_str}] Stage '{stamp.stage_name}' -> {status_name}{msg}"
            f"{took}"
        )

      print("\nLIGHTFLOW STATUS DIAGRAM:")
      print(generate_status_mermaid(lightflow_proto, passport))

    _raise_for_run_state(
        state,
        paused,
        failed,
        rejected,
        unfinished,
        resume_hint=_continue_command(
            runner,
            pm.workflow_id,
            include_lightflow=not passport.lightflow_path,
        ),
        waiting=waiting,
    )

  def dry_run(
      self,
      lightflow: str = "",
      payload: str | dict[str, Any] = "{}",
      log_id: str = "",
      *,
      payload_file: Optional[str] = None,
      workflow: Optional[str] = None,
  ) -> None:
    """Traces the DAG without side effects and reports what would happen.

    Actions whose callable accepts a `dry_run` parameter are called with
    `dry_run=True` and their output drives downstream conditions; gates get
    a mock approval from their `json_schema`; conditions that depend on
    unsimulated upstream outputs or unseeded `--payload` keys are marked
    `[DEFERRED TO RUNTIME ...]` and traced through; expressions that would
    error are marked `[WOULD FAIL]`. Ends with a `Summary:` line, a
    ready-to-paste `--payload` seed hint when applicable, and the stage →
    action pairs that lack `dry_run` support. Exit 0 when the manifest
    compiles and no stage would fail; exit 1 (after the full trace) when an
    expression would error at runtime. `--payload` may seed `outputs.<stage>`
    to trace past a stage that cannot be simulated.

    Args:
      lightflow: Path to the manifest.
      payload: Seed payload to exercise specific branches (inline JSON,
        `@file.json`, or `@-`).
      log_id: Optional; only used to render concrete `resume` commands.
      payload_file: Path to a JSON file with the seed payload.
      workflow: Deprecated alias for `lightflow`.

    Raises:
      LightflowRunFailedError: At least one stage would fail at runtime (the
        trace is printed first; exit 1).
      engine.EngineError: The manifest does not compile.
      ValueError: The manifest path does not exist or the payload is invalid.
    """
    effective_payload = _resolve_payload_input(payload, payload_file)
    resolved_lightflow = lightflow or workflow
    if not resolved_lightflow:
      raise ValueError("Missing required '--lightflow' manifest path.")
    workflow_path = _require_manifest(
        resolved_lightflow, engine.resolve_lightflow_path
    )
    workflow_proto = engine.load_lightflow(workflow_path)

    # Unlike `start`, a dry run may seed `outputs.<stage>` to trace past a
    # stage that cannot be simulated (or a gate).
    initial_payload = _parse_payload_arg(
        effective_payload, context_label="--payload", allow_outputs=True
    )
    current_payload = engine.normalize_struct_numbers(dict(initial_payload))

    print(f"=== DRY RUN: {workflow_proto.name} ===")
    print(f"Initial Payload: {json.dumps(current_payload, default=str)}\n")

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
    referenced_actions: set[str] = set()
    for st in workflow_proto.stages:
      for action in (
          getattr(st, "python_action", None),
          getattr(st, "rollback_action", None),
          getattr(
              getattr(st, "polling_policy", None), "poll_tick_action", None
          ),
      ):
        action_id = getattr(action, "action_id", "") if action else ""
        if action_id:
          referenced_actions.add(action_id)
    counts = {
        "simulated": 0,
        "not_simulated": 0,
        "gates": 0,
        "skipped": 0,
        "would_fail": 0,
    }
    deferred_stages: set[str] = set()
    would_fail_stages: list[str] = []
    not_simulated_pairs: list[tuple[str, str]] = []  # (stage, action_id)
    seed_keys: dict[str, list[str]] = {}  # key -> stages that need it
    sim_passport = schema.Passport()
    unsimulated_stages: set[str] = set()
    for stage_name in ordered_stages:
      stage_proto = runner.get_stage(stage_name)
      if not stage_proto:
        continue

      print(f"Stage: {stage_name}")
      if stage_proto.description:
        print(f"  Description: {stage_proto.description}")
      if (
          isinstance(stage_proto, schema.Stage)
          and stage_proto.HasField("timeout_seconds")
          and stage_proto.timeout_seconds is not None
      ):
        if stage_proto.timeout_seconds > 0:
          print(f"  Timeout: {stage_proto.timeout_seconds}s")
        else:
          print("  Timeout: disabled (timeout_seconds: 0)")
      _print_stage_policies(stage_proto)

      if stage_proto.run_after:
        trigger_res = runner.evaluate_trigger_rule(stage_proto, sim_passport)
        if (
            isinstance(trigger_res, tuple)
            and len(trigger_res) == 3
            and not trigger_res[0]
        ):
          _, target_status, reason = trigger_res
          print(f"  Trigger Rule: {reason}")
          if (
              target_status == schema.StampStatus.SKIPPED
              and len(stage_proto.run_after) > 1
          ):
            print(
                "  💡 If this stage is meant to converge after complementary"
                " run_if branches, set `trigger_rule: NONE_FAILED` on"
                f" '{stage_name}' (ALL_SUCCESS skips it when any branch was"
                " skipped; ALL_DONE would also run it after a failure)."
            )
          print("  Status: [SKIPPED]\n")
          counts["skipped"] += 1
          sim_stamp = sim_passport.stamps.add()
          sim_stamp.stage_name = stage_name
          sim_stamp.status = target_status
          continue

      should_run = True
      would_fail = False
      if stage_proto.run_if:
        output_refs: list[str] = []
        top_refs: list[str] = []
        if isinstance(stage_proto.run_if, str):
          try:
            ast_tree = engine.parse_expression(stage_proto.run_if)
            output_refs = engine.extract_payload_output_stage_refs(ast_tree)
            top_refs = engine.extract_top_level_payload_refs(ast_tree)
          except Exception:  # pylint: disable=broad-exception-caught
            output_refs = []
        outputs_ns = current_payload.get(engine.STAGE_OUTPUTS_KEY)
        missing_refs = [
            ref
            for ref in output_refs
            if ref in unsimulated_stages
            and (not isinstance(outputs_ns, dict) or ref not in outputs_ns)
        ]
        missing_seed = [k for k in top_refs if k not in current_payload]
        if missing_refs:
          also = ""
          if missing_seed:
            also = f"; --payload keys: {', '.join(missing_seed)}"
            for k in missing_seed:
              seed_keys.setdefault(k, []).append(stage_name)
          print(
              f"  Condition '{stage_proto.run_if}' -> [DEFERRED TO RUNTIME"
              f" (awaits outputs of: {', '.join(missing_refs)}{also})]"
          )
          deferred_stages.add(stage_name)
          should_run = True
        elif missing_seed:
          # The branch reads top-level keys that nothing has set yet. Either
          # an unsimulated upstream stage will populate them at runtime, or
          # they must come from --payload; either way, trace through rather
          # than pruning the rest of the DAG on a guess.
          upstream = sorted(
              u
              for u in unsimulated_stages
              if stage_name in runner.get_downstream_dependents(u)
          )
          if upstream:
            source = (
                f"payload keys {', '.join(missing_seed)} from"
                f" {', '.join(upstream)} or --payload"
            )
          else:
            source = f"--payload keys: {', '.join(missing_seed)}"
          for k in missing_seed:
            seed_keys.setdefault(k, []).append(stage_name)
          print(
              f"  Condition '{stage_proto.run_if}' -> [DEFERRED TO RUNTIME"
              f" (awaits {source})]"
          )
          deferred_stages.add(stage_name)
          should_run = True
        else:
          try:
            should_run = engine.evaluate_cel(
                stage_proto.run_if,
                current_payload,
                strict=bool(workflow_proto.strict_expressions),
            )
            print(f"  Condition '{stage_proto.run_if}' -> {should_run}")
          except engine.EngineError as e:
            has_unsimulated_ancestor = any(
                stage_name in runner.get_downstream_dependents(u)
                for u in unsimulated_stages
            )
            if _is_deferrable_dry_run_eval_error(
                e, output_refs, has_unsimulated_ancestor
            ):
              print(
                  f"  Condition '{stage_proto.run_if}' -> [DEFERRED TO RUNTIME"
                  f" ({e})]"
              )
              deferred_stages.add(stage_name)
              should_run = True
            else:
              print(f"  Condition evaluation failed: {e}")
              would_fail = True

      if would_fail:
        # At runtime this stage is stamped FAILED, not SKIPPED: say so, and
        # keep tracing so downstream collateral is visible too.
        print("  Status: [WOULD FAIL] ❌\n")
        counts["would_fail"] += 1
        would_fail_stages.append(stage_name)
        sim_stamp = sim_passport.stamps.add()
        sim_stamp.stage_name = stage_name
        sim_stamp.status = schema.StampStatus.FAILED
        continue

      if not should_run:
        print("  Status: [SKIPPED]\n")
        counts["skipped"] += 1
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
          static_args = engine.static_kwargs_dict(tick_action)
          if static_args:
            print(f"    Static Args: {static_args}")
          simulated = runner.simulate_dry_run_action(
              stage_name, tick_action, current_payload
          )
          if isinstance(simulated, tuple) and len(simulated) == 2:
            sim_payload, sim_msg = simulated
            collisions = engine.merge_stage_payload(
                current_payload, stage_name, sim_payload
            )
            print(f"    Dry-Run Output: {sim_msg}")
            counts["simulated"] += 1
            for key, previous_owner in collisions:
              if key in runner.top_level_keys_read:
                print(
                    f"    ⚠️ Overwrites top-level payload key '{key}'"
                    f" (previously set by {previous_owner}), which an"
                    " expression reads"
                )
          else:
            unsimulated_stages.add(stage_name)
            _print_not_simulated_note(stage_name, unsimulated_stages, runner)
            counts["not_simulated"] += 1
            not_simulated_pairs.append((stage_name, tick_action.action_id))
      elif stage_proto.HasField("python_action") and stage_proto.python_action:
        print(
            "  Action: [RUN] executes python_action"
            f" '{stage_proto.python_action.action_id}'"
        )
        static_args = engine.static_kwargs_dict(stage_proto.python_action)
        if static_args:
          print(f"    Static Args: {static_args}")
        simulated = runner.simulate_dry_run_action(
            stage_name, stage_proto.python_action, current_payload
        )
        if isinstance(simulated, tuple) and len(simulated) == 2:
          sim_payload, sim_msg = simulated
          collisions = engine.merge_stage_payload(
              current_payload, stage_name, sim_payload
          )
          print(f"    Dry-Run Output: {sim_msg}")
          counts["simulated"] += 1
          for key, previous_owner in collisions:
            if key in runner.top_level_keys_read:
              print(
                  f"    ⚠️ Overwrites top-level payload key '{key}'"
                  f" (previously set by {previous_owner}), which an"
                  " expression reads"
              )
        else:
          unsimulated_stages.add(stage_name)
          _print_not_simulated_note(stage_name, unsimulated_stages, runner)
          counts["not_simulated"] += 1
          not_simulated_pairs.append(
              (stage_name, stage_proto.python_action.action_id)
          )
      elif (
          stage_proto.HasField("operator_action")
          and stage_proto.operator_action
      ):
        operator_action = stage_proto.operator_action
        counts["gates"] += 1
        instr_refs: list[str] = []
        instr_top_refs: list[str] = []
        if isinstance(operator_action.instructions, str):
          try:
            instr_ast = engine.parse_expression(operator_action.instructions)
            instr_refs = engine.extract_payload_output_stage_refs(instr_ast)
            instr_top_refs = engine.extract_top_level_payload_refs(instr_ast)
          except Exception:  # pylint: disable=broad-exception-caught
            instr_refs = []
        outputs_ns = current_payload.get(engine.STAGE_OUTPUTS_KEY)
        missing_instr_refs = [
            ref
            for ref in instr_refs
            if ref in unsimulated_stages
            and (not isinstance(outputs_ns, dict) or ref not in outputs_ns)
        ]
        missing_instr_seed = [
            k for k in instr_top_refs if k not in current_payload
        ]
        if missing_instr_seed and not missing_instr_refs:
          upstream = sorted(
              u
              for u in unsimulated_stages
              if stage_name in runner.get_downstream_dependents(u)
          )
          if upstream:
            source = (
                f"payload keys {', '.join(missing_instr_seed)} from"
                f" {', '.join(upstream)} or --payload"
            )
          else:
            source = f"--payload keys: {', '.join(missing_instr_seed)}"
          for k in missing_instr_seed:
            seed_keys.setdefault(k, []).append(stage_name)
          print("  Action: [SUSPEND] waits at operator_action")
          print(f"    Instructions: [DEFERRED TO RUNTIME (awaits {source})]")
          deferred_stages.add(stage_name)
        elif missing_instr_refs:
          also = ""
          if missing_instr_seed:
            also = f"; --payload keys: {', '.join(missing_instr_seed)}"
            for k in missing_instr_seed:
              seed_keys.setdefault(k, []).append(stage_name)
          print("  Action: [SUSPEND] waits at operator_action")
          print(
              "    Instructions: [DEFERRED TO RUNTIME (awaits outputs of:"
              f" {', '.join(missing_instr_refs)}{also})]"
          )
          deferred_stages.add(stage_name)
        else:
          try:
            instructions = engine.evaluate_cel(
                operator_action.instructions,
                current_payload,
                strict=bool(workflow_proto.strict_expressions),
            )
            print("  Action: [SUSPEND] waits at operator_action")
            print(f"    Instructions: {instructions}")
          except Exception as e:  # pylint: disable=broad-exception-caught
            has_unsimulated_ancestor = any(
                stage_name in runner.get_downstream_dependents(u)
                for u in unsimulated_stages
            )
            if _is_deferrable_dry_run_eval_error(
                e, instr_refs, has_unsimulated_ancestor
            ):
              print("  Action: [SUSPEND] waits at operator_action")
              print(f"    Instructions: [DEFERRED TO RUNTIME ({e})]")
              deferred_stages.add(stage_name)
            else:
              print(
                  "  Action: [SUSPEND] waits at operator_action (instructions"
                  f" eval failed: {e})"
              )
              print("    ❌ At runtime this stage would FAIL before pausing.")
              counts["would_fail"] += 1
              would_fail_stages.append(stage_name)
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

    ran = counts["simulated"] + counts["not_simulated"]
    parts = [
        f"{len(ordered_stages)} stages",
        (
            f"{ran} would run ({counts['simulated']} simulated,"
            f" {counts['not_simulated']} not simulated)"
        ),
        f"{counts['gates']} operator gate(s)",
        f"{len(deferred_stages)} deferred to runtime",
        f"{counts['skipped']} skipped",
    ]
    if counts["would_fail"]:
      parts.append(f"❌ {counts['would_fail']} would fail")
    print("Summary: " + " · ".join(parts))
    dry_fail: Optional[LightflowRunFailedError] = None
    if would_fail_stages:
      print(
          f"❌ Would fail at runtime: {', '.join(would_fail_stages)}. Fix the"
          " expression(s) above before starting."
      )
      dry_fail = LightflowRunFailedError(
          f"Dry run: {len(would_fail_stages)} stage(s) would fail at runtime:"
          f" {', '.join(would_fail_stages)}.",
          already_reported=True,
      )
    unused_actions = sorted(
        a.id for a in workflow_proto.actions if a.id not in referenced_actions
    )
    if unused_actions:
      print(
          f"ℹ️ Unused action id(s) in `actions:`: {', '.join(unused_actions)}"
      )
    if seed_keys:
      seed = json.dumps({k: "<value>" for k in seed_keys})
      needing: set[str] = set()
      for stages in seed_keys.values():
        needing.update(stages)
      needed_by = ", ".join(sorted(needing))
      print(
          "💡 Seed these keys to trace the deferred branches (skip any that an"
          f" upstream stage sets at runtime): --payload='{seed}'  (needed by:"
          f" {needed_by})"
      )
    if deferred_stages and not_simulated_pairs:
      pairs = ", ".join(
          f"{stage} (action '{action_id}')"
          for stage, action_id in not_simulated_pairs
      )
      print(
          f"💡 Not simulated: {pairs}. Give those actions a `dry_run`"
          " parameter (return representative output when it is true) so the"
          " stages deferred on their outputs can be traced."
      )
    if dry_fail is not None:
      raise dry_fail

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
    workflow_path = engine.resolve_lightflow_path(resolved_lightflow)
    workflow_proto = engine.load_lightflow(workflow_path)
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
    passport_proto = None
    if log_id:
      pm = PassportManager(log_id)
      passport_proto = pm.load_passport()
      if passport_proto is not None:
        workflow_path = _manifest_for_run(
            resolved_lightflow,
            passport_proto,
            log_id,
            engine.resolve_lightflow_path,
        )
      elif not resolved_lightflow:
        raise LightflowStateNotFoundError(
            f"No workflow state found for log ID '{log_id}' in"
            f" {os.path.dirname(pm.resolved_path)} (cannot infer --lightflow)."
        )
      else:
        workflow_path = _require_manifest(
            resolved_lightflow, engine.resolve_lightflow_path
        )
    else:
      if not resolved_lightflow:
        raise ValueError("Missing required '--lightflow' manifest path.")
      workflow_path = _require_manifest(
          resolved_lightflow, engine.resolve_lightflow_path
      )
    workflow_proto = engine.load_lightflow(workflow_path)
    if passport_proto is not None:
      _check_same_lightflow(passport_proto, workflow_proto.name, log_id or "")

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
    """Iterates through topological stages, running executable steps.

    A polling stage that yields is parked, not broken: everything downstream
    of it is left untouched (no blocked-collateral stamps) while independent
    branches keep running; `resume` re-polls it.

    Args:
      runner: Compiled engine for the manifest.
      ordered_stages: Topological stage order.
      passport: The run's Passport (persisted after every stage).
      pm: Its PassportManager.

    Raises:
      engine.OperatorActionSuspended: A gate paused the run (exit 2).
      LightflowWaitingError: Only polling yields remain (exit 5).
      LightflowRunFailedError: A stage failed or a gate was rejected (exit 1);
        the footer has already been printed (`already_reported`).
    """
    waiting_shadow: set[str] = set()
    for stamp in passport.stamps:
      if _latest_stamp_is_poll_yield(passport, stamp.stage_name):
        waiting_shadow.update(
            runner.get_downstream_dependents(stamp.stage_name)
        )
    for stage_name in ordered_stages:
      latest_status = {
          stamp.stage_name: stamp.status for stamp in passport.stamps
      }

      if stage_name in waiting_shadow or latest_status.get(stage_name) in (
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
        if _latest_stamp_is_poll_yield(passport, stage_name):
          waiting_shadow.update(runner.get_downstream_dependents(stage_name))
      except engine.OperatorActionSuspended:
        pm.save_passport(passport)
        raise
      except (engine.StageTimeoutError, engine.EngineError) as err:
        # The engine stamps FAILED before raising; make sure of it, so the
        # footer and `status` never see an unfinished stage after an error.
        if (
            not passport.stamps
            or passport.stamps[-1].stage_name != stage_name
            or passport.stamps[-1].status != schema.StampStatus.FAILED
        ):
          failed_stamp = passport.stamps.add()
          failed_stamp.stage_name = stage_name
          failed_stamp.status = schema.StampStatus.FAILED
          failed_stamp.timestamp.CopyFrom(engine.get_timestamp())
          failed_stamp.message = str(err)
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
      latest_stamp_by_stage = {
          stamp.stage_name: stamp for stamp in passport.stamps
      }

      def _msg(name: str) -> str:
        stamp = latest_stamp_by_stage.get(name)
        return (stamp.message or "") if stamp else ""

      kinds = {
          n: effective_status_name(latest_stamp_by_stage[n])
          for n in failed_stages
      }
      yielded = [n for n in failed_stages if kinds[n] == "WAITING"]
      rejected = [n for n in failed_stages if kinds[n] == "REJECTED"]
      blocked = [n for n in failed_stages if kinds[n] == "BLOCKED"]
      roots = [n for n in failed_stages if kinds[n] == "FAILED"]
      resume_cmd = _continue_command(
          runner,
          pm.workflow_id,
          include_lightflow=not passport.lightflow_path,
      )

      # --- Parked on an external condition: not a failure.
      if yielded and not roots and not rejected:
        name = yielded[0]
        m = _LAST_PROGRESS_RE.search(_msg(name))
        progress = f" Last progress: {m.group(1)}." if m else ""
        headline = (
            f"⏳ Lightflow is waiting: polling stage '{name}' yielded before"
            f" its condition was met.{progress}"
        )
        self._send_status_notification(runner, passport, headline)
        print(f"\n{headline}")
        print(
            "State is preserved. Once the external condition is satisfied,"
            f" continue with:\n  {resume_cmd}"
        )
        raise LightflowWaitingError(headline, already_reported=True)

      # --- Operator said no: terminal unless deliberately re-opened.
      if rejected and not roots:
        gate = rejected[0]
        headline = (
            f"🛑 Lightflow stopped: operator gate '{gate}' was rejected"
            f" ({engine.summarise(_msg(gate))})."
        )
        self._send_status_notification(
            runner, passport, headline, is_failed=True
        )
        print(f"\n{headline}")
        print(
            "Nothing to fix. To re-open the gate and decide again in one"
            f" call:\n  {resume_cmd} --stage={gate} --resolution=APPROVE"
            " --payload='{...}'\n(or just"
            f" {resume_cmd} --stage={gate} to re-open it and pause there)."
        )
        raise LightflowRunFailedError(headline, already_reported=True)

      # --- Genuine failures: root causes first, collateral summarised.
      err_msg = (
          f"Lightflow failed. Root failure(s): {', '.join(roots or rejected)}"
      )
      self._send_status_notification(runner, passport, err_msg, is_failed=True)
      print(f"\n{err_msg}")
      print("❌ Root failure(s):")
      for s_name in roots:
        lines = _msg(s_name).splitlines()
        first = lines[0] if lines else ""
        rolled = (
            " · rolled back"
            if "Rollback executed successfully" in _msg(s_name)
            else ""
        )
        print(f"  - {s_name}: {first}{rolled}")
        rb_fail = _ROLLBACK_FAILED_RE.search(_msg(s_name))
        if rb_fail:
          print(
              f"    ⚠️ rollback '{rb_fail.group(1)}' ALSO FAILED:"
              f" {engine.summarise(rb_fail.group(2))} — side effects may be"
              " partially applied; verify by hand before resuming."
          )
      if rejected:
        print(f"🛑 Rejected gate(s): {', '.join(rejected)}")
      if yielded:
        print(f"⏳ Waiting on external condition: {', '.join(yielded)}")
      if blocked:
        print(f"⛔ Blocked downstream (not run): {', '.join(blocked)}")
      # A failure on an absent *top-level* key is usually a missing --payload
      # value rather than a code bug: say so, and put the key on the command.
      missing_seed_keys = _missing_top_level_keys_from_failures(
          {k: v.message for k, v in latest_stamp_by_stage.items()},
          roots,
      )
      if missing_seed_keys:
        seed = json.dumps({k: "<value>" for k in missing_seed_keys})
        resume_cmd += f" --payload='{seed}'"
        print(
            "\n💡 Stage(s) failed on top-level payload key(s)"
            f" {', '.join(missing_seed_keys)} that were never provided."
            " Supply them with --payload on resume (or fix the expression)."
        )
      print(
          "\nResume after fixing the root cause (preserves completed"
          f" stages):\n  {resume_cmd}"
      )
      raise LightflowRunFailedError(err_msg, already_reported=True)

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
    """Deletes the saved Passport state and lock file for a log ID.

    External workflow artifacts on disk are untouched.

    Args:
      lightflow: Unused; accepted so every subcommand shares the same flags.
      log_id: Run identifier whose state directory should be removed.
      workflow: Deprecated alias of `lightflow`; unused.
    """
    del lightflow, workflow
    if not log_id:
      raise ValueError(
          "Missing required '--log_id'. Usage: lightflow cleanup --log_id=<id>"
          " (see 'list' for saved runs)."
      )
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
      print(
          f"Successfully cleaned up state for log ID: {log_id} (removed"
          f" {pm.resolved_path}; external workflow artifacts on disk are"
          " untouched)"
      )
    except LightflowRunningError as e:
      raise LightflowRunningError(
          f"Cannot clean up state for log ID '{log_id}' because the lightflow"
          " is currently running (exit 4). Wait for it, then retry."
      ) from e

  # Shadows the builtin inside the class body from here on, so any later
  # method annotated with `list[...]` would break at import: keep this last.
  def list(  # pylint: disable=redefined-builtin
      self,
      state_dir: str = "",
  ) -> None:
    """Lists every saved run under the state directory, newest first.

    Reads each `passport.json` under `$LIGHTFLOW_STATE_DIR` (or the default
    state root) and prints one line per run with its log ID, lightflow name,
    coarse state, last activity, and recorded manifest path, plus any orphan
    `.lock` files left behind by an interrupted `start`. Use `status` for the
    authoritative per-stage view of a run.

    Args:
      state_dir: Override the directory to scan.
    """
    runs, orphan_locks, root = collect_runs(state_dir)
    if not os.path.isdir(root):
      print(f"No runs found (state directory does not exist: {root})")
      return
    rows = [
        (
            r.last_activity,
            r.log_id,
            r.lightflow_name or "?",
            r.state,
            _format_age(r.last_activity),
            r.error or _manifest_cell(r.lightflow_path),
        )
        for r in runs
    ]
    if not rows and not orphan_locks:
      print(f"No runs found under {root}")
      return

    print(f"Runs under {root}:")
    id_w = max([len(r[1]) for r in rows] + [6])
    name_w = max([len(r[2]) for r in rows] + [9])
    print(
        f"  {'LOG_ID':<{id_w}}  {'LIGHTFLOW':<{name_w}}  {'STATE':<12} "
        f" {'LAST ACTIVITY':<14}  MANIFEST"
    )
    for _, log_id, name, state, age, manifest in rows:
      icon = _STATE_ICONS.get(state, "•")
      print(
          f"  {log_id:<{id_w}}  {name:<{name_w}}  {icon} {state:<10}  {age:<14}"
          f"  {manifest}"
      )
    for log_id in orphan_locks:
      print(
          f"  ⚠️ orphan lock for '{log_id}' (no state directory);"
          f" remove with 'cleanup --log_id={log_id}'"
      )
