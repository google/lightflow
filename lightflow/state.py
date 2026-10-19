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

"""State persistence, advisory file locking, and run lifecycle helpers."""

from __future__ import annotations

import contextlib
import datetime
import getpass
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
except ImportError:
  from lightflow import engine  # pyrefly: ignore[missing-import]
  from lightflow import schema  # pyrefly: ignore[missing-import]
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
  def lock(
      self, exclusive: bool = True, *, remove_if_uninitialized: bool = False
  ):
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
      if (
          remove_if_uninitialized
          and not os.path.exists(self.resolved_path)
          and not os.path.islink(lock_path)
      ):
        try:
          os.remove(lock_path)
        except OSError:
          pass

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
      # stderr, so stdout stays clean for callers that parse it.
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
  `status`, `list` and the failure footer agree: REJECTED (an
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
