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

"""Pure-Python Lightweight DAG Workflow Engine ("Lightflow OSS")."""

from __future__ import annotations

# pylint: disable=g-long-ternary

import ast
import copy
import difflib
import graphlib
import hashlib
import importlib
import importlib.util
import inspect
import json
import keyword
import os
import queue
import random
import re
import shlex
import shutil  # pylint: disable=unused-import
import subprocess  # pylint: disable=unused-import
import sys
import threading
import time
import traceback
from typing import Any, Callable, Collection, Optional, Sequence

# pylint: disable=g-import-not-at-top,g-bad-import-order
try:
  from . import cel_lite
  from . import notifications
  from . import schema
except ImportError:
  from lightflow import cel_lite  # pyrefly: ignore[missing-import]
  from lightflow import notifications  # pyrefly: ignore[missing-import]
  from lightflow import schema  # pyrefly: ignore[missing-import]
# pylint: enable=g-import-not-at-top,g-bad-import-order
# pylint: disable=protected-access

EngineError = cel_lite.EngineError
StageTimeoutError = cel_lite.StageTimeoutError
MissingFieldError = cel_lite.MissingFieldError
OperatorActionSuspended = cel_lite.OperatorActionSuspended
ManualGateSuspended = cel_lite.ManualGateSuspended


def _quote_cli_arg(value: str) -> str:
  """Shell-quotes a CLI flag value while preserving dry-run placeholders."""
  if value == "<log_id>":
    return value
  return shlex.quote(value)


_CEL_OPERATOR_PATTERN = cel_lite._CEL_OPERATOR_PATTERN
parse_expression = cel_lite.parse_expression
_is_payload_outputs_node = cel_lite._is_payload_outputs_node
extract_payload_output_stage_refs = cel_lite.extract_payload_output_stage_refs
_suggest_key = cel_lite._suggest_key
extract_top_level_payload_refs = cel_lite.extract_top_level_payload_refs
_MAX_SEQUENCE_REPEAT = cel_lite._MAX_SEQUENCE_REPEAT
_MAX_RESULT_CHARS = cel_lite._MAX_RESULT_CHARS
_cap_result = cel_lite._cap_result
_safe_mul = cel_lite._safe_mul
_safe_add = cel_lite._safe_add
_safe_mod = cel_lite._safe_mod
SafeCelEvaluator = cel_lite.SafeCelEvaluator
evaluate_cel = cel_lite.evaluate_cel


# ------------------- NOTIFICATIONS & UTILS -------------------


def get_timestamp() -> schema.TimestampValue:
  ts = schema.TimestampValue()
  ts.GetCurrentTime()
  return ts


_SUMMARY_MAX_CHARS = notifications._SUMMARY_MAX_CHARS
summarise = notifications.summarise
_NOTIFICATION_THREADS = notifications._NOTIFICATION_THREADS
_NOTIFICATION_HANDLERS = notifications._NOTIFICATION_HANDLERS
register_notification_handler = notifications.register_notification_handler
clear_notification_handlers = notifications.clear_notification_handlers
_MILESTONE_TITLE_PREFIXES = notifications._MILESTONE_TITLE_PREFIXES
_notification_mode = notifications._notification_mode
send_notification = notifications.send_notification
wait_for_notifications = notifications.wait_for_notifications


# ------------------- STAGE OUTPUT ATTRIBUTION -------------------

STAGE_OUTPUTS_KEY = cel_lite.STAGE_OUTPUTS_KEY

# Message of the FAILED stamp a trigger rule assigns, without running the
# stage, when an upstream dependency failed. `status` matches it to report the
# stage as blocked rather than as a failure needing a fix.
UPSTREAM_FAILED_MESSAGE = "Failed: Upstream dependency failed."

# Substring of the FAILED stamp a polling stage writes when it gives up
# waiting (timeout or max_attempts). Nothing is broken in that case: the run
# is parked until an external condition is met, and `status`/`resume` treat
# it as WAITING rather than as a failure to fix.
POLL_YIELD_MARKER = "Yielding control back to operator/agent"


REDACTED = "<redacted>"


def _secret_values(
    payload_dict: dict[str, Any], secret_keys: Collection[str]
) -> list[str]:
  """Returns non-empty string values held under `secret_keys`, longest first."""
  secrets = set(secret_keys)
  if not secrets or not isinstance(payload_dict, dict):
    return []
  found: set[str] = set()

  def collect(value: Any, under_secret: bool = False) -> None:
    if isinstance(value, dict):
      for k, v in value.items():
        collect(v, under_secret or (k in secrets))
    elif isinstance(value, list):
      for item in value:
        collect(item, under_secret)
    elif under_secret and isinstance(value, str) and value:
      found.add(value)

  collect(payload_dict)
  return sorted(found, key=len, reverse=True)


def _scrub_secrets(text: str, secret_vals: list[str]) -> str:
  """Single-pass replacement of `secret_vals` in `text`."""
  if not text or not secret_vals:
    return text
  pattern = re.compile("|".join(re.escape(s) for s in secret_vals))
  return pattern.sub(lambda _: REDACTED, text)


def redact_text(
    text: str, payload_dict: dict[str, Any], secret_keys: Collection[str]
) -> str:
  """Replaces any secret value from `payload_dict` that appears in `text`."""
  return _scrub_secrets(text, _secret_values(payload_dict, secret_keys))


def redact_payload(
    payload_dict: dict[str, Any], secret_keys: Collection[str]
) -> dict[str, Any]:
  """Returns a copy with every value under a secret key replaced.

  Keys match at any depth, so the `outputs.<stage>.<key>` mirror of a stage
  result is covered as well as the top level. String values held under those
  keys are also scrubbed from any other string in the payload.

  Args:
    payload_dict: The payload as a plain dict.
    secret_keys: Key names whose values must not be shown.

  Returns:
    `payload_dict` itself when there are no secret keys, else a redacted copy.
  """
  secrets = set(secret_keys)
  if not secrets:
    return payload_dict
  secret_vals = _secret_values(payload_dict, secrets)

  def walk(value: Any) -> Any:
    if isinstance(value, dict):
      return {
          k: REDACTED if k in secrets else walk(v) for k, v in value.items()
      }
    if isinstance(value, list):
      return [walk(v) for v in value]
    if isinstance(value, str) and secret_vals:
      return _scrub_secrets(value, secret_vals)
    return value

  return walk(payload_dict)


def _announce_failure(stage_name: str, message: str) -> None:
  """Prints a stage failure when it happens; the footer summarises it later."""
  lines = (message or "").strip().splitlines()
  print(f"  ❌ [{stage_name}] {lines[0] if lines else 'failed'}", flush=True)
  send_notification(f"Stage Failed: {stage_name}", f"❌ {stage_name}: failed")


# Terminal statuses a trigger rule can assign without running the stage, and the
# icon announced for each. Any other status (e.g. STATUS_UNSPECIFIED while
# parents are still running) is not a lifecycle transition and is not announced.
_TRIGGER_NOTIFICATION_ICONS = {
    schema.StampStatus.FAILED: "❌",
    schema.StampStatus.SKIPPED: "⏭️",
}


def add_stamp(
    passport: schema.Passport,
    stage_name: str,
    status: schema.StampStatus,
    message: str = "",
    *,
    reason: Optional[schema.Stamp.Reason] = None,
) -> schema.Stamp:
  """Appends a timestamped stamp to the passport and returns it."""
  stamp = passport.stamps.add()
  stamp.stage_name = stage_name
  stamp.status = status
  stamp.timestamp.CopyFrom(get_timestamp())
  stamp.message = message
  if reason is not None:
    stamp.reason = reason
  return stamp


# Short names for the stamp statuses, so a stamp site reads on one line.
_PENDING = schema.StampStatus.PENDING
_COMPLETED = schema.StampStatus.COMPLETED
_SKIPPED = schema.StampStatus.SKIPPED
_FAILED = schema.StampStatus.FAILED
_PAUSED = schema.StampStatus.PAUSED


def record_stage_outputs(
    payload_dict: dict[str, Any],
    stage_name: str,
    stage_output: dict[str, Any],
) -> None:
  """Mirrors a stage's action result under `payload.outputs.<stage_name>`."""
  outputs = payload_dict.get(STAGE_OUTPUTS_KEY)
  if not isinstance(outputs, dict):
    outputs = {}

  snapshot = {
      key: copy.deepcopy(value)
      for key, value in stage_output.items()
      if key != STAGE_OUTPUTS_KEY
  }

  existing = outputs.get(stage_name)
  merged = dict(existing) if isinstance(existing, dict) else {}
  merged.update(snapshot)

  outputs[stage_name] = merged
  payload_dict[STAGE_OUTPUTS_KEY] = outputs


def merge_stage_payload(
    payload_dict: dict[str, Any],
    stage_name: str,
    stage_output: dict[str, Any],
) -> list[tuple[str, str]]:
  """Merges stage output into the payload and records it under `outputs`.

  Every key of `stage_output` (except the reserved `outputs`) is written to
  the top level of the payload *and* mirrored under
  `payload.outputs.<stage_name>`. The top level is last-writer-wins, so a
  later stage returning the same key as an earlier one silently changes what
  `payload.<key>` means for everything downstream. Such overwrites are
  detected here and returned so callers can surface them.

  Args:
    payload_dict: The live payload, mutated in place.
    stage_name: The stage whose output is being merged.
    stage_output: The dict returned by the stage's action.

  Returns:
    A list of `(key, previous_owner)` pairs for every top-level key whose
    value this merge changed, where `previous_owner` is the stage that last
    produced the prior value or `"initial payload"` if no stage did.
  """
  collisions: list[tuple[str, str]] = []
  for key, value in stage_output.items():
    if key == STAGE_OUTPUTS_KEY:
      continue
    if key in payload_dict and payload_dict[key] != value:
      previous_owner = _top_level_key_owner(
          payload_dict, key, prefer=stage_name
      )
      # A stage replacing a value it wrote itself (poll ticks, retries,
      # rollbacks) is progress, not a collision.
      if previous_owner != stage_name:
        collisions.append((key, previous_owner))
    payload_dict[key] = value
  record_stage_outputs(payload_dict, stage_name, stage_output)
  return collisions


def _top_level_key_owner(
    payload_dict: dict[str, Any], key: str, *, prefer: str = ""
) -> str:
  """Names the stage whose recorded output matches the top-level `key` value.

  Args:
    payload_dict: The payload to inspect.
    key: The top-level key whose current value should be attributed.
    prefer: A stage to check first, so that a stage re-writing its own value is
      attributed to itself even if another stage happened to emit the same value
      earlier.

  Returns:
    The owning stage name, or `"initial payload"` if no recorded stage output
    matches the current value (i.e. it came from `--payload` or a legacy run).
  """
  outputs = payload_dict.get(STAGE_OUTPUTS_KEY)
  current = payload_dict.get(key)
  if not isinstance(outputs, dict):
    return "initial payload"
  candidates = list(outputs.items())
  if prefer and prefer in outputs:
    candidates.sort(key=lambda item: item[0] != prefer)
  for owner, recorded in candidates:
    if (
        isinstance(recorded, dict)
        and key in recorded
        and recorded[key] == current
    ):
      return str(owner)
  return "initial payload"


def top_level_key_provenance(
    payload_dict: dict[str, Any],
) -> dict[str, tuple[str, list[str]]]:
  """Maps each top-level payload key to `(owner, other_stages_that_set_it)`.

  Used by `status --verbose` so a reader can tell which stage a top-level
  value came from, and which other stages also emitted that key (and were
  therefore overwritten, or will overwrite it if re-run).

  Args:
    payload_dict: The payload to inspect.

  Returns:
    A dict keyed by top-level payload key (excluding `outputs`).
  """
  outputs = payload_dict.get(STAGE_OUTPUTS_KEY)
  provenance: dict[str, tuple[str, list[str]]] = {}
  for key in payload_dict:
    if key == STAGE_OUTPUTS_KEY:
      continue
    owner = _top_level_key_owner(payload_dict, key)
    others: list[str] = []
    if isinstance(outputs, dict):
      others = [
          str(stage)
          for stage, recorded in outputs.items()
          if stage != owner and isinstance(recorded, dict) and key in recorded
      ]
    provenance[key] = (owner, others)
  return provenance


def _print_merge_collisions(
    stage_name: str,
    collisions: list[tuple[str, str]],
    keys_read: Collection[str],
) -> None:
  """Warns about overwritten top-level keys that an expression reads.

  Two stages emitting the same key is normal (one reusable action run from
  two stages); it only matters when a `run_if` / `condition` /
  `instructions` reads the top-level mirror `payload.<key>` and will now
  see the later stage's value.

  Args:
    stage_name: The stage whose output was just merged.
    collisions: `(key, previous_owner)` pairs from `merge_stage_payload`.
    keys_read: Top-level keys some expression in the manifest reads.
  """
  for key, previous_owner in collisions:
    if key not in keys_read:
      continue
    print(
        f"  ⚠️ [{stage_name}] overwrote top-level payload.{key} (previously"
        f" set by {previous_owner}), which an expression reads; use"
        f" payload.outputs.<stage>.{key} to be explicit.",
        flush=True,
    )


_LOADED_MODULE_HASHES: dict[str, bytes] = {}


def payload_to_dict(payload: schema.StructDict) -> dict[str, Any]:
  """Returns a Passport payload as a plain dict (whole floats as ints)."""
  return normalize_struct_numbers(payload.to_dict())


def set_payload(
    payload: schema.StructDict, payload_dict: dict[str, Any]
) -> None:
  """Replaces a Passport payload's contents with `payload_dict`."""
  payload.CopyFrom(payload_dict)


PASSPORT_FORMAT_VERSION = 1


def _check_passport_version(raw: Any) -> int:
  """Validates `passport_version` in a parsed passport JSON object."""
  if not isinstance(raw, dict):
    raise ValueError("Passport JSON must be a JSON object.")
  ver = raw.get("passport_version")
  if ver is None:
    ver = raw.get("passportVersion")
  if ver is None or ver == 0:
    return PASSPORT_FORMAT_VERSION
  if isinstance(ver, bool) or not isinstance(ver, int) or ver < 1:
    raise ValueError(
        f"Invalid passport_version={ver!r} (expected positive integer)."
    )
  if ver > PASSPORT_FORMAT_VERSION:
    raise ValueError(
        f"Passport format version {ver} is newer than this Lightflow version"
        f" supports (max: {PASSPORT_FORMAT_VERSION}); upgrade Lightflow to"
        " read this passport."
    )
  return ver


def passport_to_json(passport: schema.Passport) -> str:
  """Serialises a Passport for `passport.json`."""
  if not passport.passport_version:
    passport.passport_version = PASSPORT_FORMAT_VERSION
  return json.dumps(passport.to_dict(), indent=2)


def passport_from_json(text: str) -> schema.Passport:
  """Parses `passport.json` text, tolerating unknown fields."""
  raw = json.loads(text)
  ver = _check_passport_version(raw)
  passport = schema.Passport.from_dict(raw)
  passport.passport_version = ver
  return passport


def static_kwargs_dict(action: schema.PythonAction) -> dict[str, Any]:
  """Returns an action's `static_kwargs` as a plain dict."""
  return normalize_struct_numbers(dict(action.static_kwargs))


def load_lightflow(path: str) -> schema.Lightflow:
  """Parses the manifest at `path` (YAML, JSON, or textproto)."""
  return schema.load_lightflow(path)


def resolve_lightflow_path(lightflow_path: str) -> str:
  """Resolves a manifest file path, or a directory holding a default one."""
  resolved = schema.resolve_lightflow_path(lightflow_path)
  if os.path.isdir(resolved):
    raise FileNotFoundError(
        "No Lightflow manifest (lightflow.yaml, lightflow.yml, lightflow.json,"
        f" lightflow.textproto) found in directory: {lightflow_path}"
    )
  return resolved


def validate_json_schema(instance: Any, schema_dict: dict[str, Any]) -> None:
  """Validates `instance` against a JSON schema; raises ValueError if not."""
  try:
    schema.validate_json_schema(instance=instance, schema_dict=schema_dict)
  except Exception as e:  # pylint: disable=broad-exception-caught
    raise ValueError(str(e)) from e


def check_json_schema(schema_dict: dict[str, Any]) -> None:
  """Raises if `schema_dict` is not itself a valid JSON schema."""
  schema.check_json_schema(schema_dict)


def _file_fingerprint(path: str) -> Optional[bytes]:
  """Returns the SHA-256 digest of a source file, or None if unreadable."""
  try:
    with open(path, "rb") as f:
      return hashlib.sha256(f.read()).digest()
  except OSError:
    return None


def normalize_struct_numbers(value: Any) -> Any:
  """Normalizes struct numbers: converts whole floats to int."""
  if isinstance(value, float) and value.is_integer():
    return int(value)
  if isinstance(value, dict):
    return {k: normalize_struct_numbers(v) for k, v in value.items()}
  if isinstance(value, list):
    return [normalize_struct_numbers(item) for item in value]
  return value


# ------------------- LIGHTFLOW ENGINE -------------------


def effective_retry_policy(
    policy: Optional[schema.RetryPolicy],
) -> tuple[int, int, float]:
  """Returns `(max_attempts, initial_backoff_seconds, backoff_multiplier)`.

  Single source of the defaults (1 attempt, 5s, x2.0) so `execute_stage` and
  the dry-run policy printout cannot disagree.

  Args:
    policy: The stage's `RetryPolicy`, or None when it has none.
  """
  if policy is None:
    return 1, 5, 2.0
  attempts = (
      policy.max_attempts
      if policy.HasField("max_attempts") and policy.max_attempts is not None
      else 1
  )
  backoff = (
      policy.initial_backoff_seconds
      if policy.HasField("initial_backoff_seconds")
      and policy.initial_backoff_seconds is not None
      else 5
  )
  multiplier = (
      policy.backoff_multiplier
      if policy.HasField("backoff_multiplier")
      and policy.backoff_multiplier is not None
      else 2.0
  )
  return attempts, backoff, multiplier


def _did_you_mean(name: str, candidates: Any) -> str:
  """Returns " Did you mean 'x'?" for the closest candidate, or "".

  Args:
    name: The unknown identifier.
    candidates: Known identifiers to compare against.
  """
  matches = difflib.get_close_matches(
      str(name), [str(c) for c in candidates], n=1, cutoff=0.6
  )
  return f" Did you mean '{matches[0]}'?" if matches else ""


def _payload_placeholder(json_schema: Any) -> str:
  """Renders a pasteable `--payload` for a gate schema, or "" if none needed.

  `--resolution` already sets `approved`, so a schema that requires nothing
  else needs no `--payload` at all. Otherwise each remaining required key gets
  a typed placeholder (`"ticket": "<string>"`), which beats the generic
  `<JSON matching the stage json_schema>` because it can be edited in place.

  Args:
    json_schema: The gate's JSON schema (string or parsed dict), or empty.

  Returns:
    The `--payload='...'` argument to append, or "" when none is needed.
  """
  if not json_schema:
    return ""
  schema_obj: Any = json_schema
  if isinstance(json_schema, str):
    try:
      schema_obj = json.loads(json_schema)
    except (TypeError, ValueError):
      return "--payload='<JSON matching the stage json_schema>'"
  if not isinstance(schema_obj, dict):
    return "--payload='<JSON matching the stage json_schema>'"
  required = [k for k in schema_obj.get("required") or [] if k != "approved"]
  if not required:
    return ""
  props = schema_obj.get("properties") or {}
  parts = []
  for key in required:
    spec = props.get(key) if isinstance(props, dict) else None
    spec = spec if isinstance(spec, dict) else {}
    choices = list(spec["enum"]) if spec.get("enum") else []
    if "const" in spec:
      choices = [spec["const"]]
    if len(choices) == 1:
      # Only one legal value: write it, so the command pastes as-is.
      value = json.dumps(choices[0])
    elif choices and all(isinstance(v, str) for v in choices):
      value = '"<' + "|".join(choices) + '>"'
    elif choices:
      value = "<" + "|".join(json.dumps(v) for v in choices) + ">"
    else:
      kind = spec.get("type")
      kind = kind[0] if isinstance(kind, list) and kind else kind
      value = {
          "integer": "<integer>",
          "number": "<number>",
          "boolean": "<true|false>",
          "array": "[...]",
          "object": "{...}",
      }.get(str(kind), '"<string>"')
    parts.append(f'"{key}": {value}')
  return "--payload='{" + ", ".join(parts) + "}'"


def _schema_required_keys(json_schema: Any) -> Optional[list[str]]:
  """Returns the schema's `required` list, or None if it cannot be parsed."""
  if not json_schema:
    return []
  schema_obj: Any = json_schema
  if isinstance(json_schema, str):
    try:
      schema_obj = json.loads(json_schema)
    except (TypeError, ValueError):
      return None
  if not isinstance(schema_obj, dict):
    return None
  required = schema_obj.get("required")
  if required is None:
    return []
  if not isinstance(required, list):
    return None
  return [str(k) for k in required]


def resume_note(json_schema: Any) -> str:
  """One-line explanation printed under `Resume with:` for a paused gate.

  The generated resume command omits `approved` from `--payload` because
  `--resolution=APPROVE` records it, yet the printed JSON Schema may list
  `approved` under `required`. Without this note a reader sees a required key
  missing from the command and has to go find out why. The note names exactly
  which keys `--payload` still needs and how to decline.

  Args:
    json_schema: The stage's operator_action JSON Schema string or dict.

  Returns:
    A parenthesized one-line note explaining `--resolution=APPROVE` and
    remaining required `--payload` keys.
  """
  required = _schema_required_keys(json_schema)
  if required is None:
    remaining = "the keys the schema requires"
  else:
    others = [k for k in required if k != "approved"]
    remaining = ", ".join(others) if others else "nothing"
  needs = (
      f"--payload only needs: {remaining}."
      if remaining != "nothing"
      else "no --payload is needed."
  )
  return (
      f"(--resolution=APPROVE records approved=true for you; {needs}"
      ' To decline: --resolution=REJECT --comment="<reason>".)'
  )


class LightflowEngine:
  """Lightweight, pure-Python DAG Lightflow Compiler and In-Process Executor."""

  def __init__(
      self,
      lightflow: Optional[schema.Lightflow] = None,
      lightflow_path: Optional[str] = None,
      log_id: Optional[str] = None,
      allowed_import_prefixes: Optional[Sequence[str]] = None,
      *,
      workflow: Optional[schema.Lightflow] = None,
      workflow_path: Optional[str] = None,
  ):
    resolved_lightflow = lightflow if lightflow is not None else workflow
    if resolved_lightflow is None:
      raise ValueError("LightflowEngine requires a `lightflow` definition.")
    self.lightflow = resolved_lightflow
    self.lightflow_path = (
        lightflow_path if lightflow_path is not None else workflow_path
    )
    self.log_id = log_id
    if allowed_import_prefixes is not None:
      self.allowed_import_prefixes: Optional[tuple[str, ...]] = tuple(
          p.strip() for p in allowed_import_prefixes if p.strip()
      )
    else:
      env_prefixes = os.environ.get("LIGHTFLOW_ALLOWED_IMPORT_PREFIXES", "")
      if env_prefixes.strip():
        self.allowed_import_prefixes = tuple(
            p.strip() for p in env_prefixes.split(",") if p.strip()
        )
      else:
        self.allowed_import_prefixes = None

    self._action_imports: dict[str, str] = {
        action.id: action.python_import for action in self.lightflow.actions
    }
    self._stages_by_name: dict[str, schema.Stage] = {
        stage.name: stage for stage in self.lightflow.stages
    }
    # Top-level payload keys some expression in this lightflow reads as
    # `payload.<key>`; filled in by `compile()`. Overwrites of any other
    # key cannot change what an expression sees, so they are not warned
    # about.
    self._top_level_keys_read: set[str] = set()

  @property
  def top_level_keys_read(self) -> frozenset[str]:
    """Top-level keys read by an expression (`payload.<key>`), per `compile()`."""
    return frozenset(self._top_level_keys_read)

  @property
  def workflow(self) -> schema.Lightflow:
    return self.lightflow

  @workflow.setter
  def workflow(self, value: schema.Lightflow) -> None:
    self.lightflow = value

  @property
  def workflow_path(self) -> Optional[str]:
    return self.lightflow_path

  @workflow_path.setter
  def workflow_path(self, value: Optional[str]) -> None:
    self.lightflow_path = value

  @property
  def _strict_expressions(self) -> bool:
    """Whether the manifest opted into strict expression field access."""
    return bool(self.lightflow.strict_expressions)

  def _redact(self, text: str, payload_dict: dict[str, Any]) -> str:
    """Scrubs values held under `secret_keys` from `text`."""
    return redact_text(text, payload_dict, self.lightflow.secret_keys)

  def get_stage(self, stage_name: str) -> Optional[schema.Stage]:
    """Returns the stage object by name, or None if not found."""
    return self._stages_by_name.get(stage_name)

  def get_downstream_dependents(self, stage_name: str) -> list[str]:
    """Returns every stage that transitively depends on the given stage."""
    children: dict[str, list[str]] = {}
    for stage in self.lightflow.stages:
      for parent in stage.run_after:
        children.setdefault(parent, []).append(stage.name)

    dependents: set[str] = set()
    frontier = list(children.get(stage_name, []))
    while frontier:
      current = frontier.pop()
      if current in dependents:
        continue
      dependents.add(current)
      frontier.extend(children.get(current, []))

    dependents.discard(stage_name)
    return sorted(dependents)

  def resume_target(self) -> str:
    """Returns the runner target or CLI command prefix to resume the lightflow."""
    if self.lightflow.runner_target:
      return self.lightflow.runner_target
    argv0 = sys.argv[0] if sys.argv else ""
    normalized = argv0.replace("\\", "/")
    # Under ``python -m pkg.mod`` the interpreter rewrites argv[0] to the
    # module's file path, so argv[0] alone cannot distinguish ``-m`` from a
    # direct ``python path/to/runner.py``; ``__main__.__spec__`` can (it is
    # only populated for ``-m`` / package execution).
    main_spec = getattr(sys.modules.get("__main__"), "__spec__", None)
    main_name = getattr(main_spec, "name", "") or ""
    if main_name == "lightflow.__main__" or main_name == "lightflow":
      return "python3 -m lightflow"
    if main_name == "lightflow.runner":
      return "python3 -m lightflow.runner"
    if normalized.endswith(("lightflow/__main__.py", "lightflow/runner.py")):
      # Direct script invocation from a source checkout: echo the path the
      # user typed so the hint is copy-pasteable from their cwd.
      return f"python3 {_quote_cli_arg(argv0)}"
    if os.path.basename(normalized) == "lightflow":
      # Installed console script
      # (``pip install "git+https://github.com/google/lightflow.git"``).
      return "lightflow"
    return ""

  _resume_target = resume_target

  def resume_prefix(self) -> str:
    """Returns the CLI prefix that resumes this run.

    `python3 -m lightflow`, the script path, or the `lightflow` console
    script, depending on how this process was started.
    """
    return self._resume_target() or "lightflow"

  def build_resume_command(
      self, stage_name: str, operator_action: schema.OperatorAction
  ) -> str:
    """Renders the copy-pasteable command that resolves a paused gate."""
    if not self.log_id:
      return ""

    # No --lightflow: the run records its manifest on `start`, so the command
    # works from any cwd and survives a `resume --lightflow=<moved>`.
    args = [
        "resume",
        f"--log_id={_quote_cli_arg(self.log_id)}",
        f"--stage={_quote_cli_arg(stage_name)}",
        "--resolution=APPROVE",
    ]
    placeholder = _payload_placeholder(operator_action.json_schema)
    if placeholder:
      args.append(placeholder)

    return f"{self.resume_prefix()} " + " ".join(args)

  def compile(self) -> list[str]:
    """Sorts the workflow stages topologically and returns execution order."""
    action_ids: set[str] = set()
    for action in self.workflow.actions:
      if not action.id or not action.id.strip():
        raise EngineError("Action definition is missing a non-empty 'id'.")
      if action.id in action_ids:
        raise EngineError(
            f"Action ID '{action.id}' is defined more than once under"
            " workflow.actions."
        )
      action_ids.add(action.id)

    stage_names: set[str] = set()
    for stage in self.workflow.stages:
      if not stage.name or not stage.name.strip():
        raise EngineError("Stage definition is missing a non-empty 'name'.")
      if stage.name in stage_names:
        raise EngineError(
            f"Stage name '{stage.name}' is used by more than one stage. Stage"
            " names must be unique."
        )
      if not stage.name.isidentifier() or keyword.iskeyword(stage.name):
        reason = (
            "a Python keyword"
            if keyword.iskeyword(stage.name)
            else "not a valid identifier"
        )
        raise EngineError(
            f"Stage name '{stage.name}' is {reason}. Stage names are used for"
            " dotted access in expressions (payload.outputs.<name>.<key>), so"
            " use letters, digits, and underscores, starting with a letter or"
            " underscore."
        )
      stage_names.add(stage.name)

    ts = graphlib.TopologicalSorter()
    parsed_expressions: dict[
        str, list[tuple[str, Optional[ast.Expression]]]
    ] = {}
    for stage in self.workflow.stages:
      for dep in stage.run_after:
        if dep not in self._stages_by_name:
          raise EngineError(
              f"Stage '{stage.name}' depends on non-existent stage '{dep}'."
              + _did_you_mean(dep, self._stages_by_name)
          )
      if stage.HasField("python_action") and stage.python_action:
        action_id = stage.python_action.action_id
        if action_id not in self._action_imports:
          raise EngineError(
              f"Stage '{stage.name}' references unregistered action ID"
              f" '{action_id}'."
              + _did_you_mean(action_id, self._action_imports)
              + f" Defined actions: {list(self._action_imports.keys())}"
          )
      if (
          stage.HasField("polling_policy")
          and stage.polling_policy
          and stage.polling_policy.HasField("poll_tick_action")
          and stage.polling_policy.poll_tick_action
      ):
        action_id = stage.polling_policy.poll_tick_action.action_id
        if action_id not in self._action_imports:
          raise EngineError(
              f"Stage '{stage.name}' polling tick references unregistered"
              f" action ID '{action_id}'. Defined actions:"
              f" {list(self._action_imports.keys())}"
          )
      if stage.HasField("rollback_action") and stage.rollback_action:
        action_id = stage.rollback_action.action_id
        if action_id not in self._action_imports:
          raise EngineError(
              f"Stage '{stage.name}' rollback references unregistered action ID"
              f" '{action_id}'. Defined actions:"
              f" {list(self._action_imports.keys())}"
          )
      if (
          not stage.HasField("python_action")
          and not stage.HasField("operator_action")
          and not stage.HasField("polling_policy")
      ):
        raise EngineError(
            f"Stage '{stage.name}' has no defined action (specify"
            " python_action, operator_action, or polling_policy)."
        )
      if stage.HasField("polling_policy") and stage.polling_policy:
        if stage.HasField("retry_policy") and stage.retry_policy:
          raise EngineError(
              f"Stage '{stage.name}' combines polling_policy with"
              " retry_policy. Configure polling_policy.max_attempts instead."
          )
        if not stage.polling_policy.HasField(
            "poll_tick_action"
        ) and not stage.HasField("python_action"):
          raise EngineError(
              f"Stage '{stage.name}' defines a polling policy but has no"
              " execution actions (specify poll_tick_action or python_action)."
          )
        if stage.polling_policy.HasField("poll_tick_action") and stage.HasField(
            "python_action"
        ):
          raise EngineError(
              f"Stage '{stage.name}' defines both python_action and"
              " polling_policy.poll_tick_action. Specify only one."
          )
      if stage.HasField("retry_policy") and stage.retry_policy:
        policy = stage.retry_policy
        if policy.HasField("max_attempts") and policy.max_attempts is not None:
          if policy.max_attempts < 1 or policy.max_attempts > 20:
            raise EngineError(
                f"Stage '{stage.name}': retry_policy.max_attempts must be"
                f" between 1 and 20, got {policy.max_attempts}."
            )
        if (
            policy.HasField("initial_backoff_seconds")
            and policy.initial_backoff_seconds is not None
        ):
          if (
              policy.initial_backoff_seconds < 1
              or policy.initial_backoff_seconds > 3600
          ):
            raise EngineError(
                f"Stage '{stage.name}': retry_policy.initial_backoff_seconds"
                " must be between 1 and 3600, got"
                f" {policy.initial_backoff_seconds}."
            )
        if (
            policy.HasField("backoff_multiplier")
            and policy.backoff_multiplier is not None
        ):
          if (
              policy.backoff_multiplier < 1.0
              or policy.backoff_multiplier > 10.0
          ):
            raise EngineError(
                f"Stage '{stage.name}': retry_policy.backoff_multiplier"
                " must be between 1.0 and 10.0, got"
                f" {policy.backoff_multiplier}."
            )

      if stage.HasField("polling_policy") and stage.polling_policy:
        policy = stage.polling_policy
        if (
            policy.HasField("interval_seconds")
            and policy.interval_seconds is not None
        ):
          if policy.interval_seconds < 1 or policy.interval_seconds > 3600:
            raise EngineError(
                f"Stage '{stage.name}': polling_policy.interval_seconds must"
                f" be between 1 and 3600, got {policy.interval_seconds}."
            )
        if (
            policy.HasField("timeout_seconds")
            and policy.timeout_seconds is not None
        ):
          if policy.timeout_seconds < 0 or policy.timeout_seconds > 86400:
            raise EngineError(
                f"Stage '{stage.name}': polling_policy.timeout_seconds must"
                f" be between 0 and 86400, got {policy.timeout_seconds}."
            )
        if policy.HasField("max_attempts") and policy.max_attempts is not None:
          if policy.max_attempts < 0 or policy.max_attempts > 1000:
            raise EngineError(
                f"Stage '{stage.name}': polling_policy.max_attempts must"
                f" be between 0 and 1000, got {policy.max_attempts}."
            )
        if not policy.condition or not policy.condition.strip():
          raise EngineError(
              f"Stage '{stage.name}': polling_policy.condition must be a"
              " non-empty expression."
          )
        try:
          cond_tree = parse_expression(policy.condition)
        except (SyntaxError, ValueError) as e:
          raise EngineError(
              f"Stage '{stage.name}': polling_policy.condition is not a"
              f" parseable expression: {e}"
          ) from e
        parsed_expressions.setdefault(stage.name, []).append(
            ("polling_policy.condition", cond_tree)
        )
      if stage.run_if:
        if not stage.run_if.strip():
          raise EngineError(
              f"Stage '{stage.name}': run_if cannot be an empty or"
              " whitespace-only expression."
          )
        try:
          run_if_tree = parse_expression(stage.run_if)
        except (SyntaxError, ValueError) as e:
          raise EngineError(
              f"Stage '{stage.name}': run_if is not a parseable expression: {e}"
          ) from e
        parsed_expressions.setdefault(stage.name, []).append(
            ("run_if", run_if_tree)
        )
      if stage.HasField("operator_action") and stage.operator_action:
        if stage.HasField("python_action") and stage.python_action:
          raise EngineError(
              f"Stage '{stage.name}': cannot set both python_action and"
              " operator_action."
          )
        if stage.HasField("polling_policy") and stage.polling_policy:
          raise EngineError(
              f"Stage '{stage.name}': operator_action cannot be combined"
              " with polling_policy."
          )
        if stage.HasField("retry_policy") and stage.retry_policy:
          raise EngineError(
              f"Stage '{stage.name}' combines operator_action with"
              " retry_policy. Retry policies apply only to python_action"
              " stages."
          )
        expression = stage.operator_action.instructions
        if not expression or not expression.strip():
          raise EngineError(
              f"Stage '{stage.name}': operator_action.instructions must be a"
              " non-empty expression."
          )
        try:
          instr_tree = parse_expression(expression)
        except (SyntaxError, ValueError) as e:
          raise EngineError(
              f"Stage '{stage.name}': operator_action.instructions is not a"
              f" parseable expression: {e}. It is evaluated, not emitted"
              " verbatim, so a constant has to be quoted inside the string"
              ' -- "\'some text\'" rather than "some text".'
          ) from e
        parsed_expressions.setdefault(stage.name, []).append(
            ("operator_action.instructions", instr_tree)
        )

      if (
          stage.HasField("operator_action")
          and stage.operator_action
          and stage.operator_action.json_schema
      ):
        try:
          schema_dict = json.loads(stage.operator_action.json_schema)
          check_json_schema(schema_dict)
        except Exception as e:
          raise EngineError(
              f"Stage '{stage.name}': operator_action has an invalid"
              f" json_schema: {e}"
          ) from e
      if (
          stage.HasField("timeout_seconds")
          and stage.timeout_seconds is not None
      ):
        if stage.timeout_seconds < 0 or stage.timeout_seconds > 86400:
          raise EngineError(
              f"Stage '{stage.name}': timeout_seconds must be between 0 and"
              f" 86400, got {stage.timeout_seconds}."
          )

      ts.add(stage.name, *stage.run_after)

    try:
      execution_order = list(ts.static_order())
    except Exception as e:
      raise EngineError(f"Workflow contains cyclical dependencies: {e}") from e

    self._validate_payload_output_dependencies(parsed_expressions)
    self._top_level_keys_read = set()
    for trees in parsed_expressions.values():
      for _, tree in trees:
        self._top_level_keys_read.update(extract_top_level_payload_refs(tree))
    return execution_order

  def _validate_payload_output_dependencies(
      self,
      parsed_expressions: dict[str, list[tuple[str, Optional[ast.Expression]]]],
  ) -> None:
    """Validates `payload.outputs.<stage>` references against `run_after`."""
    direct_parents = {
        stage.name: set(stage.run_after) for stage in self.workflow.stages
    }
    ancestors_by_stage: dict[str, set[str]] = {}
    for stage in self.workflow.stages:
      visited: set[str] = set()
      stack = list(direct_parents.get(stage.name, ()))
      while stack:
        parent = stack.pop()
        if parent not in visited:
          visited.add(parent)
          stack.extend(direct_parents.get(parent, ()))
      ancestors_by_stage[stage.name] = visited

    for stage in self.workflow.stages:
      ancestors = ancestors_by_stage.get(stage.name, set())
      for field_label, tree in parsed_expressions.get(stage.name, ()):
        for ref_stage in extract_payload_output_stage_refs(tree):
          if ref_stage not in self._stages_by_name:
            raise EngineError(
                f"Stage '{stage.name}' ({field_label}) references"
                f" 'payload.outputs.{ref_stage}', but '{ref_stage}' is not a"
                " stage in this workflow."
            )
          if (
              ref_stage == stage.name
              and field_label != "polling_policy.condition"
          ):
            raise EngineError(
                f"Stage '{stage.name}' ({field_label}) cannot reference its"
                f" own output 'payload.outputs.{ref_stage}' before executing."
            )
          if ref_stage != stage.name and ref_stage not in ancestors:
            raise EngineError(
                f"Stage '{stage.name}' ({field_label}) references"
                f" 'payload.outputs.{ref_stage}', but '{ref_stage}' is not an"
                " upstream dependency in `run_after`."
            )

  def _resolve_action_callable(self, action_id: str) -> Callable[..., Any]:
    """Resolves and validates a registered Python action callable."""
    python_import = self._action_imports.get(action_id)
    if not python_import:
      raise EngineError(
          f"Action ID '{action_id}' is not defined under workflow.actions."
      )

    if "." not in python_import:
      raise EngineError(
          f"Action ID '{action_id}' has an invalid fully-qualified function"
          f" import path: '{python_import}'. Must be of the form"
          " 'package.module.function_name'."
      )
    module_path, func_name = python_import.rsplit(".", 1)
    parts = module_path.split(".")
    if not func_name or not all(parts):
      raise EngineError(
          f"Action ID '{action_id}' has an invalid import path:"
          f" '{python_import}'."
      )

    if func_name.startswith("_"):
      raise EngineError(
          f"Dynamic import of private callable '{func_name}' is blocked for"
          " security reasons."
      )

    if any(part.startswith("_") and part != "__main__" for part in parts):
      raise EngineError(
          f"Dynamic import of private module '{module_path}' is blocked for"
          " security reasons."
      )

    if self.allowed_import_prefixes is not None:

      def _matches_prefix(mod: str, raw_prefix: str) -> bool:
        clean_p = raw_prefix.strip().rstrip(".")
        if not clean_p:
          return False
        return mod == clean_p or mod.startswith(clean_p + ".")

      if not any(
          _matches_prefix(module_path, prefix)
          for prefix in self.allowed_import_prefixes
      ):
        raise EngineError(
            f"Dynamic import of module '{module_path}' is blocked for security"
            f" reasons. Allowed prefixes: {self.allowed_import_prefixes}."
        )

    if self.allowed_import_prefixes is None:
      candidate_dirs = [os.getcwd()]
      local_roots: list[str] = []
      if self.workflow_path:
        resolved_wf = schema.resolve_lightflow_path(self.workflow_path)
        wf_dir = os.path.dirname(os.path.abspath(resolved_wf))
        pkg_root = wf_dir
        while os.path.isfile(os.path.join(pkg_root, "__init__.py")):
          parent = os.path.dirname(pkg_root)
          if parent == pkg_root:
            break
          pkg_root = parent
        if pkg_root != wf_dir:
          candidate_dirs.insert(0, pkg_root)
          local_roots.append(pkg_root)
        candidate_dirs.insert(0, wf_dir)
        local_roots.insert(0, wf_dir)
      for candidate_dir in reversed(candidate_dirs):
        if candidate_dir and os.path.isdir(candidate_dir):
          if candidate_dir in sys.path:
            sys.path.remove(candidate_dir)
          sys.path.insert(0, candidate_dir)

      for root in local_roots:
        local_py = os.path.join(root, *parts) + ".py"
        local_pkg = os.path.join(root, *parts, "__init__.py")
        top_pkg = parts[0]
        top_pkg_init = os.path.join(root, top_pkg, "__init__.py")
        if (
            os.path.isfile(local_py)
            or os.path.isfile(local_pkg)
            or os.path.isfile(top_pkg_init)
        ):
          target_mod = sys.modules.get(module_path)
          existing = target_mod or sys.modules.get(top_pkg)
          existing_file = (
              getattr(existing, "__file__", None) if existing else None
          )
          target_file = (
              getattr(target_mod, "__file__", None)
              if target_mod is not None
              else existing_file
          )
          if existing is not None:
            try:
              in_same_root = bool(existing_file) and (
                  os.path.commonpath([
                      os.path.normcase(os.path.realpath(root)),
                      os.path.normcase(os.path.realpath(existing_file)),
                  ])
                  == os.path.normcase(os.path.realpath(root))
              )
            except ValueError:
              in_same_root = False
            stale_on_disk = False
            if in_same_root and target_file and os.path.isfile(target_file):
              real_target = os.path.realpath(target_file)
              current_fp = _file_fingerprint(real_target)
              prev_fp = _LOADED_MODULE_HASHES.get(real_target)
              if (
                  prev_fp is not None
                  and current_fp is not None
                  and current_fp != prev_fp
              ):
                stale_on_disk = True
            if not in_same_root or stale_on_disk:
              for mod_name in list(sys.modules):
                if mod_name == top_pkg or mod_name.startswith(top_pkg + "."):
                  evicted = sys.modules.pop(mod_name, None)
                  evicted_file = getattr(evicted, "__file__", None)
                  if stale_on_disk and evicted_file:
                    try:
                      pyc_path = importlib.util.cache_from_source(evicted_file)
                      if os.path.isfile(pyc_path):
                        os.remove(pyc_path)
                    except (OSError, ValueError, NotImplementedError):
                      # Best-effort bytecode cache invalidation on read-only FS.
                      pass
              importlib.invalidate_caches()
          break

    try:
      module = importlib.import_module(module_path)
    except ModuleNotFoundError as e:
      missing = e.name or ""
      # The action module itself (or one of its parent packages) is missing
      # vs. the action module imports something that is not installed. The
      # two have different fixes, so name the right one.
      is_action_module = (
          not missing
          or missing == module_path
          or module_path.startswith(missing + ".")
      )
      if not is_action_module:
        raise EngineError(
            f"Action module '{module_path}' (action '{action_id}') imports"
            f" '{missing}', which is not installed in this Python environment"
            f" ({sys.executable}). Install it, or guard the import. Import"
            " errors are not retried."
        ) from e
      looked = (
          os.path.dirname(os.path.abspath(self.lightflow_path))
          if self.lightflow_path
          else "the current directory"
      )
      raise EngineError(
          f"Cannot import '{python_import}' for action '{action_id}':"
          f" {e}. Lightflow resolves python_import relative to the manifest's"
          f" directory ({looked}) and sys.path; if the manifest moved, move"
          " its actions module with it (or set PYTHONPATH)."
      ) from e
    except Exception as e:  # pylint: disable=broad-exception-caught
      # SyntaxError, ImportError ("cannot import name"), or any exception the
      # module raises at import time. None of these can succeed on retry.
      raise EngineError(
          f"Cannot import '{python_import}' for action '{action_id}':"
          f" {type(e).__name__}: {e}. Import errors are not retried."
      ) from e
    mod_file = getattr(module, "__file__", None)
    if mod_file and os.path.isfile(mod_file):
      real_mod_file = os.path.realpath(mod_file)
      fp = _file_fingerprint(real_mod_file)
      if fp is not None:
        _LOADED_MODULE_HASHES[real_mod_file] = fp
    func = getattr(module, func_name, None)
    if func is None:
      raise EngineError(
          f"Module '{module_path}' has no attribute '{func_name}' (action"
          f" '{action_id}')."
      )
    if not callable(func):
      raise EngineError(
          f"'{python_import}' for action '{action_id}' is not a callable."
      )
    return func

  def _execute_action(
      self,
      stage_name: str,
      action: schema.PythonAction,
      payload_dict: dict[str, Any],
  ) -> tuple[dict[str, Any], str]:
    """Runs a PythonAction helper in-process and returns (new_payload, message)."""
    del stage_name
    action_id = action.action_id
    func = self._resolve_action_callable(action_id)
    static_kwargs = static_kwargs_dict(action)

    res = func(copy.deepcopy(payload_dict), **static_kwargs)
    payload_res = res[0] if isinstance(res, tuple) and len(res) == 2 else res
    message_res = (
        str(res[1]).strip()
        if isinstance(res, tuple) and len(res) == 2 and res[1] is not None
        else ""
    )

    if not isinstance(payload_res, dict):
      raise EngineError(
          f"Action '{action_id}' execution returned an invalid payload type."
          f" Expected dict, got {type(payload_res).__name__}."
      )

    return payload_res, message_res

  def simulate_dry_run_action(
      self,
      stage_name: str,
      action: schema.PythonAction,
      payload_dict: dict[str, Any],
  ) -> Optional[tuple[dict[str, Any], str]]:
    """Simulates a PythonAction if its callable explicitly accepts `dry_run`."""
    del stage_name
    try:
      func = self._resolve_action_callable(action.action_id)
      sig = inspect.signature(func)
    except Exception:  # pylint: disable=broad-exception-caught
      return None

    has_explicit_dry_run = "dry_run" in sig.parameters or bool(
        getattr(func, "supports_dry_run", False)
    )
    if not has_explicit_dry_run:
      return None

    try:
      static_kwargs = static_kwargs_dict(action)
      static_kwargs["dry_run"] = True
      sim_payload = copy.deepcopy(payload_dict)
      sim_payload["dry_run"] = True
      res = func(sim_payload, **static_kwargs)
      payload_res = res[0] if isinstance(res, tuple) and len(res) == 2 else res
      message_res = (
          str(res[1])
          if isinstance(res, tuple) and len(res) == 2
          else f"[DRY RUN] Action '{action.action_id}' simulated."
      )
      if isinstance(payload_res, dict):
        if "dry_run" not in payload_dict and "dry_run" in payload_res:
          payload_res = {k: v for k, v in payload_res.items() if k != "dry_run"}
        return payload_res, message_res
    except Exception:  # pylint: disable=broad-exception-caught
      return None
    return None

  def _execute_action_with_timeout(
      self,
      stage_name: str,
      action: schema.PythonAction,
      payload_dict: dict[str, Any],
      timeout: Optional[int] = None,
  ) -> tuple[dict[str, Any], str]:
    """Runs `_execute_action` optionally bounded by a stage timeout."""
    if not timeout or timeout <= 0:
      return self._execute_action(stage_name, action, payload_dict)

    result_queue: queue.Queue[tuple[bool, Any]] = queue.Queue()

    def run_worker():
      try:
        res = self._execute_action(stage_name, action, payload_dict)
        result_queue.put((True, res))
      except Exception as exc:  # pylint: disable=broad-exception-caught
        result_queue.put((False, exc))

    # Note: Python daemon threads cannot be forcibly terminated. If an action
    # times out and is retried, the timed-out thread may continue executing in
    # the background until the process exits. Actions configured with timeouts
    # and retry policies should be designed to be idempotent.
    worker = threading.Thread(
        target=run_worker,
        daemon=True,
        name=f"lightflow-stage-{stage_name}",
    )
    worker.start()
    worker.join(timeout=timeout)
    if worker.is_alive():
      err = StageTimeoutError(
          f"Stage '{stage_name}' exceeded timeout_seconds={timeout}. ⚠️ Its"
          " worker thread keeps running until the process exits; a retry or"
          " rollback is only attempted once it has finished."
      )
      err.worker = worker
      raise err
    try:
      success, res = result_queue.get_nowait()
    except queue.Empty as e:
      raise EngineError(
          f"Stage '{stage_name}' worker finished without returning a result."
      ) from e
    if not success:
      raise res
    return res

  def run_rollback_if_defined(
      self,
      stage_name: str,
      stage: schema.Stage,
      passport: schema.Passport,
      payload_dict: dict[str, Any],
      failed_stamp: schema.Stamp,
      live_workers: Collection[Any] = (),
  ) -> None:
    """Runs rollback_action if defined on the stage configuration."""
    if not stage.HasField("rollback_action") or not stage.rollback_action:
      return

    action_id = stage.rollback_action.action_id
    # A timed-out attempt may still be finishing; give it the same budget it
    # had before rolling back, rather than racing it or waiting forever.
    grace = (
        stage.timeout_seconds if stage.HasField("timeout_seconds") else None
    ) or 0
    deadline = time.monotonic() + grace
    for w in live_workers:
      w.join(timeout=max(0.0, deadline - time.monotonic()))
    if any(w.is_alive() for w in live_workers):
      note = (
          f"Rollback '{action_id}' skipped: a timed-out attempt is still"
          " running and rolling back now could race it. Verify by hand."
      )
      failed_stamp.message += f"\n{note}"
      print(f"  ⚠️ {note}")
      send_notification(
          f"Rollback Skipped: {stage_name}", f"⚠️ {stage_name}: {note}"
      )
      return
    print(
        f"  Stage '{stage_name}' failed. Executing rollback action"
        f" '{action_id}'..."
    )
    rollback_timeout = (
        stage.timeout_seconds if stage.HasField("timeout_seconds") else None
    )
    try:
      rollback_payload, rollback_msg = self._execute_action_with_timeout(
          stage_name,
          stage.rollback_action,
          payload_dict,
          timeout=rollback_timeout,
      )
      merge_stage_payload(payload_dict, stage_name, rollback_payload)
      set_payload(passport.payload, payload_dict)
      suffix = f": {rollback_msg}" if rollback_msg else "."
      failed_stamp.message += f"\nRollback executed successfully{suffix}"
      print(
          f"  Rollback executed successfully{self._redact(suffix, payload_dict)}"
      )
      send_notification(
          f"Rollback Completed: {stage_name}",
          f"🔄 {stage_name}: rolled back",
      )
    except Exception as rollback_err:  # pylint: disable=broad-exception-caught
      err_trace = traceback.format_exc()
      failed_stamp.message += (
          f"\nRollback action '{action_id}' failed: {rollback_err}\n{err_trace}"
      )
      print(
          f"  Rollback failed: {self._redact(str(rollback_err), payload_dict)}"
      )
      send_notification(
          f"Rollback Failed: {stage_name}",
          f"⚠️ {stage_name}: rollback failed",
      )

  def evaluate_trigger_rule(
      self, stage: schema.Stage, passport: schema.Passport
  ) -> tuple[bool, schema.StampStatus, str]:
    """Evaluates if the stage's trigger rule is satisfied by its dependencies."""
    if not stage.run_after:
      return True, schema.StampStatus.PENDING, ""

    latest_status = {}
    for stamp in passport.stamps:
      latest_status[stamp.stage_name] = stamp.status

    completed_count = 0
    failed_count = 0
    paused_count = 0
    skipped_count = 0
    unstarted_count = 0
    last_dep_paused = ""

    for dep in stage.run_after:
      status = latest_status.get(dep, None)
      if status == schema.StampStatus.COMPLETED:
        completed_count += 1
      elif status == schema.StampStatus.SKIPPED:
        skipped_count += 1
      elif status == schema.StampStatus.FAILED:
        failed_count += 1
      elif status == schema.StampStatus.PAUSED:
        paused_count += 1
        last_dep_paused = dep
      else:
        unstarted_count += 1

    if paused_count > 0:
      return (
          False,
          schema.StampStatus.PAUSED,
          f"Suspended: Dependency '{last_dep_paused}' is paused.",
      )

    total_deps = len(stage.run_after)
    rule = stage.trigger_rule

    if rule == schema.TriggerRule.ALL_SUCCESS:
      if completed_count == total_deps:
        return True, schema.StampStatus.PENDING, ""
      else:
        if unstarted_count == 0 and paused_count == 0:
          if failed_count > 0:
            return (
                False,
                schema.StampStatus.FAILED,
                UPSTREAM_FAILED_MESSAGE,
            )
          else:
            return (
                False,
                schema.StampStatus.SKIPPED,
                "Skipped: Upstream dependency was skipped.",
            )
        else:
          return (
              False,
              schema.StampStatus.STATUS_UNSPECIFIED,
              "Waiting for parent stages to finish.",
          )

    elif rule == schema.TriggerRule.ALL_DONE:
      if completed_count + skipped_count + failed_count == total_deps:
        return True, schema.StampStatus.PENDING, ""
      else:
        return (
            False,
            schema.StampStatus.STATUS_UNSPECIFIED,
            "Waiting for parent stages to finish.",
        )

    elif rule == schema.TriggerRule.NONE_FAILED:
      # The join after complementary `run_if` branches: skipped upstreams are
      # expected, failures still cascade.
      if failed_count > 0 and unstarted_count == 0:
        return False, schema.StampStatus.FAILED, UPSTREAM_FAILED_MESSAGE
      if completed_count + skipped_count == total_deps:
        return True, schema.StampStatus.PENDING, ""
      return (
          False,
          schema.StampStatus.STATUS_UNSPECIFIED,
          "Waiting for parent stages to finish.",
      )

    return (
        False,
        schema.StampStatus.FAILED,
        f"Failed: Unknown trigger rule '{rule}'.",
    )

  _evaluate_trigger_rule = evaluate_trigger_rule

  def execute_stage(
      self,
      stage_name: str,
      passport: schema.Passport,
      save_callback: Optional[Callable[[schema.Passport], None]] = None,
  ) -> schema.Passport:
    """Executes a single workflow stage, updating the Passport state."""
    stage = self._stages_by_name.get(stage_name)
    if not stage:
      raise EngineError(f"Stage '{stage_name}' not found in configuration.")

    # 0. Evaluate trigger rule against parent statuses
    should_run, target_status, reason = self.evaluate_trigger_rule(
        stage, passport
    )
    if not should_run:
      stamp = add_stamp(passport, stage_name, target_status, reason)
      if reason == UPSTREAM_FAILED_MESSAGE:
        stamp.reason = schema.Stamp.UPSTREAM_FAILED

      if target_status == schema.StampStatus.PAUSED:
        raise OperatorActionSuspended(reason)

      # Only terminal outcomes are lifecycle transitions worth announcing;
      # STATUS_UNSPECIFIED means the stage is still waiting on its parents.
      icon = _TRIGGER_NOTIFICATION_ICONS.get(target_status)
      if icon:
        if reason == UPSTREAM_FAILED_MESSAGE:
          # Collateral of an upstream failure, not a failure of its own: one
          # ❌ for the root cause is enough, so this title is not a milestone.
          print(f"  ⛔ [{stage_name}] blocked: upstream failed", flush=True)
          send_notification(
              f"Stage Blocked: {stage_name}", f"⛔ {stage_name}: blocked"
          )
        else:
          status_name = schema.StampStatus.Name(target_status).title()
          # Stamp messages carry a "Skipped: " prefix; the line already
          # says so.
          why = reason.removeprefix("Skipped: ")
          print(f"  ⏭️ [{stage_name}] skipped: {why}", flush=True)
          send_notification(
              f"Stage {status_name}: {stage_name}",
              f"{icon} {stage_name}: {status_name.lower()}",
          )
      return passport

    payload_dict = payload_to_dict(passport.payload)

    # 1. Evaluate skip condition if set
    if stage.run_if:
      try:
        should_run = evaluate_cel(
            stage.run_if, payload_dict, strict=self._strict_expressions
        )
      except Exception as e:  # pylint: disable=broad-exception-caught
        failed_stamp = add_stamp(passport, stage_name, _FAILED)
        failed_stamp.message = (
            f"Condition evaluation failed: {e}\n{traceback.format_exc()}"
        )
        _announce_failure(
            stage_name, self._redact(failed_stamp.message, payload_dict)
        )
        return passport

      if not should_run:
        add_stamp(
            passport,
            stage_name,
            _SKIPPED,
            f"Skipped via condition: {stage.run_if}",
        )
        print(
            f"  ⏭️ [{stage_name}] skipped: run_if is false ({stage.run_if})",
            flush=True,
        )
        send_notification(
            f"Stage Skipped: {stage_name}",
            f"⏭️ {stage_name}: skipped",
        )
        return passport

    # 2. Append PENDING stamp to register started state
    pending_stamp = add_stamp(passport, stage_name, _PENDING)
    if save_callback:
      save_callback(passport)

    send_notification(
        f"Stage Entered: {stage_name}",
        f"🎬 {stage_name}: entered",
    )

    # 3. Handle Polling execution loop if polling_policy is set
    if stage.HasField("polling_policy") and stage.polling_policy:
      policy = stage.polling_policy
      interval = (
          policy.interval_seconds
          if policy.HasField("interval_seconds")
          and policy.interval_seconds is not None
          else 30
      )
      timeout = (
          policy.timeout_seconds
          if policy.HasField("timeout_seconds")
          and policy.timeout_seconds is not None
          else 3600
      )
      max_attempts = policy.max_attempts or 0
      tick_timeout = (
          stage.timeout_seconds if stage.HasField("timeout_seconds") else None
      )

      if (
          policy.HasField("poll_tick_action")
          and policy.poll_tick_action is not None
      ):
        action = policy.poll_tick_action
      elif stage.HasField("python_action") and stage.python_action is not None:
        action = stage.python_action
      else:
        err_msg = (
            f"Stage '{stage_name}' has a polling policy but defines no"
            " execution actions."
        )
        failed_stamp = add_stamp(passport, stage_name, _FAILED, err_msg)
        raise EngineError(err_msg)

      start_time = time.monotonic()
      attempts = 0

      while True:
        attempts += 1
        try:
          new_payload, message = self._execute_action_with_timeout(
              stage_name, action, payload_dict, timeout=tick_timeout
          )
          _print_merge_collisions(
              stage_name,
              merge_stage_payload(payload_dict, stage_name, new_payload),
              self._top_level_keys_read,
          )
          if message:
            pending_stamp.message = message
          set_payload(passport.payload, payload_dict)
          if save_callback:
            save_callback(passport)
        except Exception as e:  # pylint: disable=broad-exception-caught
          failed_stamp = add_stamp(passport, stage_name, _FAILED)
          failed_stamp.message = (
              f"Polling tick failed: {e}\n{traceback.format_exc()}"
          )
          _announce_failure(
              stage_name, self._redact(failed_stamp.message, payload_dict)
          )
          tick_worker = getattr(e, "worker", None)
          self.run_rollback_if_defined(
              stage_name,
              stage,
              passport,
              payload_dict,
              failed_stamp,
              live_workers=[tick_worker] if tick_worker else [],
          )
          return passport

        try:
          condition_passed = evaluate_cel(
              policy.condition, payload_dict, strict=self._strict_expressions
          )
        except Exception as e:  # pylint: disable=broad-exception-caught
          failed_stamp = add_stamp(passport, stage_name, _FAILED)
          failed_stamp.message = (
              f"Condition evaluation failed: {e}\n{traceback.format_exc()}"
          )
          _announce_failure(
              stage_name, self._redact(failed_stamp.message, payload_dict)
          )
          self.run_rollback_if_defined(
              stage_name, stage, passport, payload_dict, failed_stamp
          )
          return passport

        if condition_passed:
          set_payload(passport.payload, payload_dict)
          completed_stamp = add_stamp(passport, stage_name, _COMPLETED)
          completed_stamp.message = (
              f"Polling condition met after {attempts} attempts: {message}"
              if message
              else f"Polling condition met after {attempts} attempts."
          )
          if message:
            print(f"  ℹ️ [{stage_name}] {self._redact(message, payload_dict)}")
          send_notification(
              f"Stage Completed: {stage_name}",
              f"✅ {stage_name}: completed",
          )
          return passport

        # Echo tick progress before the threshold check so the final tick's
        # message is still visible on the console when polling yields.
        if message:
          print(
              f"  ⏳ [{stage_name}] {self._redact(message, payload_dict)}",
              flush=True,
          )

        elapsed = time.monotonic() - start_time
        if (timeout > 0 and elapsed >= timeout) or (
            max_attempts > 0 and attempts >= max_attempts
        ):
          progress_note = (
              f" (last progress: {pending_stamp.message})"
              if pending_stamp.message
              else ""
          )
          if timeout > 0 and elapsed >= timeout:
            err_msg = (
                f"Stage '{stage_name}' polling timed out after {elapsed:.1f}s"
                f" waiting for external condition{progress_note}. Yielding"
                " control back to operator/agent. State is preserved."
            )
          else:
            err_msg = (
                f"Stage '{stage_name}' polling exceeded max attempts"
                f" ({max_attempts}){progress_note}. Yielding control back to"
                " operator/agent. State is preserved."
            )

          add_stamp(
              passport,
              stage_name,
              _FAILED,
              err_msg,
              reason=schema.Stamp.POLL_YIELD,
          )

          print(
              f"  ⏳ [{stage_name}] {self._redact(err_msg, payload_dict)}",
              flush=True,
          )
          send_notification(
              f"Stage Waiting: {stage_name}",
              f"⏳ {stage_name}: waiting for an external condition",
          )
          # A yield is a pause, not a failure: the external job is still
          # running and `resume` polls it again, so no rollback here.
          return passport

        time.sleep(interval)

    # 4. Handle Standard Python Action Execution
    elif stage.HasField("python_action") and stage.python_action:
      python_action = stage.python_action
      max_attempts, initial_backoff, backoff_multiplier = (
          effective_retry_policy(
              stage.retry_policy
              if stage.HasField("retry_policy") and stage.retry_policy
              else None
          )
      )

      timeout = (
          stage.timeout_seconds if stage.HasField("timeout_seconds") else None
      )
      current_backoff = float(initial_backoff)
      attempts = 0
      live_workers: list[Any] = []

      while True:
        attempts += 1
        try:
          new_payload, message = self._execute_action_with_timeout(
              stage_name, python_action, payload_dict, timeout=timeout
          )

          _print_merge_collisions(
              stage_name,
              merge_stage_payload(payload_dict, stage_name, new_payload),
              self._top_level_keys_read,
          )
          set_payload(passport.payload, payload_dict)

          completed_stamp = add_stamp(passport, stage_name, _COMPLETED)
          if attempts > 1:
            completed_stamp.message = (
                f"{message} (completed after {attempts} attempts)"
                if message
                else f"Completed after {attempts} attempts."
            )
          else:
            completed_stamp.message = message
          if message:
            print(f"  ℹ️ [{stage_name}] {self._redact(message, payload_dict)}")
          send_notification(
              f"Stage Completed: {stage_name}",
              f"✅ {stage_name}: completed",
          )
          return passport
        except EngineError as e:
          if isinstance(e, StageTimeoutError) and e.worker is not None:
            live_workers.append(e.worker)
          if attempts < max_attempts and isinstance(e, StageTimeoutError):
            sleep_time = min(current_backoff, 300.0)
            jitter = random.uniform(0.9, 1.1)
            actual_sleep = sleep_time * jitter
            # Spend the backoff letting the timed-out attempt finish; a new
            # attempt never overlaps one that is still running.
            deadline = time.monotonic() + actual_sleep
            for w in live_workers:
              w.join(timeout=max(0.0, deadline - time.monotonic()))
            if not any(w.is_alive() for w in live_workers):
              print(
                  f"  Attempt {attempts} timed out for stage '{stage_name}':"
                  f" {self._redact(str(e), payload_dict)}. Retrying (capped"
                  f" backoff: {sleep_time:.1f}s, backoff multiplier"
                  f" {backoff_multiplier})..."
              )
              time.sleep(max(0.0, deadline - time.monotonic()))
              current_backoff = min(current_backoff * backoff_multiplier, 300.0)
              continue

          failed_stamp = add_stamp(passport, stage_name, _FAILED)
          failed_stamp.message = (
              str(e)
              if isinstance(e, StageTimeoutError)
              else f"Engine error: {e}"
          )
          if attempts < max_attempts and isinstance(e, StageTimeoutError):
            failed_stamp.message += (
                " Not retried: that attempt was still running when the"
                " backoff ended, and a second one would overlap it."
            )

          _announce_failure(
              stage_name, self._redact(failed_stamp.message, payload_dict)
          )
          self.run_rollback_if_defined(
              stage_name,
              stage,
              passport,
              payload_dict,
              failed_stamp,
              live_workers=live_workers,
          )
          raise
        except Exception as e:  # pylint: disable=broad-exception-caught
          if attempts >= max_attempts:
            failed_stamp = add_stamp(passport, stage_name, _FAILED)
            failed_stamp.message = (
                f"Failed after {attempts}"
                f" attempt{'s' if attempts != 1 else ''}. Last error:"
                f" {e}\n{traceback.format_exc()}"
            )

            _announce_failure(
                stage_name, self._redact(failed_stamp.message, payload_dict)
            )
            self.run_rollback_if_defined(
                stage_name, stage, passport, payload_dict, failed_stamp
            )
            return passport

          sleep_time = min(current_backoff, 300.0)
          jitter = random.uniform(0.9, 1.1)
          actual_sleep = sleep_time * jitter

          print(
              f"  Attempt {attempts} failed for stage '{stage_name}':"
              f" {self._redact(str(e), payload_dict)}. Retrying in"
              f" {actual_sleep:.1f}s (capped backoff: {sleep_time:.1f}s,"
              f" backoff multiplier {backoff_multiplier})..."
          )
          time.sleep(actual_sleep)
          current_backoff = min(current_backoff * backoff_multiplier, 300.0)

    # 5. Handle Operator Action Suspended State
    elif stage.HasField("operator_action") and stage.operator_action:
      operator_action = stage.operator_action
      try:
        instructions = str(
            evaluate_cel(
                operator_action.instructions,
                payload_dict,
                strict=self._strict_expressions,
            )
        )
        if not instructions.strip():
          raise ValueError(
              "Operator action instructions evaluated to an empty or"
              " whitespace-only string."
          )
      except Exception as e:  # pylint: disable=broad-exception-caught
        failed_stamp = add_stamp(passport, stage_name, _FAILED)
        failed_stamp.message = (
            "Operator action instructions evaluation failed:"
            f" {e}\n{traceback.format_exc()}"
        )

        _announce_failure(
            stage_name, self._redact(failed_stamp.message, payload_dict)
        )
        return passport

      resume_command = self.build_resume_command(stage_name, operator_action)

      paused_stamp = add_stamp(passport, stage_name, _PAUSED)
      summary = summarise(instructions)
      paused_stamp.message = f"Suspended waiting for operator action: {summary}"
      paused_stamp.instructions = instructions
      paused_stamp.resume_command = resume_command

      send_notification(
          f"Operator Action Suspended: {stage_name}",
          f"⏸️ {stage_name}: suspended",
      )

      raise OperatorActionSuspended(
          self.gate_briefing(
              stage_name,
              self._redact(instructions, payload_dict),
              resume_command,
          )
          + GATE_FOOTER
      )

    else:
      err_msg = f"Stage '{stage_name}' has no defined action rule."
      failed_stamp = add_stamp(passport, stage_name, _FAILED, err_msg)

      _announce_failure(
          stage_name, self._redact(failed_stamp.message, payload_dict)
      )
      raise EngineError(err_msg)

  def gate_briefing(
      self, stage_name: str, instructions: str, resume_command: str
  ) -> str:
    """Renders what an operator needs to resolve a paused gate."""
    stage = self.get_stage(stage_name)
    action = stage.operator_action if stage else None
    json_schema = action.json_schema if action else ""
    briefing = (
        f"Lightflow execution suspended at operator action '{stage_name}'."
        f"\n\nInstructions: {instructions}"
    )
    if json_schema:
      briefing += f"\n\nPayload Schema:\n{json_schema}"
    if resume_command:
      briefing += f"\n\nResume with:\n{resume_command}"
      briefing += f"\n  {resume_note(json_schema)}"
    return briefing


GATE_FOOTER = (
    "\n\nOperator action: follow the Instructions above, then resume with a"
    " payload matching the Payload Schema."
)
