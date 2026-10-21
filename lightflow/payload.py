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

"""Payload, redaction, provenance, and Passport serialization helpers."""

from __future__ import annotations

# pylint: disable=g-long-ternary

import copy
import json
import re
import shlex
from typing import Any, Collection, Optional

# pylint: disable=g-import-not-at-top,g-bad-import-order
try:
  from . import cel_lite
  from . import schema
except ImportError:
  from lightflow import cel_lite  # pyrefly: ignore[missing-import]
  from lightflow import schema  # pyrefly: ignore[missing-import]
# pylint: enable=g-import-not-at-top,g-bad-import-order


def _quote_cli_arg(value: str) -> str:
  """Shell-quotes a CLI flag value while preserving dry-run placeholders."""
  if value == "<log_id>":
    return value
  return shlex.quote(value)


def get_timestamp() -> schema.TimestampValue:
  """Returns the current UTC timestamp as a TimestampValue."""
  ts = schema.TimestampValue()
  ts.GetCurrentTime()
  return ts


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


def normalize_struct_numbers(value: Any) -> Any:
  """Normalizes struct numbers: converts whole floats to int."""
  if isinstance(value, float) and value.is_integer():
    return int(value)
  if isinstance(value, dict):
    return {k: normalize_struct_numbers(v) for k, v in value.items()}
  if isinstance(value, list):
    return [normalize_struct_numbers(item) for item in value]
  return value


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
