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

"""Pure-Python schema definitions and YAML/JSON/Textproto loaders for Lightflow OSS.

Provides zero-protoc dataclass models with Protobuf compatibility shims so
workflows can be authored in YAML (.yaml/.yml), JSON (.json), or
Textproto (.textproto) across local CLI and Python environments.
"""

from __future__ import annotations

# pylint: disable=g-import-not-at-top,g-bad-import-order,g-long-ternary,invalid-name,missing-function-docstring,protected-access

import copy
import dataclasses

dataclass = dataclasses.dataclass
field = dataclasses.field
import datetime
import difflib
import enum
import json
import os
import re
from typing import Any, Optional

try:
  import yaml
except ImportError:
  yaml = None


def _opt_int(data: dict[str, Any], key: str, context: str) -> Optional[int]:
  """Reads an optional whole-number field, rejecting fractional values.

  `int(0.2)` would silently become 0 and then fail a later range check with a
  misleading "got 0". Report the raw value instead so the author sees what
  they actually wrote.

  Args:
    data: The policy mapping from the manifest.
    key: Field name to read.
    context: Policy name for the error message.

  Returns:
    The integer value, or None when the key is absent or null.

  Raises:
    ValueError: The value is a bool, fractional, or otherwise not a whole
      number.
  """
  if key not in data or data[key] is None:
    return None
  raw = data[key]
  if isinstance(raw, bool):
    raise ValueError(f"{context}.{key} must be a whole number, got {raw!r}.")
  if isinstance(raw, int):
    return raw
  if isinstance(raw, float) and raw.is_integer():
    return int(raw)
  if isinstance(raw, str) and raw.strip().lstrip("-").isdigit():
    return int(raw.strip())
  raise ValueError(
      f"{context}.{key} must be a whole number of seconds/attempts (an"
      f" integer), got {raw!r}. Fractional values are not supported."
  )


def _reject_unknown_keys(
    data: dict[str, Any], allowed: set[str], context: str
) -> None:
  """Raises ValueError if `data` contains keys outside `allowed`."""
  if not isinstance(data, dict):
    return
  unknown = sorted(set(data.keys()) - allowed)
  if not unknown:
    return
  key = unknown[0]
  matches = difflib.get_close_matches(
      str(key), sorted(allowed), n=1, cutoff=0.6
  )
  hint = f" Did you mean '{matches[0]}'?" if matches else ""
  allowed_list = ", ".join(sorted(allowed))
  raise ValueError(
      f"Unknown field '{key}' in {context}.{hint} Allowed fields:"
      f" {allowed_list}."
  )


class TriggerRule(enum.IntEnum):
  """Stage trigger rule evaluating upstream dependencies."""

  ALL_SUCCESS = 0
  ALL_DONE = 1
  NONE_FAILED = 2

  @classmethod
  def Name(cls, val: int | str) -> str:  # pylint: disable=invalid-name
    if isinstance(val, str):
      return val.upper()
    return cls(val).name

  @classmethod
  def from_value(cls, val: Any) -> TriggerRule:
    if isinstance(val, TriggerRule):
      return val
    if val is None or (isinstance(val, str) and not val.strip()):
      return cls.ALL_SUCCESS
    if isinstance(val, int) and not isinstance(val, bool):
      return cls(val)
    if isinstance(val, str):
      norm = val.strip().upper()
      if norm in cls.__members__:
        return cls[norm]
      if norm.isdigit():
        return cls(int(norm))
    raise ValueError(
        f"Unknown trigger_rule {val!r}; expected one of"
        f" {', '.join(cls.__members__)}."
    )


class StampStatus(enum.IntEnum):
  """Execution status recorded on a Passport Stamp."""

  STATUS_UNSPECIFIED = 0
  PENDING = 1
  COMPLETED = 2
  SKIPPED = 3
  FAILED = 4
  PAUSED = 5

  @classmethod
  def Name(cls, val: int | str) -> str:  # pylint: disable=invalid-name
    if isinstance(val, str):
      return val.upper()
    return cls(val).name

  @classmethod
  def from_value(cls, val: Any) -> StampStatus:
    if isinstance(val, StampStatus):
      return val
    if isinstance(val, int) and not isinstance(val, bool):
      return cls(val)
    if isinstance(val, str):
      norm = val.strip().upper()
      if norm in cls.__members__:
        return cls[norm]
      if norm.isdigit():
        return cls(int(norm))
    return cls.STATUS_UNSPECIFIED


class Resolution(enum.IntEnum):
  """Operator action resolution outcome."""

  RESOLUTION_UNSPECIFIED = 0
  APPROVE = 1
  REJECT = 2

  @classmethod
  def Name(cls, val: int | str) -> str:  # pylint: disable=invalid-name
    if isinstance(val, str):
      return val.upper()
    return cls(val).name

  @classmethod
  def from_value(cls, val: Any) -> Resolution:
    if isinstance(val, Resolution):
      return val
    if val is None or (isinstance(val, str) and not val.strip()):
      return cls.RESOLUTION_UNSPECIFIED
    if isinstance(val, int) and not isinstance(val, bool):
      return cls(val)
    if isinstance(val, str):
      norm = val.strip().upper()
      if norm in cls.__members__:
        return cls[norm]
      if norm.isdigit():
        return cls(int(norm))
    raise ValueError(
        f"Unknown resolution {val!r}; expected one of"
        f" {', '.join(cls.__members__)}."
    )


class TimestampValue:
  """Lightweight ISO-8601 UTC timestamp with Protobuf Timestamp compatibility."""

  def __init__(self, iso_string: Optional[str] = None):
    self._iso: str = iso_string or ""

  def GetCurrentTime(self) -> None:  # pylint: disable=invalid-name
    now = datetime.datetime.now(datetime.timezone.utc)
    self._iso = now.strftime("%Y-%m-%dT%H:%M:%S.%fZ")

  def CopyFrom(self, other: TimestampValue | str) -> None:  # pylint: disable=invalid-name
    if isinstance(other, TimestampValue):
      self._iso = other._iso
    else:
      self._iso = str(other)

  def ToJsonString(self) -> str:  # pylint: disable=invalid-name
    if not self._iso:
      self.GetCurrentTime()
    return self._iso

  def __str__(self) -> str:
    return self.ToJsonString()

  def __repr__(self) -> str:
    return f"TimestampValue({self._iso!r})"


class StructDict(dict):
  """Dictionary wrapper with Protobuf Struct compatibility shims."""

  @property
  def fields(self) -> dict[str, Any]:
    return self

  def Clear(self) -> None:  # pylint: disable=invalid-name
    self.clear()

  def CopyFrom(self, other: dict[str, Any]) -> None:  # pylint: disable=invalid-name
    self.clear()
    self.update(copy.deepcopy(dict(other)))

  def to_dict(self) -> dict[str, Any]:
    return copy.deepcopy(dict(self))


@dataclass
class PythonAction:
  """Runs a registered Python function in the engine registry."""

  action_id: str = ""
  static_kwargs: StructDict = field(default_factory=StructDict)

  def __post_init__(self) -> None:
    if not isinstance(self.static_kwargs, StructDict):
      self.static_kwargs = StructDict(self.static_kwargs or {})

  def HasField(self, name: str) -> bool:  # pylint: disable=invalid-name
    if name == "action_id":
      return bool(self.action_id)
    if name == "static_kwargs":
      return bool(self.static_kwargs)
    return False

  def to_dict(self) -> dict[str, Any]:
    res: dict[str, Any] = {"action_id": self.action_id}
    if self.static_kwargs:
      res["static_kwargs"] = dict(self.static_kwargs)
    return res

  @classmethod
  def from_dict(cls, data: dict[str, Any]) -> PythonAction:
    if not data:
      return cls()
    _reject_unknown_keys(data, {"action_id", "static_kwargs"}, "PythonAction")
    raw_kwargs = data.get("static_kwargs", {})
    normalized_kwargs = _unwrap_proto_struct_fields(raw_kwargs)
    return cls(
        action_id=str(data.get("action_id", "")),
        static_kwargs=StructDict(normalized_kwargs),
    )


@dataclass
class OperatorAction:
  """Pauses execution and waits for operator/agent approval and payload."""

  instructions: str = ""
  json_schema: str = ""

  def HasField(self, name: str) -> bool:  # pylint: disable=invalid-name
    val = getattr(self, name, None)
    return bool(val)

  def to_dict(self) -> dict[str, Any]:
    res: dict[str, Any] = {"instructions": self.instructions}
    if self.json_schema:
      res["json_schema"] = self.json_schema
    return res

  @classmethod
  def from_dict(cls, data: dict[str, Any]) -> OperatorAction:
    if not data:
      return cls()
    _reject_unknown_keys(
        data, {"instructions", "json_schema"}, "OperatorAction"
    )
    schema_val = data.get("json_schema", "")
    if isinstance(schema_val, dict):
      schema_val = json.dumps(schema_val)
    return cls(
        instructions=str(data.get("instructions", "")),
        json_schema=str(schema_val),
    )


@dataclass
class RetryPolicy:
  """Stage retry configuration with exponential backoff."""

  max_attempts: Optional[int] = None
  initial_backoff_seconds: Optional[int] = None
  backoff_multiplier: Optional[float] = None

  def HasField(self, name: str) -> bool:  # pylint: disable=invalid-name
    return getattr(self, name, None) is not None

  def to_dict(self) -> dict[str, Any]:
    res: dict[str, Any] = {}
    if self.max_attempts is not None:
      res["max_attempts"] = self.max_attempts
    if self.initial_backoff_seconds is not None:
      res["initial_backoff_seconds"] = self.initial_backoff_seconds
    if self.backoff_multiplier is not None:
      res["backoff_multiplier"] = self.backoff_multiplier
    return res

  @classmethod
  def from_dict(cls, data: dict[str, Any]) -> RetryPolicy:
    if not data:
      return cls()
    _reject_unknown_keys(
        data,
        {"max_attempts", "initial_backoff_seconds", "backoff_multiplier"},
        "RetryPolicy",
    )
    return cls(
        max_attempts=_opt_int(data, "max_attempts", "RetryPolicy"),
        initial_backoff_seconds=_opt_int(
            data, "initial_backoff_seconds", "RetryPolicy"
        ),
        backoff_multiplier=(
            float(data["backoff_multiplier"])
            if "backoff_multiplier" in data
            and data["backoff_multiplier"] is not None
            else None
        ),
    )


@dataclass
class PollingPolicy:
  """Re-executes stage periodically until a condition evaluates to true."""

  condition: str = ""
  interval_seconds: Optional[int] = None
  timeout_seconds: Optional[int] = None
  max_attempts: Optional[int] = None
  poll_tick_action: Optional[PythonAction] = None

  def HasField(self, name: str) -> bool:  # pylint: disable=invalid-name
    val = getattr(self, name, None)
    if name == "poll_tick_action":
      return val is not None and bool(val.action_id)
    return val is not None

  def to_dict(self) -> dict[str, Any]:
    res: dict[str, Any] = {"condition": self.condition}
    if self.interval_seconds is not None:
      res["interval_seconds"] = self.interval_seconds
    if self.timeout_seconds is not None:
      res["timeout_seconds"] = self.timeout_seconds
    if self.max_attempts is not None:
      res["max_attempts"] = self.max_attempts
    if self.poll_tick_action and self.poll_tick_action.action_id:
      res["poll_tick_action"] = self.poll_tick_action.to_dict()
    return res

  @classmethod
  def from_dict(cls, data: dict[str, Any]) -> PollingPolicy:
    if not data:
      return cls()
    _reject_unknown_keys(
        data,
        {
            "condition",
            "interval_seconds",
            "timeout_seconds",
            "max_attempts",
            "poll_tick_action",
        },
        "PollingPolicy",
    )
    tick = data.get("poll_tick_action")
    return cls(
        condition=str(data.get("condition", "")),
        interval_seconds=_opt_int(data, "interval_seconds", "PollingPolicy"),
        timeout_seconds=_opt_int(data, "timeout_seconds", "PollingPolicy"),
        max_attempts=_opt_int(data, "max_attempts", "PollingPolicy"),
        poll_tick_action=(
            PythonAction.from_dict(tick) if isinstance(tick, dict) else None
        ),
    )


@dataclass
class Stage:
  """A single stage node in a Lightflow DAG."""

  TriggerRule = TriggerRule  # Class-level alias for Protobuf compatibility

  name: str = ""
  description: str = ""
  run_after: list[str] = field(default_factory=list)
  python_action: Optional[PythonAction] = None
  operator_action: Optional[OperatorAction] = None
  run_if: str = ""
  trigger_rule: TriggerRule = TriggerRule.ALL_SUCCESS
  retry_policy: Optional[RetryPolicy] = None
  polling_policy: Optional[PollingPolicy] = None
  rollback_action: Optional[PythonAction] = None
  metadata: dict[str, str] = field(default_factory=dict)
  timeout_seconds: Optional[int] = None

  def HasField(self, name: str) -> bool:  # pylint: disable=invalid-name
    val = getattr(self, name, None)
    if name in ("python_action", "rollback_action"):
      return val is not None and bool(val.action_id)
    if name == "operator_action":
      return val is not None
    if name in ("retry_policy", "polling_policy"):
      return val is not None
    if name == "timeout_seconds":
      return val is not None
    return bool(val)

  def to_dict(self) -> dict[str, Any]:
    res: dict[str, Any] = {"name": self.name}
    if self.description:
      res["description"] = self.description
    if self.run_after:
      res["run_after"] = list(self.run_after)
    if self.HasField("python_action") and self.python_action:
      res["python_action"] = self.python_action.to_dict()
    if self.HasField("operator_action") and self.operator_action:
      res["operator_action"] = self.operator_action.to_dict()
    if self.run_if:
      res["run_if"] = self.run_if
    if self.trigger_rule != TriggerRule.ALL_SUCCESS:
      res["trigger_rule"] = TriggerRule.Name(self.trigger_rule)
    if self.HasField("retry_policy") and self.retry_policy:
      res["retry_policy"] = self.retry_policy.to_dict()
    if self.HasField("polling_policy") and self.polling_policy:
      res["polling_policy"] = self.polling_policy.to_dict()
    if self.HasField("rollback_action") and self.rollback_action:
      res["rollback_action"] = self.rollback_action.to_dict()
    if self.metadata:
      res["metadata"] = dict(self.metadata)
    if self.timeout_seconds is not None:
      res["timeout_seconds"] = self.timeout_seconds
    return res

  @classmethod
  def from_dict(cls, data: dict[str, Any]) -> Stage:
    if not data:
      return cls()
    _reject_unknown_keys(
        data,
        {
            "name",
            "description",
            "run_after",
            "python_action",
            "operator_action",
            "run_if",
            "trigger_rule",
            "retry_policy",
            "polling_policy",
            "rollback_action",
            "metadata",
            "timeout_seconds",
        },
        f"Stage '{data.get('name', '<unnamed>')}'",
    )
    run_after_raw = data.get("run_after", [])
    if isinstance(run_after_raw, str):
      run_after = [run_after_raw]
    else:
      run_after = [str(x) for x in run_after_raw]

    py_act = data.get("python_action")
    op_act = data.get("operator_action")
    retry = data.get("retry_policy")
    poll = data.get("polling_policy")
    rollback = data.get("rollback_action")

    return cls(
        name=str(data.get("name", "")),
        description=str(data.get("description", "")),
        run_after=run_after,
        python_action=(
            PythonAction.from_dict(py_act)
            if isinstance(py_act, dict)
            else (
                PythonAction(action_id=str(py_act))
                if isinstance(py_act, str)
                else None
            )
        ),
        operator_action=(
            OperatorAction.from_dict(op_act)
            if isinstance(op_act, dict)
            else None
        ),
        run_if=str(data.get("run_if", "")),
        trigger_rule=TriggerRule.from_value(
            data.get("trigger_rule", TriggerRule.ALL_SUCCESS)
        ),
        retry_policy=(
            RetryPolicy.from_dict(retry) if isinstance(retry, dict) else None
        ),
        polling_policy=(
            PollingPolicy.from_dict(poll) if isinstance(poll, dict) else None
        ),
        rollback_action=(
            PythonAction.from_dict(rollback)
            if isinstance(rollback, dict)
            else (
                PythonAction(action_id=str(rollback))
                if isinstance(rollback, str)
                else None
            )
        ),
        metadata={
            str(k): str(v) for k, v in (data.get("metadata") or {}).items()
        },
        timeout_seconds=_opt_int(
            data, "timeout_seconds", f"Stage '{data.get('name', '?')}'"
        ),
    )


@dataclass
class ActionDefinition:
  """Maps an action ID to a fully-qualified Python callable import path."""

  id: str = ""
  python_import: str = ""

  def to_dict(self) -> dict[str, Any]:
    return {"id": self.id, "python_import": self.python_import}

  @classmethod
  def from_dict(cls, data: dict[str, Any]) -> ActionDefinition:
    if not data:
      return cls()
    _reject_unknown_keys(data, {"id", "python_import"}, "ActionDefinition")
    return cls(
        id=str(data.get("id", "")),
        python_import=str(data.get("python_import", "")),
    )


@dataclass
class Lightflow:
  """Top-level Lightflow DAG manifest definition."""

  name: str = ""
  description: str = ""
  actions: list[ActionDefinition] = field(default_factory=list)
  stages: list[Stage] = field(default_factory=list)
  metadata: dict[str, str] = field(default_factory=dict)
  runner_target: str = ""
  strict_expressions: bool = False

  def to_dict(self) -> dict[str, Any]:
    res: dict[str, Any] = {"name": self.name}
    if self.description:
      res["description"] = self.description
    if self.actions:
      res["actions"] = [a.to_dict() for a in self.actions]
    if self.stages:
      res["stages"] = [s.to_dict() for s in self.stages]
    if self.metadata:
      res["metadata"] = dict(self.metadata)
    if self.runner_target:
      res["runner_target"] = self.runner_target
    if self.strict_expressions:
      res["strict_expressions"] = True
    return res

  @classmethod
  def from_dict(cls, data: dict[str, Any]) -> Lightflow:
    if not data:
      return cls()
    _reject_unknown_keys(
        data,
        {
            "name",
            "description",
            "actions",
            "stages",
            "metadata",
            "runner_target",
            "strict_expressions",
        },
        "Lightflow",
    )
    raw_actions = data.get("actions", [])
    actions_list: list[ActionDefinition] = []
    if isinstance(raw_actions, dict):
      # Allow shorthand dict in YAML: actions: {fetch_rows: my_pkg.fetch_rows}
      for act_id, py_imp in raw_actions.items():
        if isinstance(py_imp, str):
          actions_list.append(
              ActionDefinition(id=str(act_id), python_import=py_imp)
          )
        elif isinstance(py_imp, dict):
          _reject_unknown_keys(
              py_imp, {"python_import"}, f"ActionDefinition '{act_id}'"
          )
          actions_list.append(
              ActionDefinition(
                  id=str(act_id),
                  python_import=str(py_imp.get("python_import", "")),
              )
          )
    elif isinstance(raw_actions, list):
      for item in raw_actions:
        if isinstance(item, dict):
          actions_list.append(ActionDefinition.from_dict(item))
        elif isinstance(item, ActionDefinition):
          actions_list.append(item)

    raw_stages = data.get("stages", [])
    stages_list: list[Stage] = []
    if isinstance(raw_stages, list):
      for s in raw_stages:
        if isinstance(s, dict):
          stages_list.append(Stage.from_dict(s))
        elif isinstance(s, Stage):
          stages_list.append(s)

    return cls(
        name=str(data.get("name", "")),
        description=str(data.get("description", "")),
        actions=actions_list,
        stages=stages_list,
        metadata={
            str(k): str(v) for k, v in (data.get("metadata") or {}).items()
        },
        runner_target=str(data.get("runner_target", "")),
        strict_expressions=bool(data.get("strict_expressions", False)),
    )


@dataclass
class Stamp:
  """Append-only audit record of entering or exiting a workflow stage."""

  Status = StampStatus  # Class-level alias for Protobuf compatibility
  STATUS_UNSPECIFIED = StampStatus.STATUS_UNSPECIFIED
  PENDING = StampStatus.PENDING
  COMPLETED = StampStatus.COMPLETED
  SKIPPED = StampStatus.SKIPPED
  FAILED = StampStatus.FAILED
  PAUSED = StampStatus.PAUSED

  stage_name: str = ""
  status: StampStatus = StampStatus.STATUS_UNSPECIFIED
  timestamp: TimestampValue = field(default_factory=TimestampValue)
  message: str = ""
  instructions: str = ""
  resume_command: str = ""

  def to_dict(self) -> dict[str, Any]:
    res: dict[str, Any] = {
        "stage_name": self.stage_name,
        "status": StampStatus.Name(self.status),
        "timestamp": self.timestamp.ToJsonString(),
    }
    if self.message:
      res["message"] = self.message
    if self.instructions:
      res["instructions"] = self.instructions
    if self.resume_command:
      res["resume_command"] = self.resume_command
    return res

  @classmethod
  def from_dict(cls, data: dict[str, Any]) -> Stamp:
    return cls(
        stage_name=str(data.get("stage_name", "")),
        status=StampStatus.from_value(
            data.get("status", StampStatus.STATUS_UNSPECIFIED)
        ),
        timestamp=TimestampValue(str(data.get("timestamp", ""))),
        message=str(data.get("message", "")),
        instructions=str(data.get("instructions", "")),
        resume_command=str(data.get("resume_command", "")),
    )


class StampList(list):
  """Repeated Stamp container supporting Protobuf `.add()` syntax."""

  def add(self, **kwargs: Any) -> Stamp:
    stamp = Stamp(**kwargs)
    self.append(stamp)
    return stamp


@dataclass
class Passport:
  """The state ledger passed between stages as payload carrier and stamp log."""

  Stamp = Stamp  # Class-level alias for Protobuf compatibility

  payload: StructDict = field(default_factory=StructDict)
  stamps: StampList = field(default_factory=StampList)
  # Recorded on first `start` so `list`/`status` can label and locate runs.
  lightflow_name: str = ""
  lightflow_path: str = ""

  def __post_init__(self) -> None:
    if not isinstance(self.payload, StructDict):
      self.payload = StructDict(self.payload or {})
    if not isinstance(self.stamps, StampList):
      self.stamps = StampList(
          s if isinstance(s, Stamp) else Stamp.from_dict(s)
          for s in self.stamps or []
      )

  def to_dict(self) -> dict[str, Any]:
    res: dict[str, Any] = {}
    if self.payload:
      res["payload"] = dict(self.payload)
    if self.stamps:
      res["stamps"] = [s.to_dict() for s in self.stamps]
    if self.lightflow_name:
      res["lightflow_name"] = self.lightflow_name
    if self.lightflow_path:
      res["lightflow_path"] = self.lightflow_path
    return res

  @classmethod
  def from_dict(cls, data: dict[str, Any]) -> Passport:
    if not data:
      return cls()
    payload_data = data.get("payload", {})
    stamps_data = data.get("stamps", [])
    return cls(
        payload=StructDict(copy.deepcopy(payload_data)),
        stamps=StampList(
            s if isinstance(s, Stamp) else Stamp.from_dict(s)
            for s in stamps_data
        ),
        lightflow_name=str(data.get("lightflow_name", "") or ""),
        lightflow_path=str(data.get("lightflow_path", "") or ""),
    )


@dataclass
class ResumeLightflowRequest:
  """Request contract for resuming a paused operator action via CLI or API."""

  Resolution = Resolution

  stage_name: str = ""
  resolution: Resolution = Resolution.APPROVE
  payload: StructDict = field(default_factory=StructDict)
  comment: str = ""
  operator: str = ""


# ------------------- TEXTPROTO & STRUCT HELPERS -------------------


# Re-export textproto, YAML, and JSON Schema helpers for backwards compat.
try:
  from . import parsers
except ImportError:
  from lightflow import parsers  # pyrefly: ignore[missing-import]

_unwrap_proto_struct_fields = parsers._unwrap_proto_struct_fields
_unwrap_proto_value = parsers._unwrap_proto_value
_TOKEN_RE = parsers._TOKEN_RE
_tokenize_textproto = parsers._tokenize_textproto
_UNESCAPE_MAP = parsers._UNESCAPE_MAP
_unescape_proto_string = parsers._unescape_proto_string
_REPEATED_KEYS = parsers._REPEATED_KEYS
parse_textproto_to_dict = parsers.parse_textproto_to_dict
_strip_yaml_comment_outside_quotes = parsers._strip_yaml_comment_outside_quotes
_parse_yaml_scalar = parsers._parse_yaml_scalar
_parse_simple_yaml = parsers._parse_simple_yaml
_STDLIB_JSON_SCHEMA_KEYWORDS = parsers._STDLIB_JSON_SCHEMA_KEYWORDS
_STDLIB_JSON_SCHEMA_TYPES = parsers._STDLIB_JSON_SCHEMA_TYPES
_check_json_schema_stdlib = parsers._check_json_schema_stdlib
check_json_schema = parsers.check_json_schema
_matches_json_type = parsers._matches_json_type
_validate_json_schema_stdlib = parsers._validate_json_schema_stdlib
validate_json_schema = parsers.validate_json_schema


def parse_lightflow_text(content: str, fmt: Optional[str] = None) -> Lightflow:
  """Parses a Lightflow manifest from YAML, JSON, or Textproto string."""
  stripped = content.strip()
  if not stripped:
    return Lightflow()

  if fmt:
    fmt_norm = fmt.lower().lstrip(".")
  else:
    if stripped.startswith("{"):
      fmt_norm = "json"
    elif re.search(r"^\s*(?:stages|actions)\s*\{", stripped, re.MULTILINE):
      fmt_norm = "textproto"
    else:
      fmt_norm = "yaml"

  if fmt_norm == "json":
    data = json.loads(stripped)
  elif fmt_norm in ("textproto", "txtpb", "pbtxt"):
    data = parse_textproto_to_dict(stripped)
  elif fmt_norm in ("yaml", "yml"):
    if yaml is not None:
      data = yaml.safe_load(stripped)
    else:
      try:
        data = _parse_simple_yaml(stripped)
      except (ValueError, IndexError, KeyError) as e:
        raise ValueError(
            "Could not parse YAML manifest with the built-in best-effort"
            f" parser ({e}). Install PyYAML (`pip install pyyaml`) for full"
            " YAML support."
        ) from e
    if data is not None and not isinstance(data, dict):
      raise ValueError(
          "Lightflow YAML manifest must be a mapping at top level."
      )
  else:
    raise ValueError(f"Unsupported lightflow manifest format: {fmt}")

  return Lightflow.from_dict(data or {})


def resolve_lightflow_path(path: str) -> str:
  """Resolves a lightflow path or directory to the canonical manifest file."""
  expanded = os.path.expanduser(path)
  if os.path.isdir(expanded):
    for candidate in (
        "lightflow.yaml",
        "lightflow.yml",
        "lightflow.json",
        "lightflow.textproto",
    ):
      cand_path = os.path.join(expanded, candidate)
      if os.path.isfile(cand_path):
        return cand_path
  return expanded


def load_lightflow(path: str) -> Lightflow:
  """Loads a Lightflow manifest from a YAML, JSON, or Textproto file."""
  expanded = resolve_lightflow_path(path)
  _, ext = os.path.splitext(expanded)
  ext_lower = ext.lower()
  fmt = None
  if ext_lower in (".yaml", ".yml"):
    fmt = "yaml"
  elif ext_lower == ".json":
    fmt = "json"
  elif ext_lower in (".textproto", ".txtpb", ".pbtxt"):
    fmt = "textproto"

  with open(expanded, "r", encoding="utf-8") as f:
    return parse_lightflow_text(f.read(), fmt=fmt)
