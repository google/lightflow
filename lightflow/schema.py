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
Textproto (.textproto) across local CLI, MCP, and Python environments.
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
from typing import Any, Iterator, Optional

try:
  import yaml
except ImportError:
  yaml = None


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
        max_attempts=(
            int(data["max_attempts"])
            if "max_attempts" in data and data["max_attempts"] is not None
            else None
        ),
        initial_backoff_seconds=(
            int(data["initial_backoff_seconds"])
            if "initial_backoff_seconds" in data
            and data["initial_backoff_seconds"] is not None
            else None
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
        interval_seconds=(
            int(data["interval_seconds"])
            if "interval_seconds" in data
            and data["interval_seconds"] is not None
            else None
        ),
        timeout_seconds=(
            int(data["timeout_seconds"])
            if "timeout_seconds" in data and data["timeout_seconds"] is not None
            else None
        ),
        max_attempts=(
            int(data["max_attempts"])
            if "max_attempts" in data and data["max_attempts"] is not None
            else None
        ),
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
        timeout_seconds=(
            int(data["timeout_seconds"])
            if "timeout_seconds" in data and data["timeout_seconds"] is not None
            else None
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


def _unwrap_proto_struct_fields(raw: Any) -> Any:
  """Converts protobuf-style Struct `{fields: {key: ..., value: ...}}` to dict."""
  if not isinstance(raw, dict):
    return raw
  if "fields" in raw and len(raw) == 1:
    fields_val = raw["fields"]
    if isinstance(fields_val, list):
      out = {}
      for entry in fields_val:
        if isinstance(entry, dict) and "key" in entry and "value" in entry:
          out[str(entry["key"])] = _unwrap_proto_value(entry["value"])
      return out
    elif isinstance(fields_val, dict):
      if "key" in fields_val and "value" in fields_val:
        return {
            str(fields_val["key"]): _unwrap_proto_value(fields_val["value"])
        }
      return {str(k): _unwrap_proto_value(v) for k, v in fields_val.items()}
  return {k: _unwrap_proto_struct_fields(v) for k, v in raw.items()}


def _unwrap_proto_value(val: Any) -> Any:
  """Extracts scalar/list/struct values from Protobuf Value representations."""
  if not isinstance(val, dict):
    return val
  if "number_value" in val:
    num = val["number_value"]
    if isinstance(num, float) and num.is_integer():
      return int(num)
    return num
  if "string_value" in val:
    return str(val["string_value"])
  if "bool_value" in val:
    return bool(val["bool_value"])
  if "null_value" in val:
    return None
  if "struct_value" in val:
    return _unwrap_proto_struct_fields(val["struct_value"])
  if "list_value" in val:
    lv = val["list_value"]
    if isinstance(lv, dict) and "values" in lv:
      vals = lv["values"]
      if not isinstance(vals, list):
        vals = [vals]
      return [_unwrap_proto_value(x) for x in vals]
  return _unwrap_proto_struct_fields(val)


_TOKEN_RE = re.compile(
    r"""
    \s*(?:
      (\#[^\n]*)                        | # 1: Comment
      ("(?:\\.|[^"\\])*"|'(?:\\.|[^'\\])*') | # 2: Quoted string
      ([{}:\[\],])                      | # 3: Punctuation
      ([^\s{}:\[\],#"'`]+)                # 4: Identifier / Number
    )
    """,
    re.VERBOSE,
)


def _tokenize_textproto(text: str) -> Iterator[tuple[str, str]]:
  """Yields (kind, token) pairs from a textproto string."""
  pos = 0
  length = len(text)
  while pos < length:
    m = _TOKEN_RE.match(text, pos)
    if not m:
      if text[pos:].strip():
        raise ValueError(f"Unexpected textproto character at pos {pos}")
      break
    pos = m.end()
    comment, quoted, punct, ident = m.groups()
    if comment:
      continue
    if quoted is not None:
      yield ("STRING", _unescape_proto_string(quoted[1:-1]))
    elif punct is not None:
      yield ("PUNCT", punct)
    elif ident is not None:
      yield ("IDENT", ident)


_UNESCAPE_MAP = {
    '"': '"',
    "'": "'",
    "n": "\n",
    "t": "\t",
    "r": "\r",
    "\\": "\\",
}


def _unescape_proto_string(s: str) -> str:
  """Unescapes C/textproto escape sequences in a single pass."""
  return re.sub(
      r'\\(["\'ntr\\])',
      lambda m: _UNESCAPE_MAP.get(m.group(1), m.group(0)),
      s,
  )


_REPEATED_KEYS = frozenset(
    {"actions", "stages", "run_after", "fields", "values"}
)


def parse_textproto_to_dict(text: str) -> dict[str, Any]:
  """Parses a Lightflow `.textproto` manifest into a nested Python dictionary."""
  tokens = list(_tokenize_textproto(text))
  idx = 0
  n = len(tokens)

  def parse_scalar(kind: str, val: str) -> Any:
    nonlocal idx
    if kind == "STRING":
      parts = [val]
      while idx < n and tokens[idx][0] == "STRING":
        parts.append(tokens[idx][1])
        idx += 1
      return "".join(parts)
    if val in ("true", "True"):
      return True
    if val in ("false", "False"):
      return False
    if val in ("null", "None"):
      return None
    try:
      if "." in val or "e" in val.lower():
        return float(val)
      return int(val)
    except ValueError:
      return val

  def parse_block(stop_punct: Optional[str] = None) -> dict[str, Any]:
    nonlocal idx
    result: dict[str, Any] = {}
    while idx < n:
      kind, tok = tokens[idx]
      if stop_punct and kind == "PUNCT" and tok == stop_punct:
        idx += 1
        break
      if kind == "PUNCT" and tok in (",", ";"):
        idx += 1
        continue
      if kind != "IDENT":
        raise ValueError(f"Expected field identifier in textproto, got {tok!r}")
      key = tok
      idx += 1
      if idx >= n:
        break

      has_colon = False
      if tokens[idx] == ("PUNCT", ":"):
        has_colon = True
        idx += 1

      if idx < n and tokens[idx] == ("PUNCT", "{"):
        idx += 1
        val = parse_block("}")
      elif idx < n and tokens[idx] == ("PUNCT", "["):
        idx += 1
        items = []
        while idx < n and tokens[idx] != ("PUNCT", "]"):
          if tokens[idx] == ("PUNCT", ","):
            idx += 1
            continue
          if tokens[idx] == ("PUNCT", "{"):
            idx += 1
            items.append(parse_block("}"))
          else:
            k_item, v_item = tokens[idx]
            idx += 1
            items.append(parse_scalar(k_item, v_item))
        if idx < n and tokens[idx] == ("PUNCT", "]"):
          idx += 1
        val = items
      elif has_colon and idx < n:
        k_val, v_val = tokens[idx]
        idx += 1
        val = parse_scalar(k_val, v_val)
      else:
        raise ValueError(f"Unexpected token after field {key!r} in textproto")

      if key in _REPEATED_KEYS:
        existing = result.setdefault(key, [])
        if isinstance(val, list):
          existing.extend(val)
        else:
          existing.append(val)
      elif key in result:
        if not isinstance(result[key], list):
          result[key] = [result[key]]
        result[key].append(val)
      else:
        result[key] = val

    return result

  return parse_block(None)


def _strip_yaml_comment_outside_quotes(line: str) -> str:
  """Strips `#` comments only when outside single or double quotes."""
  in_single = False
  in_double = False
  escaped = False
  for i, ch in enumerate(line):
    if escaped:
      escaped = False
      continue
    if ch == "\\":
      escaped = True
      continue
    if ch == "'" and not in_double:
      in_single = not in_single
    elif ch == '"' and not in_single:
      in_double = not in_double
    elif ch == "#" and not in_single and not in_double:
      if i == 0 or line[i - 1].isspace():
        return line[:i].rstrip()
  return line


def _parse_yaml_scalar(val_str: str) -> Any:
  """Parses a YAML scalar value using pure Python stdlib."""
  s = val_str.strip()
  if not s:
    return ""
  if (s.startswith('"') and s.endswith('"')) or (
      s.startswith("'") and s.endswith("'")
  ):
    return s[1:-1]
  if s[0] in "[{" and s[-1] != {"[": "]", "{": "}"}[s[0]]:
    raise ValueError(f"unterminated flow collection: {s!r}")
  if s.startswith("[") and s.endswith("]"):
    inner = s[1:-1].strip()
    if not inner:
      return []
    return [_parse_yaml_scalar(x) for x in inner.split(",")]
  if s.startswith("{") and s.endswith("}"):
    try:
      return json.loads(s)
    except ValueError as e:
      raise ValueError(
          f"flow mapping {s!r} is not valid JSON; the built-in parser only"
          " supports JSON-style flow mappings"
      ) from e
  if s[0] in "&*!":
    raise ValueError(
        f"{s!r}: YAML anchors, aliases and tags are not supported by the"
        " built-in parser"
    )
  if s in ("true", "True"):
    return True
  if s in ("false", "False"):
    return False
  if s in ("null", "None", "~"):
    return None
  try:
    if "." in s:
      return float(s)
    return int(s)
  except ValueError:
    return s


def _parse_simple_yaml(text: str) -> dict[str, Any]:
  """Best-effort YAML subset parser used only when PyYAML is not installed.

  PyYAML (`yaml.safe_load`) is the supported parser. This fallback covers the
  subset used by Lightflow manifests: nested mappings, block lists, quoted and
  plain scalars, `#` comments, `|`/`>` block scalars and JSON-style flow
  collections. Known gaps: blank lines inside block scalars are dropped and
  relative indentation inside them is not preserved. Anything else
  (anchors/aliases, tags, YAML-style flow mappings, multi-document streams,
  multi-line plain scalars) raises rather than being guessed at.

  Args:
    text: The YAML document.

  Returns:
    The top-level mapping as a dict.

  Raises:
    ValueError: If the document uses syntax outside the supported subset or
      is not a mapping at the top level.
  """
  lines: list[tuple[int, str]] = []
  doc_starts = 0
  doc_ended = False
  block_scalar_indent: Optional[int] = None
  for lineno, raw in enumerate(text.splitlines(), start=1):
    if block_scalar_indent is not None:
      raw_stripped = raw.strip()
      if not raw_stripped:
        continue
      raw_indent = len(raw) - len(raw.lstrip(" "))
      if raw_indent > block_scalar_indent:
        lines.append((raw_indent, raw_stripped))
        continue
      block_scalar_indent = None

    stripped_comment = _strip_yaml_comment_outside_quotes(raw)
    content = stripped_comment.strip()
    if not content:
      continue
    if doc_ended:
      raise ValueError(
          f"line {lineno}: multi-document YAML streams are not supported"
          " by the built-in parser"
      )
    if content == "---":
      doc_starts += 1
      if doc_starts > 1 or lines:
        raise ValueError(
            f"line {lineno}: multi-document YAML streams are not supported"
            " by the built-in parser"
        )
      continue
    if content == "...":
      doc_ended = True
      continue
    indent = len(stripped_comment) - len(stripped_comment.lstrip(" "))
    if ":" in content:
      _, _, rhs = content.partition(":")
      if rhs.strip() in (">-", ">", "|-", "|"):
        block_scalar_indent = indent
    lines.append((indent, content))

  idx = 0
  n = len(lines)

  def parse_block_scalar(parent_indent: int, indicator: str) -> str:
    nonlocal idx
    block_lines: list[str] = []
    while idx < n and lines[idx][0] > parent_indent:
      block_lines.append(lines[idx][1])
      idx += 1
    joiner = " " if indicator.startswith(">") else "\n"
    return joiner.join(block_lines).strip()

  def parse_value(parent_indent: int, raw_val: str) -> Any:
    vs = raw_val.strip()
    if vs in (">-", ">", "|-", "|"):
      return parse_block_scalar(parent_indent, vs)
    if vs:
      return _parse_yaml_scalar(vs)
    return parse_node(parent_indent + 1, allow_same_indent_list=True)

  def parse_node(min_indent: int, allow_same_indent_list: bool = False) -> Any:
    nonlocal idx
    if idx >= n:
      return {}
    first_indent, first_content = lines[idx]
    if first_indent < min_indent and not (
        allow_same_indent_list
        and first_indent == min_indent - 1
        and first_content.startswith("- ")
    ):
      return {}

    if first_content.startswith("- "):
      arr = []
      while (
          idx < n
          and lines[idx][0] == first_indent
          and lines[idx][1].startswith("- ")
      ):
        item_str = lines[idx][1][2:].strip()
        idx += 1
        if not item_str:
          arr.append(parse_node(first_indent + 1))
        elif ":" in item_str and not (
            item_str.startswith('"') or item_str.startswith("'")
        ):
          k, _, v = item_str.partition(":")
          item_dict: dict[str, Any] = {}
          item_dict[k.strip()] = parse_value(first_indent, v)
          while (
              idx < n
              and lines[idx][0] > first_indent
              and not lines[idx][1].startswith("- ")
          ):
            sub_indent, sub_line = lines[idx]
            sk, sep, sv = sub_line.partition(":")
            if not sep:
              raise ValueError(
                  f"expected 'key: value', got {sub_line!r} (multi-line"
                  " plain scalars are not supported by the built-in parser)"
              )
            idx += 1
            item_dict[sk.strip()] = parse_value(sub_indent, sv)
          arr.append(item_dict)
        else:
          arr.append(_parse_yaml_scalar(item_str))
      return arr

    obj: dict[str, Any] = {}
    while (
        idx < n
        and lines[idx][0] == first_indent
        and not lines[idx][1].startswith("- ")
    ):
      _, content = lines[idx]
      k, sep, v = content.partition(":")
      if not sep:
        raise ValueError(
            f"expected 'key: value', got {content!r} (multi-line plain"
            " scalars are not supported by the built-in parser)"
        )
      idx += 1
      key = k.strip()
      obj[key] = parse_value(first_indent, v)
    return obj

  res = parse_node(0)
  if idx < n:
    raise ValueError(
        f"could not parse line {lines[idx][1]!r} (unexpected indentation)"
    )
  if not isinstance(res, dict):
    raise ValueError("Lightflow YAML manifest must be a mapping at top level.")
  return res


_STDLIB_JSON_SCHEMA_TYPES = frozenset({
    "object",
    "array",
    "string",
    "number",
    "integer",
    "boolean",
    "null",
})

_STDLIB_JSON_SCHEMA_KEYWORDS = frozenset({
    "type",
    "enum",
    "const",
    "required",
    "properties",
    "additionalProperties",
    "items",
    "minimum",
    "maximum",
    "minLength",
    "maxLength",
    "minItems",
    "maxItems",
    "title",
    "description",
    "default",
    "examples",
    "$schema",
    "$id",
    "$comment",
})


def _check_json_schema_stdlib(
    schema_dict: dict[str, Any], path: str = ""
) -> None:
  """Validates a JSON Schema dict for the stdlib fallback validator."""
  loc = f" at '{path}'" if path else ""
  if not isinstance(schema_dict, dict):
    raise ValueError(f"JSON Schema{loc} must be a dictionary.")
  unsupported = set(schema_dict.keys()) - _STDLIB_JSON_SCHEMA_KEYWORDS
  if unsupported:
    raise ValueError(
        f"Unsupported JSON Schema keyword(s) in stdlib fallback{loc}:"
        f" {sorted(unsupported)}. Install 'jsonschema' for full Draft 7"
        " validation."
    )
  if "type" in schema_dict:
    t = schema_dict["type"]
    types = [t] if isinstance(t, str) else t if isinstance(t, list) else None
    if not types or any(
        not isinstance(x, str) or x not in _STDLIB_JSON_SCHEMA_TYPES
        for x in types
    ):
      raise ValueError(f"Invalid JSON Schema type{loc}: {t!r}")
  if "enum" in schema_dict and (
      not isinstance(schema_dict["enum"], list) or not schema_dict["enum"]
  ):
    raise ValueError(f"'enum'{loc} must be a non-empty list.")
  if "required" in schema_dict:
    req = schema_dict["required"]
    if not isinstance(req, list) or any(not isinstance(k, str) for k in req):
      raise ValueError(f"'required'{loc} must be a list of strings.")
  for num_kw in (
      "minimum",
      "maximum",
      "minLength",
      "maxLength",
      "minItems",
      "maxItems",
  ):
    if num_kw in schema_dict:
      bound = schema_dict[num_kw]
      if not isinstance(bound, (int, float)) or isinstance(bound, bool):
        raise ValueError(f"'{num_kw}'{loc} must be a number.")
  if "properties" in schema_dict:
    props = schema_dict["properties"]
    if not isinstance(props, dict):
      raise ValueError(f"'properties'{loc} must be a dictionary.")
    for prop_key, prop_schema in props.items():
      sub_path = f"{path}.{prop_key}" if path else str(prop_key)
      _check_json_schema_stdlib(prop_schema, sub_path)
  if "additionalProperties" in schema_dict:
    addl = schema_dict["additionalProperties"]
    if isinstance(addl, dict):
      sub_path = (
          f"{path}.additionalProperties" if path else "additionalProperties"
      )
      _check_json_schema_stdlib(addl, sub_path)
    elif not isinstance(addl, bool):
      raise ValueError(
          f"'additionalProperties'{loc} must be a boolean or dictionary."
      )
  if "items" in schema_dict:
    items_schema = schema_dict["items"]
    if not isinstance(items_schema, dict):
      raise ValueError(
          f"'items'{loc} must be a schema dictionary in stdlib fallback."
          " Install 'jsonschema' for tuple validation."
      )
    sub_path = f"{path}.items" if path else "items"
    _check_json_schema_stdlib(items_schema, sub_path)


def check_json_schema(schema_dict: dict[str, Any]) -> None:
  """Validates that a JSON schema dictionary is well-formed."""
  try:
    import jsonschema as _js  # pylint: disable=g-import-not-at-top

    _js.Draft7Validator.check_schema(schema_dict)
    return
  except ImportError:
    pass
  _check_json_schema_stdlib(schema_dict)


def _matches_json_type(val: Any, expected_type: str) -> bool:
  """Returns True if `val` matches the JSON Schema primitive/container type."""
  if expected_type == "object":
    return isinstance(val, dict)
  if expected_type == "array":
    return isinstance(val, list)
  if expected_type == "string":
    return isinstance(val, str)
  if expected_type == "boolean":
    return isinstance(val, bool)
  if expected_type == "integer":
    return isinstance(val, int) and not isinstance(val, bool)
  if expected_type == "number":
    return isinstance(val, (int, float)) and not isinstance(val, bool)
  if expected_type == "null":
    return val is None
  return True


def _validate_json_schema_stdlib(
    instance: Any, schema_dict: dict[str, Any], path: str = ""
) -> None:
  """Stdlib fallback validator for JSON Schema when `jsonschema` is absent."""
  if not path:
    _check_json_schema_stdlib(schema_dict)
  loc = f" at '{path}'" if path else ""
  expected_type = schema_dict.get("type")
  if isinstance(expected_type, str):
    if not _matches_json_type(instance, expected_type):
      raise ValueError(
          f"Expected {expected_type}{loc}, got {type(instance).__name__}"
      )
  elif isinstance(expected_type, list):
    if not any(_matches_json_type(instance, t) for t in expected_type):
      raise ValueError(
          f"Expected one of {expected_type!r}{loc}, got"
          f" {type(instance).__name__}"
      )
  if "const" in schema_dict and instance != schema_dict["const"]:
    raise ValueError(
        f"Expected const {schema_dict['const']!r}{loc}, got {instance!r}"
    )
  if "enum" in schema_dict and isinstance(schema_dict["enum"], list):
    if instance not in schema_dict["enum"]:
      raise ValueError(
          f"{instance!r} is not one of {schema_dict['enum']!r}{loc}"
      )
  if isinstance(instance, (int, float)) and not isinstance(instance, bool):
    if "minimum" in schema_dict and instance < schema_dict["minimum"]:
      raise ValueError(
          f"{instance!r} is less than minimum {schema_dict['minimum']!r}{loc}"
      )
    if "maximum" in schema_dict and instance > schema_dict["maximum"]:
      raise ValueError(
          f"{instance!r} is greater than maximum"
          f" {schema_dict['maximum']!r}{loc}"
      )
  if isinstance(instance, str):
    if "minLength" in schema_dict and len(instance) < schema_dict["minLength"]:
      raise ValueError(
          f"String length {len(instance)} is less than minLength"
          f" {schema_dict['minLength']}{loc}"
      )
    if "maxLength" in schema_dict and len(instance) > schema_dict["maxLength"]:
      raise ValueError(
          f"String length {len(instance)} is greater than maxLength"
          f" {schema_dict['maxLength']}{loc}"
      )
  if isinstance(instance, dict):
    for req_key in schema_dict.get("required", []):
      if req_key not in instance:
        raise ValueError(f"'{req_key}' is a required property{loc}")
    props = schema_dict.get("properties")
    prop_keys = set(props.keys()) if isinstance(props, dict) else set()
    if isinstance(props, dict):
      for prop_key, prop_schema in props.items():
        if prop_key in instance and isinstance(prop_schema, dict):
          sub_path = f"{path}.{prop_key}" if path else prop_key
          _validate_json_schema_stdlib(
              instance[prop_key], prop_schema, sub_path
          )
    addl = schema_dict.get("additionalProperties", True)
    if isinstance(addl, bool) and not addl:
      extra = sorted(set(instance.keys()) - prop_keys)
      if extra:
        raise ValueError(
            f"Additional properties are not allowed ({extra!r} were"
            f" unexpected){loc}"
        )
    elif isinstance(addl, dict):
      for k in set(instance.keys()) - prop_keys:
        sub_path = f"{path}.{k}" if path else k
        _validate_json_schema_stdlib(instance[k], addl, sub_path)
  elif isinstance(instance, list):
    if "minItems" in schema_dict and len(instance) < schema_dict["minItems"]:
      raise ValueError(
          f"Array length {len(instance)} is less than minItems"
          f" {schema_dict['minItems']}{loc}"
      )
    if "maxItems" in schema_dict and len(instance) > schema_dict["maxItems"]:
      raise ValueError(
          f"Array length {len(instance)} is greater than maxItems"
          f" {schema_dict['maxItems']}{loc}"
      )
    items_schema = schema_dict.get("items")
    if isinstance(items_schema, dict):
      for idx, item in enumerate(instance):
        sub_path = f"{path}[{idx}]" if path else f"[{idx}]"
        _validate_json_schema_stdlib(item, items_schema, sub_path)


def validate_json_schema(instance: Any, schema_dict: dict[str, Any]) -> None:
  """Validates an instance against a JSON schema dictionary."""
  try:
    import jsonschema as _js  # pylint: disable=g-import-not-at-top

    _js.validate(instance=instance, schema=schema_dict)
    return
  except ImportError:
    pass
  _validate_json_schema_stdlib(instance, schema_dict)


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
  if os.path.isdir(expanded):
    raise FileNotFoundError(
        "No lightflow manifest (lightflow.yaml, lightflow.yml, lightflow.json,"
        f" or lightflow.textproto) found in directory: '{expanded}'"
    )
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
