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
import graphlib
import hashlib
import importlib
import inspect
import json
import operator
import os
import queue
import random
import re
import shlex
import shutil
import subprocess
import sys
import threading
import time
import traceback
from typing import Any, Callable, Optional, Sequence

# pylint: disable=g-import-not-at-top,g-bad-import-order
try:
  from . import schema
except ImportError:
  from lightflow import schema  # pyrefly: ignore[missing-import]
# pylint: enable=g-import-not-at-top,g-bad-import-order


# pylint: disable=g-bad-exception-name
class EngineError(Exception):
  """Raised when workflow compilation or execution fails."""


class StageTimeoutError(EngineError):
  """Raised when a stage execution exceeds its configured timeout."""


class OperatorActionSuspended(Exception):
  """Raised when the workflow suspends at an operator action."""


ManualGateSuspended = OperatorActionSuspended
# pylint: enable=g-bad-exception-name


def _quote_cli_arg(value: str) -> str:
  """Shell-quotes a CLI flag value while preserving dry-run placeholders."""
  if value == "<log_id>":
    return value
  return shlex.quote(value)


_CEL_OPERATOR_PATTERN = re.compile(
    r'("(?:\\.|[^"\\])*"|\'(?:\\.|[^\'\\])*\')|(&&|\|\||!(?!=))'
)


def parse_expression(expr: str) -> Optional[ast.Expression]:
  """Normalises CEL-style operators and parses an expression."""
  if not expr.strip():
    return None

  def replace_ops(match: re.Match[str]) -> str:
    op = match.group(2)
    if op == "&&":
      return " and "
    if op == "||":
      return " or "
    if op == "!":
      return "~"
    return match.group(1)

  py_expr = _CEL_OPERATOR_PATTERN.sub(replace_ops, expr).strip()
  return ast.parse(py_expr, mode="eval")


def _is_payload_outputs_node(node: ast.AST) -> bool:
  """Returns True if `node` represents `payload.outputs` or `payload['outputs']`."""
  if (
      isinstance(node, ast.Attribute)
      and node.attr == "outputs"
      and isinstance(node.value, ast.Name)
      and node.value.id == "payload"
  ):
    return True
  if (
      isinstance(node, ast.Subscript)
      and isinstance(node.value, ast.Name)
      and node.value.id == "payload"
      and isinstance(node.slice, ast.Constant)
      and node.slice.value == "outputs"
  ):
    return True
  return False


def _extract_payload_output_stage_refs(
    tree: Optional[ast.Expression],
) -> list[str]:
  """Extracts stage names referenced under `payload.outputs.<stage>` in AST."""
  if tree is None:
    return []
  refs: list[str] = []
  call_funcs = {
      node.func for node in ast.walk(tree) if isinstance(node, ast.Call)
  }
  for node in ast.walk(tree):
    if (
        isinstance(node, ast.Attribute)
        and node not in call_funcs
        and _is_payload_outputs_node(node.value)
    ):
      if node.attr not in refs:
        refs.append(node.attr)
    elif (
        isinstance(node, ast.Subscript)
        and _is_payload_outputs_node(node.value)
        and isinstance(node.slice, ast.Constant)
        and isinstance(node.slice.value, str)
    ):
      if node.slice.value not in refs:
        refs.append(node.slice.value)
  return refs


_MAX_SEQUENCE_REPEAT = 10_000
_MAX_RESULT_CHARS = 100_000


def _cap_result(value: Any) -> Any:
  """Rejects oversized str/bytes/list results to bound expression memory."""
  if isinstance(value, (str, bytes, list, tuple)):
    if len(value) > _MAX_RESULT_CHARS:
      raise ValueError(
          f"Expression result exceeds maximum length ({_MAX_RESULT_CHARS})."
      )
  return value


def _safe_mul(a: Any, b: Any) -> Any:
  """Multiplies values while capping sequence repetition to prevent OOM DoS."""
  if isinstance(a, (str, bytes, list, tuple)) and isinstance(b, int):
    if len(a) * max(b, 0) > _MAX_SEQUENCE_REPEAT:
      raise ValueError(
          "Sequence multiplication exceeds maximum length"
          f" ({_MAX_SEQUENCE_REPEAT})."
      )
  elif isinstance(b, (str, bytes, list, tuple)) and isinstance(a, int):
    if len(b) * max(a, 0) > _MAX_SEQUENCE_REPEAT:
      raise ValueError(
          "Sequence multiplication exceeds maximum length"
          f" ({_MAX_SEQUENCE_REPEAT})."
      )
  return _cap_result(operator.mul(a, b))


def _safe_add(a: Any, b: Any) -> Any:
  """Adds two values, coercing numeric/boolean primitives onto a str."""
  if isinstance(a, str) and isinstance(b, (int, float, bool)):
    return _cap_result(a + str(b))
  if isinstance(b, str) and isinstance(a, (int, float, bool)):
    return _cap_result(str(a) + b)
  return _cap_result(operator.add(a, b))


def _safe_mod(a: Any, b: Any) -> Any:
  """Numeric modulo only; printf-style `str % x` can allocate huge padding."""
  if not isinstance(a, (int, float)) or not isinstance(b, (int, float)):
    raise TypeError(
        "Operator '%' is only supported between numbers, got"
        f" {type(a).__name__} and {type(b).__name__}."
    )
  return operator.mod(a, b)


class SafeCelEvaluator:
  """A safe, lightweight CEL-like expression evaluator using Python AST.

  Supports boolean logic (&&, ||, !), comparisons, arithmetic, field access,
  string methods, and built-in size(), has(), string()/str(), int(),
  double()/float(), and bool() functions.
  """

  OPERATOR_MAP = {
      ast.Add: _safe_add,
      ast.Sub: operator.sub,
      ast.Mult: _safe_mul,
      ast.Div: operator.truediv,
      ast.Mod: _safe_mod,
      ast.Eq: operator.eq,
      ast.NotEq: operator.ne,
      ast.Lt: (
          lambda a, b: False if a is None or b is None else operator.lt(a, b)
      ),
      ast.LtE: (
          lambda a, b: False if a is None or b is None else operator.le(a, b)
      ),
      ast.Gt: (
          lambda a, b: False if a is None or b is None else operator.gt(a, b)
      ),
      ast.GtE: (
          lambda a, b: False if a is None or b is None else operator.ge(a, b)
      ),
      ast.In: lambda a, b: a in b if b is not None else False,
      ast.NotIn: lambda a, b: a not in b if b is not None else True,
  }

  UNARY_OP_MAP = {
      ast.Not: operator.not_,
      ast.Invert: operator.not_,
      ast.USub: operator.neg,
      ast.UAdd: operator.pos,
  }

  def __init__(self, context: dict[str, Any]):
    self.context = context

  def evaluate(self, expr: str) -> Any:
    """Evaluates a CEL-like string expression against context dictionary."""
    try:
      if not isinstance(expr, str):
        raise TypeError(f"Expected string, got {type(expr).__name__}")
      tree = parse_expression(expr)
      if tree is None:
        return ""
      return self._eval(tree.body)
    except Exception as e:
      raise EngineError(f"Failed to evaluate expression '{expr}': {e}") from e

  def _eval_has(self, node: ast.AST) -> bool:
    """Evaluates whether a field, attribute, or key exists and is non-null."""
    if isinstance(node, ast.Attribute):
      if node.attr.startswith("_"):
        return False
      try:
        base = self._eval(node.value)
        if base is None:
          return False
        if isinstance(base, dict):
          return node.attr in base and base[node.attr] is not None
        return hasattr(base, node.attr) and getattr(base, node.attr) is not None
      except (
          AttributeError,
          KeyError,
          IndexError,
          TypeError,
          NameError,
          ValueError,
          EngineError,
      ):
        return False
    elif isinstance(node, ast.Subscript):
      try:
        base = self._eval(node.value)
        if base is None:
          return False
        slice_val = self._eval(node.slice)
        if isinstance(slice_val, str) and slice_val.startswith("_"):
          return False
        if isinstance(base, dict):
          return slice_val in base and base[slice_val] is not None
        if isinstance(base, (list, tuple)):
          if isinstance(slice_val, int):
            return 0 <= slice_val < len(base) and base[slice_val] is not None
          return False
        return False
      except (
          AttributeError,
          KeyError,
          IndexError,
          TypeError,
          NameError,
          ValueError,
          EngineError,
      ):
        return False
    elif isinstance(node, ast.Name):
      name = node.id
      if name in self.context and self.context[name] is not None:
        return True
      return False
    return False

  def _eval(self, node: ast.AST) -> Any:
    """Evaluates an AST node recursively."""
    if isinstance(node, ast.Constant):
      return node.value

    elif isinstance(node, ast.Name):
      name = node.id
      if name == "true":
        return True
      if name == "false":
        return False
      if name in ("null", "None"):
        return None
      if name in self.context:
        return self.context[name]
      raise NameError(f"Name '{name}' is not defined in context.")

    elif isinstance(node, ast.Attribute):
      value = self._eval(node.value)
      attr = node.attr
      if attr.startswith("_"):
        raise TypeError(f"Access to private attribute '{attr}' is blocked.")

      if isinstance(value, dict):
        return value.get(attr)

      try:
        resolved = getattr(value, attr)
      except AttributeError:
        return None
      if callable(resolved):
        raise TypeError(
            f"Access to callable attribute '{attr}' outside method call is"
            " blocked."
        )
      return resolved

    elif isinstance(node, ast.Subscript):
      value = self._eval(node.value)
      slice_val = self._eval(node.slice)
      if isinstance(slice_val, str) and slice_val.startswith("_"):
        raise TypeError(
            f"Access to private subscript key '{slice_val}' is blocked."
        )
      try:
        return value[slice_val]
      except (KeyError, IndexError, TypeError):
        return None

    elif isinstance(node, ast.BinOp):
      left = self._eval(node.left)
      right = self._eval(node.right)
      op_type = type(node.op)
      if op_type in self.OPERATOR_MAP:
        return self.OPERATOR_MAP[op_type](left, right)
      raise TypeError(f"Unsupported binary operator: {op_type.__name__}")

    elif isinstance(node, ast.UnaryOp):
      operand = self._eval(node.operand)
      op_type = type(node.op)
      if op_type in self.UNARY_OP_MAP:
        return self.UNARY_OP_MAP[op_type](operand)
      raise TypeError(f"Unsupported unary operator: {op_type.__name__}")

    elif isinstance(node, ast.BoolOp):
      op_type = type(node.op)
      if op_type == ast.And:
        for val in node.values:
          if not self._eval(val):
            return False
        return True
      elif op_type == ast.Or:
        for val in node.values:
          if self._eval(val):
            return True
        return False
      raise TypeError(f"Unsupported boolean operator: {op_type.__name__}")

    elif isinstance(node, ast.Compare):
      left = self._eval(node.left)
      res = True
      current_left = left
      for op, comparator in zip(node.ops, node.comparators):
        right = self._eval(comparator)
        op_type = type(op)
        if op_type in self.OPERATOR_MAP:
          comparison_result = self.OPERATOR_MAP[op_type](current_left, right)
          res = res and comparison_result
          if not res:
            break
          current_left = right
        else:
          raise TypeError(
              f"Unsupported comparison operator: {op_type.__name__}"
          )
      return res

    elif isinstance(node, ast.Call):
      if node.keywords:
        raise TypeError(
            "Keyword arguments are not supported in CEL expressions"
        )
      if isinstance(node.func, ast.Name):
        func_name = node.func.id
        if func_name in ("size", "len"):
          if len(node.args) != 1:
            raise TypeError(
                f"{func_name}() takes exactly 1 argument ({len(node.args)}"
                " given)"
            )
          obj = self._eval(node.args[0])
          if obj is None:
            return 0
          if hasattr(obj, "__len__"):
            return len(obj)
          raise TypeError(f"Object of type '{type(obj).__name__}' has no len()")

        elif func_name == "has":
          if len(node.args) != 1:
            raise TypeError(
                f"has() takes exactly 1 argument ({len(node.args)} given)"
            )
          return self._eval_has(node.args[0])

        elif func_name in ("string", "str"):
          if len(node.args) != 1:
            raise TypeError(
                f"{func_name}() takes exactly 1 argument ({len(node.args)}"
                " given)"
            )
          val = self._eval(node.args[0])
          return "" if val is None else _cap_result(str(val))

        elif func_name == "int":
          if len(node.args) != 1:
            raise TypeError(
                f"int() takes exactly 1 argument ({len(node.args)} given)"
            )
          val = self._eval(node.args[0])
          return 0 if val is None else int(val)

        elif func_name in ("double", "float"):
          if len(node.args) != 1:
            raise TypeError(
                f"{func_name}() takes exactly 1 argument ({len(node.args)}"
                " given)"
            )
          val = self._eval(node.args[0])
          return 0.0 if val is None else float(val)

        elif func_name == "bool":
          if len(node.args) != 1:
            raise TypeError(
                f"bool() takes exactly 1 argument ({len(node.args)} given)"
            )
          return bool(self._eval(node.args[0]))

        raise TypeError(
            f"Direct function call '{func_name}()' is not supported."
        )

      elif isinstance(node.func, ast.Attribute):
        method_name = node.func.attr
        if method_name.startswith("_"):
          raise TypeError(
              f"Access to private method '{method_name}' is blocked."
          )
        obj = self._eval(node.func.value)
        args = [self._eval(arg) for arg in node.args]

        if method_name == "size":
          if args:
            raise TypeError(
                f"size() method takes 0 arguments ({len(args)} given)"
            )
          if obj is None:
            return 0
          if hasattr(obj, "__len__"):
            return len(obj)
          raise TypeError(
              f"Object of type '{type(obj).__name__}' has no size()"
          )

        if method_name == "has":
          if len(args) != 1:
            raise TypeError(
                f"has() method takes exactly 1 argument ({len(args)} given)"
            )
          if obj is None:
            return False
          attr_name = str(args[0])
          if attr_name.startswith("_"):
            return False
          if isinstance(obj, dict):
            return args[0] in obj and obj[args[0]] is not None
          return hasattr(obj, attr_name) and getattr(obj, attr_name) is not None

        if method_name in (
            "contains",
            "startsWith",
            "endsWith",
            "startswith",
            "endswith",
        ):
          if len(args) != 1:
            raise TypeError(
                f"{method_name}() method takes exactly 1 argument"
                f" ({len(args)} given)"
            )

        if obj is None:
          if method_name in (
              "contains",
              "startsWith",
              "endsWith",
              "startswith",
              "endswith",
          ):
            return False
          raise TypeError(
              f"Method '{method_name}' call not allowed or not supported on"
              " NoneType"
          )

        if method_name == "contains":
          if isinstance(obj, (str, list, tuple, dict, set)):
            return args[0] in obj
          raise TypeError(
              f"Method 'contains' not supported on {type(obj).__name__}"
          )

        if isinstance(obj, str):
          if method_name in ("startsWith", "startswith"):
            return obj.startswith(args[0])
          if method_name in ("endsWith", "endswith"):
            return obj.endswith(args[0])
          if hasattr(obj, method_name):
            method = getattr(obj, method_name)
            if method_name in (
                "replace",
                "split",
                "strip",
                "lower",
                "upper",
            ):
              return _cap_result(method(*args))

        raise TypeError(
            f"Method '{method_name}' call not allowed or not supported on"
            f" {type(obj).__name__}"
        )

      raise TypeError("Direct function calls are not supported.")

    elif isinstance(node, ast.List):
      return [self._eval(elt) for elt in node.elts]

    elif isinstance(node, ast.Tuple):
      return tuple(self._eval(elt) for elt in node.elts)

    elif isinstance(node, ast.Dict):
      res_dict = {}
      for k, v in zip(node.keys, node.values):
        if k is not None:
          res_dict[self._eval(k)] = self._eval(v)
      return res_dict

    elif isinstance(node, ast.Set):
      return {self._eval(elt) for elt in node.elts}

    raise TypeError(f"Unsupported AST node type: {type(node).__name__}")


def evaluate_cel(cel_expr: str, payload_dict: dict[str, Any]) -> Any:
  """Evaluates a CEL expression against the payload dict safely."""
  context = {
      "payload": payload_dict,
      "true": True,
      "false": False,
      "null": None,
  }
  evaluator = SafeCelEvaluator(context)
  return evaluator.evaluate(cel_expr)


# ------------------- NOTIFICATIONS & UTILS -------------------


def get_timestamp() -> schema.TimestampValue:
  ts = schema.TimestampValue()
  ts.GetCurrentTime()
  return ts


_SUMMARY_MAX_CHARS = 140


def summarise(text: str) -> str:
  """Reduces multi-line operator text to a single audit-log line."""
  stripped = text.strip()
  if not stripped:
    return ""

  first, _, rest = stripped.partition("\n")
  first = first.strip()
  overlong = len(first) > _SUMMARY_MAX_CHARS
  if overlong:
    first = first[:_SUMMARY_MAX_CHARS].rstrip()
  return first + ("..." if overlong or rest.strip() else "")


_NOTIFICATION_THREADS: list[threading.Thread] = []
_CUSTOM_NOTIFIERS: list[Callable[[str, str], None]] = []


def register_notification_handler(handler: Callable[[str, str], None]) -> None:
  """Registers a custom callback `(title, content) -> None` for notifications."""
  _CUSTOM_NOTIFIERS.append(handler)


def clear_notification_handlers() -> None:
  """Clears any custom registered notification callbacks."""
  _CUSTOM_NOTIFIERS.clear()


def send_notification(title: str, content: str) -> None:
  """Sends an asynchronous push notification to Antigravity or custom hooks."""
  for handler in list(_CUSTOM_NOTIFIERS):
    try:
      handler(title, content)
    except Exception:  # pylint: disable=broad-exception-caught
      pass

  conv_id = os.environ.get("ANTIGRAVITY_CONVERSATION_ID")
  if not conv_id:
    return

  if not shutil.which("agentapi"):
    return

  cmd = [
      "agentapi",
      "send-message",
      f"--title={title}",
      conv_id,
      content,
  ]

  _NOTIFICATION_THREADS[:] = [t for t in _NOTIFICATION_THREADS if t.is_alive()]
  prev_thread = _NOTIFICATION_THREADS[-1] if _NOTIFICATION_THREADS else None

  def run_cmd():
    if prev_thread is not None:
      try:
        prev_thread.join(timeout=5.0)
      except Exception:  # pylint: disable=broad-exception-caught
        pass
    try:
      subprocess.run(
          cmd,
          stdout=subprocess.DEVNULL,
          stderr=subprocess.DEVNULL,
          check=False,
      )
    except Exception:  # pylint: disable=broad-exception-caught
      pass

  t = threading.Thread(target=run_cmd, daemon=True)
  _NOTIFICATION_THREADS.append(t)
  t.start()


def wait_for_notifications(timeout: float = 5.0) -> None:
  """Waits for all pending notification threads to finish."""
  start_time = time.monotonic()
  for t in list(_NOTIFICATION_THREADS):
    elapsed = time.monotonic() - start_time
    remaining = timeout - elapsed
    if remaining <= 0:
      break
    t.join(timeout=remaining)
  _NOTIFICATION_THREADS.clear()


# ------------------- STAGE OUTPUT ATTRIBUTION -------------------

STAGE_OUTPUTS_KEY = "outputs"

# Message of the FAILED stamp a trigger rule assigns, without running the
# stage, when an upstream dependency failed. `status` matches it to report the
# stage as blocked rather than as a failure needing a fix.
UPSTREAM_FAILED_MESSAGE = "Failed: Upstream dependency failed."

# Terminal statuses a trigger rule can assign without running the stage, and the
# icon announced for each. Any other status (e.g. STATUS_UNSPECIFIED while
# parents are still running) is not a lifecycle transition and is not announced.
_TRIGGER_NOTIFICATION_ICONS = {
    schema.StampStatus.FAILED: "❌",
    schema.StampStatus.SKIPPED: "⏭️",
}


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
) -> None:
  """Merges stage output into payload (excluding reserved `outputs`) and records under `payload.outputs.<stage_name>`."""
  for key, value in stage_output.items():
    if key != STAGE_OUTPUTS_KEY:
      payload_dict[key] = value
  record_stage_outputs(payload_dict, stage_name, stage_output)


_LOADED_MODULE_HASHES: dict[str, bytes] = {}


def _file_fingerprint(path: str) -> Optional[bytes]:
  """Returns the SHA-256 digest of a source file, or None if unreadable."""
  try:
    with open(path, "rb") as f:
      return hashlib.sha256(f.read()).digest()
  except OSError:
    return None


def _normalize_static_kwargs(value: Any) -> Any:
  """Normalizes struct numbers: converts whole floats to int."""
  if isinstance(value, float) and value.is_integer():
    return int(value)
  if isinstance(value, dict):
    return {k: _normalize_static_kwargs(v) for k, v in value.items()}
  if isinstance(value, list):
    return [_normalize_static_kwargs(item) for item in value]
  return value


# ------------------- LIGHTFLOW ENGINE -------------------


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
    argv0 = (sys.argv[0] if sys.argv else "").replace("\\", "/")
    if argv0.endswith("lightflow/__main__.py"):
      return "python3 -m lightflow"
    if argv0.endswith("lightflow/runner.py"):
      return "python3 -m lightflow.runner"
    return ""

  _resume_target = resume_target

  def build_resume_command(
      self, stage_name: str, operator_action: schema.OperatorAction
  ) -> str:
    """Renders the copy-pasteable command that resolves a paused gate."""
    if not self.lightflow_path or not self.log_id:
      return ""

    args = [
        "resume",
        f"--lightflow={_quote_cli_arg(self.lightflow_path)}",
        f"--log_id={_quote_cli_arg(self.log_id)}",
        f"--stage={_quote_cli_arg(stage_name)}",
        "--resolution=APPROVE",
    ]
    if operator_action.json_schema:
      args.append("--payload='<JSON matching the stage json_schema>'")

    target = self._resume_target()
    prefix = f"{target} " if target else "lightflow "
    return prefix + " ".join(args)

  def compile(self) -> list[str]:
    """Sorts the workflow stages topologically and returns execution order."""
    seen_actions: set[str] = set()
    for action in self.workflow.actions:
      if not action.id or not action.id.strip():
        raise EngineError("Action definition is missing a non-empty 'id'.")
      if action.id in seen_actions:
        raise EngineError(f"Duplicate action ID '{action.id}' in workflow.")
      seen_actions.add(action.id)

    seen_stages: set[str] = set()
    ts = graphlib.TopologicalSorter()
    parsed_expressions: dict[
        str, list[tuple[str, Optional[ast.Expression]]]
    ] = {}
    for stage in self.workflow.stages:
      if not stage.name or not stage.name.strip():
        raise EngineError("Stage definition is missing a non-empty 'name'.")
      if stage.name in seen_stages:
        raise EngineError(f"Duplicate stage name '{stage.name}' in workflow.")
      seen_stages.add(stage.name)

      for dep in stage.run_after:
        if dep not in self._stages_by_name:
          raise EngineError(
              f"Stage '{stage.name}' depends on non-existent stage '{dep}'."
          )
      if stage.HasField("python_action") and stage.python_action:
        action_id = stage.python_action.action_id
        if action_id not in self._action_imports:
          raise EngineError(
              f"Stage '{stage.name}' references unregistered action ID"
              f" '{action_id}'. Defined actions:"
              f" {list(self._action_imports.keys())}"
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
          schema.check_json_schema(schema_dict)
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
        for ref_stage in _extract_payload_output_stage_refs(tree):
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
                  sys.modules.pop(mod_name, None)
              importlib.invalidate_caches()
          break

    module = importlib.import_module(module_path)
    mod_file = getattr(module, "__file__", None)
    if mod_file and os.path.isfile(mod_file):
      real_mod_file = os.path.realpath(mod_file)
      fp = _file_fingerprint(real_mod_file)
      if fp is not None:
        _LOADED_MODULE_HASHES[real_mod_file] = fp
    func = getattr(module, func_name)
    if not callable(func):
      raise TypeError(f"Object '{func_name}' is not callable.")
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
    static_kwargs = _normalize_static_kwargs(dict(action.static_kwargs))

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
      static_kwargs = _normalize_static_kwargs(dict(action.static_kwargs))
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
      raise StageTimeoutError(
          f"Stage '{stage_name}' execution timed out after {timeout}s."
      )
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
  ) -> None:
    """Runs rollback_action if defined on the stage configuration."""
    if not stage.HasField("rollback_action") or not stage.rollback_action:
      return

    action_id = stage.rollback_action.action_id
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
      passport.payload.CopyFrom(payload_dict)
      suffix = f": {rollback_msg}" if rollback_msg else "."
      failed_stamp.message += f"\nRollback executed successfully{suffix}"
      print(f"  Rollback executed successfully{suffix}")
      send_notification(
          f"Rollback Completed: {stage_name}",
          f"🔄 {stage_name}: rolled back",
      )
    except Exception as rollback_err:  # pylint: disable=broad-exception-caught
      err_trace = traceback.format_exc()
      failed_stamp.message += (
          f"\nRollback action '{action_id}' failed: {rollback_err}\n{err_trace}"
      )
      print(f"  Rollback failed: {rollback_err}")
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
      stamp = passport.stamps.add()
      stamp.stage_name = stage_name
      stamp.status = target_status
      stamp.timestamp.CopyFrom(get_timestamp())
      stamp.message = reason

      if target_status == schema.StampStatus.PAUSED:
        raise OperatorActionSuspended(reason)

      # Only terminal outcomes are lifecycle transitions worth announcing;
      # STATUS_UNSPECIFIED means the stage is still waiting on its parents.
      icon = _TRIGGER_NOTIFICATION_ICONS.get(target_status)
      if icon:
        status_name = schema.StampStatus.Name(target_status).title()
        send_notification(
            f"Stage {status_name}: {stage_name}",
            f"{icon} {stage_name}: {status_name.lower()}",
        )
      return passport

    payload_dict = passport.payload.to_dict()

    # 1. Evaluate skip condition if set
    if stage.run_if:
      try:
        should_run = evaluate_cel(stage.run_if, payload_dict)
      except Exception as e:  # pylint: disable=broad-exception-caught
        failed_stamp = passport.stamps.add()
        failed_stamp.stage_name = stage_name
        failed_stamp.status = schema.StampStatus.FAILED
        failed_stamp.timestamp.CopyFrom(get_timestamp())
        failed_stamp.message = (
            f"Condition evaluation failed: {e}\n{traceback.format_exc()}"
        )
        send_notification(
            f"Stage Failed: {stage_name}",
            f"❌ {stage_name}: failed",
        )
        return passport

      if not should_run:
        skipped_stamp = passport.stamps.add()
        skipped_stamp.stage_name = stage_name
        skipped_stamp.status = schema.StampStatus.SKIPPED
        skipped_stamp.timestamp.CopyFrom(get_timestamp())
        skipped_stamp.message = f"Skipped via condition: {stage.run_if}"
        send_notification(
            f"Stage Skipped: {stage_name}",
            f"⏭️ {stage_name}: skipped",
        )
        return passport

    # 2. Append PENDING stamp to register started state
    pending_stamp = passport.stamps.add()
    pending_stamp.stage_name = stage_name
    pending_stamp.status = schema.StampStatus.PENDING
    pending_stamp.timestamp.CopyFrom(get_timestamp())
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
        raise EngineError(
            f"Stage '{stage_name}' has a polling policy but defines no"
            " execution actions."
        )

      start_time = time.monotonic()
      attempts = 0

      while True:
        attempts += 1
        try:
          new_payload, message = self._execute_action_with_timeout(
              stage_name, action, payload_dict, timeout=tick_timeout
          )
          merge_stage_payload(payload_dict, stage_name, new_payload)
          passport.payload.CopyFrom(payload_dict)
          if save_callback:
            save_callback(passport)
        except Exception as e:  # pylint: disable=broad-exception-caught
          failed_stamp = passport.stamps.add()
          failed_stamp.stage_name = stage_name
          failed_stamp.status = schema.StampStatus.FAILED
          failed_stamp.timestamp.CopyFrom(get_timestamp())
          failed_stamp.message = (
              f"Polling tick failed: {e}\n{traceback.format_exc()}"
          )
          send_notification(
              f"Stage Failed: {stage_name}",
              f"❌ {stage_name}: failed",
          )
          self.run_rollback_if_defined(
              stage_name, stage, passport, payload_dict, failed_stamp
          )
          return passport

        try:
          condition_passed = evaluate_cel(policy.condition, payload_dict)
        except Exception as e:  # pylint: disable=broad-exception-caught
          failed_stamp = passport.stamps.add()
          failed_stamp.stage_name = stage_name
          failed_stamp.status = schema.StampStatus.FAILED
          failed_stamp.timestamp.CopyFrom(get_timestamp())
          failed_stamp.message = (
              f"Condition evaluation failed: {e}\n{traceback.format_exc()}"
          )
          send_notification(
              f"Stage Failed: {stage_name}",
              f"❌ {stage_name}: failed",
          )
          self.run_rollback_if_defined(
              stage_name, stage, passport, payload_dict, failed_stamp
          )
          return passport

        if condition_passed:
          passport.payload.CopyFrom(payload_dict)
          completed_stamp = passport.stamps.add()
          completed_stamp.stage_name = stage_name
          completed_stamp.status = schema.StampStatus.COMPLETED
          completed_stamp.timestamp.CopyFrom(get_timestamp())
          completed_stamp.message = (
              f"Polling condition met after {attempts} attempts: {message}"
              if message
              else f"Polling condition met after {attempts} attempts."
          )
          if message:
            print(f"  ℹ️ [{stage_name}] {message}")
          send_notification(
              f"Stage Completed: {stage_name}",
              f"✅ {stage_name}: completed",
          )
          return passport

        elapsed = time.monotonic() - start_time
        if (timeout > 0 and elapsed >= timeout) or (
            max_attempts > 0 and attempts >= max_attempts
        ):
          if timeout > 0 and elapsed >= timeout:
            err_msg = (
                f"Stage '{stage_name}' polling timed out after {elapsed:.1f}s"
                " waiting for external condition. Yielding control back to"
                " operator/agent. State is preserved."
            )
          else:
            err_msg = (
                f"Stage '{stage_name}' polling exceeded max attempts"
                f" ({max_attempts}). Yielding control back to operator/agent."
                " State is preserved."
            )

          failed_stamp = passport.stamps.add()
          failed_stamp.stage_name = stage_name
          failed_stamp.status = schema.StampStatus.FAILED
          failed_stamp.timestamp.CopyFrom(get_timestamp())
          failed_stamp.message = err_msg

          send_notification(
              f"Stage Failed: {stage_name}",
              f"❌ {stage_name}: failed",
          )
          self.run_rollback_if_defined(
              stage_name, stage, passport, payload_dict, failed_stamp
          )
          return passport

        if message:
          print(f"  ⏳ [{stage_name}] {message}", flush=True)
        time.sleep(interval)

    # 4. Handle Standard Python Action Execution
    elif stage.HasField("python_action") and stage.python_action:
      python_action = stage.python_action
      max_attempts = 1
      initial_backoff = 5
      backoff_multiplier = 2.0

      if stage.HasField("retry_policy") and stage.retry_policy:
        policy = stage.retry_policy
        max_attempts = (
            policy.max_attempts
            if policy.HasField("max_attempts")
            and policy.max_attempts is not None
            else 1
        )
        initial_backoff = (
            policy.initial_backoff_seconds
            if policy.HasField("initial_backoff_seconds")
            and policy.initial_backoff_seconds is not None
            else 5
        )
        backoff_multiplier = (
            policy.backoff_multiplier
            if policy.HasField("backoff_multiplier")
            and policy.backoff_multiplier is not None
            else 2.0
        )

      timeout = (
          stage.timeout_seconds if stage.HasField("timeout_seconds") else None
      )
      current_backoff = float(initial_backoff)
      attempts = 0

      while True:
        attempts += 1
        try:
          new_payload, message = self._execute_action_with_timeout(
              stage_name, python_action, payload_dict, timeout=timeout
          )

          merge_stage_payload(payload_dict, stage_name, new_payload)
          passport.payload.CopyFrom(payload_dict)

          completed_stamp = passport.stamps.add()
          completed_stamp.stage_name = stage_name
          completed_stamp.status = schema.StampStatus.COMPLETED
          completed_stamp.timestamp.CopyFrom(get_timestamp())
          if attempts > 1:
            completed_stamp.message = (
                f"{message} (completed after {attempts} attempts)"
                if message
                else f"Completed after {attempts} attempts."
            )
          else:
            completed_stamp.message = message
          if message:
            print(f"  ℹ️ [{stage_name}] {message}")
          send_notification(
              f"Stage Completed: {stage_name}",
              f"✅ {stage_name}: completed",
          )
          return passport
        except EngineError as e:
          if attempts < max_attempts and isinstance(e, StageTimeoutError):
            sleep_time = min(current_backoff, 300.0)
            jitter = random.uniform(0.9, 1.1)
            actual_sleep = sleep_time * jitter
            print(
                f"  Attempt {attempts} timed out for stage '{stage_name}': {e}."
                f" Retrying in {actual_sleep:.1f}s (capped backoff:"
                f" {sleep_time:.1f}s, backoff multiplier"
                f" {backoff_multiplier})..."
            )
            time.sleep(actual_sleep)
            current_backoff = min(current_backoff * backoff_multiplier, 300.0)
            continue

          failed_stamp = passport.stamps.add()
          failed_stamp.stage_name = stage_name
          failed_stamp.status = schema.StampStatus.FAILED
          failed_stamp.timestamp.CopyFrom(get_timestamp())
          failed_stamp.message = f"Engine error: {e}"

          send_notification(
              f"Stage Failed: {stage_name}",
              f"❌ {stage_name}: failed",
          )
          self.run_rollback_if_defined(
              stage_name, stage, passport, payload_dict, failed_stamp
          )
          raise
        except Exception as e:  # pylint: disable=broad-exception-caught
          if attempts >= max_attempts:
            failed_stamp = passport.stamps.add()
            failed_stamp.stage_name = stage_name
            failed_stamp.status = schema.StampStatus.FAILED
            failed_stamp.timestamp.CopyFrom(get_timestamp())
            failed_stamp.message = (
                f"Failed after {attempts} attempts. Last error: {e}\n"
                f"{traceback.format_exc()}"
            )

            send_notification(
                f"Stage Failed: {stage_name}",
                f"❌ {stage_name}: failed",
            )
            self.run_rollback_if_defined(
                stage_name, stage, passport, payload_dict, failed_stamp
            )
            return passport

          sleep_time = min(current_backoff, 300.0)
          jitter = random.uniform(0.9, 1.1)
          actual_sleep = sleep_time * jitter

          print(
              f"  Attempt {attempts} failed for stage '{stage_name}': {e}."
              f" Retrying in {actual_sleep:.1f}s (capped backoff:"
              f" {sleep_time:.1f}s, backoff multiplier {backoff_multiplier})..."
          )
          time.sleep(actual_sleep)
          current_backoff = min(current_backoff * backoff_multiplier, 300.0)

    # 5. Handle Operator Action Suspended State
    elif stage.HasField("operator_action") and stage.operator_action:
      operator_action = stage.operator_action
      try:
        instructions = str(
            evaluate_cel(operator_action.instructions, payload_dict)
        )
        if not instructions.strip():
          raise ValueError(
              "Operator action instructions evaluated to an empty or"
              " whitespace-only string."
          )
      except Exception as e:  # pylint: disable=broad-exception-caught
        failed_stamp = passport.stamps.add()
        failed_stamp.stage_name = stage_name
        failed_stamp.status = schema.StampStatus.FAILED
        failed_stamp.timestamp.CopyFrom(get_timestamp())
        failed_stamp.message = (
            "Operator action instructions evaluation failed:"
            f" {e}\n{traceback.format_exc()}"
        )

        send_notification(
            f"Stage Failed: {stage_name}",
            f"❌ {stage_name}: failed",
        )
        return passport

      resume_command = self.build_resume_command(stage_name, operator_action)

      paused_stamp = passport.stamps.add()
      paused_stamp.stage_name = stage_name
      paused_stamp.status = schema.StampStatus.PAUSED
      paused_stamp.timestamp.CopyFrom(get_timestamp())
      summary = summarise(instructions)
      paused_stamp.message = f"Suspended waiting for operator action: {summary}"
      paused_stamp.instructions = instructions
      paused_stamp.resume_command = resume_command

      briefing = ""
      if operator_action.json_schema:
        briefing += f"\n\nPayload Schema:\n{operator_action.json_schema}"
      if resume_command:
        briefing += f"\n\nResume with:\n{resume_command}"
      briefing += (
          "\n\nOperator Action Required:\n"
          "STOP and present the Instructions and Payload Schema above to the"
          " human operator/user. Do NOT self-approve or run 'resume' until the"
          " operator provides their explicit decision."
      )

      send_notification(
          f"Operator Action Suspended: {stage_name}",
          f"⏸️ {stage_name}: suspended",
      )

      raise OperatorActionSuspended(
          f"Lightflow execution suspended at operator action '{stage_name}'."
          f"\n\nInstructions: {instructions}{briefing}"
      )

    else:
      err_msg = f"Stage '{stage_name}' has no defined action rule."
      failed_stamp = passport.stamps.add()
      failed_stamp.stage_name = stage_name
      failed_stamp.status = schema.StampStatus.FAILED
      failed_stamp.timestamp.CopyFrom(get_timestamp())
      failed_stamp.message = err_msg

      send_notification(
          f"Stage Failed: {stage_name}",
          f"❌ {stage_name}: failed",
      )
      raise EngineError(err_msg)
