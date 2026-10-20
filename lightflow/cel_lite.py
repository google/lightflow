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

"""Safe CEL-lite expression parser and AST evaluator for Lightflow."""

from __future__ import annotations

import ast
import difflib
import operator
import re
from typing import Any, Optional

# Top-level payload key under which each stage's action result is mirrored.
# Action results are also merged into the payload root for backwards
# compatibility, but that flat namespace is shared: two stages returning the
# same key silently overwrite one another, and the surviving value carries no
# record of which stage produced it. The mirror under this key is keyed by
# stage name, so a condition can name its producer explicitly -- e.g.
# `run_if: "payload.outputs.fetch_rows.row_count > 0"`.
STAGE_OUTPUTS_KEY = "outputs"


# pylint: disable=g-bad-exception-name
class EngineError(Exception):
  """Raised when workflow compilation or execution fails."""


class StageTimeoutError(EngineError):
  """Raised when a stage execution exceeds its configured timeout.

  `worker` is the abandoned thread (threads cannot be cancelled); the engine
  consults it so a retry or rollback never overlaps an attempt still running.
  """

  worker: Any = None


class MissingFieldError(EngineError):
  """Raised in strict mode when an expression selects an absent payload key.

  Only raised when the lightflow sets `strict_expressions: true`. In the
  default lenient mode an absent key evaluates to null, which makes
  `payload.aproved != true` silently true on a typo; strict mode surfaces
  that as a stage failure instead. Guard genuinely optional keys with
  `has(...)`, which never raises.
  """


class OperatorActionSuspended(Exception):
  """Raised when the workflow suspends at an operator action."""


ManualGateSuspended = OperatorActionSuspended
# pylint: enable=g-bad-exception-name


_CEL_OPERATOR_PATTERN = re.compile(
    r'("(?:\\.|[^"\\])*"|\'(?:\\.|[^\'\\])*\')|(&&|\|\||!(?!=)|~)'
)


def parse_expression(expr: str) -> Optional[ast.Expression]:
  """Normalises CEL-style operators and parses an expression.

  Split out of evaluation so that `compile()` can reject a malformed
  expression without a payload to evaluate it against, using exactly the
  grammar the evaluator will later accept.

  Args:
    expr: The expression source.

  Returns:
    The parsed tree, or None if the expression is blank.

  Raises:
    SyntaxError: If the source is not a parseable expression.
  """
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
    if op == "~":
      raise SyntaxError("Unsupported operator '~' in CEL expression.")
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


def extract_payload_output_stage_refs(
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
    elif (
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "has"
        and _is_payload_outputs_node(node.func.value)
        and len(node.args) == 1
        and isinstance(node.args[0], ast.Constant)
        and isinstance(node.args[0].value, str)
    ):
      if node.args[0].value not in refs:
        refs.append(node.args[0].value)
  return refs


def _suggest_key(missing: str, available: dict[str, Any]) -> str:
  """Renders a `; did you mean 'x'?` / `; available: a, b` hint for errors."""
  keys = sorted(str(k) for k in available)
  if not keys:
    return ""
  close = difflib.get_close_matches(missing, keys, n=1, cutoff=0.6)
  if close:
    return f"; did you mean '{close[0]}'?"
  shown = ", ".join(keys[:6]) + (", ..." if len(keys) > 6 else "")
  return f"; available: {shown}"


def extract_top_level_payload_refs(tree: Optional[ast.Expression]) -> list[str]:
  """Lists top-level payload keys an expression reads (`payload.<key>`).

  Used by dry-run to tell an author which `--payload` keys a branch needs
  before it can be traced. References inside `has(...)` are excluded, since
  their whole point is to tolerate absence. `payload.outputs.*` is excluded
  too; those are covered by `extract_payload_output_stage_refs`.

  Args:
    tree: A parsed expression, or None.

  Returns:
    Distinct key names in first-seen order.
  """
  refs: list[str] = []
  if tree is None:
    return refs

  def _is_payload(node: ast.AST) -> bool:
    return isinstance(node, ast.Name) and node.id == "payload"

  def _visit(node: ast.AST) -> None:
    if (
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "has"
    ):
      return
    key: Optional[str] = None
    if isinstance(node, ast.Attribute) and _is_payload(node.value):
      key = node.attr
    elif (
        isinstance(node, ast.Subscript)
        and _is_payload(node.value)
        and isinstance(node.slice, ast.Constant)
        and isinstance(node.slice.value, str)
    ):
      key = node.slice.value
    if key is not None and key != STAGE_OUTPUTS_KEY and key not in refs:
      refs.append(key)
    for child in ast.iter_child_nodes(node):
      _visit(child)

  _visit(tree)
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


def _safe_mul(left: Any, right: Any) -> Any:
  """Multiplies values while capping sequence repetition to prevent OOM DoS."""
  if isinstance(left, (str, bytes, list, tuple)) and isinstance(right, int):
    if len(left) * max(right, 0) > _MAX_SEQUENCE_REPEAT:
      raise ValueError(
          "Sequence multiplication exceeds maximum length"
          f" ({_MAX_SEQUENCE_REPEAT})."
      )
  elif isinstance(right, (str, bytes, list, tuple)) and isinstance(left, int):
    if len(right) * max(left, 0) > _MAX_SEQUENCE_REPEAT:
      raise ValueError(
          "Sequence multiplication exceeds maximum length"
          f" ({_MAX_SEQUENCE_REPEAT})."
      )
  return _cap_result(operator.mul(left, right))


def _safe_add(left: Any, right: Any) -> Any:
  """Adds two values, coercing numeric/boolean primitives onto a str."""
  if isinstance(left, str) and isinstance(right, (int, float, bool)):
    right = str(right)
  elif isinstance(right, str) and isinstance(left, (int, float, bool)):
    left = str(left)
  if isinstance(left, (str, bytes, list, tuple)) and isinstance(
      right, (str, bytes, list, tuple)
  ):
    if len(left) + len(right) > _MAX_RESULT_CHARS:
      raise ValueError(
          f"Expression result exceeds maximum length ({_MAX_RESULT_CHARS})."
      )
  return _cap_result(operator.add(left, right))


def _safe_replace(obj: str, args: list[Any]) -> str:
  """Calls `str.replace` after pre-checking the result length cap."""
  if (
      len(args) in (2, 3)
      and isinstance(args[0], str)
      and isinstance(args[1], str)
  ):
    old, new = args[0], args[1]
    if len(new) > len(old):
      occurrences = len(obj) + 1 if not old else obj.count(old)
      if (
          len(args) == 3
          and isinstance(args[2], int)
          and not isinstance(args[2], bool)
          and args[2] >= 0
      ):
        occurrences = min(occurrences, args[2])
      if len(obj) + occurrences * (len(new) - len(old)) > _MAX_RESULT_CHARS:
        raise ValueError(
            f"Expression result exceeds maximum length ({_MAX_RESULT_CHARS})."
        )
  return _cap_result(obj.replace(*args))


def _safe_mod(left: Any, right: Any) -> Any:
  """Numeric modulo only; printf-style `str % x` can allocate huge padding."""
  if not isinstance(left, (int, float)) or not isinstance(right, (int, float)):
    raise TypeError(
        "Operator '%' is only supported between numbers, got"
        f" {type(left).__name__} and {type(right).__name__}."
    )
  return operator.mod(left, right)


class SafeCelEvaluator:
  """A safe, lightweight CEL-like expression evaluator using Python AST.

  Supports boolean logic (&&, ||, !), comparisons, arithmetic, field access,
  string methods, and built-in size(), has(), string()/str(), int(),
  double()/float(), and bool() functions.
  """

  # pylint: disable=g-long-ternary
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
  # pylint: enable=g-long-ternary

  UNARY_OP_MAP = {
      ast.Not: operator.not_,
      ast.Invert: operator.not_,
      ast.USub: operator.neg,
      ast.UAdd: operator.pos,
  }

  def __init__(self, context: dict[str, Any], *, strict: bool = False):
    self.context = context
    self.strict = strict

  def evaluate(self, expr: str) -> Any:
    """Evaluates a CEL-like string expression against context dictionary."""
    try:
      if not isinstance(expr, str):
        # We need this check because Mock might mimic strip() but fail later
        raise TypeError(f"Expected string, got {type(expr).__name__}")
      tree = parse_expression(expr)
      if tree is None:
        return ""
      return self._eval(tree.body)
    except MissingFieldError as e:
      # Lead with the key so it survives `status`'s 140-char summary; the
      # expression (often long) follows for context.
      raise MissingFieldError(f"{e} (in expression '{expr}')") from e
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

      # Prioritize dictionary key lookups to avoid shadowing built-in dict
      # attributes (e.g. keys, values, items)
      if isinstance(value, dict):
        if self.strict and attr not in value:
          raise MissingFieldError(
              f"no such key '{attr}'{_suggest_key(attr, value)}"
              " (strict_expressions is on; guard optional keys with has(...))"
          )
        return value.get(attr)

      if self.strict and value is None:
        raise MissingFieldError(
            f"cannot select '{attr}' from null (strict_expressions is on;"
            " guard optional keys with has(...))"
        )
      try:
        resolved = getattr(value, attr)
      except AttributeError:
        if self.strict:
          raise MissingFieldError(
              f"no such field '{attr}' on {type(value).__name__}"
              " (strict_expressions is on; guard optional keys with has(...))"
          ) from None
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
      except (KeyError, IndexError, TypeError) as e:
        if self.strict:
          hint = (
              _suggest_key(str(slice_val), value)
              if isinstance(value, dict) and isinstance(slice_val, str)
              else ""
          )
          raise MissingFieldError(
              f"no such key/index {slice_val!r}{hint} (strict_expressions is"
              " on; guard optional keys with has(...))"
          ) from e
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
            if method_name == "replace":
              return _safe_replace(obj, args)
            if method_name in (
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


def evaluate_cel(
    cel_expr: str, payload_dict: dict[str, Any], *, strict: bool = False
) -> Any:
  """Evaluates a CEL expression against the payload dict safely.

  Args:
    cel_expr: The expression to evaluate.
    payload_dict: The payload exposed as `payload`.
    strict: When true, selecting an absent key raises `MissingFieldError`
      instead of yielding null (see `Lightflow.strict_expressions`).

  Returns:
    The evaluated value.
  """
  context = {
      "payload": payload_dict,
      "true": True,
      "false": False,
      "null": None,
  }
  evaluator = SafeCelEvaluator(context, strict=strict)
  return evaluator.evaluate(cel_expr)
