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

"""Manifest loading, JSON Schema helpers, and DAG compilation for Lightflow."""

from __future__ import annotations

import ast
import difflib
import graphlib
import json
import keyword
import os
from typing import Any, Optional

# pylint: disable=g-import-not-at-top,g-bad-import-order
try:
  from . import cel_lite
  from . import schema
except ImportError:
  from lightflow import cel_lite  # pyrefly: ignore[missing-import]
  from lightflow import schema  # pyrefly: ignore[missing-import]
# pylint: enable=g-import-not-at-top,g-bad-import-order

EngineError = cel_lite.EngineError
parse_expression = cel_lite.parse_expression
extract_payload_output_stage_refs = cel_lite.extract_payload_output_stage_refs
extract_top_level_payload_refs = cel_lite.extract_top_level_payload_refs


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


class CompilerMixin:
  """Mixin providing DAG validation, expression checking, and compilation."""

  lightflow: schema.Lightflow
  lightflow_path: Optional[str]
  _action_imports: dict[str, str]
  _stages_by_name: dict[str, schema.Stage]
  _top_level_keys_read: set[str]

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

  def _validate_actions_and_stage_names(self) -> None:
    """Validates action ID uniqueness and stage name identifier rules."""
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

  def _validate_stage(
      self,
      stage: schema.Stage,
      parsed_expressions: dict[str, list[tuple[str, Optional[ast.Expression]]]],
  ) -> None:
    """Validates a single stage's dependencies, actions, policies, and expressions."""
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
        if policy.backoff_multiplier < 1.0 or policy.backoff_multiplier > 10.0:
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
    if stage.HasField("timeout_seconds") and stage.timeout_seconds is not None:
      if stage.timeout_seconds < 0 or stage.timeout_seconds > 86400:
        raise EngineError(
            f"Stage '{stage.name}': timeout_seconds must be between 0 and"
            f" 86400, got {stage.timeout_seconds}."
        )

  def compile(self) -> list[str]:
    """Sorts the workflow stages topologically and returns execution order."""
    self._validate_actions_and_stage_names()

    ts = graphlib.TopologicalSorter()
    parsed_expressions: dict[
        str, list[tuple[str, Optional[ast.Expression]]]
    ] = {}
    for stage in self.workflow.stages:
      self._validate_stage(stage, parsed_expressions)
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
