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

"""Dry-run DAG simulation and Mermaid status diagram generation."""

from __future__ import annotations

import html
import json
import re
from typing import Any, Optional

# pylint: disable=g-import-not-at-top,g-bad-import-order
try:
  from . import engine
  from . import schema
  from . import state
except ImportError:
  from lightflow import engine  # pyrefly: ignore[missing-import]
  from lightflow import schema  # pyrefly: ignore[missing-import]
  from lightflow import state  # pyrefly: ignore[missing-import]
# pylint: enable=g-import-not-at-top,g-bad-import-order
# pylint: disable=protected-access


def generate_status_mermaid(
    workflow: schema.Lightflow,
    passport: Optional[schema.Passport] = None,
) -> str:
  """Generates a color-coded Mermaid diagram representing the workflow DAG."""
  stage_statuses = {}
  stage_messages = {}
  if passport is not None:
    for stamp in passport.stamps:
      stage_statuses[stamp.stage_name] = state.effective_status_name(stamp)
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
    status = stage_statuses.get(name, "STATUS_UNSPECIFIED")
    msg = stage_messages.get(name, "")
    rollback_suffix = ""
    if passport is not None and status in ("FAILED", "REJECTED"):
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

    if status == "COMPLETED":
      lines.append(
          f"  style {node_id} fill:#e2f0d9,stroke:#385723,stroke-width:2px"
      )
    elif status in ("SKIPPED", "BLOCKED"):
      lines.append(
          f"  style {node_id}"
          " fill:#f2f2f2,stroke:#7f7f7f,stroke-width:1px,stroke-dasharray: 5 5"
      )
    elif status in ("FAILED", "REJECTED"):
      lines.append(
          f"  style {node_id} fill:#f8cbad,stroke:#c00000,stroke-width:2px"
      )
    elif status in ("PAUSED", "WAITING"):
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
  rule_name = state._trigger_rule_name(stage_proto)
  if rule_name != "ALL_SUCCESS":
    print(f"  Trigger: {rule_name}")


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


def run_dry_run(
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
    state.LightflowRunFailedError: At least one stage would fail at runtime
      (the trace is printed first; exit 1).
    engine.EngineError: The manifest does not compile.
    ValueError: The manifest path does not exist or the payload is invalid.
  """
  effective_payload = state._resolve_payload_input(payload, payload_file)
  resolved_lightflow = lightflow or workflow
  if not resolved_lightflow:
    raise ValueError("Missing required '--lightflow' manifest path.")
  workflow_path = state._require_manifest(
      resolved_lightflow, engine.resolve_lightflow_path
  )
  workflow_proto = engine.load_lightflow(workflow_path)

  # Unlike `start`, a dry run may seed `outputs.<stage>` to trace past a
  # stage that cannot be simulated (or a gate).
  initial_payload = state._parse_payload_arg(
      effective_payload, context_label="--payload", allow_outputs=True
  )
  current_payload = engine.normalize_struct_numbers(dict(initial_payload))

  print(f"=== DRY RUN: {workflow_proto.name} ===")
  shown = engine.redact_payload(current_payload, workflow_proto.secret_keys)
  print(f"Initial Payload: {json.dumps(shown, default=str)}\n")

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
        getattr(getattr(st, "polling_policy", None), "poll_tick_action", None),
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
          shown_err = engine.redact_text(
              str(e), current_payload, workflow_proto.secret_keys
          )
          if _is_deferrable_dry_run_eval_error(
              e, output_refs, has_unsimulated_ancestor
          ):
            print(
                f"  Condition '{stage_proto.run_if}' -> [DEFERRED TO RUNTIME"
                f" ({shown_err})]"
            )
            deferred_stages.add(stage_name)
            should_run = True
          else:
            print(f"  Condition evaluation failed: {shown_err}")
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
          shown_msg = engine.redact_text(
              sim_msg, current_payload, workflow_proto.secret_keys
          )
          print(f"    Dry-Run Output: {shown_msg}")
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
          if not runner.action_accepts_dry_run(tick_action.action_id):
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
        shown_msg = engine.redact_text(
            sim_msg, current_payload, workflow_proto.secret_keys
        )
        print(f"    Dry-Run Output: {shown_msg}")
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
        if not runner.action_accepts_dry_run(
            stage_proto.python_action.action_id
        ):
          not_simulated_pairs.append(
              (stage_name, stage_proto.python_action.action_id)
          )
    elif (
        stage_proto.HasField("operator_action") and stage_proto.operator_action
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
          shown_instr = engine.redact_text(
              str(instructions), current_payload, workflow_proto.secret_keys
          )
          print(f"    Instructions: {shown_instr}")
        except Exception as e:  # pylint: disable=broad-exception-caught
          has_unsimulated_ancestor = any(
              stage_name in runner.get_downstream_dependents(u)
              for u in unsimulated_stages
          )
          shown_err = engine.redact_text(
              str(e), current_payload, workflow_proto.secret_keys
          )
          if _is_deferrable_dry_run_eval_error(
              e, instr_refs, has_unsimulated_ancestor
          ):
            print("  Action: [SUSPEND] waits at operator_action")
            print(f"    Instructions: [DEFERRED TO RUNTIME ({shown_err})]")
            deferred_stages.add(stage_name)
          else:
            print(
                "  Action: [SUSPEND] waits at operator_action (instructions"
                f" eval failed: {shown_err})"
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
      resume_command = runner.build_resume_command(stage_name, operator_action)
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
  dry_fail: Optional[state.LightflowRunFailedError] = None
  if would_fail_stages:
    print(
        f"❌ Would fail at runtime: {', '.join(would_fail_stages)}. Fix the"
        " expression(s) above before starting."
    )
    dry_fail = state.LightflowRunFailedError(
        f"Dry run: {len(would_fail_stages)} stage(s) would fail at runtime:"
        f" {', '.join(would_fail_stages)}.",
        already_reported=True,
    )
  unused_actions = sorted(
      a.id for a in workflow_proto.actions if a.id not in referenced_actions
  )
  if unused_actions:
    print(f"ℹ️ Unused action id(s) in `actions:`: {', '.join(unused_actions)}")
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
