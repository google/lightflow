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

"""Deterministic verification actions for the create_lightflow meta-workflow.

Demonstrates how Lightflow pairs human/agent `operator_action` checkpoints with
deterministic `python_action` guardrails that inspect, smoke-test, and validate
generated code and DAG contracts before allowing the workflow to advance.
"""

from __future__ import annotations

import ast
import copy
import importlib
import json
import os
import subprocess
import sys
from typing import Any

# pylint: disable=g-import-not-at-top,g-bad-import-order
try:
  from ...lightflow import engine
  from ...lightflow import lib
  from ...lightflow import schema
  from ...lightflow import visualizer
except ImportError:
  from lightflow import engine  # pyrefly: ignore[missing-import]
  from lightflow import lib  # pyrefly: ignore[missing-import]
  from lightflow import schema  # pyrefly: ignore[missing-import]
  from lightflow import visualizer  # pyrefly: ignore[missing-import]
# pylint: enable=g-import-not-at-top,g-bad-import-order


def _is_dry_run(payload: dict[str, Any], static_kwargs: dict[str, Any]) -> bool:
  """Returns True when either payload or static_kwargs requests dry_run mode."""
  return bool(payload.get("dry_run") or static_kwargs.get("dry_run"))


def _dict_literal_keys(node: ast.AST) -> set[str] | None:
  """Extracts string keys from a dict literal if all keys are string constants."""
  if not isinstance(node, ast.Dict):
    return None
  keys: set[str] = set()
  for k in node.keys:
    if not (isinstance(k, ast.Constant) and isinstance(k.value, str)):
      return None
    keys.add(k.value)
  return keys


def _extract_return_key_sets(
    fn: ast.FunctionDef,
) -> list[tuple[int, set[str]]]:
  """Collects `(lineno, key_set)` for return statements in `fn`."""
  results: list[tuple[int, set[str]]] = []

  def _return_payload_expr(val: ast.AST | None) -> ast.AST | None:
    if val is None:
      return None
    if isinstance(val, ast.Tuple) and len(val.elts) == 2:
      return val.elts[0]
    return val

  def _walk_block(stmts: list[ast.stmt], env: dict[str, set[str]]) -> bool:
    for stmt in stmts:
      if isinstance(
          stmt, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)
      ):
        continue
      if isinstance(stmt, ast.Return):
        expr = _return_payload_expr(stmt.value)
        if expr is not None:
          lit_keys = _dict_literal_keys(expr)
          if lit_keys is not None:
            results.append((stmt.lineno, lit_keys))
          elif isinstance(expr, ast.Name) and expr.id in env:
            results.append((stmt.lineno, set(env[expr.id])))
        return True
      if isinstance(stmt, ast.Raise):
        return True
      if isinstance(stmt, ast.Assign) and len(stmt.targets) == 1:
        target = stmt.targets[0]
        if isinstance(target, ast.Name):
          lit_keys = _dict_literal_keys(stmt.value)
          if lit_keys:
            env[target.id] = lit_keys
          else:
            env.pop(target.id, None)
        elif (
            isinstance(target, ast.Subscript)
            and isinstance(target.value, ast.Name)
            and target.value.id in env
        ):
          if isinstance(target.slice, ast.Constant) and isinstance(
              target.slice.value, str
          ):
            env[target.value.id] = env[target.value.id] | {target.slice.value}
          else:
            env.pop(target.value.id, None)
      elif isinstance(stmt, ast.AnnAssign) and isinstance(
          stmt.target, ast.Name
      ):
        if stmt.value is not None:
          lit_keys = _dict_literal_keys(stmt.value)
          if lit_keys:
            env[stmt.target.id] = lit_keys
          else:
            env.pop(stmt.target.id, None)
      elif (
          isinstance(stmt, ast.Expr)
          and isinstance(stmt.value, ast.Call)
          and isinstance(stmt.value.func, ast.Attribute)
          and isinstance(stmt.value.func.value, ast.Name)
          and stmt.value.func.value.id in env
      ):
        var_name = stmt.value.func.value.id
        attr = stmt.value.func.attr
        if (
            attr == "update"
            and len(stmt.value.args) == 1
            and not stmt.value.keywords
        ):
          upd_keys = _dict_literal_keys(stmt.value.args[0])
          if upd_keys is not None:
            env[var_name] = env[var_name] | upd_keys
          else:
            env.pop(var_name, None)
        elif attr in ("pop", "clear", "popitem", "setdefault", "update"):
          env.pop(var_name, None)
      elif isinstance(stmt, ast.If):
        then_env = copy.deepcopy(env)
        else_env = copy.deepcopy(env)
        then_term = _walk_block(stmt.body, then_env)
        else_term = _walk_block(stmt.orelse, else_env)
        if then_term and else_term:
          return True
        if then_term:
          env.clear()
          env.update(else_env)
        elif else_term:
          env.clear()
          env.update(then_env)
        else:
          merged: dict[str, set[str]] = {}
          for k in set(then_env) & set(else_env):
            if then_env[k] == else_env[k]:
              merged[k] = then_env[k]
          env.clear()
          env.update(merged)
      elif isinstance(stmt, (ast.For, ast.AsyncFor, ast.While)):
        _walk_block(stmt.body, copy.deepcopy(env))
        _walk_block(stmt.orelse, copy.deepcopy(env))
      elif isinstance(stmt, (ast.With, ast.AsyncWith)):
        if _walk_block(stmt.body, env):
          return True
      elif isinstance(stmt, ast.Try):
        try_env = copy.deepcopy(env)
        try_term = _walk_block(stmt.body, try_env)
        for handler in stmt.handlers:
          _walk_block(handler.body, copy.deepcopy(env))
        _walk_block(stmt.orelse, try_env)
        _walk_block(stmt.finalbody, try_env)
        if not try_term:
          env.clear()
          env.update(try_env)
    return False

  _walk_block(fn.body, {})
  return results


def _unpack_action_result(name: str, res: Any) -> dict[str, Any]:
  """Validates that `res` is `(dict, str)` or `dict` and JSON-serializable."""
  if (
      isinstance(res, tuple)
      and len(res) == 2
      and isinstance(res[0], dict)
      and isinstance(res[1], str)
  ):
    delta, _ = res
  elif isinstance(res, dict):
    delta = res
  else:
    raise ValueError(
        f"Action '{name}' must return `(dict, str)` or `dict`, got"
        f" `{type(res).__name__}`."
    )
  try:
    json.dumps(delta)
  except (TypeError, OverflowError) as e:
    raise ValueError(
        f"Action '{name}' returned non-JSON-serializable payload dict: {e}"
    ) from e
  return delta


def verify_actions(
    payload: dict[str, Any], dry_run: bool = False, **static_kwargs: Any
) -> tuple[dict[str, Any], str]:
  """Verifies that drafted Python actions parse, match signatures, and pass tests.

  Read-only verification stage: safe to re-run at any time after fixing syntax,
  return-contract, or unit test errors in the target directory.

  Args:
    payload: Workflow state payload containing `outputs.align_on_design` and
      `outputs.implement_and_test_actions`.
    dry_run: When True, simulates verification without reading target files.
    **static_kwargs: Optional static parameters.

  Returns:
    Tuple of `(payload_delta, summary_message)`.

  Raises:
    FileNotFoundError: If the actions file or test file does not exist.
    ValueError: If syntax, signature, or sample_payload contract check fails.
    RuntimeError: If unit tests fail.
  """
  raw_outputs = payload.get("outputs")
  outputs = raw_outputs if isinstance(raw_outputs, dict) else {}
  design = outputs.get("align_on_design") or payload.get("design") or payload
  actions_step = (
      outputs.get("implement_and_test_actions")
      or payload.get("actions_step")
      or payload
  )
  target_dir = os.path.abspath(str(design.get("target_dir", ".")))
  actions_rel = str(actions_step.get("actions_file", "actions.py"))
  test_rel = str(actions_step.get("test_file", "")).strip()
  sample_payload = actions_step.get("sample_payload")
  expected_actions = [str(name) for name in design.get("action_names", [])]

  if dry_run or _is_dry_run(payload, static_kwargs):
    return (
        {
            "verified_actions": expected_actions,
            "dry_run_capable": expected_actions,
            "output_keys_by_action": {},
            "tests_ran": False,
            "simulated": True,
        },
        (
            f"[DRY RUN] Would verify {len(expected_actions)} action(s) in"
            f" {os.path.join(target_dir, actions_rel)}."
        ),
    )

  actions_path = os.path.join(target_dir, actions_rel)
  if not os.path.isfile(actions_path):
    raise FileNotFoundError(
        f"Drafted actions file not found at '{actions_path}'. Write the"
        " Python actions file and run `lightflow resume`."
    )

  with open(actions_path, "r", encoding="utf-8") as f:
    source = f.read()

  try:
    tree = ast.parse(source, filename=actions_path)
  except SyntaxError as e:
    raise ValueError(
        f"Syntax error in '{actions_path}' at line {e.lineno}: {e.msg}"
    ) from e

  functions: dict[str, ast.FunctionDef | ast.AsyncFunctionDef] = {
      node.name: node
      for node in tree.body
      if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
      and not node.name.startswith("_")
  }

  missing = [name for name in expected_actions if name not in functions]
  if missing:
    raise ValueError(
        f"Missing expected public action function(s) {missing} in"
        f" '{actions_path}'. Found: {sorted(functions.keys())}."
    )

  targets_to_check = expected_actions or sorted(functions.keys())
  dry_run_capable: list[str] = []
  static_return_keys: dict[str, set[str]] = {}
  accepts_dry_run_kwarg: dict[str, bool] = {}
  for name in targets_to_check:
    fn = functions[name]
    if isinstance(fn, ast.AsyncFunctionDef):
      raise ValueError(
          f"Action '{name}' in '{actions_path}' must be a synchronous `def`"
          " function (not `async def`)."
      )
    arg_names = [a.arg for a in fn.args.args] + [
        a.arg for a in fn.args.kwonlyargs
    ]
    if not arg_names and fn.args.vararg is None:
      raise ValueError(
          f"Action '{name}' in '{actions_path}' must accept `payload` as its"
          " first parameter."
      )
    accepts_dry_run_kwarg[name] = (
        "dry_run" in arg_names or fn.args.kwarg is not None
    )
    has_dry_run_param = (
        accepts_dry_run_kwarg[name]
        or "dry_run" in ast.unparse(fn)
        or "offline" in ast.unparse(fn)
    )
    if has_dry_run_param:
      dry_run_capable.append(name)

    return_key_sets = _extract_return_key_sets(fn)
    if return_key_sets:
      first_lineno, first_keys = return_key_sets[0]
      for lineno, keys in return_key_sets[1:]:
        if keys != first_keys:
          missing_keys = sorted(first_keys - keys)
          extra_keys = sorted(keys - first_keys)
          raise ValueError(
              f"Action '{name}' in '{actions_path}' has return paths with"
              f" mismatched output keys: line {first_lineno} returns"
              f" {sorted(first_keys)} vs line {lineno} returns"
              f" {sorted(keys)} (missing={missing_keys}, extra={extra_keys})."
              " Every return path (including dry_run/offline) must return"
              " the exact same output dictionary keys."
          )
      static_return_keys[name] = first_keys

  output_keys_by_action: dict[str, list[str]] = {}
  if isinstance(sample_payload, dict):
    mod_name = os.path.splitext(os.path.basename(actions_rel))[0]
    inserted_path = False
    if target_dir not in sys.path:
      sys.path.insert(0, target_dir)
      inserted_path = True
    try:
      sys.modules.pop(mod_name, None)
      importlib.invalidate_caches()
      mod = importlib.import_module(mod_name)
      running_payload = copy.deepcopy(sample_payload)
      for name in targets_to_check:
        fn_obj = getattr(mod, name)
        res = fn_obj(copy.deepcopy(running_payload))
        delta = _unpack_action_result(name, res)
        if (
            name in static_return_keys
            and set(delta.keys()) != static_return_keys[name]
        ):
          raise ValueError(
              f"Action '{name}' in '{actions_path}' returned keys"
              f" {sorted(delta.keys())} for sample_payload, which do not"
              " match its static return path keys"
              f" {sorted(static_return_keys[name])}. Every return path must"
              " return the exact same output dictionary keys."
          )
        if accepts_dry_run_kwarg.get(name):
          dry_res = fn_obj(copy.deepcopy(running_payload), dry_run=True)
          dry_delta = _unpack_action_result(name, dry_res)
          if set(dry_delta.keys()) != set(delta.keys()):
            raise ValueError(
                f"Action '{name}' in '{actions_path}' returned different"
                f" output keys between sample_payload ({sorted(delta.keys())})"
                f" and dry_run=True ({sorted(dry_delta.keys())}). Every return"
                " path must return the exact same output dictionary keys."
            )
        elif name in dry_run_capable:
          dry_payload = copy.deepcopy(running_payload)
          dry_payload["dry_run"] = True
          dry_res = fn_obj(dry_payload)
          dry_delta = _unpack_action_result(name, dry_res)
          if set(dry_delta.keys()) != set(delta.keys()):
            raise ValueError(
                f"Action '{name}' in '{actions_path}' returned different"
                f" output keys between sample_payload ({sorted(delta.keys())})"
                f" and dry_run=True ({sorted(dry_delta.keys())}). Every return"
                " path must return the exact same output dictionary keys."
            )
        output_keys_by_action[name] = sorted(delta.keys())
        running_payload.update(delta)
        engine.record_stage_outputs(running_payload, name, delta)
    finally:
      if inserted_path and target_dir in sys.path:
        sys.path.remove(target_dir)

  tests_ran = False
  if test_rel:
    test_path = os.path.join(target_dir, test_rel)
    if not os.path.isfile(test_path):
      raise FileNotFoundError(
          f"Declared test file '{test_path}' does not exist."
      )
    env = os.environ.copy()
    existing_py_path = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = (
        f"{target_dir}{os.pathsep}{existing_py_path}"
        if existing_py_path
        else target_dir
    )
    proc = subprocess.run(
        [sys.executable, "-B", "-m", "unittest", test_rel],
        cwd=target_dir,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    if proc.returncode != 0:
      output = (proc.stderr or proc.stdout).strip()
      raise RuntimeError(f"Unit tests failed in '{test_path}':\n{output}")
    tests_ran = True

  return (
      {
          "verified_actions": targets_to_check,
          "dry_run_capable": dry_run_capable,
          "output_keys_by_action": output_keys_by_action,
          "tests_ran": tests_ran,
          "simulated": False,
      },
      (
          f"Verified {len(targets_to_check)} action(s) in '{actions_rel}'"
          f" (smoke_tested={bool(output_keys_by_action)},"
          f" tests_ran={tests_ran})."
      ),
  )


def validate_manifest_dry_run(
    payload: dict[str, Any], dry_run: bool = False, **static_kwargs: Any
) -> tuple[dict[str, Any], str]:
  """Compiles the drafted workflow manifest, cross-checks stage outputs, and runs dry_run.

  Idempotent validation stage: parses `<target_dir>/<manifest_file>`, compiles
  the DAG topological order and CEL expressions, verifies that every
  `payload.outputs.<stage>` reference points to a transitive upstream stage in
  `run_after`, verifies all registered `python_import` targets resolve to
  callables, executes `dry_run` simulation, and emits
  `<target_dir>/visualizer.html`.

  Args:
    payload: Workflow state payload containing `outputs.align_on_design` and
      `outputs.draft_and_explain_manifest`.
    dry_run: When True, simulates validation without reading target files.
    **static_kwargs: Optional static parameters.

  Returns:
    Tuple of `(payload_delta, summary_message)`.

  Raises:
    FileNotFoundError: If the workflow manifest file does not exist.
    TypeError: If a registered action import is not callable.
    ValueError: If a stage references `payload.outputs.<stage>` for a
    non-upstream stage.
  """
  raw_outputs = payload.get("outputs")
  outputs = raw_outputs if isinstance(raw_outputs, dict) else {}
  design = outputs.get("align_on_design") or payload.get("design") or payload
  manifest_step = (
      outputs.get("draft_and_explain_manifest")
      or payload.get("manifest_step")
      or payload
  )
  target_dir = os.path.abspath(str(design.get("target_dir", ".")))
  manifest_rel = str(manifest_step.get("manifest_file", "lightflow.yaml"))
  manifest_path = os.path.join(target_dir, manifest_rel)

  if dry_run or _is_dry_run(payload, static_kwargs):
    return (
        {
            "manifest_path": manifest_path,
            "workflow_name": str(design.get("workflow_name", "")),
            "compiled_stages": [],
            "resolved_imports": [],
            "visualizer_html": os.path.join(target_dir, "visualizer.html"),
            "dry_run_status": "DRY_RUN_COMPLETE",
            "simulated": True,
        },
        f"[DRY RUN] Would compile and dry-run '{manifest_path}'.",
    )

  if not os.path.isfile(manifest_path):
    raise FileNotFoundError(
        f"Workflow manifest not found at '{manifest_path}'. Create the"
        " manifest and run `lightflow resume`."
    )

  wf = schema.load_lightflow(manifest_path)
  wf_engine = engine.LightflowEngine(wf, lightflow_path=manifest_path)
  try:
    execution_order = wf_engine.compile()
  except engine.EngineError as e:
    if "payload.outputs." in str(e):
      raise ValueError(str(e)) from e
    raise

  inserted_paths: list[str] = []
  cur_dir = target_dir
  if cur_dir not in sys.path:
    sys.path.insert(0, cur_dir)
    inserted_paths.append(cur_dir)
  while os.path.isfile(os.path.join(cur_dir, "__init__.py")):
    parent_dir = os.path.dirname(cur_dir)
    if not parent_dir or parent_dir == cur_dir:
      break
    if parent_dir not in sys.path:
      sys.path.insert(0, parent_dir)
      inserted_paths.append(parent_dir)
    cur_dir = parent_dir

  resolved_imports: list[str] = []
  try:
    importlib.invalidate_caches()
    for action_def in wf.actions:
      module_path, func_name = action_def.python_import.rsplit(".", 1)
      sys.modules.pop(module_path, None)
      mod = importlib.import_module(module_path)
      fn = getattr(mod, func_name, None)
      if not callable(fn):
        raise TypeError(
            f"Action '{action_def.id}' import '{action_def.python_import}' is"
            " not callable."
        )
      resolved_imports.append(action_def.python_import)

    dry_run_report = lib.LightflowRunnerCLI().dry_run(lightflow=manifest_path)
  finally:
    for p in inserted_paths:
      if p in sys.path:
        sys.path.remove(p)

  dry_run_status = (
      dry_run_report.get("status", "DRY_RUN_COMPLETE")
      if isinstance(dry_run_report, dict)
      else "DRY_RUN_COMPLETE"
  )

  viz_path = os.path.join(target_dir, "visualizer.html")
  html_content = visualizer.generate_visualizer_html(
      lightflow=wf,
      title=f"{wf.name} — DAG Preview",
  )
  with open(viz_path, "w", encoding="utf-8") as f:
    f.write(html_content)

  return (
      {
          "manifest_path": manifest_path,
          "workflow_name": wf.name,
          "compiled_stages": execution_order,
          "resolved_imports": resolved_imports,
          "visualizer_html": viz_path,
          "dry_run_status": dry_run_status,
          "simulated": False,
      },
      (
          f"Compiled and dry-ran workflow '{wf.name}'"
          f" ({len(execution_order)} stages: {execution_order});"
          f" interactive DAG written to '{viz_path}'."
      ),
  )
