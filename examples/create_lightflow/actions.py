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
    has_dry_run_param = (
        "dry_run" in arg_names
        or fn.args.kwarg is not None
        or "dry_run" in ast.unparse(fn)
        or "offline" in ast.unparse(fn)
    )
    if has_dry_run_param:
      dry_run_capable.append(name)

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
              f"Action '{name}' returned non-JSON-serializable payload dict:"
              f" {e}"
          ) from e
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
            "compiled_stages": [],
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
