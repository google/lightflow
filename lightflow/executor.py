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

"""Action import sandboxing, dry-run simulation, timeout workers, and rollbacks."""

from __future__ import annotations

import copy
import hashlib
import importlib
import importlib.util
import inspect
import os
import queue
import sys
import threading
import time
import traceback
from typing import Any, Callable, Collection, Optional

# pylint: disable=g-import-not-at-top,g-bad-import-order
try:
  from . import cel_lite
  from . import notifications
  from . import payload as payload_lib
  from . import schema
except ImportError:
  from lightflow import cel_lite  # pyrefly: ignore[missing-import]
  from lightflow import notifications  # pyrefly: ignore[missing-import]
  from lightflow import payload as payload_lib  # pyrefly: ignore[missing-import]
  from lightflow import schema  # pyrefly: ignore[missing-import]
# pylint: enable=g-import-not-at-top,g-bad-import-order

EngineError = cel_lite.EngineError
StageTimeoutError = cel_lite.StageTimeoutError
redact_text = payload_lib.redact_text
merge_stage_payload = payload_lib.merge_stage_payload
set_payload = payload_lib.set_payload
static_kwargs_dict = payload_lib.static_kwargs_dict

_LOADED_MODULE_HASHES: dict[str, bytes] = {}


def _file_fingerprint(path: str) -> Optional[bytes]:
  """Returns the SHA-256 digest of a source file, or None if unreadable."""
  try:
    with open(path, "rb") as f:
      return hashlib.sha256(f.read()).digest()
  except OSError:
    return None


class ActionExecutorMixin:
  """Mixin providing action resolution, worker execution, and rollbacks."""

  lightflow: schema.Lightflow
  lightflow_path: Optional[str]
  _action_imports: dict[str, str]
  allowed_import_prefixes: Optional[tuple[str, ...]] = None

  def _redact(self, text: str, payload_dict: dict[str, Any]) -> str:
    """Scrubs values held under `secret_keys` from `text`."""
    return redact_text(text, payload_dict, self.lightflow.secret_keys)

  def _notify(self, title: str, message: str) -> None:
    """Dispatches a stage or rollback notification."""
    notifications.send_notification(title, message)

  def validate_import_path(
      self, action_id: str, python_import: str
  ) -> tuple[str, str]:
    """Validates `python_import` and returns `(module_path, func_name)`."""
    if "." not in python_import:
      raise EngineError(
          f"Action ID '{action_id}' has an invalid fully-qualified function"
          f" import path: '{python_import}'. Must be of the form"
          " 'package.module.function_name'."
      )
    module_path, func_name = python_import.rsplit(".", 1)
    if (
        not module_path
        or not func_name
        or any(not p for p in module_path.split("."))
    ):
      raise EngineError(
          f"Action ID '{action_id}' has an invalid fully-qualified function"
          f" import path: '{python_import}'. Must be of the form"
          " 'package.module.function_name'."
      )
    if func_name.startswith("_"):
      raise EngineError(
          f"Execution of private function '{func_name}' is blocked for"
          " security reasons."
      )
    parts = module_path.split(".")
    if any(part.startswith("_") and part != "__main__" for part in parts):
      raise EngineError(
          f"Dynamic import of private module '{module_path}' is blocked for"
          " security reasons."
      )
    self._check_import_prefix(module_path)
    return module_path, func_name

  # --- Platform hooks for action imports. ---------------------------------
  # Resolves modules relative to the manifest and honours
  # `allowed_import_prefixes`. Everything else about resolving an action is
  # shared across these four hooks.

  def _check_import_prefix(self, module_path: str) -> None:
    """Enforces `allowed_import_prefixes` when configured."""
    if not self.allowed_import_prefixes:
      return

    def _matches_prefix(mod: str, raw_prefix: str) -> bool:
      prefix = raw_prefix.rstrip(".")
      if not prefix:
        return False
      return mod == prefix or mod.startswith(prefix + ".")

    if not any(
        _matches_prefix(module_path, p) for p in self.allowed_import_prefixes
    ):
      raise EngineError(
          f"Dynamic import of module '{module_path}' is blocked for security"
          " reasons. Allowed prefixes:"
          f" {list(self.allowed_import_prefixes)}."
      )

  def _import_action_module(self, module_path: str) -> Any:
    """Imports `module_path`, preferring a sibling of the manifest when unpinned.

    Without `allowed_import_prefixes`, the manifest's directory (and the
    package root enclosing it) are put on `sys.path`, and a module already
    loaded from elsewhere -- or whose source changed on disk since it was
    loaded -- is evicted so the manifest-local edition wins. The loaded
    file's fingerprint is recorded for that staleness check.

    Args:
      module_path: Dotted module path already vetted by `validate_import_path`.

    Returns:
      The imported module.
    """
    parts = module_path.split(".")
    if self.allowed_import_prefixes is None:
      candidate_dirs = [os.getcwd()]
      local_roots: list[str] = []
      if self.lightflow_path:
        resolved_wf = schema.resolve_lightflow_path(self.lightflow_path)
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

    module = importlib.import_module(module_path)
    mod_file = getattr(module, "__file__", None)
    if mod_file and os.path.isfile(mod_file):
      real_mod_file = os.path.realpath(mod_file)
      fp = _file_fingerprint(real_mod_file)
      if fp is not None:
        _LOADED_MODULE_HASHES[real_mod_file] = fp
    return module

  def _missing_dependency_hint(self) -> str:
    """Completes "imports 'x', which ..." when an action's import is missing."""
    return (
        f"is not installed in this Python environment ({sys.executable})."
        " Install it, or guard the import"
    )

  def _missing_action_module_hint(self) -> str:
    """Trailing advice when the action module itself cannot be found."""
    looked = (
        os.path.dirname(os.path.abspath(self.lightflow_path))
        if self.lightflow_path
        else "the current directory"
    )
    return (
        "Lightflow resolves python_import relative to the manifest's"
        f" directory ({looked}) and sys.path; if the manifest moved, move its"
        " actions module with it (or set PYTHONPATH)."
    )

  def _resolve_action_callable(self, action_id: str) -> Callable[..., Any]:
    """Validates and dynamically imports a registered action callable."""
    python_import = self._action_imports.get(action_id)
    if not python_import:
      raise EngineError(
          f"Action ID '{action_id}' is not defined under workflow.actions."
      )
    module_path, func_name = self.validate_import_path(action_id, python_import)
    try:
      module = self._import_action_module(module_path)
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
            f" '{missing}', which {self._missing_dependency_hint()}. Import"
            " errors are not retried."
        ) from e
      raise EngineError(
          f"Cannot import '{python_import}' for action '{action_id}': {e}."
          f" {self._missing_action_module_hint()}"
      ) from e
    except Exception as e:  # pylint: disable=broad-exception-caught
      # SyntaxError, ImportError ("cannot import name"), or any exception the
      # module raises at import time. None of these can succeed on retry.
      raise EngineError(
          f"Cannot import '{python_import}' for action '{action_id}':"
          f" {type(e).__name__}: {e}. Import errors are not retried."
      ) from e
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
      self._notify(
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
          "  Rollback executed"
          f" successfully{self._redact(suffix, payload_dict)}"
      )
      self._notify(
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
      self._notify(
          f"Rollback Failed: {stage_name}",
          f"⚠️ {stage_name}: rollback failed",
      )
