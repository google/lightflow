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

import os
import random
import sys
import time
import traceback
from typing import Any, Callable, Optional, Sequence

# pylint: disable=g-import-not-at-top,g-bad-import-order
try:
  from . import cel_lite
  from . import compiler as compiler_lib
  from . import executor as executor_lib
  from . import notifications
  from . import payload as payload_lib
  from . import schema
except ImportError:
  from lightflow import cel_lite  # pyrefly: ignore[missing-import]
  from lightflow import compiler as compiler_lib  # pyrefly: ignore[missing-import]
  from lightflow import executor as executor_lib  # pyrefly: ignore[missing-import]
  from lightflow import notifications  # pyrefly: ignore[missing-import]
  from lightflow import payload as payload_lib  # pyrefly: ignore[missing-import]
  from lightflow import schema  # pyrefly: ignore[missing-import]
# pylint: enable=g-import-not-at-top,g-bad-import-order

# pylint: disable=protected-access
EngineError = cel_lite.EngineError
StageTimeoutError = cel_lite.StageTimeoutError
MissingFieldError = cel_lite.MissingFieldError
OperatorActionSuspended = cel_lite.OperatorActionSuspended
ManualGateSuspended = cel_lite.ManualGateSuspended

parse_expression = cel_lite.parse_expression
extract_payload_output_stage_refs = cel_lite.extract_payload_output_stage_refs
extract_top_level_payload_refs = cel_lite.extract_top_level_payload_refs
SafeCelEvaluator = cel_lite.SafeCelEvaluator
evaluate_cel = cel_lite.evaluate_cel

# ------------------- NOTIFICATIONS & UTILS -------------------

summarise = notifications.summarise
register_notification_handler = notifications.register_notification_handler
clear_notification_handlers = notifications.clear_notification_handlers
send_notification = notifications.send_notification
wait_for_notifications = notifications.wait_for_notifications

# ------------------- PAYLOAD & PASSPORT HELPERS -------------------

get_timestamp = payload_lib.get_timestamp
STAGE_OUTPUTS_KEY = payload_lib.STAGE_OUTPUTS_KEY
UPSTREAM_FAILED_MESSAGE = payload_lib.UPSTREAM_FAILED_MESSAGE
POLL_YIELD_MARKER = payload_lib.POLL_YIELD_MARKER
REDACTED = payload_lib.REDACTED
redact_text = payload_lib.redact_text
redact_payload = payload_lib.redact_payload
add_stamp = payload_lib.add_stamp
_PENDING = payload_lib._PENDING
_COMPLETED = payload_lib._COMPLETED
_SKIPPED = payload_lib._SKIPPED
_FAILED = payload_lib._FAILED
_PAUSED = payload_lib._PAUSED
record_stage_outputs = payload_lib.record_stage_outputs
merge_stage_payload = payload_lib.merge_stage_payload
top_level_key_provenance = payload_lib.top_level_key_provenance
normalize_struct_numbers = payload_lib.normalize_struct_numbers
payload_to_dict = payload_lib.payload_to_dict
set_payload = payload_lib.set_payload
PASSPORT_FORMAT_VERSION = payload_lib.PASSPORT_FORMAT_VERSION
passport_to_json = payload_lib.passport_to_json
passport_from_json = payload_lib.passport_from_json
static_kwargs_dict = payload_lib.static_kwargs_dict
effective_retry_policy = payload_lib.effective_retry_policy

# ------------------- COMPILER & EXECUTOR HELPERS -------------------

load_lightflow = compiler_lib.load_lightflow
resolve_lightflow_path = compiler_lib.resolve_lightflow_path
validate_json_schema = compiler_lib.validate_json_schema
check_json_schema = compiler_lib.check_json_schema
resume_note = compiler_lib.resume_note
CompilerMixin = compiler_lib.CompilerMixin
ActionExecutorMixin = executor_lib.ActionExecutorMixin


def _announce_failure(stage_name: str, message: str) -> None:
  """Prints a stage failure when it happens; the footer summarises it later."""
  lines = (message or "").strip().splitlines()
  print(f"  ❌ [{stage_name}] {lines[0] if lines else 'failed'}", flush=True)
  send_notification(f"Stage Failed: {stage_name}", f"❌ {stage_name}: failed")


# Terminal statuses a trigger rule can assign without running the stage, and the
# icon announced for each. Any other status (e.g. STATUS_UNSPECIFIED while
# parents are still running) is not a lifecycle transition and is not announced.
_TRIGGER_NOTIFICATION_ICONS = {
    schema.StampStatus.FAILED: "❌",
    schema.StampStatus.SKIPPED: "⏭️",
}


# ------------------- LIGHTFLOW ENGINE -------------------


class LightflowEngine(
    compiler_lib.CompilerMixin, executor_lib.ActionExecutorMixin  # pyrefly: ignore[invalid-inheritance]
):
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
    self._init_common(
        lightflow, lightflow_path, log_id, workflow, workflow_path
    )

  def _init_common(
      self,
      lightflow: Optional[schema.Lightflow],
      lightflow_path: Optional[str],
      log_id: Optional[str],
      workflow: Optional[schema.Lightflow],
      workflow_path: Optional[str],
  ) -> None:
    """Binds the manifest and builds the per-run indexes (both editions)."""
    resolved_lightflow = lightflow if lightflow is not None else workflow
    if resolved_lightflow is None:
      raise ValueError("LightflowEngine requires a `lightflow` definition.")
    self.lightflow = resolved_lightflow
    self.lightflow_path = (
        lightflow_path if lightflow_path is not None else workflow_path
    )
    self.log_id = log_id
    self._action_imports: dict[str, str] = {
        action.id: action.python_import for action in self.lightflow.actions
    }
    # Index stages by name
    self._stages_by_name: dict[str, schema.Stage] = {
        stage.name: stage for stage in self.lightflow.stages
    }
    # Top-level payload keys some expression in this lightflow reads as
    # `payload.<key>`; filled in by `compile()`. Overwrites of any other
    # key cannot change what an expression sees, so they are not warned
    # about.
    self._top_level_keys_read: set[str] = set()

  def _notify(self, title: str, message: str) -> None:
    """Dispatches a stage or rollback notification via `engine.send_notification`."""
    send_notification(title, message)

  def resume_target(self) -> str:
    """Returns the runner target or CLI command prefix to resume the lightflow."""
    if self.lightflow.runner_target:
      return self.lightflow.runner_target
    argv0 = sys.argv[0] if sys.argv else ""
    normalized = argv0.replace("\\", "/")
    # Under ``python -m pkg.mod`` the interpreter rewrites argv[0] to the
    # module's file path, so argv[0] alone cannot distinguish ``-m`` from a
    # direct ``python path/to/runner.py``; ``__main__.__spec__`` can (it is
    # only populated for ``-m`` / package execution).
    main_spec = getattr(sys.modules.get("__main__"), "__spec__", None)
    main_name = getattr(main_spec, "name", "") or ""
    if main_name == "lightflow.__main__" or main_name == "lightflow":
      return "python3 -m lightflow"
    if main_name == "lightflow.runner":
      return "python3 -m lightflow.runner"
    if normalized.endswith(("lightflow/__main__.py", "lightflow/runner.py")):
      # Direct script invocation from a source checkout: echo the path the
      # user typed so the hint is copy-pasteable from their cwd.
      return f"python3 {payload_lib._quote_cli_arg(argv0)}"
    if os.path.basename(normalized) == "lightflow":
      # Installed console script
      # (``pip install "git+https://github.com/google/lightflow.git"``).
      return "lightflow"
    return ""

  _resume_target = resume_target

  def resume_prefix(self) -> str:
    """Returns the CLI prefix that resumes this run.

    `python3 -m lightflow`, the script path, or the `lightflow` console
    script, depending on how this process was started.
    """
    return self._resume_target() or "lightflow"

  def build_resume_command(
      self, stage_name: str, operator_action: schema.OperatorAction
  ) -> str:
    """Renders the copy-pasteable command that resolves a paused gate."""
    if not self.log_id:
      return ""

    # No --lightflow: the run records its manifest on `start`, so the command
    # works from any cwd and survives a `resume --lightflow=<moved>`.
    args = [
        "resume",
        f"--log_id={payload_lib._quote_cli_arg(self.log_id)}",
        f"--stage={payload_lib._quote_cli_arg(stage_name)}",
        "--resolution=APPROVE",
    ]
    placeholder = compiler_lib._payload_placeholder(operator_action.json_schema)
    if placeholder:
      args.append(placeholder)

    return f"{self.resume_prefix()} " + " ".join(args)

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

    elif rule == schema.TriggerRule.NONE_FAILED:
      # The join after complementary `run_if` branches: skipped upstreams are
      # expected, failures still cascade.
      if failed_count > 0 and unstarted_count == 0:
        return (
            False,
            schema.StampStatus.FAILED,
            UPSTREAM_FAILED_MESSAGE,
        )
      if completed_count + skipped_count == total_deps:
        return True, schema.StampStatus.PENDING, ""
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

  def _execute_polling_stage(
      self,
      stage_name: str,
      stage: schema.Stage,
      passport: schema.Passport,
      payload_dict: dict[str, Any],
      pending_stamp: schema.Stamp,
      save_callback: Optional[Callable[[schema.Passport], None]] = None,
  ) -> schema.Passport:
    """Executes a stage configured with a `polling_policy`."""
    policy = stage.polling_policy
    assert policy is not None
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
      err_msg = (
          f"Stage '{stage_name}' has a polling policy but defines no"
          " execution actions."
      )
      add_stamp(passport, stage_name, _FAILED, err_msg)
      raise EngineError(err_msg)

    start_time = time.monotonic()
    attempts = 0

    while True:
      attempts += 1
      # Bound each tick by what is left of the overall polling budget, capped
      # by the stage timeout when one is set. Without this, an unbounded tick
      # can block past the polling timeout because that budget was only
      # checked after the tick returned.
      elapsed = time.monotonic() - start_time
      remaining = timeout - elapsed if timeout > 0 else None
      tick_budget = remaining
      if tick_timeout is not None and tick_timeout > 0:
        tick_budget = (
            tick_timeout
            if tick_budget is None
            else min(tick_budget, tick_timeout)
        )
      if tick_budget is not None:
        tick_budget = max(1, int(tick_budget))
      try:
        new_payload, message = self._execute_action_with_timeout(
            stage_name, action, payload_dict, timeout=tick_budget
        )
        payload_lib._print_merge_collisions(
            stage_name,
            merge_stage_payload(payload_dict, stage_name, new_payload),
            self._top_level_keys_read,
        )
        if message:
          pending_stamp.message = message
        set_payload(passport.payload, payload_dict)
        if save_callback:
          save_callback(passport)
      except Exception as e:  # pylint: disable=broad-exception-caught
        failed_stamp = add_stamp(passport, stage_name, _FAILED)
        failed_stamp.message = (
            f"Polling tick failed: {e}\n{traceback.format_exc()}"
        )
        _announce_failure(
            stage_name, self._redact(failed_stamp.message, payload_dict)
        )
        tick_worker = getattr(e, "worker", None)
        self.run_rollback_if_defined(
            stage_name,
            stage,
            passport,
            payload_dict,
            failed_stamp,
            live_workers=[tick_worker] if tick_worker else [],
        )
        return passport

      try:
        condition_passed = evaluate_cel(
            policy.condition, payload_dict, strict=self._strict_expressions
        )
      except Exception as e:  # pylint: disable=broad-exception-caught
        failed_stamp = add_stamp(passport, stage_name, _FAILED)
        failed_stamp.message = (
            f"Condition evaluation failed: {e}\n{traceback.format_exc()}"
        )
        _announce_failure(
            stage_name, self._redact(failed_stamp.message, payload_dict)
        )
        self.run_rollback_if_defined(
            stage_name, stage, passport, payload_dict, failed_stamp
        )
        return passport

      if condition_passed:
        set_payload(passport.payload, payload_dict)

        completed_stamp = add_stamp(passport, stage_name, _COMPLETED)
        completed_stamp.message = (
            f"Polling condition met after {attempts} attempts: {message}"
            if message
            else f"Polling condition met after {attempts} attempts."
        )
        if message:
          print(f"  ℹ️ [{stage_name}] {self._redact(message, payload_dict)}")
        send_notification(
            f"Stage Completed: {stage_name}",
            f"✅ {stage_name}: completed",
        )
        return passport

      # Echo tick progress before the threshold check so the final tick's
      # message is still visible on the console when polling yields.
      if message:
        print(
            f"  ⏳ [{stage_name}] {self._redact(message, payload_dict)}",
            flush=True,
        )

      elapsed = time.monotonic() - start_time
      if (timeout > 0 and elapsed >= timeout) or (
          max_attempts > 0 and attempts >= max_attempts
      ):
        progress_note = (
            f" (last progress: {pending_stamp.message})"
            if pending_stamp.message
            else ""
        )
        if timeout > 0 and elapsed >= timeout:
          err_msg = (
              f"Stage '{stage_name}' polling timed out after {elapsed:.1f}s"
              f" waiting for external condition{progress_note}. Yielding"
              " control back to operator/agent. State is preserved."
          )
        else:
          err_msg = (
              f"Stage '{stage_name}' polling exceeded max attempts"
              f" ({max_attempts}){progress_note}. Yielding control back to"
              " operator/agent. State is preserved."
          )

        add_stamp(
            passport,
            stage_name,
            _FAILED,
            err_msg,
            reason=schema.Stamp.POLL_YIELD,
        )

        print(
            f"  ⏳ [{stage_name}] {self._redact(err_msg, payload_dict)}",
            flush=True,
        )
        send_notification(
            f"Stage Waiting: {stage_name}",
            f"⏳ {stage_name}: waiting for an external condition",
        )
        # A yield is a pause, not a failure: the external job is still
        # running and `resume` polls it again, so no rollback here.
        return passport

      time.sleep(interval)

  def _execute_python_stage(
      self,
      stage_name: str,
      stage: schema.Stage,
      passport: schema.Passport,
      payload_dict: dict[str, Any],
  ) -> schema.Passport:
    """Executes a standard `python_action` stage with retry and rollback support."""
    python_action = stage.python_action
    assert python_action is not None
    max_attempts, initial_backoff, backoff_multiplier = effective_retry_policy(
        stage.retry_policy
        if stage.HasField("retry_policy") and stage.retry_policy
        else None
    )

    timeout = (
        stage.timeout_seconds if stage.HasField("timeout_seconds") else None
    )
    current_backoff = float(initial_backoff)
    attempts = 0
    live_workers: list[Any] = []

    while True:
      attempts += 1
      try:
        new_payload, message = self._execute_action_with_timeout(
            stage_name, python_action, payload_dict, timeout=timeout
        )

        payload_lib._print_merge_collisions(
            stage_name,
            merge_stage_payload(payload_dict, stage_name, new_payload),
            self._top_level_keys_read,
        )
        set_payload(passport.payload, payload_dict)

        completed_stamp = add_stamp(passport, stage_name, _COMPLETED)
        if attempts > 1:
          completed_stamp.message = (
              f"{message} (completed after {attempts} attempts)"
              if message
              else f"Completed after {attempts} attempts."
          )
        else:
          completed_stamp.message = message
        if message:
          print(f"  ℹ️ [{stage_name}] {self._redact(message, payload_dict)}")
        send_notification(
            f"Stage Completed: {stage_name}",
            f"✅ {stage_name}: completed",
        )
        return passport
      except EngineError as e:
        if isinstance(e, StageTimeoutError) and e.worker is not None:
          live_workers.append(e.worker)
        if attempts < max_attempts and isinstance(e, StageTimeoutError):
          sleep_time = min(current_backoff, 300.0)
          jitter = random.uniform(0.9, 1.1)
          actual_sleep = sleep_time * jitter
          # Spend the backoff letting the timed-out attempt finish; a new
          # attempt never overlaps one that is still running.
          deadline = time.monotonic() + actual_sleep
          for w in live_workers:
            w.join(timeout=max(0.0, deadline - time.monotonic()))
          if not any(w.is_alive() for w in live_workers):
            print(
                f"  Attempt {attempts} timed out for stage '{stage_name}':"
                f" {self._redact(str(e), payload_dict)}. Retrying (capped"
                f" backoff: {sleep_time:.1f}s, backoff multiplier"
                f" {backoff_multiplier})..."
            )
            time.sleep(max(0.0, deadline - time.monotonic()))
            current_backoff = min(current_backoff * backoff_multiplier, 300.0)
            continue

        failed_stamp = add_stamp(passport, stage_name, _FAILED)
        failed_stamp.message = (
            str(e) if isinstance(e, StageTimeoutError) else f"Engine error: {e}"
        )
        if attempts < max_attempts and isinstance(e, StageTimeoutError):
          failed_stamp.message += (
              " Not retried: that attempt was still running when the"
              " backoff ended, and a second one would overlap it."
          )

        _announce_failure(
            stage_name, self._redact(failed_stamp.message, payload_dict)
        )
        self.run_rollback_if_defined(
            stage_name,
            stage,
            passport,
            payload_dict,
            failed_stamp,
            live_workers=live_workers,
        )
        raise
      except Exception as e:  # pylint: disable=broad-exception-caught
        if attempts >= max_attempts:
          failed_stamp = add_stamp(passport, stage_name, _FAILED)
          failed_stamp.message = (
              f"Failed after {attempts}"
              f" attempt{'s' if attempts != 1 else ''}. Last error:"
              f" {e}\n{traceback.format_exc()}"
          )

          _announce_failure(
              stage_name, self._redact(failed_stamp.message, payload_dict)
          )
          self.run_rollback_if_defined(
              stage_name, stage, passport, payload_dict, failed_stamp
          )
          return passport

        sleep_time = min(current_backoff, 300.0)
        jitter = random.uniform(0.9, 1.1)
        actual_sleep = sleep_time * jitter

        print(
            f"  Attempt {attempts} failed for stage '{stage_name}':"
            f" {self._redact(str(e), payload_dict)}. Retrying in"
            f" {actual_sleep:.1f}s (capped backoff: {sleep_time:.1f}s,"
            f" backoff multiplier {backoff_multiplier})..."
        )
        time.sleep(actual_sleep)
        current_backoff = min(current_backoff * backoff_multiplier, 300.0)

  def _execute_operator_stage(
      self,
      stage_name: str,
      stage: schema.Stage,
      passport: schema.Passport,
      payload_dict: dict[str, Any],
  ) -> schema.Passport:
    """Suspends execution at an `operator_action` gate."""
    operator_action = stage.operator_action
    assert operator_action is not None
    try:
      instructions = str(
          evaluate_cel(
              operator_action.instructions,
              payload_dict,
              strict=self._strict_expressions,
          )
      )
      if not instructions.strip():
        raise ValueError(
            "Operator action instructions evaluated to an empty or"
            " whitespace-only string."
        )
    except Exception as e:  # pylint: disable=broad-exception-caught
      failed_stamp = add_stamp(passport, stage_name, _FAILED)
      failed_stamp.message = (
          "Operator action instructions evaluation failed:"
          f" {e}\n{traceback.format_exc()}"
      )

      _announce_failure(
          stage_name, self._redact(failed_stamp.message, payload_dict)
      )
      return passport

    resume_command = self.build_resume_command(stage_name, operator_action)

    paused_stamp = add_stamp(passport, stage_name, _PAUSED)
    summary = summarise(instructions)
    paused_stamp.message = f"Suspended waiting for operator action: {summary}"
    paused_stamp.instructions = instructions
    paused_stamp.resume_command = resume_command

    send_notification(
        f"Operator Action Suspended: {stage_name}",
        f"⏸️ {stage_name}: suspended",
    )

    raise OperatorActionSuspended(
        self.gate_briefing(
            stage_name,
            self._redact(instructions, payload_dict),
            resume_command,
        )
        + GATE_FOOTER
    )

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
      stamp = add_stamp(passport, stage_name, target_status, reason)
      if reason == UPSTREAM_FAILED_MESSAGE:
        stamp.reason = schema.Stamp.UPSTREAM_FAILED

      if target_status == schema.StampStatus.PAUSED:
        raise OperatorActionSuspended(reason)

      # Only terminal outcomes are lifecycle transitions worth announcing;
      # STATUS_UNSPECIFIED means the stage is still waiting on its parents.
      icon = _TRIGGER_NOTIFICATION_ICONS.get(target_status)
      if icon:
        if reason == UPSTREAM_FAILED_MESSAGE:
          # Collateral of an upstream failure, not a failure of its own: one
          # ❌ for the root cause is enough, so this title is not a milestone.
          print(f"  ⛔ [{stage_name}] blocked: upstream failed", flush=True)
          send_notification(
              f"Stage Blocked: {stage_name}", f"⛔ {stage_name}: blocked"
          )
        else:
          status_name = schema.StampStatus.Name(target_status).title()
          # Stamp messages carry a "Skipped: " prefix; the line already
          # says so.
          why = reason.removeprefix("Skipped: ")
          print(f"  ⏭️ [{stage_name}] skipped: {why}", flush=True)
          send_notification(
              f"Stage {status_name}: {stage_name}",
              f"{icon} {stage_name}: {status_name.lower()}",
          )
      return passport

    payload_dict = payload_to_dict(passport.payload)

    # 1. Evaluate skip condition if set
    if stage.run_if:
      try:
        should_run = evaluate_cel(
            stage.run_if, payload_dict, strict=self._strict_expressions
        )
      except Exception as e:  # pylint: disable=broad-exception-caught
        failed_stamp = add_stamp(passport, stage_name, _FAILED)
        failed_stamp.message = (
            f"Condition evaluation failed: {e}\n{traceback.format_exc()}"
        )
        _announce_failure(
            stage_name, self._redact(failed_stamp.message, payload_dict)
        )
        return passport

      if not should_run:
        add_stamp(
            passport,
            stage_name,
            _SKIPPED,
            f"Skipped via condition: {stage.run_if}",
        )
        print(
            f"  ⏭️ [{stage_name}] skipped: run_if is false ({stage.run_if})",
            flush=True,
        )
        send_notification(
            f"Stage Skipped: {stage_name}",
            f"⏭️ {stage_name}: skipped",
        )
        return passport

    # 2. Append PENDING stamp to register started state
    pending_stamp = add_stamp(passport, stage_name, _PENDING)
    if save_callback:
      save_callback(passport)

    send_notification(
        f"Stage Entered: {stage_name}",
        f"🎬 {stage_name}: entered",
    )

    if stage.HasField("polling_policy") and stage.polling_policy:
      return self._execute_polling_stage(
          stage_name,
          stage,
          passport,
          payload_dict,
          pending_stamp,
          save_callback,
      )
    if stage.HasField("python_action") and stage.python_action:
      return self._execute_python_stage(
          stage_name, stage, passport, payload_dict
      )
    if stage.HasField("operator_action") and stage.operator_action:
      return self._execute_operator_stage(
          stage_name, stage, passport, payload_dict
      )

    err_msg = f"Stage '{stage_name}' has no defined action rule."
    failed_stamp = add_stamp(passport, stage_name, _FAILED, err_msg)
    _announce_failure(
        stage_name, self._redact(failed_stamp.message, payload_dict)
    )
    raise EngineError(err_msg)

  def gate_briefing(
      self, stage_name: str, instructions: str, resume_command: str
  ) -> str:
    """Renders what an operator needs to resolve a paused gate."""
    stage = self.get_stage(stage_name)
    action = stage.operator_action if stage else None
    json_schema = action.json_schema if action else ""
    briefing = (
        f"Lightflow execution suspended at operator action '{stage_name}'."
        f"\n\nInstructions: {instructions}"
    )
    if json_schema:
      briefing += f"\n\nPayload Schema:\n{json_schema}"
    if resume_command:
      briefing += f"\n\nResume with:\n{resume_command}"
      briefing += f"\n  {resume_note(json_schema)}"
    return briefing


GATE_FOOTER = (
    "\n\nOperator action: follow the Instructions above, then resume with a"
    " payload matching the Payload Schema."
)
