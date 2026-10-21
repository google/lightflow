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

"""Library for managing Lightflow OSS execution loop and Passport state."""

from __future__ import annotations

import difflib
import json
import os
import sys
from typing import Any, Optional

# pylint: disable=g-import-not-at-top,g-bad-import-order,protected-access
try:
  from . import dry_run as dry_run_lib
  from . import engine
  from . import schema
  from . import state as state_lib
  from . import visualizer
except ImportError:
  from lightflow import dry_run as dry_run_lib  # pyrefly: ignore[missing-import]
  from lightflow import engine  # pyrefly: ignore[missing-import]
  from lightflow import schema  # pyrefly: ignore[missing-import]
  from lightflow import state as state_lib  # pyrefly: ignore[missing-import]
  from lightflow import visualizer  # pyrefly: ignore[missing-import]
# pylint: enable=g-import-not-at-top,g-bad-import-order
# Re-export state and dry-run symbols for backwards compatibility.
StateCorruptedError = state_lib.StateCorruptedError
LightflowAlreadyPausedError = state_lib.LightflowAlreadyPausedError
LightflowAlreadyCompletedError = state_lib.LightflowAlreadyCompletedError
LightflowRunFailedError = state_lib.LightflowRunFailedError
LightflowStateNotFoundError = state_lib.LightflowStateNotFoundError
LightflowRunningError = state_lib.LightflowRunningError
LightflowWaitingError = state_lib.LightflowWaitingError
EXIT_FAILED = state_lib.EXIT_FAILED
EXIT_SUSPENDED = state_lib.EXIT_SUSPENDED
EXIT_NO_STATE = state_lib.EXIT_NO_STATE
EXIT_RUNNING = state_lib.EXIT_RUNNING
EXIT_WAITING = state_lib.EXIT_WAITING
_STAMP_ICONS = state_lib._STAMP_ICONS
_REJECTED_PREFIX = state_lib._REJECTED_PREFIX
_current_operator = state_lib._current_operator
PassportManager = state_lib.PassportManager
resolve_lightflow_path = state_lib.resolve_lightflow_path
_replace_stage_outputs = state_lib._replace_stage_outputs
_latest_stamp_is_poll_yield = state_lib._latest_stamp_is_poll_yield
_rearm_yielded_polls = state_lib._rearm_yielded_polls
_ROLLBACK_FAILED_RE = state_lib._ROLLBACK_FAILED_RE
_LAST_PROGRESS_RE = state_lib._LAST_PROGRESS_RE
effective_status_name = state_lib.effective_status_name
run_state = state_lib.run_state
_interrupted_stages = state_lib._interrupted_stages
_missing_top_level_keys_from_failures = (
    state_lib._missing_top_level_keys_from_failures
)
_continue_command = state_lib._continue_command
_raise_for_run_state = state_lib._raise_for_run_state
_parse_payload_arg = state_lib._parse_payload_arg
_resolve_payload_input = state_lib._resolve_payload_input
RunSummary = state_lib.RunSummary
default_state_root = state_lib.default_state_root
collect_runs = state_lib.collect_runs
_STATE_ICONS = state_lib._STATE_ICONS
_require_manifest = state_lib._require_manifest
_manifest_for_run = state_lib._manifest_for_run
_check_same_lightflow = state_lib._check_same_lightflow
_record_manifest_path = state_lib._record_manifest_path
_describe_discarded_state = state_lib._describe_discarded_state
_ran_because_of = state_lib._ran_because_of
_stamp_epoch = state_lib._stamp_epoch
_manifest_cell = state_lib._manifest_cell
_format_duration = state_lib._format_duration
_format_age = state_lib._format_age
generate_status_mermaid = dry_run_lib.generate_status_mermaid


class LightflowRunnerCLI:
  """CLI interface for executing, inspecting, and resuming Lightflow DAGs.

  Core Lifecycle & Exit Codes:
    - start --lightflow=<path> --log_id=<id> [--payload='{...}' |
      --payload=@file.json | --payload_file=<file.json>] [--dry_run]:
      Starts a new run (or previews the DAG trace with --dry_run).
      * Exit 0: Completed all stages.
      * Exit 2 (SUSPENDED): Paused at an `operator_action` checkpoint. Follow
        the printed Instructions, then `resume` with a payload matching the
        Payload Schema.
      * Exit 5 (WAITING): A polling stage yielded before its external
        condition was met. Nothing to fix; run `resume` once it is.
      * Exit 1 (FAILED): A stage failed. Fix the root cause and run `resume`
        (never `start --force`, which wipes the Passport and repeats completed
        upstream side effects; it prints what it discarded).
    - resume [--lightflow=<path>] --log_id=<id> [--stage=<gate>
      --resolution=APPROVE|REJECT --payload='{...}' | --payload=@file.json |
      --payload_file=<file.json>] [--cascade=false]:
      Resolves a paused `operator_action` gate or re-arms failed stages and
      their downstream dependents without re-running completed upstream stages.
      On a rejected gate, an explicit --resolution re-opens it and applies the
      decision in one call. --lightflow is only needed if the manifest moved
      (the new path is then recorded for later commands).
    - list [--state_dir=<dir>]:
      Lists every saved run (log ID, lightflow, state, last activity,
      manifest) plus orphan lock files. Exit 0.
    - status [--lightflow=<path>] --log_id=<id> [--verbose]:
      Prints each stage's latest status and any paused gate's Instructions,
      Payload Schema, and resume command. Exits like the run: 0 completed,
      1 failed, rejected, or interrupted, 2 paused, 3 no saved state,
      4 running in another process, 5 waiting on a polling stage. `--verbose`
      adds the Passport payload, full stamp log, and Mermaid progress diagram.
    - visualize --lightflow=<path> [--log_id=<id>] [--out=<file.html>]:
      Generates a standalone interactive HTML5 DAG & Passport visualizer.
    - cleanup --log_id=<id> [--lightflow=<path>]:
      Deletes the saved Passport state directory for `--log_id`.
  """

  def start(
      self,
      lightflow: str = "",
      log_id: str = "",
      force: bool = False,
      payload: str | dict[str, Any] = "{}",
      dry_run: bool = False,
      *,
      payload_file: Optional[str] = None,
      workflow: Optional[str] = None,
  ) -> None:
    """Starts a new run, or continues an existing one for the same log ID.

    Prints the Passport path, one progress line per stage, and on a pause the
    gate's Instructions, Payload Schema, and exact `resume` command. Exit 0
    when every stage finished, 2 when paused at an `operator_action`, 1 when
    a stage failed (the footer prints the `resume` command to continue with).

    Args:
      lightflow: Path to the manifest (YAML/JSON/textproto).
      log_id: Run identifier; state lives under `$LIGHTFLOW_STATE_DIR`.
      force: Discard any existing state for `log_id` first (re-runs completed
        stages and repeats their side effects).
      payload: Initial payload as inline JSON, `@file.json`, or `@-` (stdin).
      dry_run: Equivalent to the `dry_run` subcommand.
      payload_file: Path to a JSON file with the initial payload (alternative to
        `--payload=@file.json`).
      workflow: Deprecated alias for `lightflow`.

    Raises:
      LightflowAlreadyPausedError: The run is paused at a gate; use `resume`.
      LightflowAlreadyCompletedError: The run already completed; use `--force`
        to rerun from scratch.
      LightflowRunFailedError: A stage failed or a gate was rejected (exit 1).
      LightflowWaitingError: A polling stage yielded (exit 5).
      LightflowRunningError: Another process holds the run's lock (exit 4).
      ValueError: Bad arguments, a payload that uses a reserved key, or a
        manifest whose name differs from the run's recorded lightflow.
    """
    effective_payload = _resolve_payload_input(payload, payload_file)
    if dry_run:
      self.dry_run(
          lightflow=lightflow,
          payload=effective_payload,
          log_id=log_id,
          workflow=workflow,
      )
      return
    resolved_lightflow = lightflow or workflow
    if not resolved_lightflow:
      raise ValueError("Missing required '--lightflow' manifest path.")
    workflow_path = _require_manifest(
        resolved_lightflow, engine.resolve_lightflow_path
    )
    # Parse and compile the manifest before taking the lock so a bad path or
    # a manifest error cannot leave an orphan lock file behind.
    workflow_proto = engine.load_lightflow(workflow_path)
    pm = PassportManager(log_id)
    runner = engine.LightflowEngine(
        workflow_proto, lightflow_path=workflow_path, log_id=pm.workflow_id
    )
    try:
      ordered_stages = runner.compile()
    except Exception as e:
      raise engine.EngineError(f"Compilation Error: {e}") from e

    # Validate the payload before taking the lock, so a rejected payload
    # cannot leave an orphan lock file either.
    initial_payload = _parse_payload_arg(
        effective_payload, context_label="--payload", allow_outputs=False
    )

    with pm.lock(exclusive=True, remove_if_uninitialized=True):
      if force:
        previous = pm.load_passport()
        if previous is not None:
          print(_describe_discarded_state(pm.workflow_id, previous))
        pm.delete_state(remove_lock=False)

      passport = pm.load_passport()
      if passport:
        _check_same_lightflow(passport, workflow_proto.name, pm.workflow_id)
        if not passport.lightflow_name or not passport.lightflow_path:
          # Backfill for runs started before the manifest was recorded.
          passport.lightflow_name = workflow_proto.name
          passport.lightflow_path = os.path.abspath(workflow_path)
          pm.save_passport(passport)
        latest_status = {}
        for stamp in passport.stamps:
          latest_status[stamp.stage_name] = stamp.status

        for stage_name, status in latest_status.items():
          if status == schema.StampStatus.PAUSED:
            raise LightflowAlreadyPausedError(
                f"Lightflow is paused at operator action '{stage_name}'."
                " Please run 'resume'."
            )

        if ordered_stages and all(
            latest_status.get(stage)
            in (
                schema.StampStatus.COMPLETED,
                schema.StampStatus.SKIPPED,
            )
            for stage in ordered_stages
        ):
          raise LightflowAlreadyCompletedError(
              "Lightflow has already completed successfully. Use --force to"
              " restart."
          )

        payload_updated = False
        if initial_payload:
          payload_dict = engine.payload_to_dict(passport.payload)
          payload_dict.update(initial_payload)
          engine.set_payload(passport.payload, payload_dict)
          payload_updated = True

        rearmed = self._rearm_failed_stages(
            runner, ordered_stages, passport, operator=_current_operator()
        )
        if rearmed or payload_updated:
          pm.save_passport(passport)

      if not passport:
        passport = schema.Passport()
        passport.lightflow_name = workflow_proto.name
        passport.lightflow_path = os.path.abspath(workflow_path)
        if "operator" not in initial_payload:
          initial_payload["operator"] = _current_operator()

        engine.set_payload(passport.payload, initial_payload)
        pm.save_passport(passport)
        print(f"Starting execution of lightflow '{workflow_proto.name}'...")
      else:
        for interrupted_name in _interrupted_stages(
            ordered_stages, passport.stamps
        ):
          print(
              f"⚠️ Stage '{interrupted_name}' was interrupted mid-run and will"
              " be executed again; make sure it is idempotent."
          )
        print(f"Resuming execution of lightflow '{workflow_proto.name}'...")
      print(f"Passport: {pm.passport_file}")
      self._execute_engine_loop(runner, ordered_stages, passport, pm)

  def resume(
      self,
      lightflow: str = "",
      log_id: str = "",
      stage: Optional[str] = None,
      payload: str | dict[str, Any] = "{}",
      resolution: Optional[str] = None,
      comment: Optional[str] = None,
      operator: Optional[str] = None,
      cascade: bool = True,
      *,
      payload_file: Optional[str] = None,
      workflow: Optional[str] = None,
  ) -> None:
    """Resolves a paused gate, or continues a failed/interrupted run.

    With `--stage` naming a paused `operator_action`: records the operator's
    decision (`--resolution=APPROVE` sets `approved=true`; REJECT sets it
    false and stops the run) merged with `--payload`, then continues. Without
    `--stage`: re-arms failed stages (and their dependents) and continues
    without re-running completed stages; `--payload` keys are merged into
    the Passport first, which is how to supply a top-level key a stage failed
    on. `--lightflow` is only needed for runs that predate manifest tracking
    or whose manifest has moved.

    Args:
      lightflow: Path to the manifest; inferred from the Passport if omitted.
      log_id: Run identifier.
      stage: Paused `operator_action` stage to resolve (omit to continue a
        failed run).
      payload: JSON object, `@file.json`, or `@-`; the gate's schema is
        validated against it (plus the injected `approved`).
      resolution: `APPROVE` (default when omitted) or `REJECT`. Passing it
        explicitly on a *rejected* gate re-opens the gate and applies the
        decision in one call; omitting it only re-opens (pauses) the gate.
        Requires `--stage`.
      comment: Optional operator comment recorded on the stamp. Requires
        `--stage`.
      operator: Optional operator identity; defaults to the current user.
      cascade: When re-arming a named failed stage, also re-arm its dependents
        (default true).
      payload_file: Path to a JSON file with the payload.
      workflow: Deprecated alias for `lightflow`.

    Raises:
      LightflowStateNotFoundError: No state exists for `log_id` (exit 3).
      LightflowAlreadyCompletedError: Nothing left to run.
      LightflowRunFailedError: A stage failed or a gate was rejected (exit 1).
      LightflowWaitingError: A polling stage yielded (exit 5).
      LightflowRunningError: Another process holds the run's lock (exit 4).
      ValueError: Bad arguments, a payload that fails the gate schema, an
        unknown `--stage`, or a manifest of a different lightflow.
    """
    effective_payload = _resolve_payload_input(payload, payload_file)
    if not log_id:
      raise ValueError("Missing required '--log_id' run identifier.")
    pm = PassportManager(log_id)

    # `operator` is who decided (recorded in the payload); `actor` is the full
    # audit identity for stamp messages, keeping the session that made the
    # call visible when an operator is supplied explicitly.
    session = _current_operator()
    actor = (
        f"{operator} (via {session})"
        if operator and operator != session
        else session
    )
    if not operator:
      operator = session

    resolution_given = resolution is not None and bool(str(resolution).strip())
    norm_resolution = (
        str(resolution).upper().strip() if resolution_given else "APPROVE"
    )
    if norm_resolution not in ("APPROVE", "REJECT", "1", "2"):
      raise ValueError(
          f"Invalid resolution '{resolution}'. Must be 'APPROVE' or 'REJECT'."
      )
    is_reject = norm_resolution in ("REJECT", "2")
    comment_given = comment is not None and bool(str(comment).strip())
    if stage is None and (resolution_given or comment_given):
      raise ValueError(
          "The --resolution and --comment flags require --stage to name the"
          " paused operator_action gate they apply to. To continue a failed"
          " run, omit them; to decide a gate, pass --stage=<name>."
      )

    with pm.lock(exclusive=True, remove_if_uninitialized=True):
      passport = pm.load_passport()
      if not passport:
        raise LightflowStateNotFoundError(
            f"No execution state found for log ID '{log_id}'. Nothing to"
            " resume; use 'start' to begin a new run, or 'list' to"
            " find the right log ID."
        )
      workflow_path = _manifest_for_run(
          lightflow or workflow, passport, log_id, engine.resolve_lightflow_path
      )
      workflow_proto = engine.load_lightflow(workflow_path)
      _check_same_lightflow(passport, workflow_proto.name, pm.workflow_id)
      _record_manifest_path(
          pm,
          passport,
          workflow_path,
          workflow_proto.name,
          explicit=bool(lightflow or workflow),
      )

      latest_status = {}
      for stamp in passport.stamps:
        latest_status[stamp.stage_name] = stamp.status

      if (
          stage is not None
          and latest_status.get(stage) == schema.StampStatus.FAILED
      ):
        latest_stamp_for = {}
        for stamp in passport.stamps:
          latest_stamp_for[stamp.stage_name] = stamp
        rejected_stamp = latest_stamp_for.get(stage)
        if (
            rejected_stamp is not None
            and effective_status_name(rejected_stamp) == "REJECTED"
        ):
          decided = (
              bool(effective_payload and effective_payload not in ("{}", ""))
              or resolution_given
              or bool(comment)
          )
          if decided:
            # Re-open in place and fall through to the normal resolution path
            # below, so the caller's --resolution/--payload are applied now.
            reopened = engine.add_stamp(
                passport,
                stage,
                schema.StampStatus.PAUSED,
                f"Re-opened after rejection by {actor}.",
            )
            for prior in reversed(list(passport.stamps)):
              if (
                  prior.stage_name == stage
                  and prior.status == schema.StampStatus.PAUSED
                  and prior is not reopened
              ):
                reopened.instructions = prior.instructions
                reopened.resume_command = prior.resume_command
                break
            latest_status[stage] = schema.StampStatus.PAUSED
            # Downstream stages were stamped as blocked collateral by the
            # rejection; re-arm them so the decision can flow through.
            reopen_runner = engine.LightflowEngine(
                workflow_proto,
                lightflow_path=workflow_path,
                log_id=pm.workflow_id,
            )
            try:
              reopen_runner.compile()
            except Exception as e:
              raise engine.EngineError(f"Compilation Error: {e}") from e
            for dep in reopen_runner.get_downstream_dependents(stage):
              consequential = _ran_because_of(reopen_runner, dep, latest_status)
              if consequential:
                print(
                    f"↻ {dep} re-armed: its ALL_DONE rule ran it after the"
                    " rejection; it will run again at the end."
                )
              if consequential or latest_status.get(dep) in (
                  schema.StampStatus.FAILED,
                  schema.StampStatus.SKIPPED,
              ):
                rearmed = engine.add_stamp(
                    passport, dep, schema.StampStatus.PENDING
                )
                rearmed.message = (
                    f"Re-armed by {actor} after gate '{stage}' was re-opened."
                )
                latest_status[dep] = schema.StampStatus.PENDING
            print(
                f"Re-opening rejected gate '{stage}' to apply your decision..."
            )
          else:
            print(
                f"Re-opening rejected gate '{stage}' (no --payload/--resolution"
                " given, so it pauses there; pass them to decide in one call)."
            )

      if stage is None or (
          latest_status.get(stage) != schema.StampStatus.PAUSED
      ):
        self._resume_execution(
            workflow_proto,
            workflow_path,
            log_id,
            pm,
            passport,
            latest_status,
            stage,
            cascade,
            operator,
            payload=effective_payload,
            actor=actor,
        )
        return

      action_stage = None
      for s in workflow_proto.stages:
        if s.name == stage and s.HasField("operator_action"):
          action_stage = s.operator_action
          break

      if not action_stage:
        raise ValueError(
            f"Operator action stage '{stage}' not found in configuration."
        )

      runner = engine.LightflowEngine(
          workflow_proto, lightflow_path=workflow_path, log_id=pm.workflow_id
      )
      try:
        ordered_stages = runner.compile()
      except Exception as e:
        raise engine.EngineError(f"Compilation Error: {e}") from e

      payload_dict = engine.payload_to_dict(passport.payload)
      payload_dict["operator"] = operator

      if is_reject:
        if comment:
          payload_dict["rejection_comment"] = comment
        payload_dict["resolution"] = "REJECT"
        input_dict: dict[str, Any] = {}

        if effective_payload and effective_payload not in ("{}", ""):
          input_dict = _parse_payload_arg(
              effective_payload,
              context_label=f"Operator action stage '{stage}' resume payload",
              allow_outputs=False,
          )
          payload_dict.update(input_dict)

        payload_dict["operator"] = operator
        payload_dict["approved"] = False
        payload_dict["resolution"] = "REJECT"
        stage_output: dict[str, Any] = {
            **input_dict,
            "approved": False,
            "resolution": "REJECT",
        }
        if comment:
          stage_output["comment"] = comment
        payload_dict.pop("approval_note", None)
        _replace_stage_outputs(payload_dict, stage, stage_output)
        engine.set_payload(passport.payload, payload_dict)

        failed_stamp = engine.add_stamp(
            passport, stage, schema.StampStatus.FAILED
        )
        reject_msg = f"{_REJECTED_PREFIX}{actor}."
        if comment:
          reject_msg += f" Reason: {comment}"
        failed_stamp.message = reject_msg
        failed_stamp.reason = schema.Stamp.REJECTED

        stage_proto = runner.get_stage(stage)
        if stage_proto:
          runner.run_rollback_if_defined(
              stage, stage_proto, passport, payload_dict, failed_stamp
          )

        pm.save_passport(passport)
        engine.send_notification(
            f"Stage Rejected: {stage}", f"🛑 {stage}: rejected"
        )

        print(f"Resuming execution after rejection of stage '{stage}'...")
        print(f"Passport: {pm.passport_file}")
        self._execute_engine_loop(runner, ordered_stages, passport, pm)
        return

      input_dict = _parse_payload_arg(
          effective_payload,
          context_label=f"Operator action stage '{stage}' resume payload",
          allow_outputs=False,
      )

      approved_value = input_dict.get("approved", True)
      if not (isinstance(approved_value, bool) and approved_value):
        raise ValueError(
            f"Operator action stage '{stage}' resume payload sets"
            f" approved={input_dict['approved']!r} but --resolution is APPROVE"
            " (the default). To reject the gate, pass --resolution=REJECT; to"
            " approve it, drop 'approved' from the payload (it is set to true"
            " automatically)."
        )

      if action_stage.json_schema:
        try:
          schema_dict = json.loads(action_stage.json_schema)
        except json.JSONDecodeError as e:
          raise ValueError(
              "Failed to parse stage operator action json_schema"
              f" configuration: {e}"
          ) from e
        validation_dict = dict(input_dict)
        if isinstance(schema_dict, dict):
          props = schema_dict.get("properties")
          req_keys = schema_dict.get("required")
          if (isinstance(props, dict) and "approved" in props) or (
              isinstance(req_keys, list) and "approved" in req_keys
          ):
            input_dict.setdefault("approved", True)
            validation_dict.setdefault("approved", True)
          if isinstance(req_keys, list):
            stage_output_keys: set[str] = set()
            raw_outputs = payload_dict.get(engine.STAGE_OUTPUTS_KEY)
            if isinstance(raw_outputs, dict):
              for stage_out in raw_outputs.values():
                if isinstance(stage_out, dict):
                  stage_output_keys.update(stage_out.keys())
            for req_key in req_keys:
              if (
                  isinstance(req_key, str)
                  and req_key not in validation_dict
                  and req_key in payload_dict
                  and req_key not in stage_output_keys
                  and req_key
                  not in (
                      "outputs",
                      "operator",
                      "resolution",
                      "approved",
                      "approval_note",
                      "rejection_comment",
                  )
              ):
                validation_dict[req_key] = payload_dict[req_key]
        try:
          engine.validate_json_schema(validation_dict, schema_dict)
        except ValueError as e:
          detail = engine.redact_text(
              str(e),
              {**payload_dict, **input_dict},
              workflow_proto.secret_keys,
          )
          raise ValueError(
              "Operator action resume payload failed JSON schema"
              f" validation: {detail}"
          ) from e

      payload_dict.update(input_dict)
      if comment:
        payload_dict["approval_note"] = comment
      payload_dict["operator"] = operator
      payload_dict["approved"] = True
      payload_dict["resolution"] = "APPROVE"
      stage_output = {**input_dict, "approved": True, "resolution": "APPROVE"}
      if comment:
        stage_output["comment"] = comment
      payload_dict.pop("rejection_comment", None)
      # A fresh decision replaces (not merges with) any earlier rejection's
      # output, so stale `comment`/fields do not leak into the approval.
      _replace_stage_outputs(payload_dict, stage, stage_output)

      engine.set_payload(passport.payload, payload_dict)

      stamp = engine.add_stamp(passport, stage, schema.StampStatus.COMPLETED)
      stamp_msg = f"Resumed and approved by {actor}."
      if comment:
        stamp_msg += f" Note: {comment}"
      stamp.message = stamp_msg

      pm.save_passport(passport)
      engine.send_notification(
          f"Stage Completed: {stage}", f"✅ {stage}: completed"
      )

      print(f"Resuming execution of lightflow starting at stage '{stage}'...")
      print(f"Passport: {pm.passport_file}")
      _rearm_yielded_polls(ordered_stages, passport, actor)
      self._execute_engine_loop(runner, ordered_stages, passport, pm)

  def _resume_execution(
      self,
      workflow_proto: schema.Lightflow,
      workflow_path: str,
      log_id: str,
      pm: PassportManager,
      passport: schema.Passport,
      latest_status: dict[str, int],
      stage: Optional[str],
      cascade: bool,
      operator: str,
      payload: str | dict[str, Any] = "{}",
      *,
      actor: str = "",
  ) -> None:
    """Re-arms failed stages, then runs whatever is left of the graph."""
    del log_id
    runner = engine.LightflowEngine(
        workflow_proto, lightflow_path=workflow_path, log_id=pm.workflow_id
    )
    try:
      ordered_stages = runner.compile()
    except Exception as e:
      raise engine.EngineError(f"Compilation Error: {e}") from e

    if stage and stage not in ordered_stages:
      close = difflib.get_close_matches(stage, ordered_stages, n=1, cutoff=0.6)
      hint = f" Did you mean '{close[0]}'?" if close else ""
      raise ValueError(
          f"Unknown stage '{stage}'.{hint} Stages in this lightflow:"
          f" {ordered_stages}."
      )

    paused = sorted(
        name
        for name, status in latest_status.items()
        if status == schema.StampStatus.PAUSED
    )
    if paused and not any(
        status == schema.StampStatus.FAILED for status in latest_status.values()
    ):
      raise LightflowAlreadyPausedError(
          f"Lightflow is paused at operator action(s) {', '.join(paused)}."
          f" Answer with 'resume --stage={paused[0]}"
          " --resolution=APPROVE|REJECT'."
      )

    if ordered_stages and all(
        latest_status.get(name)
        in (
            schema.StampStatus.COMPLETED,
            schema.StampStatus.SKIPPED,
        )
        for name in ordered_stages
    ):
      raise LightflowAlreadyCompletedError(
          "Lightflow has already completed successfully. Use 'start --force'"
          " to restart."
      )

    extra_payload: dict[str, Any] = {}
    if payload and payload not in ("{}", ""):
      extra_payload = _parse_payload_arg(
          payload, context_label="Resume payload", allow_outputs=False
      )

    rearmed = self._rearm_failed_stages(
        runner, ordered_stages, passport, stage, cascade, actor or operator
    )
    if rearmed or extra_payload:
      payload_dict = engine.payload_to_dict(passport.payload)
      if extra_payload:
        payload_dict.update(extra_payload)
      payload_dict["operator"] = operator
      engine.set_payload(passport.payload, payload_dict)
      pm.save_passport(passport)

    for interrupted_name in _interrupted_stages(
        ordered_stages, passport.stamps
    ):
      print(
          f"⚠️ Stage '{interrupted_name}' was interrupted mid-run and will be"
          " executed again; make sure it is idempotent."
      )
    print(f"Resuming execution of lightflow '{workflow_proto.name}'...")
    print(f"Passport: {pm.passport_file}")
    self._execute_engine_loop(runner, ordered_stages, passport, pm)

  def _rearm_failed_stages(
      self,
      runner: engine.LightflowEngine,
      ordered_stages: list[str],
      passport: schema.Passport,
      stage: Optional[str] = None,
      cascade: bool = True,
      operator: Optional[str] = None,
  ) -> list[str]:
    """Marks failed stages eligible to run again, preserving completed work.

    Prints one summary line naming the root stage(s) re-armed and, separately,
    the downstream stages (skipped or blocked because of them) that will run
    again as a consequence.

    Args:
      runner: Compiled engine, used to walk `run_after` edges.
      ordered_stages: Topologically ordered stage names.
      passport: Passport to stamp. Mutated in place; the caller saves it.
      stage: Specific failed stage to re-arm. Defaults to every root failure.
      cascade: Also re-arm stages downstream of the re-armed ones.
      operator: LDAP or agent ID recorded on the re-arm stamp.

    Returns:
      Every re-armed stage, in topological order.
    """
    latest_status = {}
    latest_stamp = {}
    for stamp in passport.stamps:
      latest_status[stamp.stage_name] = stamp.status
      latest_stamp[stamp.stage_name] = stamp

    failed_stages = sorted(
        name
        for name, status in latest_status.items()
        if status == schema.StampStatus.FAILED
    )

    if stage:
      current = latest_status.get(stage)
      if current != schema.StampStatus.FAILED:
        status_name = (
            schema.StampStatus.Name(current).title()
            if current is not None
            else "Unstarted"
        )
        raise ValueError(
            f"Stage '{stage}' is not failed. Current status: {status_name}."
            f" Failed stages: {', '.join(failed_stages) or 'none'}."
        )
      targets = [stage]
    else:
      if not failed_stages:
        return []
      # Only root failures are re-armed implicitly: REJECTED gates need an
      # explicit --stage, and BLOCKED collateral whose upstream is still FAILED
      # is picked up below when cascade is True.
      blocked_by_failed_upstream = set()
      for failed_name in failed_stages:
        blocked_by_failed_upstream.update(
            runner.get_downstream_dependents(failed_name)
        )
      targets = [
          name
          for name in failed_stages
          if effective_status_name(latest_stamp[name]) != "REJECTED"
          and (
              effective_status_name(latest_stamp[name]) != "BLOCKED"
              or name not in blocked_by_failed_upstream
          )
      ]

    rearm = set(targets)
    # Decide the wording before PENDING stamps hide the previous state.
    repolled = {s for s in targets if _latest_stamp_is_poll_yield(passport, s)}
    reran: list[str] = []
    if cascade:
      for target in targets:
        for dep in runner.get_downstream_dependents(target):
          if latest_status.get(dep) in (
              schema.StampStatus.FAILED,
              schema.StampStatus.SKIPPED,
          ):
            rearm.add(dep)
          elif _ran_because_of(runner, dep, latest_status):
            rearm.add(dep)
            reran.append(dep)
    for dep in sorted(set(reran)):
      print(
          f"↻ {dep} re-armed: its ALL_DONE rule ran it on the failure path;"
          " it will run again at the end."
      )

    for stage_name in ordered_stages:
      if stage_name not in rearm:
        continue
      previous = schema.StampStatus.Name(latest_status[stage_name]).title()
      stamp = engine.add_stamp(passport, stage_name, schema.StampStatus.PENDING)
      stamp.message = (
          f"Re-armed for retry by {operator} (previous status: {previous})."
      )

    rearmed = [s for s in ordered_stages if s in rearm]
    if rearmed:
      failed = [s for s in rearmed if s in targets and s not in repolled]
      polls = [s for s in rearmed if s in repolled]
      downstream = [s for s in rearmed if s not in targets]
      parts = []
      if failed:
        parts.append(f"Re-armed failed stage(s): {', '.join(failed)}")
      if polls:
        parts.append(f"Polling again: {', '.join(polls)}")
      line = "; ".join(parts)
      if downstream:
        line += f" (+{len(downstream)} downstream: {', '.join(downstream)})"
      print(line)
    return rearmed

  def status(
      self,
      lightflow: str = "",
      log_id: str = "",
      verbose: bool = False,
      *,
      workflow: Optional[str] = None,
  ) -> None:
    """Prints a compact run summary and signals the run's state.

    Args:
      lightflow: Path to the Lightflow manifest. Optional when the run was
        started by a version that records the manifest path in the Passport.
      log_id: Run identifier whose Passport to inspect.
      verbose: Also print the Passport payload, the full stamp log, and the
        Mermaid progress diagram.
      workflow: Deprecated alias for `lightflow`.

    Raises:
      LightflowStateNotFoundError: No state exists for `log_id` (exit 3).
      LightflowRunningError: Another process is executing the run (exit 4).
      LightflowAlreadyPausedError: The run is paused at a gate (exit 2).
      LightflowRunFailedError: A stage failed or a gate was rejected, or the
        run was interrupted with stages unfinished (exit 1).
      LightflowWaitingError: A polling stage yielded before its condition was
        met (exit 5).
      ValueError: `--lightflow` names a different lightflow than the run's.
    """
    if not log_id:
      raise ValueError("Missing required '--log_id' run identifier.")
    pm = PassportManager(log_id)
    passport = pm.load_passport()
    if not passport:
      state_root = os.path.dirname(pm.resolved_path)
      where = (
          f"under {state_root}"
          if os.path.isdir(state_root)
          else f"under {state_root} (that directory does not exist)"
      )
      raise LightflowStateNotFoundError(
          f"No execution state found for log ID '{log_id}' {where}."
          " Check LIGHTFLOW_STATE_DIR, or run 'list'."
      )
    lightflow_path = _manifest_for_run(
        lightflow or workflow, passport, log_id, engine.resolve_lightflow_path
    )
    lightflow_proto = engine.load_lightflow(lightflow_path)
    running = pm.is_locked()
    _check_same_lightflow(passport, lightflow_proto.name, pm.workflow_id)
    explicit_path = bool(lightflow or workflow)
    recorded_path = passport.lightflow_path or ""
    if (
        not running
        and explicit_path
        and recorded_path
        and os.path.abspath(lightflow_path) != recorded_path
    ):
      # A read verb that writes: re-recording a moved manifest is the one
      # mutation `status` performs, and it does so under the run lock on a
      # fresh copy so a concurrent `resume` cannot be overwritten.
      try:
        with pm.lock(exclusive=True):
          fresh = pm.load_passport()
          if fresh is not None:
            _record_manifest_path(
                pm, fresh, lightflow_path, lightflow_proto.name, explicit=True
            )
            passport = fresh
      except LightflowRunningError:
        running = True

    runner = engine.LightflowEngine(
        lightflow_proto, lightflow_path=lightflow_path, log_id=pm.workflow_id
    )
    try:
      ordered_stages = runner.compile()
    except Exception as e:  # pylint: disable=broad-exception-caught
      # `status` only needs an order; still show the run for a manifest that
      # was edited or broke after it started.
      print(
          f"⚠️ Manifest does not compile ({engine.summarise(str(e))});"
          " listing stages in manifest order.",
          file=sys.stderr,
      )
      ordered_stages = [s.name for s in lightflow_proto.stages]

    latest: dict[str, schema.Stamp] = {}
    for stamp in passport.stamps:
      latest[stamp.stage_name] = stamp

    # Effective status per stage. A rejected gate is stamped FAILED, and so is
    # every stage the engine never ran because an upstream stage failed; tell
    # those apart from stages whose own work failed (even ones that ran after a
    # failure, like ALL_DONE cleanups), which are the only ones needing a fix.
    stages_by_name = {s.name: s for s in lightflow_proto.stages}
    effective: dict[str, str] = {}
    for stage_name in ordered_stages:
      stamp = latest.get(stage_name)
      if stamp is None:
        effective[stage_name] = ""
        continue
      effective[stage_name] = effective_status_name(stamp)

    paused = [s for s in ordered_stages if effective[s] == "PAUSED"]
    failed = [s for s in ordered_stages if effective[s] == "FAILED"]
    rejected = [s for s in ordered_stages if effective[s] == "REJECTED"]
    waiting = [s for s in ordered_stages if effective[s] == "WAITING"]
    unfinished = [
        s
        for s in ordered_stages
        if effective[s] not in ("COMPLETED", "SKIPPED")
    ]
    # Same precedence as the run footer: a decided failure outranks an open
    # gate, and an open gate outranks a polling yield.
    state = run_state(ordered_stages, passport.stamps, running=running)
    payload_dict = engine.payload_to_dict(passport.payload)
    secret_keys = lightflow_proto.secret_keys

    shown_id = pm.workflow_id
    if not os.path.isabs(os.path.expanduser(shown_id)):
      shown_id = shown_id.lower()
    print(f"Lightflow '{lightflow_proto.name}' (log ID: {shown_id}): {state}")
    print(f"Passport: {pm.passport_file}")
    for stage_name in ordered_stages:
      name = effective[stage_name]
      if not name:
        print(f"  ⬜ {stage_name}: not started")
        continue
      detail = ""
      if name in ("FAILED", "REJECTED", "PENDING", "SKIPPED", "WAITING"):
        message = engine.redact_text(
            latest[stage_name].message or "", payload_dict, secret_keys
        )
        if name == "WAITING":
          # Lead with what the operator needs: the last progress seen.
          m = _LAST_PROGRESS_RE.search(message)
          progress = m.group(1) if m else ""
          why = (
              "polling timed out"
              if "timed out" in message
              else "polling exhausted max attempts"
          )
          summary = f"{why}; last progress: {progress}" if progress else why
        else:
          summary = engine.summarise(message).removeprefix("Skipped: ")
          if "Rollback executed successfully" in message:
            summary = f"{summary} · rolled back" if summary else "rolled back"
          elif "Rollback action" in message and "failed" in message:
            summary = (
                f"{summary} · rollback FAILED" if summary else "rollback FAILED"
            )
        detail = f" ({summary})" if summary else ""
      elif name == "BLOCKED":
        detail = " (upstream failed or was rejected)"
      if name == "PENDING" and not running and not detail:
        print(
            f"  ⚠️ {stage_name}: interrupted (was in flight when the process"
            " exited; will re-run on resume)"
        )
        continue
      icon = _STAMP_ICONS.get(name, "•")
      print(f"  {icon} {stage_name}: {name.lower()}{detail}")

    for stage_name in paused:
      stamp = latest[stage_name]
      action = stages_by_name[stage_name].operator_action
      json_schema = action.json_schema if action else ""
      print(f"\nPaused at operator action '{stage_name}':")
      if stamp.instructions:
        shown_instr = engine.redact_text(
            stamp.instructions, payload_dict, secret_keys
        )
        print(f"  Instructions: {shown_instr}")
      if json_schema:
        print(f"  Payload Schema: {json_schema}")
      if stamp.resume_command:
        print(f"  Resume with: {stamp.resume_command}")
        print(f"    {engine.resume_note(json_schema)}")

    if verbose:
      print("\nPASSPORT PAYLOAD (JSON):")
      print(
          json.dumps(
              engine.redact_payload(payload_dict, secret_keys),
              indent=2,
              default=str,
          )
      )

      provenance = engine.top_level_key_provenance(payload_dict)
      if provenance:
        print("\nTOP-LEVEL KEY PROVENANCE (payload.<key> ← last writer):")
        for key, (owner, others) in sorted(provenance.items()):
          also = f"  (also set by: {', '.join(others)})" if others else ""
          print(f"  {key} ← {owner}{also}")

      print("\nSTAGE STAMPS LOG:")
      pending_since: dict[str, float] = {}
      for stamp in passport.stamps:
        status_name = schema.StampStatus.Name(stamp.status).title()
        time_str = stamp.timestamp.ToJsonString()
        redacted_msg = engine.redact_text(
            stamp.message, payload_dict, secret_keys
        )
        msg = f": {redacted_msg}" if redacted_msg else ""
        # Elapsed time from the stage's most recent PENDING stamp to this
        # terminal stamp, so slow stages stand out in the audit log.
        took = ""
        epoch = _stamp_epoch(stamp)
        if stamp.status == schema.StampStatus.PENDING:
          pending_since[stamp.stage_name] = epoch
        elif stamp.stage_name in pending_since and epoch:
          started = pending_since.pop(stamp.stage_name)
          if started:
            took = f" (took {_format_duration(epoch - started)})"
        print(
            f"  [{time_str}] Stage '{stamp.stage_name}' -> {status_name}{msg}"
            f"{took}"
        )

      print("\nLIGHTFLOW STATUS DIAGRAM:")
      print(generate_status_mermaid(lightflow_proto, passport))

    _raise_for_run_state(
        state,
        paused,
        failed,
        rejected,
        unfinished,
        resume_hint=_continue_command(
            runner,
            pm.workflow_id,
            include_lightflow=not passport.lightflow_path,
        ),
        waiting=waiting,
    )

  def dry_run(
      self,
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
      LightflowRunFailedError: At least one stage would fail at runtime (the
        trace is printed first; exit 1).
      engine.EngineError: The manifest does not compile.
      ValueError: The manifest path does not exist or the payload is invalid.
    """
    dry_run_lib.run_dry_run(
        lightflow=lightflow,
        payload=payload,
        log_id=log_id,
        payload_file=payload_file,
        workflow=workflow,
    )

  def render(
      self,
      lightflow: str = "",
      *,
      workflow: Optional[str] = None,
  ) -> None:
    """Generates a Mermaid visual representation of the lightflow DAG."""
    resolved_lightflow = lightflow or workflow
    if not resolved_lightflow:
      raise ValueError("Missing required '--lightflow' manifest path.")
    workflow_path = engine.resolve_lightflow_path(resolved_lightflow)
    workflow_proto = engine.load_lightflow(workflow_path)
    print(generate_status_mermaid(workflow_proto))

  def visualize(
      self,
      lightflow: str = "",
      log_id: Optional[str] = None,
      output: Optional[str] = None,
      title: Optional[str] = None,
      *,
      out: Optional[str] = None,
      workflow: Optional[str] = None,
  ) -> str:
    """Generates an interactive Generative UI HTML visualizer for the lightflow."""
    resolved_lightflow = lightflow or workflow
    passport_proto = None
    if log_id:
      pm = PassportManager(log_id)
      passport_proto = pm.load_passport()
      if passport_proto is not None:
        workflow_path = _manifest_for_run(
            resolved_lightflow,
            passport_proto,
            log_id,
            engine.resolve_lightflow_path,
        )
      elif not resolved_lightflow:
        raise LightflowStateNotFoundError(
            f"No workflow state found for log ID '{log_id}' in"
            f" {os.path.dirname(pm.resolved_path)} (cannot infer --lightflow)."
        )
      else:
        workflow_path = _require_manifest(
            resolved_lightflow, engine.resolve_lightflow_path
        )
    else:
      if not resolved_lightflow:
        raise ValueError("Missing required '--lightflow' manifest path.")
      workflow_path = _require_manifest(
          resolved_lightflow, engine.resolve_lightflow_path
      )
    workflow_proto = engine.load_lightflow(workflow_path)
    if passport_proto is not None:
      _check_same_lightflow(passport_proto, workflow_proto.name, log_id or "")

    html_content = visualizer.generate_visualizer_html(
        lightflow=workflow_proto,
        passport=passport_proto,
        title=title,
        standalone=True,
    )

    resolved_output = output or out
    if resolved_output:
      out_path = os.path.expanduser(resolved_output)
      os.makedirs(os.path.dirname(os.path.abspath(out_path)), exist_ok=True)
      with open(out_path, "w", encoding="utf-8") as f:
        f.write(html_content)
      print(f"Visualizer HTML generated at: {out_path}")
      return out_path

    return html_content

  def _send_status_notification(
      self,
      runner: engine.LightflowEngine,
      passport: schema.Passport,
      message: str,
      is_failed: bool = False,
  ) -> None:
    """Sends a concise status notification to the active conversation."""
    latest_status = {st.stage_name: st.status for st in passport.stamps}
    if is_failed:
      title = "Lightflow Failed"
    elif schema.StampStatus.PAUSED in latest_status.values():
      title = "Lightflow Suspended"
    elif all(
        latest_status.get(stage.name)
        in (
            schema.StampStatus.COMPLETED,
            schema.StampStatus.SKIPPED,
        )
        for stage in runner.lightflow.stages
    ):
      title = "Lightflow Completed"
    else:
      title = "Lightflow Status Update"

    engine.send_notification(title, message)

  def _execute_engine_loop(
      self,
      runner: engine.LightflowEngine,
      ordered_stages: list[str],
      passport: schema.Passport,
      pm: PassportManager,
  ) -> None:
    """Iterates through topological stages, running executable steps.

    A polling stage that yields is parked, not broken: everything downstream
    of it is left untouched (no blocked-collateral stamps) while independent
    branches keep running; `resume` re-polls it.

    Args:
      runner: Compiled engine for the manifest.
      ordered_stages: Topological stage order.
      passport: The run's Passport (persisted after every stage).
      pm: Its PassportManager.

    Raises:
      engine.OperatorActionSuspended: A gate paused the run (exit 2).
      LightflowWaitingError: Only polling yields remain (exit 5).
      LightflowRunFailedError: A stage failed or a gate was rejected (exit 1);
        the footer has already been printed (`already_reported`).
    """
    # A gate that pauses is parked exactly like a polling yield: its
    # dependents are shadowed and independent branches keep running, so one
    # run surfaces every failure and every open gate, not just the first.
    waiting_shadow: set[str] = set()
    latest_status = {
        stamp.stage_name: stamp.status for stamp in passport.stamps
    }
    for name, status in latest_status.items():
      if status == schema.StampStatus.PAUSED or _latest_stamp_is_poll_yield(
          passport, name
      ):
        waiting_shadow.update(runner.get_downstream_dependents(name))
    for stage_name in ordered_stages:
      latest_status = {
          stamp.stage_name: stamp.status for stamp in passport.stamps
      }

      if stage_name in waiting_shadow or latest_status.get(stage_name) in (
          schema.StampStatus.COMPLETED,
          schema.StampStatus.SKIPPED,
          schema.StampStatus.FAILED,
          schema.StampStatus.PAUSED,
      ):
        continue

      try:
        passport = runner.execute_stage(
            stage_name, passport, save_callback=pm.save_passport
        )
        pm.save_passport(passport)
        if _latest_stamp_is_poll_yield(passport, stage_name):
          waiting_shadow.update(runner.get_downstream_dependents(stage_name))
      except engine.OperatorActionSuspended:
        pm.save_passport(passport)
        waiting_shadow.update(runner.get_downstream_dependents(stage_name))
      except (engine.StageTimeoutError, engine.EngineError) as err:
        # The engine stamps FAILED before raising; make sure of it, so the
        # footer and `status` never see an unfinished stage after an error.
        if (
            not passport.stamps
            or passport.stamps[-1].stage_name != stage_name
            or passport.stamps[-1].status != schema.StampStatus.FAILED
        ):
          engine.add_stamp(
              passport, stage_name, schema.StampStatus.FAILED, str(err)
          )
        # Stage failure or timeout has already been stamped as FAILED (and
        # rolled back if configured) inside execute_stage. Persist the passport
        # and continue walking the DAG so independent branches and ALL_DONE
        # cleanup stages can still execute before failing the overall workflow
        # at the end.
        pm.save_passport(passport)
      except Exception as e:
        pm.save_passport(passport)
        redacted_err = engine.redact_text(
            str(e),
            engine.payload_to_dict(passport.payload),
            runner.workflow.secret_keys,
        )
        self._send_status_notification(
            runner,
            passport,
            f"Execution Failed: {redacted_err}",
            is_failed=True,
        )
        print(f"\nExecution Failed: {redacted_err}")
        raise

    latest_status = {}
    for stamp in passport.stamps:
      latest_status[stamp.stage_name] = stamp.status

    failed_stages = []
    for s_name in ordered_stages:
      if latest_status.get(s_name) == schema.StampStatus.FAILED:
        failed_stages.append(s_name)
    latest_stamp_by_stage = {
        stamp.stage_name: stamp for stamp in passport.stamps
    }

    # Open gates win over polling yields, but a genuine failure or rejection
    # wins over an open gate: the footer below then lists the gate too.
    paused = [
        s
        for s in ordered_stages
        if latest_status.get(s) == schema.StampStatus.PAUSED
    ]
    kinds = {
        n: effective_status_name(latest_stamp_by_stage[n])
        for n in failed_stages
    }
    yielded = [n for n in failed_stages if kinds[n] == "WAITING"]
    rejected = [n for n in failed_stages if kinds[n] == "REJECTED"]
    blocked = [n for n in failed_stages if kinds[n] == "BLOCKED"]
    roots = [n for n in failed_stages if kinds[n] == "FAILED"]
    payload_dict = (
        engine.payload_to_dict(passport.payload)
        if paused or failed_stages
        else {}
    )
    secret_keys = runner.workflow.secret_keys

    def _redact(text: str) -> str:
      return engine.redact_text(text, payload_dict, secret_keys)

    if paused and not (roots or rejected):
      briefing = "\n\n".join(
          runner.gate_briefing(
              n,
              _redact(latest_stamp_by_stage[n].instructions),
              latest_stamp_by_stage[n].resume_command,
          )
          for n in paused
      )
      if yielded:
        briefing += (
            "\n\n⏳ Also waiting on external condition(s):"
            f" {', '.join(yielded)} (re-polled on resume)."
        )
      raise engine.OperatorActionSuspended(briefing + engine.GATE_FOOTER)

    if failed_stages:
      def _msg(name: str) -> str:
        stamp = latest_stamp_by_stage.get(name)
        return (stamp.message or "") if stamp else ""

      resume_cmd = _continue_command(
          runner,
          pm.workflow_id,
          include_lightflow=not passport.lightflow_path,
      )

      # --- Parked on an external condition: not a failure.
      if yielded and not roots and not rejected:
        name = yielded[0]
        m = _LAST_PROGRESS_RE.search(_msg(name))
        progress = f" Last progress: {_redact(m.group(1))}." if m else ""
        headline = (
            f"⏳ Lightflow is waiting: polling stage '{name}' yielded before"
            f" its condition was met.{progress}"
        )
        self._send_status_notification(runner, passport, headline)
        print(f"\n{headline}")
        print(
            "State is preserved. Once the external condition is satisfied,"
            f" continue with:\n  {resume_cmd}"
        )
        raise LightflowWaitingError(headline, already_reported=True)

      # --- Operator said no: terminal unless deliberately re-opened.
      if rejected and not roots:
        gate = rejected[0]
        headline = (
            f"🛑 Lightflow stopped: operator gate '{gate}' was rejected"
            f" ({engine.summarise(_redact(_msg(gate)))})."
        )
        self._send_status_notification(
            runner, passport, headline, is_failed=True
        )
        print(f"\n{headline}")
        if paused:
          print(
              f"⏸️ Open gate(s), still awaiting a decision: {', '.join(paused)}"
          )
        print(
            "Nothing to fix. To re-open the gate and decide again in one"
            f" call:\n  {resume_cmd} --stage={gate} --resolution=APPROVE"
            " --payload='{...}'\n(or just"
            f" {resume_cmd} --stage={gate} to re-open it and pause there)."
        )
        raise LightflowRunFailedError(headline, already_reported=True)

      # --- Root failure cleared with --cascade=false; downstream still blocked.
      if not roots:
        headline = (
            "Lightflow incomplete. Blocked downstream stage(s) (not re-armed):"
            f" {', '.join(blocked)}"
        )
        self._send_status_notification(
            runner, passport, headline, is_failed=True
        )
        print(f"\n{headline}")
        print(f"⛔ Blocked downstream (not run): {', '.join(blocked)}")
        print(
            "\nContinue with the remaining stage(s) (preserves completed"
            f" stages):\n  {resume_cmd}"
        )
        raise LightflowRunFailedError(headline, already_reported=True)

      # --- Genuine failures: root causes first, collateral summarised.
      err_msg = (
          f"Lightflow failed. Root failure(s): {', '.join(roots or rejected)}"
      )
      self._send_status_notification(runner, passport, err_msg, is_failed=True)
      print(f"\n{err_msg}")
      print("❌ Root failure(s):")
      for s_name in roots:
        lines = _redact(_msg(s_name)).splitlines()
        first = lines[0] if lines else ""
        rolled = (
            " · rolled back"
            if "Rollback executed successfully" in _msg(s_name)
            else ""
        )
        print(f"  - {s_name}: {first}{rolled}")
        rb_fail = _ROLLBACK_FAILED_RE.search(_msg(s_name))
        if rb_fail:
          print(
              f"    ⚠️ rollback '{rb_fail.group(1)}' ALSO FAILED:"
              f" {engine.summarise(_redact(rb_fail.group(2)))} — side effects"
              " may be partially applied; verify by hand before resuming."
          )
      if rejected:
        print(f"🛑 Rejected gate(s): {', '.join(rejected)}")
      if yielded:
        print(f"⏳ Waiting on external condition: {', '.join(yielded)}")
      if blocked:
        print(f"⛔ Blocked downstream (not run): {', '.join(blocked)}")
      if paused:
        print(
            f"⏸️ Open gate(s), still awaiting a decision: {', '.join(paused)}"
        )
      # A failure on an absent *top-level* key is usually a missing --payload
      # value rather than a code bug: say so, and put the key on the command.
      missing_seed_keys = _missing_top_level_keys_from_failures(
          {k: v.message for k, v in latest_stamp_by_stage.items()},
          roots,
      )
      if missing_seed_keys:
        seed = json.dumps({k: "<value>" for k in missing_seed_keys})
        resume_cmd += f" --payload='{seed}'"
        print(
            "\n💡 Stage(s) failed on top-level payload key(s)"
            f" {', '.join(missing_seed_keys)} that were never provided."
            " Supply them with --payload on resume (or fix the expression)."
        )
      print(
          "\nResume after fixing the root cause (preserves completed"
          f" stages):\n  {resume_cmd}"
      )
      raise LightflowRunFailedError(err_msg, already_reported=True)

    self._send_status_notification(
        runner, passport, "Lightflow completed successfully!"
    )
    print("\nLightflow completed successfully!")

  def cleanup(
      self,
      lightflow: str = "",
      log_id: str = "",
      *,
      workflow: Optional[str] = None,
  ) -> None:
    """Deletes the saved Passport state and lock file for a log ID.

    External workflow artifacts on disk are untouched.

    Args:
      lightflow: Unused; accepted so every subcommand shares the same flags.
      log_id: Run identifier whose state directory should be removed.
      workflow: Deprecated alias of `lightflow`; unused.
    """
    del lightflow, workflow
    if not log_id:
      raise ValueError(
          "Missing required '--log_id'. Usage: lightflow cleanup --log_id=<id>"
          " (see 'list' for saved runs)."
      )
    pm = PassportManager(log_id)
    try:
      with pm.lock(exclusive=True):
        pm.delete_state(remove_lock=False)
      lock_path = pm.resolved_path + ".lock"
      if os.path.exists(lock_path) and not os.path.islink(lock_path):
        try:
          os.remove(lock_path)
        except OSError:
          pass
      print(
          f"Successfully cleaned up state for log ID: {log_id} (removed"
          f" {pm.resolved_path}; external workflow artifacts on disk are"
          " untouched)"
      )
    except LightflowRunningError as e:
      raise LightflowRunningError(
          f"Cannot clean up state for log ID '{log_id}' because the lightflow"
          " is currently running (exit 4). Wait for it, then retry."
      ) from e

  # Shadows the builtin inside the class body from here on, so any later
  # method annotated with `list[...]` would break at import: keep this last.
  def list(  # pylint: disable=redefined-builtin
      self,
      state_dir: str = "",
  ) -> None:
    """Lists every saved run under the state directory, newest first.

    Reads each `passport.json` under `$LIGHTFLOW_STATE_DIR` (or the default
    state root) and prints one line per run with its log ID, lightflow name,
    coarse state, last activity, and recorded manifest path, plus any orphan
    `.lock` files left behind by an interrupted `start`. Use `status` for the
    authoritative per-stage view of a run.

    Args:
      state_dir: Override the directory to scan.
    """
    runs, orphan_locks, root = collect_runs(state_dir)
    if not os.path.isdir(root):
      print(f"No runs found (state directory does not exist: {root})")
      return
    rows = [
        (
            r.last_activity,
            r.log_id,
            r.lightflow_name or "?",
            r.state,
            _format_age(r.last_activity),
            r.error or _manifest_cell(r.lightflow_path),
        )
        for r in runs
    ]
    if not rows and not orphan_locks:
      print(f"No runs found under {root}")
      return

    print(f"Runs under {root}:")
    id_w = max([len(r[1]) for r in rows] + [6])
    name_w = max([len(r[2]) for r in rows] + [9])
    print(
        f"  {'LOG_ID':<{id_w}}  {'LIGHTFLOW':<{name_w}}  {'STATE':<12} "
        f" {'LAST ACTIVITY':<14}  MANIFEST"
    )
    for _, log_id, name, state, age, manifest in rows:
      icon = _STATE_ICONS.get(state, "•")
      print(
          f"  {log_id:<{id_w}}  {name:<{name_w}}  {icon} {state:<10}  {age:<14}"
          f"  {manifest}"
      )
    for log_id in orphan_locks:
      print(
          f"  ⚠️ orphan lock for '{log_id}' (no state directory);"
          f" remove with 'cleanup --log_id={log_id}'"
      )
