---
name: create_lightflows
description: Guides the design, authoring, compilation, and verification of new Python DAG workflows using Lightflow (YAML or JSON). Use when building a new multi-stage pipeline, authoring `actions.py` and `lightflow.yaml`, or scaffolding a workflow with the `create_lightflow` meta-workflow. For merely running or resuming an existing workflow, use `run_lightflows` instead.
---

# Creating Lightflow Workflows (`create_lightflows`)

Lightflow coordinates multi-step operations between AI agents and human
operators using an append-only state ledger (`passport.json`), safe AST-based
CEL-like expressions, and selective subgraph re-arming.

> **Note**: If you only need to execute, inspect, or resume an *existing*
> Lightflow workflow, read the `run_lightflows` skill instead.

## 1. Scaffolding a New Workflow via `create_lightflow` (3-Phase Meta-Workflow)

Instead of authoring a workflow from scratch in one shot, run the built-in
3-phase meta-workflow, which gates progress on deterministic AST signature
checks, `sample_payload` return-contract smoke tests, unit test execution, and
`payload.outputs.<stage>` DAG cross-checks. The meta-workflow ships in the
Lightflow repository under `examples/create_lightflow/`: run it from a clone of
`github.com/google/lightflow`, or pass the absolute path to
`examples/create_lightflow` in your clone:

```bash
lightflow start \
  --lightflow=/path/to/lightflow/examples/create_lightflow \
  --log_id=scaffold_<workflow_name>
```

(`python3 -m lightflow` is equivalent to `lightflow` whenever the console script
is not on `PATH`.)

1.  **`align_on_design`** (`operator_action`): Align with the user on stage
    boundaries (separate read/compute stages from external write/mutate stages),
    `operator_action` gates, and the output keys each stage produces under
    `payload.outputs.<stage>`.
2.  **`implement_and_test_actions` $\rightarrow$ `verify_actions`**: Write
    `actions.py` + unit tests (or pass `sample_payload`); `verify_actions`
    parses the AST, smoke-tests that every action returns a JSON-serializable
    `(dict, str)` tuple, and runs `unittest`.
3.  **`draft_and_explain_manifest` $\rightarrow$ `prove_manifest_dry_run`**:
    Write `lightflow.yaml` and explain the DAG to the user;
    `prove_manifest_dry_run` compiles the DAG, verifies that every
    `payload.outputs.<stage>` reference points to an upstream dependency in
    `run_after`, runs `dry_run`, and emits `visualizer.html`.

## 1b. Starting From an Existing `SKILL.md`

Most procedures worth moving into Lightflow already exist as a skill: a numbered
list of steps an agent follows by hand. The translation is mechanical once each
step is classified:

A step that…                                                                             | becomes                                                               | because
---------------------------------------------------------------------------------------- | --------------------------------------------------------------------- | -------
runs a command, calls an API, transforms data, writes a file                             | `python_action`                                                       | deterministic, testable, retryable; the agent never re-derives it
needs judgment but no outside decision (triage findings, draft text, pick a remediation) | `operator_action` whose `instructions` tell the agent what to produce | the gate pauses the run, the agent does the work, then resumes with the result as payload
needs a decision from a person (approve spend, confirm a destructive change)             | `operator_action` whose `instructions` say who must decide            | the resume stamp records who decided and from which session

Worked example. A skill that reads:

```
1. Query last week's incidents from the tracker.
2. Group them by service and draft a summary.
3. Get the on-call lead to confirm the summary.
4. Post it to the channel.
```

maps to:

```yaml
name: weekly_incident_digest
actions:
  - {id: fetch,  python_import: digest.actions.fetch_incidents}
  - {id: group,  python_import: digest.actions.group_by_service}
  - {id: post,   python_import: digest.actions.post_to_channel}
stages:
  - name: fetch_incidents                       # step 1: mechanical
    python_action: {action_id: fetch}
    retry_policy: {max_attempts: 3, initial_backoff_seconds: 5}
  - name: group_by_service                      # step 2a: mechanical
    run_after: [fetch_incidents]
    python_action: {action_id: group}
  - name: draft_summary                         # step 2b: agent judgment
    run_after: [group_by_service]
    operator_action:
      instructions: "'Draft a 5-line summary of ' + string(payload.outputs.group_by_service.count) + ' incidents grouped by service; resume with summary_md.'"
      json_schema: '{"type": "object", "required": ["summary_md"], "properties": {"summary_md": {"type": "string"}}}'
  - name: lead_confirms                         # step 3: a person decides
    run_after: [draft_summary]
    operator_action:
      instructions: "'Ask the on-call lead to confirm this summary before it is posted: ' + payload.outputs.draft_summary.summary_md"
  - name: post_to_channel                       # step 4: mechanical
    run_after: [lead_confirms]
    python_action: {action_id: post}
```

Keep the skill: replace its step list with `lightflow start --lightflow=<dir>`
and the gate-handling rules from `skills/run_lightflows/SKILL.md`. The judgment
and the decision stay where they were; only the mechanical steps and the
ordering move into code. Run the `create_lightflow` meta-workflow above to
scaffold and prove the result.

## 2. Workflow Manifest Reference (`lightflow.yaml`)

Define actions and stages in `lightflow.yaml` (or `.json`). `python_import`
resolves relative to the workflow's directory (e.g. `actions.fetch_metrics` for
`actions.py` in the same folder) as well as package paths:

```yaml
name: release_verification_pipeline
description: Fetch metrics, pause for operator review, and publish report.
# Optional: payload keys (any depth) shown as <redacted> in the visualizer,
# status --verbose and dry-run output. passport.json keeps the real values.
secret_keys: [api_token]

actions:
  - id: fetch_metrics
    python_import: actions.fetch_metrics
  - id: publish_report
    python_import: actions.publish_report
  - id: cleanup_temp
    python_import: actions.cleanup_temp

stages:
  - name: fetch
    description: Fetch latest metrics
    timeout_seconds: 60
    python_action:
      action_id: fetch_metrics
      static_kwargs:
        limit: 500
    retry_policy:
      max_attempts: 3
      initial_backoff_seconds: 5
      backoff_multiplier: 2.0
    rollback_action:
      action_id: cleanup_temp

  - name: approve
    description: Operator approval gate
    run_after: [fetch]
    operator_action:
      # CEL-like expression: string literals MUST be quoted inside the string.
      instructions: "'Ready to publish: ' + payload.outputs.fetch.summary"
      json_schema:
        type: object
        required: [approved]
        properties:
          approved:
            type: boolean

  - name: publish
    description: Publish verified report
    run_after: [approve]
    run_if: "payload.outputs.approve.approved == true"
    python_action:
      action_id: publish_report

  - name: always_cleanup
    run_after: [publish]
    trigger_rule: ALL_DONE
    python_action:
      action_id: cleanup_temp
```

## 3. Rules for Crash-Free Python Actions

```python
from typing import Any

def fetch_metrics(
    payload: dict[str, Any], **static_kwargs: Any
) -> tuple[dict[str, Any], str]:
  limit = static_kwargs.get("limit", 100)
  return {"row_count": limit, "summary": f"{limit} rows"}, "Fetched metrics"
```

1.  **One Side Effect per Stage**: Separate read/compute stages from external
    write/mutate stages. Lightflow's `Passport` never re-runs `COMPLETED` stages
    on `resume`; make write stages safe to retry if they fail mid-execution
    (overwrite files instead of appending, or attach a `rollback_action`).
2.  **Consistent Output Keys (`payload.outputs.<stage_name>`)**: Every return
    path of an action (including any `offline` test mode for external APIs) must
    return a JSON-serializable `(dict, str)` with the **exact same dictionary
    keys** so downstream `run_if` and `instructions` expressions never crash.
3.  **Cross-Check `payload.outputs.<stage>` Against `run_after`**: Any stage
    whose `run_if` or `instructions` reads `payload.outputs.<upstream>.<key>`
    must list `<upstream>` (directly or transitively) in `run_after`.
4.  **Timeouts, Polling & Resilience**: Set stage-level `timeout_seconds`
    (`0`–`86400`; `0` disables the bound) on a stage to bound a `python_action`
    (or individual poll tick); for long-running external jobs, prefer
    `polling_policy` (`condition`, `interval_seconds`, `timeout_seconds`,
    `max_attempts`, optional `poll_tick_action`), which persists each tick's
    progress message to `passport.json` so `lightflow status` shows live
    progress. Use `retry_policy` for transient errors and `rollback_action` /
    `trigger_rule: ALL_DONE` for cleanup; keep cleanup stages idempotent, since
    an `ALL_DONE` stage that ran on a failure path runs again after `resume`.

    A polling stage in YAML (the tick action goes under `poll_tick_action`; a
    stage may not also set `python_action`):

    ```yaml
    - name: await_job
      run_after: [submit_job]
      polling_policy:
        condition: "payload.outputs.await_job.status == 'READY'"
        interval_seconds: 5
        timeout_seconds: 600        # or max_attempts: 120
        poll_tick_action:
          action_id: check_job      # (dict, message) per tick
    ```

    Each tick's returned dict is merged into `payload.outputs.await_job` before
    `condition` is evaluated, so a tick can read its own previous output.
5.  **Converging After a Conditional Branch**: Two stages with complementary
    `run_if` conditions leave one of them `SKIPPED`. A stage that `run_after`s
    both must set `trigger_rule: NONE_FAILED` to run (the default `ALL_SUCCESS`
    skips it because an upstream was skipped; `ALL_DONE` would also run it after
    an upstream *failure*, which is only right for cleanup stages). `dry_run`
    points this out when it sees it.
6.  **Optional Keys with `has(...)`**: Under `strict_expressions`, read a key
    that may be absent (e.g. an optional gate field) as
    `!has(payload.outputs.gate.note) || payload.outputs.gate.note != 'skip'`.
    `has()` never raises.
7.  **Strict Expressions \& Explicit Namespaces**: Set top-level
    `strict_expressions: true` on new lightflows so a misspelled or
    not-yet-populated key in `run_if` / `condition` / `instructions` fails the
    stage instead of silently evaluating to `null` (guard optional keys with
    `has(...)`). Read upstream values as `payload.outputs.<stage>.<key>` rather
    than the top-level mirror, which is last-writer-wins across stages; the
    engine warns when a stage overwrites a top-level key that an expression
    reads. `outputs` is a reserved top-level key (`--payload` may not set it).

## 4. Validating Your New Workflow

Before handing off or running in production, always verify compilation, CEL-like
expression evaluation, and gate schemas:

```bash
lightflow dry_run --lightflow=lightflow.yaml
lightflow render --lightflow=lightflow.yaml
lightflow visualize --lightflow=lightflow.yaml --output=/tmp/visualizer.html
```
