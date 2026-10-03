---
name: create_lightflows
description: Guides the design, authoring, compilation, and verification of new Python DAG workflows using Lightflow (YAML, JSON, or Textproto). Use when building a new multi-stage pipeline, authoring `actions.py` and `lightflow.yaml`, or scaffolding a workflow with the `create_lightflow` meta-workflow. For merely running or resuming an existing workflow, use `run_lightflows` instead.
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

## 2. Workflow Manifest Reference (`lightflow.yaml`)

Define actions and stages in `lightflow.yaml` (or `.json` / `.textproto`).
`python_import` resolves relative to the workflow's directory (e.g.
`actions.fetch_metrics` for `actions.py` in the same folder) as well as package
paths:

```yaml
name: release_verification_pipeline
description: Fetch metrics, pause for operator review, and publish report.

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

## 3. Three Rules for Crash-Free Python Actions

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
    keys** so downstream `run_if` and `instructions` expressions never crash. If
    an action cannot do its job (including its own plumbing failing), raise an
    exception: the stage is stamped `FAILED` and `resume` re-runs only it (or
    use `lightflow resume --lightflow=. --log_id=<id> --rerun=<stage>` to re-run
    an already completed stage and its downstream dependents). Never return a
    failure verdict as a normal result.
3.  **Cross-Check `payload.outputs.<stage>` Against `run_after`**: Any stage
    whose `run_if` or `instructions` reads `payload.outputs.<upstream>.<key>`
    must list `<upstream>` (directly or transitively) in `run_after`.

## 4. Validating Your New Workflow

Before handing off or running in production, always verify compilation, CEL-like
expression evaluation, and gate schemas:

```bash
lightflow dry_run --lightflow=.
lightflow render --lightflow=.
lightflow visualize --lightflow=. --out=/tmp/visualizer.html
```
