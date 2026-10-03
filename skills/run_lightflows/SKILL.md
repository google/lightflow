---
name: run_lightflows
description: Executes, inspects, and resumes existing Lightflow DAG workflows via CLI (`lightflow`, or the equivalent `python3 -m lightflow`) or MCP (`lightflow-mcp`). Use when asked to run, dry-run, check status, visualize, resolve an `operator_action` human-in-the-loop checkpoint (`EXIT_CODE=2`), or recover a failed Lightflow run without authoring a new workflow. Run `start` directly without pre-reading the manifest or `actions.py`.
---

# Running & Resuming Lightflow Workflows (`run_lightflows`)

> **Authoring new workflows?** Read `create_lightflows` instead.

## Fast-Path Rules

1.  **Read only `SKILL.md`, then run `start` directly**: Do **not** read
    `lightflow.yaml`, `actions.py`, or `README.md` before `start`. The CLI
    compiles the manifest and prints any gate `Instructions`, `Payload Schema`,
    and exact `resume` command. `--lightflow=<dir>` accepts a workflow directory
    (or manifest file path).
2.  **Skip redundant `status` calls & chain cleanup**: When `start` or `resume`
    exits `0` (`Lightflow completed successfully!`), do **not** run `status`.
    When asked to clean up on completion (or inspect an output file), chain
    `start ... && cleanup ...` or `resume ... && cleanup ...` in a single shell
    command (`&&` naturally stops if `start` suspends at an `EXIT_CODE=2` gate).

## 1. Commands

(`python3 -m lightflow` is equivalent to `lightflow` whenever the console script
is not on `PATH`.)

```bash
# Start a run (add --payload='{...}' for initial args, or --dry_run only if asked to preview)
lightflow start --lightflow=<dir> --log_id=<run_id>

# Approve a paused operator_action gate (sets approved=true automatically)
lightflow resume --lightflow=<dir> --log_id=<id> \
  --stage=<gate_stage> --resolution=APPROVE --payload='{...}'

# Reject a paused gate (runs any rollback_action; blocks ALL_SUCCESS, allows ALL_DONE)
lightflow resume --lightflow=<dir> --log_id=<id> \
  --stage=<gate_stage> --resolution=REJECT --comment="Rejected by operator"

# Recover from a failed stage (re-arms only failed stages; preserves completed work; optional --payload overrides)
lightflow resume --lightflow=<dir> --log_id=<id> [--payload='{...}']

# Re-run a completed or skipped stage and its downstream dependents without wiping upstream work
lightflow resume --lightflow=<dir> --log_id=<id> --rerun=<stage> [--cascade=true]

# Inspect state (one line per stage + any paused gate's Instructions, Payload
# Schema, and resume command; --verbose adds payload + stamp log), generate
# HTML visualizer, or delete state directory
lightflow status --lightflow=<dir> --log_id=<id>
lightflow visualize --lightflow=<dir> --log_id=<id> --out=/tmp/viz.html
lightflow cleanup --log_id=<id>
```

**MCP (`lightflow-mcp`)**: If connected, prefer the structured tools
`run_lightflow`, `resume_lightflow`, `get_lightflow_status`,
`dry_run_lightflow`, and `visualize_lightflow` over the CLI; the same rules
below apply to their `status` / `exit_code` fields (pass `fields=[...]` for
targeted passport reads or `verbose=true` for the full passport and Mermaid
diagram).

## 2. Exit Codes & Governance

-   **Exit `0` (`COMPLETED`)**: Done. Report the stage outputs printed to
    `stdout`.
-   **Exit `2` (`SUSPENDED` at `operator_action`)**: Follow the printed
    `Instructions`. If they ask for a person's decision, present the
    `Instructions` and `Payload Schema` to that person and wait for their answer
    — **never decide or self-approve for them.** Then run `resume` with a
    payload matching the `Payload Schema`.
-   **Exit `1` (`FAILED`)**: Fix the root cause and run `resume` (**never**
    `start --force`, which wipes completed upstream stages).
-   **Gate payload validation**: `resume --resolution=APPROVE` validates
    `--payload` against the gate's `json_schema` (required keys, types, `enum`
    values). On a validation error the gate stays `PAUSED`; fix the payload and
    re-run `resume`. If the gate's `Payload Schema` is no longer in context
    (e.g. a new conversation), run `status`, which reprints it.
-   **`status` exits like the run**: `0` completed, `1` failed, rejected, or
    interrupted, `2` paused at a gate, `3` no saved state for the log ID, `4`
    still running in another process (wait; don't `resume`). Add `--verbose`
    only when you need the full payload, stamp log, or Mermaid diagram.
