---
name: run_lightflows
description: Executes, inspects, and resumes existing Lightflow DAG workflows via CLI (`lightflow`, or the equivalent `python3 -m lightflow`). Use when asked to run, dry-run, check status, visualize, resolve an `operator_action` human-in-the-loop checkpoint (`EXIT_CODE=2`), or recover a failed Lightflow run without authoring a new workflow. Run `start` directly without pre-reading the manifest or `actions.py`.
---

# Running & Resuming Lightflow Workflows (`run_lightflows`)

> **Authoring new workflows?** Read `create_lightflows` instead.

## Fast-Path Rules

1.  **Read only `SKILL.md`, then run `start` directly**: do **not** read
    `lightflow.yaml`, `actions.py`, or `README.md` first. The CLI compiles the
    manifest and prints any gate's `Instructions`, `Payload Schema`, and the
    exact `Resume with:` command. `--lightflow=<path>` accepts a manifest file
    or its directory.
2.  **Skip redundant `status` calls & chain cleanup**: when `start` or `resume`
    exits `0` (`Lightflow completed successfully!`), do **not** run `status`. If
    asked to clean up afterwards, chain `start ... && cleanup ...` (`&&` stops
    naturally at an `EXIT_CODE=2` gate).

## 1. Commands

`python3 -m lightflow` is equivalent to `lightflow` when the console script is
not on `PATH`. Runs remember their manifest, so `status`, `resume`, `visualize`,
and `cleanup` need only `--log_id`; if the manifest file moved, pass
`--lightflow=<new path>` once and it is re-recorded.

```bash
lightflow start --lightflow=<dir_or_manifest> --log_id=<run_id> [--payload='{...}' | --payload=@file.json]
lightflow start ... --dry_run      # only when asked to preview; ends with a Summary: line and a 💡 --payload seed hint

# Resolve a paused gate (only after the human decided; APPROVE sets approved=true itself)
lightflow resume --log_id=<id> --stage=<gate_stage> --resolution=APPROVE --payload='{...}'
lightflow resume --log_id=<id> --stage=<gate_stage> --resolution=REJECT --comment="..."   # runs rollback_action; dependents are blocked

# After fixing a failed stage: re-arms only failed stages, keeps completed work
lightflow resume --log_id=<id> [--payload='{...}']

lightflow status --log_id=<id>     # one line per stage + any paused gate's Instructions, Schema, resume command; --verbose adds payload + stamp log
lightflow list                     # every saved run: log ID, lightflow, state, last activity, manifest
lightflow visualize --log_id=<id> --output=/tmp/viz.html
lightflow cleanup --log_id=<id>    # deletes the run's state only; workflow artifacts on disk are untouched
```

## 2. Exit Codes & Governance

-   **`0` COMPLETED**: done; report the stage outputs printed to `stdout`.
-   **`2` SUSPENDED at an `operator_action`**: follow the printed
    `Instructions`, then `resume` with a payload matching the `Payload Schema`.
    `--resolution=APPROVE` validates `--payload` against the gate's
    `json_schema`; on a validation error the gate stays paused, so fix the
    payload and re-run (`status` reprints the schema in a new conversation).
-   **`1` FAILED or REJECTED**: the footer names the root cause and the exact
    `resume` command. Fix the cause, then `resume` — **never `start --force`**,
    which discards completed stages and is only for rerunning a *completed* run
    from scratch. A rejected gate (`🛑`) is terminal unless deliberately
    re-opened: `resume --stage=<gate> --resolution=APPROVE --payload='{...}'`
    decides again in one call; a bare `resume --stage=<gate>` only re-opens it.
-   **`status` exits like the run**: `0` completed · `1` failed, rejected, or
    interrupted · `2` paused · `3` no saved state · `4` running in another
    process (wait; don't `resume`) · `5` waiting: a `polling_policy` stage
    yielded before its condition was met, nothing is broken, `resume` once it
    holds. Use `--verbose` only when you need the payload, stamp log, or Mermaid
    diagram.
