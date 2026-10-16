# Lightflow Integration Guide for Coding Agents (`AGENTS.md`)

When orchestrating multi-step workflows with side effects (e.g., database
migrations, API batch modifications, code deployments, or multi-stage reports),
use **Lightflow** to guarantee topological execution order, atomic state
checkpoints, and human-in-the-loop approval gates.

## Available MCP Tools (`lightflow-mcp`)

If the `lightflow` MCP server is connected, prefer calling its structured tools
directly:

1.  **`dry_run_lightflow`**: Validate DAG dependencies, CEL-like expressions
    (`run_if`, `instructions`), and JSON schemas before executing.
2.  **`run_lightflow`**: Launch or continue a lightflow (`lightflow`, `log_id`,
    `payload`).
    -   When `state == "SUSPENDED"` (`exit_code == 2`), present the
        `instructions` and `json_schema` to the user and wait for explicit human
        confirmation before calling `resume_lightflow`; `next_action` names the
        call and its `required_payload_keys`.
3.  **`resume_lightflow`**: Resolve an `operator_action` (`resolution="APPROVE"`
    or `"REJECT"`) or re-arm a repaired failed stage without repeating
    already-completed upstream stages. `lightflow` is optional: runs remember
    their manifest. On a rejected gate, passing `resolution` re-opens it and
    applies the decision in one call.
4.  **`get_lightflow_status`** / **`list_lightflow_runs`**: One run (compact:
    `state`, one entry per stage, gate details, `exit_code`, `next_action`;
    `verbose: true` adds the stamp log and Mermaid diagram) or every saved run.
    `isError` is true only when the call could not do what was asked or a stage
    genuinely failed; `REJECTED`, `SUSPENDED`, `WAITING` (a polling stage
    yielded, `exit_code == 5`, nothing is broken) and `NOT_FOUND` are answers,
    not errors.
5.  **`visualize_lightflow`**: Generate a self-contained HTML5 DAG canvas and
    timeline playback artifact (`lightflow` is optional when `log_id` is given).

## Terminal CLI Commands

When working via a shell/bash tool (`python3 -m lightflow` is equivalent to
`lightflow` whenever the console script is not on `PATH`):

```bash
# 1. Validate manifest
lightflow dry_run --lightflow=lightflow.yaml

# 2. Start run (supports --payload='{...}', --payload=@file.json, or --payload_file=file.json)
lightflow start --lightflow=lightflow.yaml --log_id=my_run --payload='{}'

# 3. Check status (exit 0 done, 1 failed, 2 paused, 3 no state, 4 running, 5 waiting on a poll;
#    prints Passport path + per-stage lines with failure/pending progress messages;
#    add --verbose for payload, per-key provenance + Mermaid). --lightflow is
#    optional for runs started by this version. `lightflow list` shows all runs.
lightflow status --log_id=my_run

# 4. Resume paused gate after user approval (--resolution=APPROVE sets
#    approved=true automatically; --payload only needs the keys the gate's
#    json_schema requires; --lightflow optional as for status)
lightflow resume --log_id=my_run \
  --stage=approve --resolution=APPROVE --payload='{}'
```

## Guardrail Rules for Agents

-   **Scaffolding new workflows (`create_lightflow`)**: When asked to build a
    new Lightflow workflow, run the `create_lightflow` meta-workflow that ships
    in the Lightflow repository to step through design alignment, AST + unit
    test verification, and `dry_run` + `visualizer.html` proof. Run it from a
    clone of `github.com/google/lightflow`, or pass the absolute path to
    `examples/create_lightflow` in your clone: `lightflow start
    --lightflow=/path/to/lightflow/examples/create_lightflow
    --log_id=scaffold_<name>`.
-   **Never bypass an `operator_action` gate**: If a workflow exits with code
    `2` (`SUSPENDED`), always surface the stage's `instructions` to the human
    operator and await their decision before invoking `resume`.
-   **Prefer `resume` over `start --force` after failures**: Lightflow's
    append-only `Passport` preserves completed stages so side effects are never
    duplicated. Fix the failing action and call `resume`.
