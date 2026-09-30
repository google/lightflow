# Lightflow Integration Guide for Claude Code & Claude Desktop

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
    -   When `status == "SUSPENDED"` (`exit_code == 2`), present the
        `instructions` and `json_schema` to the user and wait for explicit human
        confirmation before calling `resume_lightflow`.
3.  **`resume_lightflow`**: Resolve an `operator_action` (`resolution="APPROVE"`
    or `"REJECT"`) or re-arm a repaired failed stage without repeating
    already-completed upstream stages.
4.  **`get_lightflow_status`**: Inspect the `Passport` append-only stamp log and
    Mermaid diagram.
5.  **`visualize_lightflow`**: Generate a self-contained HTML5 DAG canvas and
    timeline playback artifact.

## Terminal CLI Commands

When working via the bash tool in Claude Code (`python3 -m lightflow` is
equivalent to `lightflow` whenever the console script is not on `PATH`):

```bash
# 1. Validate manifest
lightflow dry_run --lightflow=lightflow.yaml

# 2. Start run
lightflow start --lightflow=lightflow.yaml --log_id=my_run --payload='{}'

# 3. Check status (exit 0 done, 1 failed, 2 paused, 3 no state, 4 running;
#    add --verbose for payload + Mermaid)
lightflow status --lightflow=lightflow.yaml --log_id=my_run

# 4. Resume paused gate after user approval (--resolution=APPROVE sets
#    approved=true automatically; --payload only needs the keys the gate's
#    json_schema requires)
lightflow resume --lightflow=lightflow.yaml --log_id=my_run \
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
