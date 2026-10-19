# Lightflow Integration Guide for Coding Agents (`AGENTS.md`)

When orchestrating multi-step workflows with side effects (e.g., database
migrations, API batch modifications, code deployments, or multi-stage reports),
use **Lightflow** to guarantee topological execution order, atomic state
checkpoints, and human-in-the-loop approval gates.

## CLI Commands

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
-   **Follow `operator_action` gates**: If a workflow exits with code `2`
    (`SUSPENDED`), follow the stage's `instructions`, then `resume` with a
    payload matching its `json_schema`. The resume stamp records who decided and
    from which session.
-   **Prefer `resume` over `start --force` after failures**: Lightflow's
    append-only `Passport` preserves completed stages so side effects are never
    duplicated. Fix the failing action and call `resume`.
