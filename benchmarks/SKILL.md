---
name: benchmark_lightflow
description: Runs the Lightflow benchmark suite (`python3 benchmarks/run_benchmark.py`) and live A/B subagent trials to measure the runtime token footprint (`cmd + stdout`), cold-start skill overhead, and off-context state (`passport.json`) across multi-stage workflows.
---

# Benchmarking Lightflow Token & Context Footprint (`benchmark_lightflow`)

Lightflow separates **control-plane messages** (which go to `stdout` for the
agent) from **data-plane state** (which stays on disk in `passport.json`):

1.  **On-disk state bus (`passport.json`)**: Intermediate data
    (`payload.outputs.<stage>`) flows between Python stages on disk via
    `passport.json` rather than being printed to `stdout` and re-emitted
    token-by-token by the LLM in subsequent tool calls.
2.  **Direct entry (`lightflow start`)**: Because stage ordering, `run_if`
    branching, JSON schemas, and `rollback_action` hooks live in
    `lightflow.yaml` + `actions.py`, an agent executing an existing workflow
    runs `lightflow start` directly and responds to each gate as it arrives.
3.  **Single reusable runner skill**: One generic ~1,123-token runner skill
    (`skills/run_lightflows/SKILL.md`) drives any workflow without custom
    per-workflow `/tmp/state.json` or `--step` CLI plumbing.

## 1. Run the Reproducible Benchmark Suite

From the repository root, run the deterministic, zero-dependency benchmark
harness:

```bash
# Markdown summary table + step-by-step breakdown
python3 benchmarks/run_benchmark.py

# Machine-readable JSON metrics (includes all computed properties)
python3 benchmarks/run_benchmark.py --json
```

`run_benchmark.py` executes all 6 included example Lightflows (`hn_digest`,
`usgs_seismic_alert`, `pypi_upgrade_guard`, `async_job_watcher`,
`create_lightflow`, and `tenant_gitops_onboarding`) end-to-end in an isolated
temporary directory (normalizing OS temp paths, PIDs, and timestamps for 100%
bit-for-bit reproducibility) and measures:

1.  **Warm Runtime Context (`cmd + stdout`)**: Every CLI command string emitted
    by the agent (`lightflow start ...`, `lightflow resume ...`, `lightflow
    cleanup ...`), any files authored between gates in `create_lightflow`
    (`actions.py`, `actions_test.py`, `lightflow.yaml`), and the exact
    `stdout`/`stderr` returned to the agent's context window.
2.  **Cold-Start Context (`N=1` and `N=6`)**: Explicitly adds
    `skills/run_lightflows/SKILL.md` (4,492 chars / ~1,123 tokens) for execution
    workflows (`1–4 and 6`) and both `run_lightflows/SKILL.md` +
    `create_lightflows/SKILL.md` (+8,177 chars / ~2,044 tokens) for the
    `create_lightflow` authoring meta-workflow.
3.  **Multi-Turn Cumulative Tokens**: Accounts for stateful LLM conversation
    billing where Turn $k$ re-sends the accumulated tool calls and outputs from
    Turns $1 \dots k-1$.
4.  **Persisted in `passport.json` (Off-Context)**: Structured stage outputs
    (`payload.outputs.<stage>`) and audit stamps persisted on disk between
    stages without entering the LLM conversation history.
5.  **On-Disk Blueprint (`lightflow.yaml` + `actions.py`)**: Total byte/token
    size of the workflow manifest and Python actions on disk.

## 2. Reporting & Comparison Guidelines

Whenever you report benchmark numbers or run a custom A/B comparison, follow
these rules:

-   **Always count both directions of tool traffic**: Include the agent's CLI
    command string (and any file-write payloads) alongside the tool's `stdout`
    response—never `stdout` alone.
-   **Always report both Warm (marginal) and Cold-Start (`N=1` and session `N`)
    numbers**:
    -   *Warm / Marginal*: The per-run cost once
        `skills/run_lightflows/SKILL.md` (or MCP tool schemas) is in context.
    -   *Single-Workflow Cold Start (`N=1`)*: Add
        `skills/run_lightflows/SKILL.md` (~1,123 tokens) to a single execution
        run, and also add `skills/create_lightflows/SKILL.md` (~2,044 tokens)
        when benchmarking `create_lightflow`.
    -   *Multi-Workflow Session (`N=6`)*: Count each required `SKILL.md` once
        across the session.

## 3. Running a Live Subagent A/B Benchmark (Optional)

To compare Lightflow against unstructured execution on a live LLM agent:

1.  **Define an identical task and acceptance test** (e.g.,
    `pypi_upgrade_guard`: audit offline packages, pause for approval, trigger a
    simulated smoke-test failure + rollback, then recover and upgrade).
2.  **Arm A — Lightflow Subagent**: Give a fresh zero-context subagent
    `skills/run_lightflows/SKILL.md` and the target
    `--lightflow=examples/pypi_upgrade_guard` path.
3.  **Arm B — Unstructured Subagent**: Give a fresh zero-context subagent the
    same task without the Lightflow CLI engine.
4.  **Compare**:
    -   Agent-generated tool-input tokens (how much JSON/code the LLM had to
        emit to pass state and handle rollbacks),
    -   Agent reasoning/thinking tokens and total turns,
    -   Cumulative prompt tokens across turns ($\sum_{k=1}^{T}
        \text{context}_k$),
    -   Structural guarantees (deterministic `json_schema`, `rollback_action`,
        and surgical `resume` vs. prompt compliance).
