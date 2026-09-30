---
name: benchmark_lightflow
description: Runs the Lightflow benchmark suite (`python3 benchmarks/run_benchmark.py`) and live A/B subagent trials to verify that Lightflow's deterministic structural backbone (human gates, JSON Schema validation, automatic rollback, surgical resume) operates at lower token and latency cost than unstructured skills.
---

# Benchmarking Lightflow: Structural Rigor at Negative Cost (`benchmark_lightflow`)

Lightflow gives AI agents a deterministic structural backbone—hard `EXIT_CODE=2`
human gates, JSON Schema validation, automatic `rollback_action` hooks, and
surgical `resume` without re-running upstream side effects—**at *lower* token
and latency cost than an unstructured skill, not higher.**

Why structural rigor is negative-cost in Lightflow:

1.  **Zero-cost guardrails**: The skill blueprint lives in `lightflow.yaml` +
    `actions.py`, so the agent runs `lightflow start` directly (**Zero-Context
    Entry**) without loading workflow source code or long guardrail prompts into
    context.
2.  **$O(1)$ agent context vs. $O(N)$ payload size (No "LLM as State Bus")**:
    Intermediate data (`payload.outputs.<stage>`) flows between stages on disk
    via `passport.json` rather than being printed to `stdout` and re-emitted
    token-by-token by the LLM in subsequent tool calls.
3.  **No bespoke state-file plumbing per skill**: One reusable 1,014-token
    runner skill (`skills/run_lightflows/SKILL.md`) replaces writing custom
    `/tmp/state.json`, `--step`, and rollback CLI glue inside every skill.

## 1. Run the Reproducible Benchmark Suite

From the repository root, run the deterministic, zero-dependency benchmark
harness:

```bash
# Markdown summary table + step-by-step breakdown
python3 benchmarks/run_benchmark.py

# Machine-readable JSON metrics (includes all computed properties)
python3 benchmarks/run_benchmark.py --json
```

`run_benchmark.py` executes all 5 included example Lightflows (`hn_digest`,
`usgs_seismic_alert`, `pypi_upgrade_guard`, `async_job_watcher`, and
`create_lightflow`) end-to-end in an isolated temporary directory (normalizing
OS temp paths, PIDs, and timestamps for 100% bit-for-bit reproducibility) and
measures:

1.  **Warm Runtime Context (`cmd + stdout`)**: Every CLI command string emitted
    by the agent (`lightflow start ...`, `lightflow resume ...`, `lightflow
    cleanup ...`), any files authored between gates in `create_lightflow`
    (`actions.py`, `actions_test.py`, `lightflow.yaml`), and the exact
    `stdout`/`stderr` returned to the agent's context window.
2.  **Cold-Start Context (`N=1` and `N=5`)**: Explicitly adds
    `skills/run_lightflows/SKILL.md` (4,056 chars / ~1,014 tokens) for execution
    workflows (`1–4`) and both `run_lightflows/SKILL.md` +
    `create_lightflows/SKILL.md` (+5,641 chars / ~1,410 tokens) for the
    `create_lightflow` authoring meta-workflow.
3.  **Multi-Turn Cumulative Tokens**: Accounts for stateful LLM conversation
    billing where Turn $k$ re-sends the accumulated tool calls and outputs from
    Turns $1 \dots k-1$.
4.  **Out-of-Context State (`passport.json`)**: Structured stage outputs
    (`payload.outputs.<stage>`) and audit stamps persisted on disk between
    stages without entering the LLM conversation history.
5.  **Source Files Avoided (`lightflow.yaml` + `actions.py`) & Zero-Context
    Savings**: Measures $\frac{\text{Source Avoided}}{\text{Runtime Context} +
    \text{Source Avoided}}$, comparing Zero-Context Entry (`lightflow start`
    directly) against a white-box agent that reads `lightflow.yaml` +
    `actions.py` before running.

## 2. Fairness Protocol (Mandatory When Reporting or Comparing)

Whenever you report benchmark numbers or run a custom A/B comparison, follow
these rules:

-   **Frame around Structural Rigor + Output Token Latency (not just raw input
    tokens)**: Emphasize how `passport.json` eliminates the "LLM as State Bus"
    anti-pattern (preventing the LLM from having to re-generate upstream JSON
    payloads in tool-call inputs, which is $5\times$–$10\times$ slower and
    error-prone).
-   **Always count both directions of tool traffic**: Include the agent's CLI
    command string (and any file-write payloads) alongside the tool's `stdout`
    response—never `stdout` alone.
-   **Always report both Warm (marginal) and Cold-Start (`N=1` and session `N`)
    numbers**:
    -   *Warm / Marginal*: The per-run cost once
        `skills/run_lightflows/SKILL.md` (or MCP tool schemas) is in context.
    -   *Single-Workflow Cold Start (`N=1`)*: Add
        `skills/run_lightflows/SKILL.md` (~1,014 tokens) to a single execution
        run, and also add `skills/create_lightflows/SKILL.md` (~1,410 tokens)
        when benchmarking `create_lightflow`.
    -   *Multi-Workflow Session (`N=5`)*: Count each required `SKILL.md` once
        across the session.
-   **Never conflate `Source Avoided (yaml + actions.py)` with a prompt-only
    `SKILL.md`**:
    -   `yaml + actions.py` represents the **white-box source inspection
        baseline** (what a coding agent reads if it inspects the workflow
        directory without the Zero-Context Entry Rule).

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
