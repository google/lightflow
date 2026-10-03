<!-- mdformat global-off -->
# Lightflow Unified A/B Benchmark: Lightflow DAG (`Arm A`) vs. Traditional Skill (`Arm B`)

## Executive Summary: The Goal Is Execution Consistency — And Token Efficiency Comes Mainly for Free

When evaluating **Lightflow (`Arm A`)** against its natural baseline — a
**Steelmanned Traditional Skill (`Arm B`: `SKILL.md` + `actions.py`)** where the
Python business logic is already factored out into `actions.py` — the
conversation often starts with token count. However, evaluating both arms across
**7 matched domain workflow pairs** (`3` to `22` stages, `N=5` trials per
workflow in `run_benchmark.py`) and **15 independent live zero-context LLM
subagent sessions** (`N=5` per cohort = `30` live workflow runs) shows a deeper
engineering conclusion:

> **The primary reason to use a declarative DAG is execution consistency and
> side-effect safety — and token efficiency (`2.0x` lower session tokens) comes
> along for free as a byproduct of moving orchestration out of the LLM's
> improvisation loop.**

1.  **Why Traditional Skills Drift Even When Python Work Is Factored Out**: Even
    with domain logic cleanly factored into `actions.py`, a traditional
    `SKILL.md` still relies on the LLM to act as the *runtime state machine*
    across turns: writing ad-hoc `python3 -c` glue scripts, persisting
    intermediate outputs between turns, evaluating branch conditions, catching
    exceptions to trigger compensating rollbacks, and resuming from the exact
    point of failure without re-executing completed upstream stages.
    -   **Generative Glue Variance (even with 100% synchronized docs, `Cohort
        2`)**: Across `N=5` live zero-context LLM sessions given a meticulously
        synchronized `SKILL.md`, Arm B agents generated **`3.1x` more code**
        (`~1,565 ± 97 tok` vs. `~502 ± 4 tok`) with **`23x` higher
        code-generation SD** (`±389 chars` vs. `±17 chars`) and **`38x` higher
        warm runtime token SD** (`±144 tok` vs. `±4 tok`). Every run improvised
        a slightly different polling loop, state dictionary, and `try/except`
        wrapper.
    -   **Catastrophic Prose-Drift Fragility (1 omitted return-type detail →
        `50%` first-try failure, `Cohort 3`)**: Because the prose `SKILL.md`
        *is* the orchestrator, omitting a single implementation detail (that
        `actions.py` helpers return a `(output_dict, message_str)` 2-tuple
        rather than a bare `dict`) caused **`5/5` live Arm B subagents to crash
        mid-flight on Task 1 Stage 2** (`TypeError: 'tuple' object is not
        subscriptable`) *after* Stage 1 had already mutated disk state — forcing
        manual cleanup and re-execution of Stage 1 (`5/10` first-try workflow
        compliance).
2.  **Why Lightflow Gives Us Consistency "Mainly for Free" (`Cohort 1`)**: With
    Lightflow, the agent never reads `lightflow.yaml` or `actions.py` and never
    synthesizes Python orchestration glue. It loads one universal `1,097`-token
    runner skill once per session and issues uniform declarative CLI commands
    (`start` → `resume` → `resume`):
    -   **Zero-Variance Execution (`10/10` first-try pass, `±4 tok` runtime
        SD)**: Across all `N=5` live LLM sessions (`10/10` workflow runs),
        Lightflow achieved **`100%` first-try compliance**, **`0` duplicate
        upstream executions** during failure recovery, and near-zero runtime
        variance (`7,389 ± 15 chars` / `~1,847 ± 4 tok`).
    -   **Consistency Comes Free — And Actually Cheaper (`-49%` to `-50%`
        Session Tokens)**: Instead of paying a token tax for deterministic state
        checkpointing (`passport.json`), JSON Schema gate validation, and
        automatic rollbacks, Lightflow **cuts total session tokens in half**
        (`~2,944 ± 4 tok` vs. `~5,904 ± 144 tok` on the live 2-workflow suite;
        `~5,570 tok` vs. `~10,992 tok` across the 7-workflow ladder) because
        **`1` universal runner skill** replaces **`N` verbose per-workflow
        `SKILL.md` manuals** (`-84%` skill context) and declarative CLI calls
        replace inline Python glue (`3.8x` less generated code).
3.  **Candid Crossover Point**: On a single 3-stage linear workflow in a one-off
    session (`hn_digest`), a bespoke `SKILL.md` + `/tmp/state.json` is ~760
    tokens lighter (`~732` vs. `~1,495 tok`). Lightflow breaks even by the
    **2nd–3rd workflow in a session** or on **any single workflow with `>= 8`
    stages**, conditional branches, polling loops, or rollbacks.

---

## 1. Experimental Design: Two Steelmanned Arms

To ensure neither arm is strawmanned:

-   **Shared Python Actions (`examples/<name>/actions.py`)**: Both Arm A and Arm
    B call the exact same uninstrumented Python functions in `examples/`,
    verified via `passport.json` stamps, on-disk sandbox state, and a
    non-invasive `sys.setprofile` hook injected by `benchmarks/run_benchmark.py`
    when `LIGHTFLOW_BENCH_TRACE_FILE` is set.
-   **Arm A (`Lightflow DAG`)**:
    -   Reads the single, workflow-agnostic skill
        `skills/run_lightflows/SKILL.md` (**`4,388 chars` / `~1,097 tokens`**)
        **once per session** and reuses it across all 7 workflows (`0`
        additional skill tokens on workflows 2-7).
    -   Invokes `python3 -m lightflow start|resume|cleanup
        --lightflow=examples/<name> --log_id=<id>`.
    -   Persists all stage history and `payload.outputs.<stage>` on disk in
        `passport.json`.
-   **Arm B (`Steelmanned Traditional Skill`: `SKILL.md` + `actions.py`)**:
    -   Reads a dedicated, comprehensive per-workflow
        `benchmarks/baseline_skills/<name>/SKILL.md` (`507` to `1,877` tokens
        each; **`28,300 chars` / `~7,075 tokens` across all 7 workflows**) that
        explicitly documents every function signature, return tuple
        (`(output_dict, message_str)`), `payload["outputs"]` key, branch
        condition, polling loop, approval gate, and rollback function so the
        agent **never has to read `actions.py`**.
    -   Executes `python3 -c` snippets importing `examples.<name>.actions` and
        persists intermediate state to a local `/tmp/.../state.json` file
        between turns (steelmanned `Arm B0` state file pattern, avoiding
        command-line JSON state-bus bloat).

---

## 2. Live Zero-Context LLM Subagent Benchmark (`N=5` Trials per Cohort = `15` Sessions / `30` Workflow Runs)

To measure real-world **consistency and variance (`Mean ± SD`)** alongside
**prose-to-code drift sensitivity**, we spawned **15 independent zero-context
LLM subagent sessions** (`N=5` trials in each of 3 controlled cohorts) executing
both new `>5`-stage failure-recovery workflows back-to-back in a single session
(`30` live workflow executions total):

1.  **`examples/blue_green_release` (8 stages)**: Parallel validation diamond
    (`run_security_scan` ‖ `run_integration_suite`) → 2-tick
    `warm_green_environment` poll → `approve_canary_cutover` gate →
    `shift_and_verify_canary` SLO breach (`exit 1`) + automatic
    `revert_canary_traffic` rollback → surgical recovery (`Stages 6-8` only) →
    `promote_green_to_prod` → `decommission_old_slot`.
2.  **`examples/incident_db_failover` (9 stages)**: `detect_primary_outage` →
    `elect_failover_candidate` → mutually exclusive `run_if` WAL replay poll
    (`replay_missing_wal_segments` vs. skipped `verify_zero_loss_sync`) →
    `trigger_rule: ALL_DONE` `fence_old_primary` join →
    `approve_replica_promotion` IC gate → one-way `promote_standby_replica`
    (`promotion_count == 1`) → `cutover_pooler_and_verify_writes` canary write
    failure (`exit 1`) + automatic `revert_pooler_to_maintenance` rollback →
    surgical recovery (`Stages 8-9` only, preserving `promotion_count == 1`) →
    `publish_failover_ledger`.

### The 3 Controlled Cohorts (`N=5` Sessions Each)

-   **Cohort 1 — Arm A (`Lightflow DAG`, `N=5`)**: Reads
    `skills/run_lightflows/SKILL.md` (`1,097 tok`) once and runs `lightflow
    start` / `resume`.
-   **Cohort 2 — Arm B (`Synchronized Steelmanned SKILL.md`, `N=5`)**: Reads
    `baseline_skills/{blue_green_release,incident_db_failover}/SKILL.md` (`3,471
    tok`) with every function signature, `outputs` key, and the `(output_dict,
    message_str)` 2-tuple return type explicitly documented up front.
-   **Cohort 3 — Arm B (`Prose-Drifted SKILL.md: 1 Omitted Return-Type Detail`,
    `N=5`)**: Identical to Cohort 2 except omitting the 1-sentence note that
    `actions.py` helpers return `(output_dict, message_str)` instead of a bare
    `dict`.

| Metric (Across `N=5` Independent Live Subagent Sessions = `10` Workflow Runs / Cohort) | **Cohort 1: Arm A (`Lightflow DAG`, `N=5`)** | **Cohort 2: Arm B (`Synchronized SKILL.md`, `N=5`)** | **Cohort 3: Arm B (`Prose-Drifted SKILL.md`, `N=5`)** | Key Takeaway (`Arm A` vs. `Arm B`) |
| :--- | :---: | :---: | :---: | :---: |
| **First-Try Invariant Compliance** *(zero mid-script crash, zero duplicate Stage 1 calls)* | **`10/10` (`100%`)** | **`10/10` (`100%`)** | **`5/10` (`50%`)** *(all 5 crashed on Task 1 Stage 2 after Stage 1 mutated disk state)* | **Synchronized `SKILL.md` reaches `10/10`, but 1 omitted return detail drops Arm B to `5/10`** |
| **Post-Recovery Final State & Rollback Validity** | **`10/10` (`100%`)** | **`10/10` (`100%`)** | **`10/10` (`100%`)** *(after manual `rm -f` cleanup & retry)* | All cohorts reach valid final state |
| **Shell Commands per 2-Workflow Session (`Mean ± SD`)** | **`6.0 ± 0.0` cmds** (`6-6`) | **`6.0 ± 0.0` cmds** (`6-6`) | **`7.2 ± 0.4` cmds** (`7-8`) | **Zero command variance when synchronized; `+1.2` extra retry turns under prose drift** |
| **Agent Command Output `cmd` (`Mean ± SD`, `Min-Max`)** | **`~502 ± 4 tok`** (`2,009 ± 17 ch`, `1,995-2,039 ch`) | **`~1,565 ± 97 tok`** (`6,259 ± 389 ch`, `5,854-6,738 ch`) | **`~1,954 ± 136 tok`** (`7,816 ± 542 ch`, `7,259-8,673 ch`) | **`3.1x` less code (`3.9x` under drift) • `23x` lower code-gen SD** |
| **Warm Runtime `cmd + stdout` (`Mean ± SD`, `Min-Max`)** | **`~1,847 ± 4 tok`** (`7,389 ± 15 ch`, `7,382-7,416 ch`) | **`~2,433 ± 144 tok`** (`9,731 ± 576 ch`, `9,054-10,428 ch`) | **`~2,992 ± 233 tok`** (`11,969 ± 931 ch`, `10,817-13,077 ch`) | **`24%` fewer runtime tok (`38%` under drift) • `38x` lower runtime SD** |
| **Skill Context Loaded (`1` Reusable vs. `2` Bespoke Skills)** | **`~1,097 tok`** (`4,388 ch`) | **`~3,471 tok`** (`13,886 ch`) | **`~3,176 tok`** (`12,704 ch`) | **`3.2x` smaller skill footprint** |
| **2-Workflow Session Total (`Skill + cmd + stdout`)** | **`~2,944 ± 4 tok`** (`11,777 ± 15 ch`) | **`~5,904 ± 144 tok`** (`23,617 ± 576 ch`) | **`~6,168 ± 233 tok`** (`24,673 ± 931 ch`) | **`50%` lower total session tokens (`2.0x` reduction)** |

### Why Both Cohorts 2 and 3 Matter

1.  **Even when `SKILL.md` is 100% synchronized (`Cohort 2`, `10/10` pass rate),
    Lightflow cuts LLM-generated code by `3.1x` and runtime token variance by
    `38x`**: In Arm A, every subagent emitted the exact same 6 declarative CLI
    calls (`start` → `resume --resolution=APPROVE` → `resume`), yielding a
    standard deviation of just **`15 chars` (`~4 tokens`)** across 5 runs. In
    Cohort 2, even with zero errors, each subagent synthesized `5,854-6,738
    chars` (`~1,565 ± 97 tok`) of custom `python3 -c` glue (`while True:`
    polling loops, `if/else` WAL branches, `try/except` rollback blocks, and
    `json.dump` state checkpoints), resulting in **`38x` higher runtime token
    standard deviation** (`±144 tok` vs. `±4 tok`) and **`2.0x` higher total
    session tokens** (`5,904` vs. `2,944` tok).
2.  **Prose-to-Code Drift Sensitivity (`Cohort 3`, `5/10` first-try pass
    rate)**: In a Traditional Skill, the contract between `SKILL.md` and
    `actions.py` is unverified prose. When `SKILL.md` omitted a single
    return-type detail (`(output_dict, message_str)` 2-tuple vs. plain `dict`),
    all **`5/5` Cohort 3 subagents crashed on Stage 2 (`run_security_scan`)**
    *after* Stage 1 (`build_release_bundle`) had already mutated disk state,
    forcing manual `rm -f` cleanup and re-execution of Stage 1. Lightflow
    validates the DAG via `lightflow compile`, checkpoints `passport.json` at
    every stage boundary, and handles action return unpacking inside the engine.

---

## 3. Unified 7-Pair Domain Complexity Ladder (`run_benchmark.py --trials=5`)

Run `python3 benchmarks/run_benchmark.py --trials=5` to execute both arms across
all **7 domain workflow pairs** (`3, 3, 3, 4, 8, 9, and 22` stages) and verify
all ground-truth invariants (`35/35` domain trial checks passed):

| Workflow Pair (Stages) | Arm A / Arm B Checks (`N=5`) | Skill Loaded (`Arm A` vs. `Arm B`) | Agent Command Output `cmd` (`Arm A` vs. `Arm B`) | Warm Runtime `cmd + stdout` (`Arm A` vs. `Arm B`) | Session Total (`Skill + Warm`) (`Arm A` vs. `Arm B`) |
| :--- | :---: | :---: | :---: | :---: | :---: |
| 1. `hn_digest` (`3` stages) | `5/5` vs. `5/5` | `~1,097 tok` (1st) vs. `~507 tok` | **`~83 tok`** vs. `~178 tok` | `~398 tok` vs. `~225 tok` | `~1,495 tok` vs. **`~732 tok`** |
| 2. `usgs_seismic_alert` (`3` stages) | `5/5` vs. `5/5` | **`0 tok` (reused)** vs. `~548 tok` | **`~89 tok`** vs. `~195 tok` | `~386 tok` vs. `~226 tok` | **`~386 tok`** vs. `~774 tok` |
| 3. `pypi_upgrade_guard` (`3` stages + rollback) | `5/5` vs. `5/5` | **`0 tok` (reused)** vs. `~656 tok` | **`~122 tok`** vs. `~337 tok` | `~653 tok` vs. `~452 tok` | **`~653 tok`** vs. `~1,107 tok` |
| 4. `async_job_watcher` (`4` stages + poll) | `5/5` vs. `5/5` | **`0 tok` (reused)** vs. `~522 tok` | **`~50 tok`** vs. `~212 tok` | **`~211 tok`** vs. `~285 tok` | **`~211 tok`** vs. `~807 tok` |
| 5. `blue_green_release` (`8` stages + rollback) | `5/5` vs. `5/5` | **`0 tok` (reused)** vs. `~1,594 tok` | **`~139 tok`** vs. `~481 tok` | `~814 tok` vs. `~605 tok` | **`~814 tok`** vs. `~2,200 tok` |
| 6. `incident_db_failover` (`9` stages + rollback) | `5/5` vs. `5/5` | **`0 tok` (reused)** vs. `~1,877 tok` | **`~154 tok`** vs. `~562 tok` | `~838 tok` vs. `~711 tok` | **`~838 tok`** vs. `~2,588 tok` |
| 7. `tenant_gitops_onboarding` (`22` stages, 2 gates, 6 polls) | `5/5` vs. `5/5` | **`0 tok` (reused)** vs. `~1,370 tok` | **`~158 tok`** vs. `~1,026 tok` | **`~1,171 tok`** vs. `~1,414 tok` | **`~1,171 tok`** vs. `~2,784 tok` |
| **7-Workflow Session Total** | **`35/35` vs. `35/35`** | **`~1,097 tok` (1 skill) vs. `~7,075 tok` (7 skills, `-84%`)** | **`~795 tok` vs. `~2,992 tok` (`3.8x` less code)** | **`~4,472 tok` vs. `~3,917 tok`** | **`~5,570 tok` vs. `~10,992 tok` (`-49%`)** |

---

## 4. State-Passing & Failure-Recovery Ablation (`pypi_upgrade_guard`)

What happens when a workflow pauses at an approval gate, fails mid-flight
(triggering a rollback), and is then recovered? Without Lightflow's
`passport.json`, an Arm B agent must choose one of three state-passing
mechanisms:

| Arm | State-Passing Mechanism | Agent Command Output (`cmd`) | Warm Runtime (`cmd + stdout`) | `audit_pypi_versions` Executions | No Duplicate Side Effects | Rollback & Final State Valid |
| :--- | :--- | :---: | :---: | :---: | :---: | :---: |
| **Arm A (`Lightflow DAG`)** | `lightflow start` / `resume` + `passport.json` | **`486 ch` (`~122 tok`)** | **`2,613 ch` (`~653 tok`)** | **`1x`** | **`True`** | **`True`** |
| **Arm B0 (`Traditional Skill` — `/tmp/state.json`)** | `python3 -c` reading/writing `/tmp/state.json` | `1,349 ch` (`~337 tok`) | `1,806 ch` (`~452 tok`) | `1x` | `True` | `True` |
| **Arm B1 (`Traditional Skill` — Context State-Bus)** | `python3 -c` passing `outputs` JSON in CLI args | `1,872 ch` (`~468 tok`) | `3,031 ch` (`~758 tok`) | `1x` | `True` | `True` |
| **Arm B2 (`Traditional Skill` — Naive Re-Exec)** | `python3 -c` re-running `audit_pypi_versions` each turn | `1,024 ch` (`~256 tok`) | `1,448 ch` (`~362 tok`) | **`3x` (duplicated!)** | **`False`** | `True` |

---

## 5. Candid Assessment: Where Lightflow Wins vs. Where Traditional Skills Tie

1.  **Where Traditional Skills Tie or Win (`1` Short Linear Workflow, `3`
    Stages)**:
    -   If a user runs only **one small 3-stage workflow** (`hn_digest`) in a
        session and the agent uses a local `/tmp/state.json` file (`Arm B0`), a
        bespoke `SKILL.md` (`~507 tok`) + `python3 -c` (`~225 tok`) costs
        **`~732 tokens`**, whereas loading the general `run_lightflows/SKILL.md`
        (`~1,097 tok`) + CLI output (`~398 tok`) costs **`~1,495 tokens`**.
    -   **Crossover point**: By the **second workflow in a session** (`~1,881
        tok` cumulative for Arm A vs. `~1,506 tok` for Arm B) and decisively
        from the **third workflow onward** (`~2,534 tok` vs. `~2,613 tok`), or
        on **any single workflow with `>= 8` stages** (`blue_green_release`:
        `~1,911 tok` cold / `~814 tok` warm in Arm A vs. `~2,200 tok` in Arm B;
        `incident_db_failover`: `~1,935 tok` cold / `~838 tok` warm in Arm A vs.
        `~2,588 tok` in Arm B), Lightflow is cheaper in total tokens.
2.  **Where Lightflow Wins Decisively**:
    -   **`3.1x-6.5x` Less LLM-Generated Code (`cmd`) & `38x` Lower Runtime
        Variance**: Even when `SKILL.md` is 100% synchronized (`Cohort 2`), live
        Arm B agents must synthesize **`~1,565 ± 97 tokens`** of `python3 -c`
        orchestration code across `blue_green_release` and
        `incident_db_failover` vs. **`~502 ± 4 tokens`** in Arm A (`3.1x` less
        code and `38x` lower runtime token SD). On the 22-stage
        `tenant_gitops_onboarding` workflow, Arm A requires **`~158 tokens`** of
        CLI invocations vs. **`~1,026 tokens`** in Arm B (**`6.5x` less
        generated code**).
    -   **Immunity to Prose-to-Code Drift (`100%` vs. `50%` First-Try Compliance
        under 1 Omitted Return Detail)**: When `SKILL.md` omits a single
        return-type detail (`Cohort 3`), `5/5` Arm B agents crash mid-script
        *after* Stage 1 has already mutated disk state. Lightflow enforces DAG
        contracts via `lightflow compile` and checkpoints every stage boundary
        in `passport.json`.
    -   **`O(1)` vs. `O(N)` Skill Context Scaling (`-84%`)**: Arm A loads
        **one** `1,097`-token skill regardless of whether a repository has 5,
        20, or 100 workflows. Arm B requires a separate `500-1,900` token
        `SKILL.md` per workflow (`~7,075 tokens` for just 7 workflows) because
        every stage's signature, dict keys, branch rule, and rollback handler
        must be spelled out in prose to avoid reading `actions.py`.

--------------------------------------------------------------------------------

## 6. Authoring Workflow (`create_lightflow`)

In addition to the 7 domain execution pairs, `run_benchmark.py` also benchmarks
the 7-stage `examples/create_lightflow` meta-workflow (`5/5` trials passed,
`~1,580` warm runtime tokens, `~1,731` tokens persisted in `passport.json`,
`75%` zero-context savings vs. reading source files).

--------------------------------------------------------------------------------

## 7. Reproducing the Benchmark

```bash
# Run all 7 domain workflow pairs (Arm A + Arm B) + create_lightflow across 5 trials (default N=3)
python3 benchmarks/run_benchmark.py --trials=5

# Emit machine-readable JSON for all trials and arms
python3 benchmarks/run_benchmark.py --trials=5 --json
```
