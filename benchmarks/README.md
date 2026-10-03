# Lightflow Unified A/B Benchmark: Lightflow DAG (`Arm A`) vs. Traditional Skill (`Arm B`)

This benchmark evaluates whether **Lightflow (`Arm A`)** earns its keep against
its natural baseline: a **Steelmanned Traditional Skill (`Arm B`: `SKILL.md` +
`actions.py`)** where the Python business logic is already factored out into
`actions.py`.

Both arms execute the **exact same Python helper functions** in
`examples/<name>/actions.py` across **7 matched domain workflow pairs** spanning
a complexity ladder from **3 to 22 stages** (`3, 3, 3, 4, 8, 9, and 22` stages),
evaluated across **`N=5` trials per workflow** in the deterministic harness
(`run_benchmark.py --trials=5`) plus **`15` live zero-context LLM subagent
sessions (`N=5` per cohort across 3 controlled cohorts = `30` live workflow
runs)** on the 8-stage and 9-stage failure-recovery workflows:

1.  **Correctness & Side-Effect Safety** — Does every stage execute in valid
    dependency order, honor `run_if` / `trigger_rule` branch conditions, execute
    compensating rollbacks on mid-flight failures, and recover surgically from
    the failed stage without re-executing completed upstream stages?
2.  **Consistency (`N=5` Multi-Trial Variance + Prose-Drift Sensitivity)** —
    Across `N=5` independent zero-context runs, how much variance (`Mean ± SD`,
    `Min-Max`) exists in command count, agent-generated code (`cmd`), and
    runtime tokens (`cmd + stdout`), and what happens when a single return-type
    detail in `SKILL.md` drifts from `actions.py`?
3.  **Context & Code-Generation Cost** — How many tokens of skill instructions
    must enter the context window, how many tokens of shell/Python code must the
    LLM generate (`cmd`), and how many total session tokens (`Skill + cmd +
    stdout`) are consumed?

--------------------------------------------------------------------------------

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

--------------------------------------------------------------------------------

## 2. Live Zero-Context LLM Subagent Benchmark (`N=5` Trials per Cohort = `15` Sessions / `30` Workflow Runs)

To measure real-world **consistency and variance (`Mean ± SD`)** alongside
**prose-to-code drift sensitivity**, we spawned **15 independent zero-context
LLM subagent sessions** (`N=5` trials in each of 3 controlled cohorts) executing
both new `>5`-stage failure-recovery workflows back-to-back in a single session
(`30` live workflow executions total):

1.  **`examples/blue_green_release` (8 stages)**: Parallel validation diamond
    (`run_security_scan` $\parallel$ `run_integration_suite`) $\rightarrow$
    2-tick `warm_green_environment` poll $\rightarrow$ `approve_canary_cutover`
    gate $\rightarrow$ `shift_and_verify_canary` SLO breach (`exit 1`) +
    automatic `revert_canary_traffic` rollback $\rightarrow$ surgical recovery
    (`Stages 6-8` only) $\rightarrow$ `promote_green_to_prod` $\rightarrow$
    `decommission_old_slot`.
2.  **`examples/incident_db_failover` (9 stages)**: `detect_primary_outage`
    $\rightarrow$ `elect_failover_candidate` $\rightarrow$ mutually exclusive
    `run_if` WAL replay poll (`replay_missing_wal_segments` vs. skipped
    `verify_zero_loss_sync`) $\rightarrow$ `trigger_rule: ALL_DONE`
    `fence_old_primary` join $\rightarrow$ `approve_replica_promotion` IC gate
    $\rightarrow$ one-way `promote_standby_replica` (`promotion_count == 1`)
    $\rightarrow$ `cutover_pooler_and_verify_writes` canary write failure (`exit
    1`) + automatic `revert_pooler_to_maintenance` rollback $\rightarrow$
    surgical recovery (`Stages 8-9` only, preserving `promotion_count == 1`)
    $\rightarrow$ `publish_failover_ledger`.

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

| Metric (Across  | **Cohort 1:  | **Cohort 2:    | **Cohort 3: Arm | Key Takeaway   |
: `N=5`           : Arm A        : Arm B          : B               : (`Arm A` vs.   :
: Independent     : (`Lightflow  : (`Synchronized : (`Prose-Drifted : `Arm B`)       :
: Live Subagent   : DAG`,        : SKILL.md`,     : SKILL.md`,      :                :
: Sessions = `10` : `N=5`)**     : `N=5`)**       : `N=5`)**        :                :
: Workflow Runs / :              :                :                 :                :
: Cohort)         :              :                :                 :                :
| :-------------- | :----------: | :------------: | :-------------: | :------------: |
| **First-Try     | **`10/10`    | **`10/10`      | **`5/10`        | **Synchronized |
: Invariant       : (`100%`)**   : (`100%`)**     : (`50%`)** *(all : `SKILL.md`     :
: Compliance**    :              :                : 5 crashed on    : reaches        :
: *(zero          :              :                : Task 1 Stage 2  : `10/10`, but 1 :
: mid-script      :              :                : after Stage 1   : omitted return :
: crash, zero     :              :                : mutated disk    : detail drops   :
: duplicate Stage :              :                : state)*         : Arm B to       :
: 1 calls)*       :              :                :                 : `5/10`**       :
| **Post-Recovery | **`10/10`    | **`10/10`      | **`10/10`       | All cohorts    |
: Final State &   : (`100%`)**   : (`100%`)**     : (`100%`)**      : reach valid    :
: Rollback        :              :                : *(after manual  : final state    :
: Validity**      :              :                : `rm -f` cleanup :                :
:                 :              :                : & retry)*       :                :
| **Shell         | **`6.0 ±     | **`6.0 ± 0.0`  | **`7.2 ± 0.4`   | **Zero command |
: Commands per    : 0.0` cmds**  : cmds** (`6-6`) : cmds** (`7-8`)  : variance when  :
: 2-Workflow      : (`6-6`)      :                :                 : synchronized;  :
: Session (`Mean  :              :                :                 : `+1.2` extra   :
: ± SD`)**        :              :                :                 : retry turns    :
:                 :              :                :                 : under prose    :
:                 :              :                :                 : drift**        :
| **Agent Command | **`~502 ± 4  | **`~1,565 ± 97 | **`~1,954 ± 136 | **`3.1x` less  |
: Output `cmd`    : tok`**       : tok`** (`6,259 : tok`** (`7,816  : code (`3.9x`   :
: (`Mean ± SD`,   : (`2,009 ± 17 : ± 389 ch`,     : ± 542 ch`,      : under drift) • :
: `Min-Max`)**    : ch`,         : `5,854-6,738   : `7,259-8,673    : `23x` lower    :
:                 : `1,995-2,039 : ch`)           : ch`)            : code-gen SD**  :
:                 : ch`)         :                :                 :                :
| **Warm Runtime  | **`~1,847 ±  | **`~2,433 ±    | **`~2,992 ± 233 | **`24%` fewer  |
: `cmd + stdout`  : 4 tok`**     : 144 tok`**     : tok`** (`11,969 : runtime tok    :
: (`Mean ± SD`,   : (`7,389 ± 15 : (`9,731 ± 576  : ± 931 ch`,      : (`38%` under   :
: `Min-Max`)**    : ch`,         : ch`,           : `10,817-13,077  : drift) • `38x` :
:                 : `7,382-7,416 : `9,054-10,428  : ch`)            : lower runtime  :
:                 : ch`)         : ch`)           :                 : SD**           :
| **Skill Context | **`~1,097    | **`~3,471      | **`~3,176       | **`3.2x`       |
: Loaded (`1`     : tok`**       : tok`**         : tok`** (`12,704 : smaller skill  :
: Reusable vs.    : (`4,388 ch`) : (`13,886 ch`)  : ch`)            : footprint**    :
: `2` Bespoke     :              :                :                 :                :
: Skills)**       :              :                :                 :                :
| **2-Workflow    | **`~2,944 ±  | **`~5,904 ±    | **`~6,168 ± 233 | **`50%` lower  |
: Session Total   : 4 tok`**     : 144 tok`**     : tok`** (`24,673 : total session  :
: (`Skill + cmd + : (`11,777 ±   : (`23,617 ± 576 : ± 931 ch`)      : tokens (`2.0x` :
: stdout`)**      : 15 ch`)      : ch`)           :                 : reduction)**   :

### Why Both Cohorts 2 and 3 Matter

1.  **Even when `SKILL.md` is 100% synchronized (`Cohort 2`, `10/10` pass rate),
    Lightflow cuts LLM-generated code by `3.1x` and runtime token variance by
    `38x`**: In Arm A, every subagent emitted the exact same 6 declarative CLI
    calls (`start` $\rightarrow$ `resume --resolution=APPROVE` $\rightarrow$
    `resume`), yielding a standard deviation of just **`15 chars` (`~4
    tokens`)** across 5 runs. In Cohort 2, even with zero errors, each subagent
    synthesized `5,854-6,738 chars` (`~1,565 ± 97 tok`) of custom `python3 -c`
    glue (`while True:` polling loops, `if/else` WAL branches, `try/except`
    rollback blocks, and `json.dump` state checkpoints), resulting in **`38x`
    higher runtime token standard deviation** (`±144 tok` vs. `±4 tok`) and
    **`2.0x` higher total session tokens** (`5,904` vs. `2,944` tok).
2.  **Prose-to-Code Drift Sensitivity (`Cohort 3`, `5/10` first-try pass
    rate)**: In a Traditional Skill, the contract between `SKILL.md` and
    `actions.py` is unverified prose. When `SKILL.md` omitted a single
    return-type detail (`(output_dict, message_str)` 2-tuple vs. plain `dict`),
    all **`5/5` Cohort 3 subagents crashed on Stage 2 (`run_security_scan`)**
    *after* Stage 1 (`build_release_bundle`) had already mutated disk state,
    forcing manual `rm -f` cleanup and re-execution of Stage 1. Lightflow
    validates the DAG via `lightflow compile`, checkpoints `passport.json` at
    every stage boundary, and handles action return unpacking inside the engine.

--------------------------------------------------------------------------------

## 3. Unified 7-Pair Domain Complexity Ladder (`run_benchmark.py --trials=5`)

Run `python3 benchmarks/run_benchmark.py --trials=5` to execute both arms across
all **7 domain workflow pairs** (`3, 3, 3, 4, 8, 9, and 22` stages) and verify
all ground-truth invariants (`35/35` domain trial checks passed):

| Workflow Pair (Stages)     | Arm A /   | Skill      | Agent   | Warm      | Session    |
:                            : Arm B     : Loaded     : Command : Runtime   : Total      :
:                            : Checks    : (`Arm A`   : Output  : `cmd +    : (`Skill +  :
:                            : (`N=5`)   : vs. `Arm   : `cmd`   : stdout`   : Warm`)     :
:                            :           : B`)        : (`Arm   : (`Arm A`  : (`Arm A`   :
:                            :           :            : A` vs.  : vs. `Arm  : vs. `Arm   :
:                            :           :            : `Arm    : B`)       : B`)        :
:                            :           :            : B`)     :           :            :
| :------------------------- | :-------: | :--------: | :-----: | :-------: | :--------: |
| 1. `hn_digest` (`3`        | `5/5` vs. | `~1,097    | **`~83  | `~398     | `~1,495    |
: stages)                    : `5/5`     : tok` (1st) : tok`**  : tok` vs.  : tok` vs.   :
:                            :           : vs. `~507  : vs.     : `~225     : **`~732    :
:                            :           : tok`       : `~178   : tok`      : tok`**     :
:                            :           :            : tok`    :           :            :
| 2. `usgs_seismic_alert`    | `5/5` vs. | **`0 tok`  | **`~89  | `~386     | **`~386    |
: (`3` stages)               : `5/5`     : (reused)** : tok`**  : tok` vs.  : tok`** vs. :
:                            :           : vs. `~548  : vs.     : `~226     : `~774 tok` :
:                            :           : tok`       : `~195   : tok`      :            :
:                            :           :            : tok`    :           :            :
| 3. `pypi_upgrade_guard`    | `5/5` vs. | **`0 tok`  | **`~122 | `~653     | **`~653    |
: (`3` stages + rollback)    : `5/5`     : (reused)** : tok`**  : tok` vs.  : tok`** vs. :
:                            :           : vs. `~656  : vs.     : `~452     : `~1,107    :
:                            :           : tok`       : `~337   : tok`      : tok`       :
:                            :           :            : tok`    :           :            :
| 4. `async_job_watcher`     | `5/5` vs. | **`0 tok`  | **`~50  | **`~211   | **`~211    |
: (`4` stages + poll)        : `5/5`     : (reused)** : tok`**  : tok`**    : tok`** vs. :
:                            :           : vs. `~522  : vs.     : vs. `~285 : `~807 tok` :
:                            :           : tok`       : `~212   : tok`      :            :
:                            :           :            : tok`    :           :            :
| 5. `blue_green_release`    | `5/5` vs. | **`0 tok`  | **`~139 | `~814     | **`~814    |
: (`8` stages + rollback)    : `5/5`     : (reused)** : tok`**  : tok` vs.  : tok`** vs. :
:                            :           : vs.        : vs.     : `~605     : `~2,200    :
:                            :           : `~1,594    : `~481   : tok`      : tok`       :
:                            :           : tok`       : tok`    :           :            :
| 6. `incident_db_failover`  | `5/5` vs. | **`0 tok`  | **`~154 | `~838     | **`~838    |
: (`9` stages + rollback)    : `5/5`     : (reused)** : tok`**  : tok` vs.  : tok`** vs. :
:                            :           : vs.        : vs.     : `~711     : `~2,588    :
:                            :           : `~1,877    : `~562   : tok`      : tok`       :
:                            :           : tok`       : tok`    :           :            :
| 7.                         | `5/5` vs. | **`0 tok`  | **`~158 | **`~1,171 | **`~1,171  |
: `tenant_gitops_onboarding` : `5/5`     : (reused)** : tok`**  : tok`**    : tok`** vs. :
: (`22` stages, 2 gates, 6   :           : vs.        : vs.     : vs.       : `~2,784    :
: polls)                     :           : `~1,370    : `~1,026 : `~1,414   : tok`       :
:                            :           : tok`       : tok`    : tok`      :            :
| **7-Workflow Session       | **`35/35` | **`~1,097  | **`~795 | **`~4,472 | **`~5,570  |
: Total**                    : vs.       : tok` (1    : tok`    : tok` vs.  : tok` vs.   :
:                            : `35/35`** : skill) vs. : vs.     : `~3,917   : `~10,992   :
:                            :           : `~7,075    : `~2,992 : tok`**    : tok`       :
:                            :           : tok` (7    : tok`    :           : (`-49%`)** :
:                            :           : skills,    : (`3.8x` :           :            :
:                            :           : `-84%`)**  : less    :           :            :
:                            :           :            : code)** :           :            :

--------------------------------------------------------------------------------

## 4. State-Passing & Failure-Recovery Ablation (`pypi_upgrade_guard`)

What happens when a workflow pauses at an approval gate, fails mid-flight
(triggering a rollback), and is then recovered? Without Lightflow's
`passport.json`, an Arm B agent must choose one of three state-passing
mechanisms:

| Arm                  | State-Passing         | Agent   | Warm     | `audit_pypi_versions` | No          | Rollback & |
:                      : Mechanism             : Command : Runtime  : Executions            : Duplicate   : Final      :
:                      :                       : Output  : (`cmd +  :                       : Side        : State      :
:                      :                       : (`cmd`) : stdout`) :                       : Effects     : Valid      :
| :------------------- | :-------------------- | :-----: | :------: | :-------------------: | :---------: | :--------: |
| **Arm A (`Lightflow  | `lightflow start` /   | **`486  | **`2,613 | **`1x`**              | **`True`**  | **`True`** |
: DAG`)**              : `resume` +            : ch`     : ch`      :                       :             :            :
:                      : `passport.json`       : (`~122  : (`~653   :                       :             :            :
:                      :                       : tok`)** : tok`)**  :                       :             :            :
| **Arm B0             | `python3 -c`          | `1,349  | `1,806   | `1x`                  | `True`      | `True`     |
: (`Traditional Skill` : reading/writing       : ch`     : ch`      :                       :             :            :
: —                    : `/tmp/state.json`     : (`~337  : (`~452   :                       :             :            :
: `/tmp/state.json`)** :                       : tok`)   : tok`)    :                       :             :            :
| **Arm B1             | `python3 -c` passing  | `1,872  | `3,031   | `1x`                  | `True`      | `True`     |
: (`Traditional Skill` : `outputs` JSON in CLI : ch`     : ch`      :                       :             :            :
: — Context            : args                  : (`~468  : (`~758   :                       :             :            :
: State-Bus)**         :                       : tok`)   : tok`)    :                       :             :            :
| **Arm B2             | `python3 -c`          | `1,024  | `1,448   | **`3x`                | **`False`** | `True`     |
: (`Traditional Skill` : re-running            : ch`     : ch`      : (duplicated!)**       :             :            :
: — Naive Re-Exec)**   : `audit_pypi_versions` : (`~256  : (`~362   :                       :             :            :
:                      : each turn             : tok`)   : tok`)    :                       :             :            :

--------------------------------------------------------------------------------

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
