---
name: benchmark-lightflows
description: "Run and interpret the unified A/B Lightflow benchmark (benchmarks/run_benchmark.py) comparing Arm A (Lightflow DAG) against Arm B (Traditional Skill) across 7 matched domain workflow pairs (3 to 22 stages)."
---

# Benchmarking Lightflow Workflows

Use `benchmarks/run_benchmark.py` to measure and verify **Arm A (`Lightflow
DAG`)** against **Arm B (`Steelmanned Traditional Skill`:
`benchmarks/baseline_skills/<name>/SKILL.md` + `examples/<name>/actions.py`)**
across all 7 domain workflow pairs (`hn_digest`, `usgs_seismic_alert`,
`pypi_upgrade_guard`, `async_job_watcher`, `blue_green_release`,
`incident_db_failover`, `tenant_gitops_onboarding`) plus the `create_lightflow`
authoring workflow.

--------------------------------------------------------------------------------

## Commands

```bash
# Run the unified A/B benchmark (Markdown report)
python3 benchmarks/run_benchmark.py

# Run N=5 independent trials to verify multi-run consistency
python3 benchmarks/run_benchmark.py --trials=5

# Emit machine-readable JSON metrics
python3 benchmarks/run_benchmark.py --trials=5 --json
```

--------------------------------------------------------------------------------

## What Is Verified on Every Run

1.  **Ground-Truth Correctness & Invariants**:
    -   `order_match`: Completed stages in `passport.json` match the exact
        expected topological sequence.
    -   `no_duplicate_side_effects`: Every stage completes at most once (`1x`),
        including across mid-run failures and `lightflow resume` recoveries
        (`pypi_upgrade_guard`, `blue_green_release`, `incident_db_failover`).
    -   `gate_discipline`: Every `operator_action` stage records a `PAUSED`
        stamp (`exit 2`) before its `COMPLETED` stamp.
    -   `external_state_valid` & `baseline_external_state_valid`: Both Arm A and
        Arm B produce the exact expected on-disk artifacts, intermediate
        rollbacks, and cleanups.
2.  **Context & Code Generation Cost (`1 token ≈ 4 chars`)**:
    -   **Skill Loaded**: Universal `skills/run_lightflows/SKILL.md` (`~1,097
        tok` once per session) vs. per-workflow
        `benchmarks/baseline_skills/<name>/SKILL.md` (`~7,075 tok` across the 7
        domain workflows).
    -   **Agent Command Output (`cmd`)**: Compact declarative `lightflow start /
        resume` CLI calls (`~795 tok` across 7 workflows) vs. inline `python3
        -c` orchestration snippets (`~2,992 tok` across 7 workflows).
    -   **Out-of-Context State**: `passport.json` bytes persisted on disk
        (`52,958 chars` / `~13,240 tok`) and `lightflow.yaml + actions.py`
        source bytes avoided (`115,321 chars` / `~28,830 tok`, **83% warm
        zero-context savings**).
