---
name: benchmark_lightflow
description: "Runs, verifies and extends the Lightflow benchmark suites: Part I, step-adherence under operator pressure (benchmarks/reliability_benchmark.py, workflow prod_hotfix_rollout, 20 matched seeds, Arm A Lightflow vs Arm B prose runbook); Part II, deterministic context-footprint measurement (benchmarks/context_footprint.py) across the 7 bundled example workflows."
---

# Benchmarking Lightflow (`benchmark_lightflow`)

`benchmarks/README.md` is the report: question, protocol, grading rubric, the
design-v1 pilot results and why they are not quotable, the design-v2 results,
and the parked follow-ups. This skill is the operating manual.

## 1. Part I — step-adherence under operator pressure

### Verify what is checked in (no model needed)

```bash
# from the repository root
python3 -m unittest tests.test_reliability_benchmark            # harness tests: scripted agents through the real state machine
python3 benchmarks/reliability_benchmark.py --self-test        # Arm A end to end through the Lightflow CLI, 4 seeds
python3 benchmarks/reliability_benchmark.py --verify-results   # recompute statistics for every bundle under benchmarks/results/
```

### Run live trials (needs an agent harness)

One trial = one stateful multi-turn agent session in a fresh sandbox. The runner
must:

1.  Create the agent with the checked-in system prompt for its arm
    (`load_agent_system_prompt("A"|"B")`), on the model under test, with no
    access outside `/tmp/ops_bench_*`.
2.  Call the operator state machine and let it decide every turn:

    ```python
    record = await run_multi_turn_trial_loop(
        arm, seed,                       # "A"|"B", 1..20
        send_turn_fn,                    # async (prompt: str) -> reply: str, same session every time
        conversation_id=<agent conv id>, # optional but makes token metrics exact
        model="gemini-flash",            # recorded verbatim
        # pressure_variant defaults to pressure_variant_for_seed(seed): P1,P2,P3 round-robin
        # rule_variant="explicit" (v2 primary) | "plain" (realistic runbook) | "none"
    )
    ```

3.  Write each `record` to `result_seed<NN>_<arm>.json` in one directory, then
    bundle:

    ```bash
    python3 benchmarks/reliability_benchmark.py --aggregate /path/to/records --suite primary_n20 --model gemini-flash
    ```

    This writes `benchmarks/results/reliability_trials.json` (records +
    recomputed statistics) and prints the summary. Use `--output` for ablations
    (`--suite ablation_<name>`), and never mix layouts or design versions in one
    suite — `compute_suite_statistics` refuses.

Do not re-implement the turn logic in the runner: the loop gates checkpoint
messages on the audit trail, bounds the nudges, records every turn sent and
stamps the design version, layout, pressure variant and model into the record.
Trials flagged `harness_fault` are excluded from statistics automatically;
re-run those seeds.

### Inspect materials and prompts

```bash
python3 benchmarks/reliability_benchmark.py --materialize --arm=B --seed=7 --sandbox-dir=/tmp/ops_bench_demo
python3 benchmarks/reliability_benchmark.py --materialize --arm=A --seed=7 --sandbox-dir=/tmp/ops_bench_demo --unencapsulated   # design-v1 layout
python3 benchmarks/reliability_benchmark.py --print-prompts --seed=7 --variant=P2
```

Both arms' `SKILL.md` files, the domain CLI, the Lightflow manifest/adapters and
the agent system prompts are plain files under
`benchmarks/prod_hotfix_rollout/`; diff them side by side before changing either
arm. The checkpoint rule is one constant (`CHECKPOINT_RULE`) injected into both
files and asserted equal on every materialisation — if you change the wording,
bump `DESIGN_VERSION`.

## 2. Part II — context footprint

```bash
python3 benchmarks/context_footprint.py                  # table + per-step breakdown
python3 benchmarks/context_footprint.py --json           # machine-readable
python3 benchmarks/context_footprint.py --check-readme   # exit 1 if the README table is stale
python3 benchmarks/context_footprint.py --update-readme  # regenerate it
```

A measurement, not a comparison: there is no baseline arm. Runs all 7 example
Lightflows in-process against an isolated state directory (temp paths, PIDs and
timestamps normalised, so the output is bit-for-bit reproducible) and reports,
per workflow, warm runtime context (`cmd + stdout`), cold-start cost including
`skills/run_lightflows/SKILL.md` (and `skills/create_lightflows/SKILL.md` for
authoring), multi-turn cumulative input, and the sizes of `passport.json` and of
the sources. Report the last two as sizes, never as savings. The table in
`benchmarks/README.md` sits between `<!-- context_footprint:begin/end -->`
markers; `tests/test_context_footprint.py` fails when it is stale, and
`--update-readme` regenerates it.

## 3. Reporting rules

-   Report Part I as paired statistics (McNemar exact p, Agresti–Min CI on the
    difference) per suite; Fisher is secondary. Quote the primary endpoint (all
    7 invariants) together with I6 so a reader sees that I1–I5/I7 are saturated.
-   Always state design version, layout, model and pressure variants next to a
    number; never pool v1 and v2.
-   Token counts in Part II are a 4-chars-per-token heuristic; Part I token
    metrics come from the agent transcripts and are exact only when the runner
    passed `conversation_id`.
-   Count both directions of tool traffic (the agent's command strings and the
    tool's `stdout`); report warm and cold-start numbers separately.
