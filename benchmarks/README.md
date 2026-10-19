<!-- mdformat global-off -->
# Lightflow Token & Context Footprint Benchmark

Adding orchestration and guardrails to an AI agent workflow often increases prompt bloat because intermediate step outputs have to pass through tool `stdout` and get re-emitted by the LLM on subsequent turns.

Lightflow separates **control-plane messages** (which go to `stdout` for the agent) from **data-plane state** (which stays on disk in `passport.json`):

1. **On-Disk State Bus (`passport.json`)**: Structured stage outputs (`payload.outputs.<stage>`) are merged and persisted on disk between Python stages rather than printed to `stdout` and re-serialized by the LLM into the next tool call. `stdout` carries only 1-line stage status summaries and the active `operator_action` gate's instructions and JSON Schema.
2. **Direct Entry (`lightflow start`)**: Because stage ordering, `run_if` branching, JSON schemas, and `rollback_action` hooks are enforced by the engine from `lightflow.yaml` + `actions.py`, an agent executing an existing workflow runs `lightflow start` directly and responds to each gate as it arrives (`--log_id` remembers the manifest across `resume`, `status`, `visualize`, and `cleanup`).
3. **Single Reusable Runner Skill**: A single generic runner skill ([`skills/run_lightflows/SKILL.md`](../skills/run_lightflows/SKILL.md), **4,489 chars / ~1,122 tokens**) drives any Lightflow workflow without custom per-workflow state-file plumbing.

---

## Reproduce

This directory contains a deterministic, zero-dependency benchmark harness ([`run_benchmark.py`](./run_benchmark.py)) and an agent skill ([`SKILL.md`](./SKILL.md)):

```bash
python3 benchmarks/run_benchmark.py
python3 benchmarks/run_benchmark.py --json
```

---

## 1. Live Subagent A/B Trial (`pypi_upgrade_guard`, Cold Start `N=1`)

Two fresh zero-context LLM subagents executed the exact same end-to-end workflow (`pypi_upgrade_guard`: audit offline PyPI packages, pause for operator approval, hit a simulated smoke-test failure + rollback, then recover and upgrade):

| Metric | Arm A: Lightflow `v0.3.0` (`N=1` Cold Start incl. `run_lightflows/SKILL.md`) | Arm B: Unstructured Execution (No Lightflow CLI) | Delta |
| :--- | :---: | :---: | :---: |
| **Total Tool Calls** | **4** (`1 skill read` + `3 CLI`, chaining `resume && cleanup`) | **5** (`3 file reads` + `2 inline Python scripts`) | **20% fewer calls** |
| **Agent-Generated Tool Inputs** | **1,410 ch (~352 tok)** | **4,429 ch (~1,107 tok)** | **68% fewer tool-input tokens ($3.1\times$ smaller)** |
| **Billed LLM Output Tokens** | **1,923 tok** | **3,439 tok** | **44% fewer billed output tokens** |
| **Agent Thinking / Reasoning** | **1,081 ch (~270 tok)** | **1,732 ch (~433 tok)** | **38% fewer reasoning tokens** |
| **End-of-Run Transcript Size** | **10,135 ch (~2,534 tok)** | **18,624 ch (~4,656 tok)** | **46% smaller transcript** |
| **Cumulative Multi-Turn Task Input** | **41,286 ch (~10,322 tok)** | **70,005 ch (~17,501 tok)** | **41% fewer cumulative tokens** |
| **Guardrail Enforcement** | **Deterministic Python** (`json_schema`, auto-`rollback_action`, surgical `resume`) | **Prompt compliance only** (agent hand-wrote `try/except` rollback glue) | **Engine-enforced vs. ad-hoc** |

*Why Arm B generated $3.1\times$ more tool-input tokens*: Without `passport.json` persisting `payload.outputs.audit_pypi_versions` on disk across steps, the unstructured agent had to write custom Python scripts to wire the audit output into `apply_and_smoke_test`, catch `RuntimeError`, call `restore_requirements_backup`, and re-run recovery—whereas Arm A ran `lightflow resume --log_id=pypi_bench_a --payload='{"simulate_smoke_failure": false}' && lightflow cleanup --log_id=pypi_bench_a`.

---

## 2. Measured Per-Workflow Context Footprint (`run_benchmark.py`)

| Workflow | CLI Calls | Warm Runtime (`cmd + stdout`) | Cold Start (`N=1` incl. `SKILL.md`) | Multi-Turn Cumulative | Persisted in `passport.json` (Off-Context) | On-Disk Blueprint (`yaml + actions.py`) |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: |
| [`hn_digest`](../examples/hn_digest/) | 3 | 1,673 ch (~**418 tok**) | 6,162 ch (~1,540 tok) | ~1,058 tok | 4,250 ch (~1,062 tok) | 7,387 ch (~1,847 tok) |
| [`usgs_seismic_alert`](../examples/usgs_seismic_alert/) | 3 | 1,664 ch (~**416 tok**) | 6,153 ch (~1,538 tok) | ~1,039 tok | 3,450 ch (~862 tok) | 6,801 ch (~1,700 tok) |
| [`pypi_upgrade_guard`](../examples/pypi_upgrade_guard/) *(incl. fail + rollback + recovery)* | 4 | 2,625 ch (~**656 tok**) | 7,114 ch (~1,778 tok) | ~2,058 tok | 5,928 ch (~1,482 tok) | 8,375 ch (~2,094 tok) |
| [`async_job_watcher`](../examples/async_job_watcher/) | 2 | 1,012 ch (~**253 tok**) | 5,501 ch (~1,375 tok) | ~457 tok | 2,714 ch (~678 tok) | 10,213 ch (~2,553 tok) |
| [`create_lightflow`](../examples/create_lightflow/) *(3 gates + 2 verifiers + authored files)* | 5 | 6,797 ch (~**1,699 tok**) | 19,463 ch (~4,866 tok) | ~6,135 tok | 7,192 ch (~1,798 tok) | 18,426 ch (~4,606 tok) |
| [`tenant_gitops_onboarding`](../examples/tenant_gitops_onboarding/) *(22 stages: 2 gates, 3 branch patterns, 2 PRs, 6 polls)* | 4 | 4,962 ch (~**1,240 tok**) | 9,451 ch (~2,363 tok) | ~3,502 tok | 15,187 ch (~3,797 tok) | 37,409 ch (~9,352 tok) |
| **Total (6 workflows, warm)** | **21** | **18,733 ch (~4,683 tok)** | — | **~14,249 tok** | **38,721 ch (~9,680 tok)** | **88,611 ch (~22,153 tok)** |
| **Session Total (`N=6`, cold start incl. both `SKILL.md`s)** | **21 + 2 reads** | — | **31,399 ch (~7,850 tok)** | — | **38,721 ch (~9,680 tok)** | **88,611 ch (~22,153 tok)** |

---

## 3. Methodology & Disclosures

1. **Both Agent Inputs and Tool `stdout` Are Counted**:
   - `Warm Runtime (cmd + stdout)` counts every CLI command string emitted by the agent (`lightflow start ...`, `lightflow resume ...`, `lightflow cleanup ...`), any files authored between gates in `create_lightflow` (`actions.py`, `actions_test.py`, `lightflow.yaml`), and the full `stdout`/`stderr` returned by the process.
2. **Single-Workflow (`N=1`) and Session (`N=6`) Cold Starts Are Reported Explicitly**:
   - Running an existing workflow (rows 1–4 and 6) from a cold start requires reading [`skills/run_lightflows/SKILL.md`](../skills/run_lightflows/SKILL.md) once (**4,489 chars / ~1,122 tokens**).
   - Authoring a new workflow (`create_lightflow`, row 5) from a cold start also counts [`skills/create_lightflows/SKILL.md`](../skills/create_lightflows/SKILL.md) (**8,177 chars / ~2,044 tokens**).
   - Because `run_lightflows/SKILL.md` is generic across all Lightflow workflows, its ~1,122-token cost is paid **once per session** rather than once per workflow.
3. **Agent Push Notifications Are Opt-In (`LIGHTFLOW_NOTIFY`)**:
   - Inside an `agentapi`-compatible host, the engine can also push messages into the agent's conversation. Because synchronous CLI runs already return the terminal state on `stdout`, `LIGHTFLOW_NOTIFY` defaults to `off` (**0 pushes**). For detached/background runs, `LIGHTFLOW_NOTIFY=milestones` forwards only gate suspensions, failures, rejections, rollbacks, and the final outcome (**3 pushes / ~80 tokens** for the 22-stage `tenant_gitops_onboarding` PCI path, versus **44 pushes / ~1,150 tokens** with `LIGHTFLOW_NOTIFY=all`).
