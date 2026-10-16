<!-- mdformat global-off -->
# Lightflow Benchmark: Structural Rigor at Negative Context & Latency Cost

Usually, adding guardrails and orchestration to an AI agent **increases** prompt bloat and latency. **Lightflow gives you a deterministic structural backbone**—hard `EXIT_CODE=2` human gates, JSON Schema validation, automatic `rollback_action` hooks, and surgical `resume` without re-running upstream side effects—**at *lower* token and latency cost than an unstructured skill, not higher.**

Why structural rigor is **negative-cost** in Lightflow:

1. **Zero-Cost (and Negative-Cost) Guardrails**: Instead of stuffing step ordering, conditional branching (`run_if`), JSON schemas, and recovery instructions into the LLM prompt, the skill blueprint lives on disk in `lightflow.yaml` + `actions.py`. The agent runs `lightflow start` directly (**Zero-Context Entry**) and only receives the active checkpoint's instructions and schema when needed.
2. **$O(1)$ Agent Context vs. $O(N)$ Payload Size (No "LLM as State Bus")**: Without `passport.json`, the LLM itself has to act as the state bus—reading stage 1's output JSON from `stdout` and re-emitting it token-by-token into stage 2's tool call (which is $5\times$–$10\times$ slower to generate and risks silent data corruption). In Lightflow, structured stage outputs (`payload.outputs.<stage>`) flow between Python stages on disk via `passport.json`, while `stdout` stays a 1-line summary.
3. **No Bespoke State-File Plumbing per Skill**: If a skill author tries to avoid passing JSON through the LLM without Lightflow, they end up hand-rolling custom `/tmp/state.json` files, `--step` flags, and rollback glue inside every skill. Lightflow provides that engine once, driven by a single reusable ~1,123-token runner skill ([`skills/run_lightflows/SKILL.md`](../skills/run_lightflows/SKILL.md)).

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

| Metric | Arm A: Lightflow DAG (`N=1` Cold Start incl. `run_lightflows/SKILL.md`) | Arm B: Unstructured Execution (No Lightflow CLI) | Delta |
| :--- | :---: | :---: | :---: |
| **Total Tool Calls** | **5** (`1 skill read` + `4 CLI`) | **7** (`2 file reads` + `5 python -c`) | **29% fewer calls** |
| **Agent-Generated Tool Inputs (Output Tokens)** | **3,229 ch (~807 tok)** | **9,483 ch (~2,371 tok)** | **66% fewer output tokens ($2.9\times$ smaller)** |
| **Agent Thinking / Reasoning** | **1,093 ch (~273 tok)** | **1,675 ch (~419 tok)** | **35% fewer reasoning tokens** |
| **End-of-Run Transcript Size** | **16,315 ch (~4,079 tok)** | **24,255 ch (~6,064 tok)** | **33% smaller transcript** |
| **Cumulative Multi-Turn Input** | **60,921 ch (~15,230 tok)** | **97,595 ch (~24,399 tok)** | **38% fewer cumulative tokens** |
| **Guardrail Enforcement** | **Deterministic Python** (`json_schema`, auto-`rollback_action`, surgical `resume`) | **Prompt compliance only** (agent hand-wrote `try/except` rollback glue) | **Hard vs. soft guarantees** |

*Why Arm B generated $2.9\times$ more tool-input tokens*: Without `passport.json` persisting `payload.outputs.audit_pypi_versions` on disk across steps, the unstructured agent had to act as the state bus—re-serializing the upstream audit payload dictionary into inline Python scripts on both Step 2 (smoke test failure + manual rollback call) and Step 3 (recovery), whereas Arm A ran `lightflow resume --payload='{"simulate_smoke_failure": false}'`.

---

## 2. Deterministic Per-Workflow Footprint (`run_benchmark.py`)

| Workflow | CLI Calls | Warm Runtime (`cmd + stdout`) | Cold Start (`N=1` incl. `SKILL.md`) | Multi-Turn Cumulative | Out-of-Context State (`passport.json`) | Source Avoided (`yaml + actions.py`) | Zero-Context Savings (Warm) |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| [`hn_digest`](../examples/hn_digest/) | 3 | 1,704 ch (~**426 tok**) | 6,196 ch (~1,549 tok) | ~1,074 tok | 4,235 ch (~1,059 tok) | 7,387 ch (~1,847 tok) | **81%** |
| [`usgs_seismic_alert`](../examples/usgs_seismic_alert/) | 3 | 1,704 ch (~**426 tok**) | 6,196 ch (~1,549 tok) | ~1,059 tok | 3,435 ch (~859 tok) | 6,801 ch (~1,700 tok) | **80%** |
| [`pypi_upgrade_guard`](../examples/pypi_upgrade_guard/) *(incl. fail + rollback + recovery)* | 4 | 2,705 ch (~**676 tok**) | 7,197 ch (~1,799 tok) | ~2,108 tok | 5,913 ch (~1,478 tok) | 8,375 ch (~2,094 tok) | **76%** |
| [`async_job_watcher`](../examples/async_job_watcher/) | 2 | 1,012 ch (~**253 tok**) | 5,504 ch (~1,376 tok) | ~457 tok | 2,705 ch (~676 tok) | 10,213 ch (~2,553 tok) | **91%** |
| [`create_lightflow`](../examples/create_lightflow/) *(3 gates + 2 verifiers + authored files)* | 5 | 6,803 ch (~**1,701 tok**) | 19,499 ch (~4,875 tok) | ~6,106 tok | 7,052 ch (~1,763 tok) | 18,273 ch (~4,568 tok) | **73%** |
| [`tenant_gitops_onboarding`](../examples/tenant_gitops_onboarding/) *(22 stages: 2 gates, 3 branch patterns, 2 PRs, 6 polls)* | 4 | 5,103 ch (~**1,276 tok**) | 9,595 ch (~2,399 tok) | ~3,594 tok | 15,172 ch (~3,793 tok) | 37,409 ch (~9,352 tok) | **88%** |
| **Total (6 workflows, warm)** | **21** | **19,031 ch (~4,758 tok)** | — | **~14,398 tok** | **38,512 ch (~9,628 tok)** | **88,458 ch (~22,114 tok)** | **82%** |
| **Session Total (`N=6`, cold start incl. both `SKILL.md`s)** | **21 + 2 reads** | — | **31,727 ch (~7,932 tok)** | — | **38,512 ch (~9,628 tok)** | **88,458 ch (~22,114 tok)** | **74%** |

---

## 3. Methodology & Fairness Disclosures

1. **Both Agent Inputs and Tool `stdout` Are Counted**:
   - `Warm Runtime (cmd + stdout)` counts every CLI command string emitted by the agent (`lightflow start ...`, `lightflow resume ...`, `lightflow cleanup ...`), any files authored between gates in `create_lightflow` (`actions.py`, `actions_test.py`, `lightflow.yaml`), and the full `stdout`/`stderr` returned by the process.
2. **Single-Workflow (`N=1`) and Session (`N=6`) Cold Starts Are Reported Explicitly**:
   - Running an existing workflow (rows 1–4 and 6) from a cold start requires reading [`skills/run_lightflows/SKILL.md`](../skills/run_lightflows/SKILL.md) once (**4,492 chars / ~1,123 tokens**).
   - Authoring a new workflow (`create_lightflow`, row 5) from a cold start also counts [`skills/create_lightflows/SKILL.md`](../skills/create_lightflows/SKILL.md) (**8,204 chars / ~2,051 tokens**).
   - Because `run_lightflows/SKILL.md` is generic across all Lightflow workflows, its ~1,123-token cost is paid **once per session** rather than once per workflow.
3. **Agent Push Notifications Are Opt-In (`LIGHTFLOW_NOTIFY`)**:
   - Inside an `agentapi`-compatible host, the engine can also push messages into the agent's conversation. Because synchronous CLI and MCP runs already return the terminal state on `stdout`/JSON, `LIGHTFLOW_NOTIFY` defaults to `off` (**0 pushes**). For detached/background runs, `LIGHTFLOW_NOTIFY=milestones` forwards only gate suspensions, failures, rejections, rollbacks, and the final outcome (**3 pushes / ~80 tokens** for the 22-stage `tenant_gitops_onboarding` PCI path, versus **44 pushes / ~1,150 tokens** with `LIGHTFLOW_NOTIFY=all`).
4. **Definition of `Zero-Context Savings`**:
   - Defined as $\frac{\text{Source Avoided}}{\text{Runtime Context} + \text{Source Avoided}}$, comparing an agent that follows the Zero-Context Entry Rule (`lightflow start` directly) against a white-box coding agent that reads `lightflow.yaml` + `actions.py` before executing the workflow.
   - Note that a traditional prompt-only skill would not paste Python source files into a prompt; instead, without `passport.json` persisting intermediate data on disk (`38,512 chars / ~9,628 tokens` across the 6 runs), intermediate step outputs (`payload.outputs.<stage>`) must pass through tool `stdout` into the LLM conversation history—or the skill author must hand-roll custom state files, `--step` flags, and rollback glue per skill.
