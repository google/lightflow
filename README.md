<!-- mdformat global-off -->

# Lightflow: Deterministic DAG Workflow Engine for AI Agents

Agent skills (`SKILL.md`) are typically written as prose checklists: run a query, analyze the output, ask the user to review, render an artifact, ask before sending. When an agent executes a multi-step procedure from prose alone, every intermediate output has to round-trip through the model's context window to parameterize the next command, failed runs risk repeating earlier side effects, and nothing in the runtime enforces stage order or parameter schemas.

**Lightflow** replaces the prose checklist with an executable DAG (`lightflow.yaml`). Mechanical steps become Python functions (`python_action`); steps that require agent synthesis or human sign-off become explicit checkpoints (`operator_action`). You interact with your agent in chat as usual while the agent drives Lightflow through its CLI:

![Lightflow Demo](./docs/demo.svg)

### Before & After: Prose Checklist vs. `lightflow.yaml`

<table>
<tr>
<th width="45%">Before: Prose <code>SKILL.md</code> Checklist</th>
<th width="55%">After: Executable <code>lightflow.yaml</code></th>
</tr>
<tr>
<td>

```markdown
1. Run `fetch_rows --limit=500` and save the
   summary.
2. Ask the user to approve publishing the
   summary. Do not continue until approved.
3. If approved, run `publish_report` with the
   summary from step 1.
```

*What goes wrong at runtime:*
- Step 1's raw output is pulled into the LLM context window just to copy arguments into Step 3.
- If Step 3 fails mid-run, a naive retry re-runs Step 1 and duplicates its side effects.
- No schema check guards the parameters passed into Step 3.

</td>
<td>

```yaml
name: daily_revenue_report
actions:
  - {id: fetch_rows, python_import: my_pkg.actions.fetch_rows}
  - {id: publish_report, python_import: my_pkg.actions.publish_report}
stages:
  - name: fetch
    python_action: {action_id: fetch_rows, static_kwargs: {limit: 500}}
    retry_policy: {max_attempts: 3}
  - name: approve
    run_after: [fetch]
    operator_action:
      instructions: "'Ready to publish: ' + payload.outputs.fetch.summary"
      json_schema:
        type: object
        required: [approved]
        properties: {approved: {type: boolean}}
  - name: publish
    run_after: [approve]
    run_if: "payload.outputs.approve.approved == true"
    python_action: {action_id: publish_report}
```

</td>
</tr>
</table>

### Why Not Just a Python Script — or Just a Prose `SKILL.md`?

- **Why not a plain Python script?** A regular script cannot pause halfway through, return control to an agent's conversational turn so the user can review intermediate findings in chat, and resume in a new OS process minutes or hours later. Lightflow halts the process at each `operator_action` with **`exit 2`**, prints the checkpoint's instructions, JSON Schema, and exact `lightflow resume` command, and continues from disk once resolved.
- **Why not a prose `SKILL.md` over individual CLI commands?**
  1. **Data stays on disk, not in model context (−23% turns, −30% tokens, −26% time):** In a controlled [`N = 20` matched-pair benchmark](./benchmarks/README.md), driving a 12-stage hotfix procedure through Lightflow reduced model API turns by **23%**, cumulative context tokens by **30%**, and wall-clock time by **26%** ($p \le 5 \times 10^{-5}$). Multi-stage Python spans execute in a single CLI call and pass intermediate outputs on disk (`passport.json`), leaving only compact status lines and checkpoint prompts ([~250–1,750 tokens](./benchmarks/README.md)) in `stdout`.
  2. **Mechanical ordering, schema validation & safe retries:** Downstream stages cannot execute until their `run_after` dependencies succeed and gate payloads pass JSON Schema validation. If a stage fails or a session compacts, `lightflow resume` re-runs only the failed subgraph—completed upstream stages are never re-executed. *(In interactive sessions where a user pre-shares a sign-off token early, `exit 2` pairs with the checkpoint rule in [`skills/run_lightflows/SKILL.md`](./skills/run_lightflows/SKILL.md) so the agent pauses for fresh confirmation rather than auto-resuming in the same turn; see [`benchmarks/README.md` §1.5–1.6](./benchmarks/README.md#15-the-v1-pilot-and-its-confounds-v1_pilot_reliability_trialsjson)).*

Lightflow is a pure-Python package (`PyYAML` and `jsonschema`, with stdlib fallbacks from a checkout) with no server or daemon. Existing `SKILL.md` procedures can be converted into manifests using the bundled [`create_lightflows`](./skills/create_lightflows/SKILL.md) skill.

---

## Core Concepts

| Term | Meaning |
| :--- | :--- |
| **Manifest (`lightflow.yaml`)** | Declarative YAML or JSON file defining `actions` (Python import paths) and `stages` (`python_action`, `operator_action`, or `polling_policy`) linked by `run_after` dependencies and CEL-like `run_if` expressions. |
| **Gate (`operator_action`)** | A human- or agent-in-the-loop checkpoint. Reaching a gate suspends the CLI process with **`exit 2`** and prints the gate's `Instructions`, `Payload Schema`, and copy-pasteable `lightflow resume` command. |
| **Passport (`passport.json`)** | Append-only JSON state file on disk (`~/.lightflow/lightflow_state_<log_id>/passport.json`) storing the shared `payload`, per-stage outputs (`payload.outputs.<stage>`), and the chronological audit log of **stamps** (`PENDING`, `COMPLETED`, `PAUSED`, `SKIPPED`, `FAILED`) with operator and session attribution. |
| **Re-arming (`lightflow resume`)** | Selective recovery after a failure or polling yield: `resume` appends a fresh `PENDING` stamp only to failed stages and their downstream dependents, keeping completed upstream work intact. |

---

## When to Use Lightflow (and When Not To)

Reach for Lightflow when **all** of these are true:

- The task has several ordered steps with real side effects (modifying files, calling external APIs, publishing artifacts) that must not be repeated on a retry.
- You want a human or an agent to inspect intermediate output and approve a checkpoint partway through (`operator_action`).
- You want the run to be inspectable on disk (`passport.json`) and resumable across separate CLI invocations, scheduled automations, or compacted agent sessions.

Look elsewhere when:

- **You need concurrent execution.** Execution is single-threaded and evaluates stages sequentially in deterministic topological order.
- **You need shared multi-user state.** State is a local file guarded by an advisory lock (`flock` / `msvcrt`), not a networked service.
- **It is a single step.** Just call the function directly.

| Category | Representative Tools | Infrastructure | Execution Model | Where Lightflow Differs |
| :--- | :--- | :--- | :--- | :--- |
| **Distributed Orchestrators** | Temporal, Prefect, Airflow | Server daemon + database | Distributed workers | Zero daemon or DB; state is a single atomic local JSON ledger (`passport.json`). |
| **Cyclic Agent Graphs** | LangGraph, Burr | In-process Python library | Cyclic state machines | Process-boundary suspension (`exit 2`) with auto-generated CLI `resume` commands and AST-validated CEL-like conditions. |
| **Function / Task DAGs** | Hamilton, doit, Kedro | Local Python library | Single-run in-memory DAG | Built-in `operator_action` JSON Schema gates, `rollback_action` hooks, and append-only `Stamp` history. |

---

## Installation

Install the `lightflow` CLI and Python package directly from GitHub:

```bash
# Into your current Python environment (pinned to a release tag)
pip install "git+https://github.com/google/lightflow.git@v0.7.1"

# Or install globally as a standalone CLI tool via uv or pipx
uv tool install "git+https://github.com/google/lightflow.git@v0.7.1"
pipx install "git+https://github.com/google/lightflow.git@v0.7.1"
```

Or clone the repository to run the bundled `examples/`, `benchmarks/`, and `create_lightflow` scaffolding workflow locally:

```bash
git clone https://github.com/google/lightflow.git
cd lightflow
pip install -e .
```

*(You can also run directly from a checkout without installing dependencies using `PYTHONPATH=. python3 -m lightflow ...`)*

> **Note (Pre-1.0):** Lightflow is pre-1.0 (`0.x`): the CLI, manifest fields, and `passport.json` format (`passport_version: 1`) may still change between minor versions. Pin a release tag (`@v0.7.1`) if you depend on it.

- **Where state lives:** `~/.lightflow/lightflow_state_<log_id>/passport.json` by default (mode `0700`); set `LIGHTFLOW_STATE_DIR=/some/dir` to relocate it. Runs remember their manifest path, so `status`, `resume`, `visualize`, and `cleanup` need only `--log_id`.
- **Security model:** `python_import` executes arbitrary Python by design — **treat manifests as code**. For agent-authored manifests, set `LIGHTFLOW_ALLOWED_IMPORT_PREFIXES=my_pkg.actions,examples` to restrict imports to allowlisted prefixes and disable manifest-local `sys.path` shadowing. Use top-level `secret_keys: [...]` to redact sensitive values as `<redacted>` across console output, `status`, `dry_run`, and `visualizer.html`. See the **[Technical Reference (`docs/reference.md`)](./docs/reference.md)** for full details.

---

## 60-Second Runnable Quickstart

Run the workflow shown in the demo above. It uses only the standard library (`sqlite3`) and writes to `/tmp`:

```bash
# 1. Dry-run simulation (validates DAG topology, CEL-like expressions & JSON Schemas)
lightflow dry_run --lightflow=examples/quarterly_revenue_brief/lightflow.yaml

# 2. Start execution (runs SQLite query + Python variance analysis, then pauses at 'draft_commentary' with exit code 2)
lightflow start --lightflow=examples/quarterly_revenue_brief/lightflow.yaml --log_id=q3

# 3. Supply the reviewed commentary (renders /tmp/q3_revenue_brief.md, then pauses at 'approve_send' with exit code 2)
lightflow resume --log_id=q3 \
  --stage=draft_commentary --resolution=APPROVE \
  --payload='{"approved": true, "commentary": "Q3 revenue grew +12.4% QoQ led by NA and APAC; EMEA trailed plan by 6.2% on Q4 renewal timing (closed Oct 4)."}'

# 4. Approve final delivery (dispatches report, writes receipt, and exits 0)
lightflow resume --log_id=q3 \
  --stage=approve_send --resolution=APPROVE

# 5. Generate a standalone interactive HTML5 DAG & timeline visualizer
lightflow visualize --log_id=q3 --output=/tmp/viz.html
```

`python3 -m lightflow ...` is equivalent to `lightflow ...` whenever the console script is not on your `PATH`. For larger JSON objects or shells that strip inner double quotes (such as Windows PowerShell 5.1), pass payloads from a file or stdin via `--payload=@payload.json`, `--payload=@-`, or `--payload_file=payload.json`.

---

## Included Runnable Examples (`examples/`)

| # | Example | Pattern Demonstrated | External API / Mechanism |
| :-- | :-- | :-- | :-- |
| 1 | **[`examples/quarterly_revenue_brief/`](./examples/quarterly_revenue_brief/README.md)** | **SQL Query → Python Analysis → Commentary Checkpoint → Render → Send Checkpoint → Send**: 6-stage reporting workflow with two `operator_action` checkpoints. | Python `sqlite3` (standard library) |
| 2 | **[`examples/hn_digest/`](./examples/hn_digest/README.md)** | **Live API → Human Editorial Gate → Idempotent Publish**: Fetches top stories, previews the #1 story in the gate prompt, and writes a Markdown digest. | Hacker News Firebase API (zero auth) |
| 3 | **[`examples/usgs_seismic_alert/`](./examples/usgs_seismic_alert/README.md)** | **Live Telemetry Triage**: Polls 24h M4.5+ global seismic events, pauses for severity classification (`INFO` / `ADVISORY` / `WATCH`), and emits a bulletin. | USGS GeoJSON Feed (zero auth) |
| 4 | **[`examples/pypi_upgrade_guard/`](./examples/pypi_upgrade_guard/README.md)** | **Live Audit → Approval Gate → Automatic `rollback_action`**: Compares pinned packages against live PyPI, applies upgrades, and automatically restores `.bak` if smoke tests fail. | PyPI JSON API (zero auth) |
| 5 | **[`examples/async_job_watcher/`](./examples/async_job_watcher/README.md)** | **Async Background Polling & `ALL_DONE` Cleanup**: Spawns a 10–20s background process, polls its status file via `polling_policy` until `READY`, and cleans up temp files. | Detached Worker + Local JSON Status File |
| 6 | **[`examples/create_lightflow/`](./examples/create_lightflow/README.md)** | **Staggered Agent Skill Delivery ("A Lightflow to Create Lightflows")**: 3 human/agent design & coding gates interleaved with deterministic AST, `unittest`, and `dry_run` verification stages. | Python `ast` + `unittest` + `LightflowEngine` |
| 7 | **[`examples/tenant_gitops_onboarding/`](./examples/tenant_gitops_onboarding/README.md)** | **22-Stage Multi-Branch Kubernetes, Cloud IAM & GitOps Onboarding**: 4-phase orchestration with 3 branching patterns, 2 human gates, 2 GitOps PR merge loops, and ArgoCD sync polling. | Local Mocked GitOps / Cloud IAM / SCIM / KMS / ArgoCD Sandbox |

---

## Authoring Your Own Workflow

Already have the procedure as a `SKILL.md`? See [Starting From an Existing `SKILL.md`](./skills/create_lightflows/SKILL.md#1b-starting-from-an-existing-skillmd) for the step-by-step mapping (mechanical steps become `python_action`s; judgment and approvals become `operator_action` gates).

1. **Write Python Actions (`my_pkg/actions.py`)**: Each action receives a copy of `payload` (plus any `static_kwargs` from the manifest) and returns `(payload_delta_dict, message_str)` or `payload_delta_dict`. Lightflow records each stage's delta under `payload.outputs.<stage_name>` (and merges it into top-level `payload`):

   ```python
   from typing import Any

   def fetch_rows(
       payload: dict[str, Any], limit: int = 500, dry_run: bool = False
   ) -> tuple[dict[str, Any], str]:
       if dry_run or payload.get("dry_run"):
           return {"count": 0, "summary": "0 rows (dry run)"}, "[DRY RUN] Skipped fetch"
       return {"count": limit, "summary": f"{limit} rows"}, f"Fetched {limit} rows"

   def publish_report(payload: dict[str, Any]) -> tuple[dict[str, Any], str]:
       summary = payload["outputs"]["fetch"]["summary"]
       return {"published": True}, f"Published report ({summary})"
   ```

2. **Define the Manifest (`lightflow.yaml`)**: Wire the actions into stages using `run_after`, `run_if`, `operator_action`, `retry_policy`, `polling_policy`, and `rollback_action` (as shown in the [Before & After example](#before--after-prose-checklist-vs-lightflowyaml) above).
3. **Validate & Preview**: Run `lightflow dry_run --lightflow=lightflow.yaml` to compile the DAG, verify that every `payload.outputs.<stage>` reference matches an upstream `run_after` dependency, check JSON Schemas, and simulate `dry_run`-capable actions.

📖 **See [`docs/reference.md`](./docs/reference.md)** for the complete manifest field table, CEL-lite expression builtins, timeout/retry/rollback semantics, `secret_keys` redaction, stage lifecycle state diagram, and CLI exit codes (`0`–`5`).

---

## Agent Ecosystem Setup

### For Antigravity (`agy`) & Skill-Based Agents

This repository ships with [`plugin.json`](./plugin.json) and two progressive-disclosure skills ([`skills/run_lightflows/SKILL.md`](./skills/run_lightflows/SKILL.md) and [`skills/create_lightflows/SKILL.md`](./skills/create_lightflows/SKILL.md)):

```bash
# Option A: Mount the entire repository as a single plugin
ln -s "$(pwd)" ~/.gemini/config/plugins/lightflow

# Option B: Mount only the on-demand skills
ln -s "$(pwd)/skills/run_lightflows" ~/.gemini/config/skills/run_lightflows
ln -s "$(pwd)/skills/create_lightflows" ~/.gemini/config/skills/create_lightflows
```

### For Claude Code, Gemini CLI, Codex & Cursor

Copy [`AGENTS.md`](./AGENTS.md) or [`CLAUDE.md`](./CLAUDE.md) into your repository root (or install [`skills/run_lightflows/SKILL.md`](./skills/run_lightflows/SKILL.md) and [`skills/create_lightflows/SKILL.md`](./skills/create_lightflows/SKILL.md)) to instruct your agent on executing, resuming, and authoring Lightflow workflows. To scaffold a new workflow through the built-in 3-phase verification meta-workflow, run `lightflow start --lightflow=/path/to/lightflow/examples/create_lightflow --log_id=scaffold_<name>`.

---

## License & Disclaimer

Licensed under the [Apache 2.0 License](./LICENSE).

This is not an officially supported Google product. Eligibility for the [Google Open Source Software Vulnerability Rewards Program](https://bughunters.google.com/open-source-security) is determined by the [Google Open Source Software Vulnerability Reward Program Rules](https://bughunters.google.com/about/rules/open-source/google-open-source-software-vulnerability-reward-program-rules).
