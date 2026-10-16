<!-- mdformat global-off -->

# Lightflow: Deterministic DAG Workflow Engine for AI Agents

**Lightflow** is a lightweight, local-first workflow compiler and execution
engine that gives AI agents a structural backbone for multi-step
workflows—turning steps an agent would otherwise follow from prose instructions
into an explicit DAG of `python_action` stages and `operator_action` checkpoints
with dependencies, retries, and rollbacks enforced by the engine.

Every step runs across process boundaries as a plain CLI command that an agent
or operator can start, pause, and resume against a single append-only JSON
ledger (`passport.json`). Rather than burying *"ask before step 7"* inside a
long prompt blob where context decay and momentum cause agents to skip ahead,
Lightflow halts the process (`exit 2`) and delivers each checkpoint as a fresh,
dedicated turn at the exact moment of decision. It provides **zero-daemon,
append-only state guardrails** in pure Python—depending only on `PyYAML` and
`jsonschema`, with built-in standard-library fallbacks when running directly
from a checkout.

```mermaid
flowchart LR
    S1["1. fetch_top_stories<br/>(python_action + retry)<br/>Fetch live API data"] --> S2{"2. editorial_gate<br/>(operator_action · exit 2)<br/>Human/Agent approval + JSON Schema"}
    S2 -->|payload.outputs.editorial_gate.approved == true| S3["3. publish_digest<br/>(python_action)<br/>Idempotent side effect"]
```

--------------------------------------------------------------------------------

## Key Features

-   **Append-Only Passport Ledger (`passport.json`)**: Every stage transition
    appends an immutable `Stamp` (`PENDING`, `COMPLETED`, `PAUSED`, `SKIPPED`,
    `FAILED`) guarded by cross-platform file locks and atomic `os.replace`,
    recording operator and agent session provenance (`LIGHTFLOW_OPERATOR` /
    `ANTIGRAVITY_CONVERSATION_ID`).
-   **Selective Subgraph Re-Arming**: When a stage fails, fix the underlying
    code or environment and run `resume`. Lightflow re-arms *only* the failed
    stage and its downstream collateral subgraph—never re-executing completed
    upstream side effects.
-   **Human-in-the-Loop Checkpoints (`operator_action`)**: Suspends execution
    with exit code `2` so the gate arrives as its own dedicated turn rather than
    a buried prompt instruction, validates operator payloads against JSON Schema,
    and auto-generates drift-free `resume` commands.
-   **Safe, CEL-like Expressions**: `run_if`, polling conditions, and dynamic
    gate instructions use a small subset of [CEL](https://cel.dev) syntax
    (`string()`, `int()`, `size()`, `has()`), evaluated by a pure-Python AST
    walker without native C/Go dependencies or unsafe `eval()`.
-   **YAML & JSON Manifests**: Author workflows declaratively in **YAML
    (`.yaml`)** or **JSON (`.json`)**.
-   **Zero-Dependency Interactive Visualizer (`visualizer.html`)**: Generates a
    standalone, single-file HTML5 DAG canvas, execution trace log, and
    payload/manifest inspector with zero external CDN requests.
-   **First-Class Agent Integrations**:
    -   **Model Context Protocol (MCP)**: Built-in stdio MCP server
        (`lightflow-mcp`) for Claude Code, Claude Desktop, Gemini CLI, and
        Cursor, plus [`AGENTS.md`](./AGENTS.md) and [`CLAUDE.md`](./CLAUDE.md).
    -   **Agent Skills & Streaming Notifications**: Includes
        [`skills/run_lightflows/SKILL.md`](./skills/run_lightflows/SKILL.md)
        (executing and resuming workflows),
        [`skills/create_lightflows/SKILL.md`](./skills/create_lightflows/SKILL.md)
        (authoring new workflows), and
        [`benchmarks/SKILL.md`](./benchmarks/SKILL.md), plus optional milestone
        notifications when running inside `agentapi`-compatible hosts.
-   **Compact Context Footprint ([Benchmarks](./benchmarks/README.md))**:
    Intermediate stage outputs (`payload.outputs.<stage>`) flow between Python
    actions on disk via `passport.json` rather than through tool `stdout`, so
    the agent only sees 1-line stage status summaries and active gate prompts
    (`~250–1,700 tokens` end-to-end across the 6 included workflows, and
    $3.1\times$ fewer agent-generated tool-input tokens than an unstructured run
    where the LLM re-serializes intermediate state across turns).

--------------------------------------------------------------------------------

## When to Use Lightflow (and When Not To)

Reach for Lightflow when **all** of these are true:

-   The task has several ordered steps with real side effects (modifying files,
    calling external APIs, publishing artifacts) that must not be repeated on a
    retry.
-   You want a human or an agent to inspect intermediate output and approve a
    checkpoint partway through (`operator_action`).
-   You want the run to be inspectable on disk (`passport.json`) and resumable
    across separate CLI invocations or compacted agent sessions.

Look elsewhere when:

-   **You need concurrent execution.** Execution is strictly single-threaded and
    evaluates stages sequentially in deterministic topological order.
-   **You need unattended scheduling.** Lightflow does not include a cron
    daemon; trigger `lightflow start` from `cron`, `systemd`, or CI if you need
    a recurring run.
-   **You need shared multi-user state.** State is a local file guarded by an
    advisory lock (`flock` / `msvcrt`), not a networked service.
-   **It is a single step.** Just call the function directly.

| Category | Representative Tools | Infrastructure | Execution Model | Where Lightflow Differs |
|:--------------|:-------------|:-------------|:----------|:-----------------|
| **Distributed Orchestrators** | Temporal, Prefect, Airflow | Server daemon + database | Distributed workers | Zero daemon or DB; state is a single atomic local JSON ledger (`passport.json`). |
| **Cyclic Agent Graphs** | LangGraph, Burr | In-process Python library | Cyclic state machines | Process-boundary suspension (`exit 2`) with auto-generated CLI `resume` commands and AST-restricted CEL-like conditions. |
| **Function / Task DAGs** | Hamilton, doit, Kedro | Local Python library | Single-run in-memory DAG | Built-in `operator_action` JSON Schema gates, `rollback_action` hooks, and append-only `Stamp` history. |

--------------------------------------------------------------------------------

## Installation

```bash
git clone https://github.com/google/lightflow.git
cd lightflow
pip install -e .
```

*(Or run directly from a checkout without installing dependencies using
`PYTHONPATH=. python3 -m lightflow ...`)*

### Where state lives

Each run's `passport.json` is written to
`~/.lightflow/lightflow_state_<log_id>/passport.json` by default. Set
`LIGHTFLOW_STATE_DIR=/some/dir` to relocate the `lightflow_state_<log_id>/`
directories (for example into a project-local or CI-scoped folder). Gate and
re-arm stamps attribute the action to `--operator` if passed, `LIGHTFLOW_OPERATOR`
if set, or `<user> (agent:<ANTIGRAVITY_CONVERSATION_ID>)` when invoked inside an
agent session so you can trace any approval back to its conversation transcript.

### Security model

`python_import` executes arbitrary Python by design — **treat manifests as
code** and review them like you would a script. For manifests authored by an
agent, set `LIGHTFLOW_ALLOWED_IMPORT_PREFIXES` to a comma-separated list of
module prefixes (e.g.
`LIGHTFLOW_ALLOWED_IMPORT_PREFIXES=my_pkg.actions,examples`) and the engine will
refuse to import any action outside those prefixes, while disabling
manifest-local `sys.path` injection and module eviction so an untrusted manifest
directory cannot shadow allowlisted packages.

--------------------------------------------------------------------------------

## 60-Second Runnable Quickstart

Run the included live Hacker News curation workflow right out of the box:

```bash
# 1. Dry-run simulation (validates DAG topology, CEL-like expressions & JSON Schemas)
lightflow dry_run --lightflow=examples/hn_digest/lightflow.yaml

# 2. Start execution (fetches live HN top stories, then suspends at 'editorial_gate' with exit code 2)
lightflow start --lightflow=examples/hn_digest/lightflow.yaml --log_id=hn_demo

# 3. Approve the human gate and finish the workflow (--log_id remembers the manifest)
lightflow resume --log_id=hn_demo \
  --stage=editorial_gate --resolution=APPROVE \
  --payload='{"editor_note": "Top engineering reads today.", "output_path": "/tmp/hn_digest.md"}'

# 4. Generate a standalone interactive HTML5 DAG & timeline visualizer
lightflow visualize --log_id=hn_demo --output=/tmp/viz.html
```

`python3 -m lightflow ...` is equivalent to `lightflow ...` whenever the console
script is not on your `PATH`. On shells that strip inner double quotes (such as
Windows PowerShell 5.1) or when passing larger JSON objects, you can pass
payloads from a file or stdin via `--payload=@payload.json`, `--payload=@-`, or
`--payload_file=payload.json`.

--------------------------------------------------------------------------------

## Included Runnable Examples (`examples/`)

| # | Example | Pattern Demonstrated | External API / Mechanism |
| :-- | :-- | :-- | :-- |
| 1 | **[`examples/hn_digest/`](./examples/hn_digest/README.md)** | **Live API → Human Editorial Gate → Idempotent Publish**: Fetches top stories, previews the #1 story in the gate prompt, and writes a Markdown digest. | Hacker News Firebase API (zero auth) |
| 2 | **[`examples/usgs_seismic_alert/`](./examples/usgs_seismic_alert/README.md)** | **Live Telemetry Triage**: Polls 24h M4.5+ global seismic events, pauses for severity classification (`INFO` / `ADVISORY` / `WATCH`), and emits a bulletin. | USGS GeoJSON Feed (zero auth) |
| 3 | **[`examples/pypi_upgrade_guard/`](./examples/pypi_upgrade_guard/README.md)** | **Live Audit → Approval Gate → Automatic `rollback_action`**: Compares pinned packages against live PyPI, applies upgrades, and automatically restores `.bak` if smoke tests fail. | PyPI JSON API (zero auth) |
| 4 | **[`examples/async_job_watcher/`](./examples/async_job_watcher/README.md)** | **Async Background Polling & `ALL_DONE` Cleanup**: Spawns a 10–20s background process, polls its status file via `polling_policy` until `READY`, and cleans up temp files. | Detached Worker + Local JSON Status File |
| 5 | **[`examples/create_lightflow/`](./examples/create_lightflow/README.md)** | **Staggered Agent Skill Delivery ("A Lightflow to Create Lightflows")**: 3 human/agent design & coding gates interleaved with deterministic AST, `unittest`, and `dry_run` verification stages. | Python `ast` + `unittest` + `LightflowEngine` |
| 6 | **[`examples/tenant_gitops_onboarding/`](./examples/tenant_gitops_onboarding/README.md)** | **22-Stage Multi-Branch Kubernetes, Cloud IAM & GitOps Onboarding**: 4-phase orchestration with 3 branching patterns (role-based early handoff, 3-way IAM status fan-out + `ALL_DONE` join, parallel S3/Vault + compliance-tier KMS diamond), 2 human gates, 2 GitOps PR merge loops, and ArgoCD sync polling. | Local Mocked GitOps / Cloud IAM / SCIM / KMS / ArgoCD Sandbox |

--------------------------------------------------------------------------------

## Authoring Your Own Workflow

### 1. Write Python Actions (`my_pkg/actions.py`)

Each `python_action` is a regular Python function that receives a copy of the
current `payload` dictionary (plus any `static_kwargs` from the manifest) and
returns either `(payload_delta_dict, message_str)` or just `payload_delta_dict`
(which Lightflow merges into `payload` and records under
`payload.outputs.<stage_name>`):

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

### 2. Define the Workflow Manifest (`lightflow.yaml`)

```yaml
name: daily_revenue_report
description: Fetch rows, wait for human approval, and publish report.

actions:
  - id: fetch_rows
    python_import: my_pkg.actions.fetch_rows
  - id: publish_report
    python_import: my_pkg.actions.publish_report

stages:
  - name: fetch
    description: Fetch daily rows
    python_action:
      action_id: fetch_rows
      static_kwargs:
        limit: 500
    retry_policy:
      max_attempts: 3

  - name: approve
    description: Human review gate
    run_after:
      - fetch
    operator_action:
      instructions: "'Ready to publish: ' + payload.outputs.fetch.summary"
      json_schema:
        type: object
        required: [approved]
        properties:
          approved:
            type: boolean

  - name: publish
    description: Publish verified report
    run_after:
      - approve
    run_if: "payload.outputs.approve.approved == true"
    python_action:
      action_id: publish_report
```

### 3. Stage & Expression Reference

| Stage Field | Purpose |
|:----------------|:-------------------------------------------------------|
| `run_after` | Upstream stage names that must finish before this stage evaluates. |
| `run_if` | CEL-like expression evaluated against `payload`; stage is stamped `SKIPPED` if `false`. With top-level `strict_expressions: true`, selecting an absent key fails the stage instead of yielding `null`, naming the key and the closest match (guard optional keys with `has()`). |
| `trigger_rule` | `ALL_SUCCESS` (default), `NONE_FAILED` (runs once upstreams finished and none failed — the join after complementary `run_if` branches), or `ALL_DONE` (runs even after an upstream failure; cleanup stages). |
| `timeout_seconds` | Bounds each attempt (`0`–`86400`s; `0` disables the bound) using a background worker thread (shown in `dry_run`). |
| `retry_policy` | Opt-in retry (`max_attempts`, `initial_backoff_seconds`, `backoff_multiplier`) with ±10% jitter for exceptions and timeouts. |
| `polling_policy` | Polls `poll_tick_action` (mutually exclusive with `python_action`) until `condition` evaluates to `true` (`interval_seconds`, `timeout_seconds`, `max_attempts`); each tick's dict is merged into `payload.outputs.<stage>` before `condition` runs, and tick messages are persisted to `passport.json`. |
| `rollback_action` | Compensating `python_action` invoked automatically after a stage exhausts its retries; its output delta is merged into `payload`. |

-   **Stage Output Namespacing & Collision Detection**: Stage outputs land both
    under `payload.outputs.<stage>` and at the top level of `payload` (last
    writer wins; `outputs` is a reserved top-level key). The engine warns `⚠️
    [stage] overwrote top-level payload.<key>` when a later stage changes a key
    an earlier one set *and* some expression reads `payload.<key>` (two stages
    sharing a reusable action is otherwise silent); prefer
    `payload.outputs.<stage>.<key>` in expressions and check `status --verbose`
    for per-key provenance.
-   **Actionable `dry_run` Diagnostics**: `dry_run` ends with a `Summary:` line
    (including `❌ N would fail` for expressions that would error at runtime), a
    ready-to-paste `💡 --payload` hint when a branch reads unseeded keys, and
    the `stage→action` pairs that lack `dry_run` support.
-   **Run Discovery & Remembered Manifests**: `lightflow list` (and the
    `list_lightflow_runs` MCP tool) enumerates saved runs with the manifest each
    was started from. Runs record their manifest in `passport.json`, so
    `status`, `resume`, `visualize`, and `cleanup` need only `--log_id=<id>`; if
    the manifest moves, pass `--lightflow=<new path>` once and it is
    re-recorded.
-   **Safe Re-Arming & Force-Start Summary**: `start --force` prints what it
    discards (completed stages, paused gate, history) before wiping. An
    `ALL_DONE` stage that ran because an upstream failed or a gate was rejected
    is automatically re-armed (`↻ cleanup re-armed …`) when that cause is
    re-armed or the gate re-opened, so it runs again at the real end of the run.
-   **Milestone Push Notifications (`agentapi`)**: Agent push notifications via
    `agentapi` are disabled by default (`LIGHTFLOW_NOTIFY=off`) so synchronous
    CLI/MCP runs do not double-notify a caller that already reads stdout/JSON;
    set `LIGHTFLOW_NOTIFY=milestones` (for detached/background runs) to push
    only gate suspensions, failures, rejections, rollbacks, and the final
    outcome, or `LIGHTFLOW_NOTIFY=all` for every stage transition.

**Expression Evaluator (`run_if`, `polling_policy.condition`,
`operator_action.instructions`)**:

-   **Operators**: `&&`, `||`, `!`, `==`, `!=`, `<`, `<=`, `>`, `>=`, `+`, `-`,
    `*`, `/`
-   **Built-ins**: `size(x)` / `len(x)`, `has(x.field)`, `string(x)`, `int(x)`,
    `double(x)`, `bool(x)` (all null-safe: `string(null) == ""`, `int(null) ==
    0`)
-   **String & Collection Methods**: `contains()`, `startsWith()`, `endsWith()`,
    `replace()`, `split()`, `strip()`, `lower()`, `upper()`
-   **Per-Stage Output Namespacing**: Every stage's delta is merged into
    `payload` and also recorded under `payload.outputs.<stage_name>` to prevent
    key collisions across stages.

--------------------------------------------------------------------------------

## Execution & Failure Semantics

### Stage Lifecycle (`passport.json`)

Each stage carries zero or more **stamps** in the passport. A stage's current
state is its *most recent* stamp; earlier stamps are retained as an audit log.

```mermaid
stateDiagram-v2
    [*] --> Unstamped: never reached
    Unstamped --> PENDING: engine starts stage
    PENDING --> COMPLETED: action returned
    PENDING --> FAILED: action raised (retries exhausted)
    PENDING --> PAUSED: operator_action reached (exit 2)
    Unstamped --> SKIPPED: run_if false / upstream skipped
    Unstamped --> FAILED: upstream failed (ALL_SUCCESS)
    PAUSED --> COMPLETED: resume --resolution=APPROVE
    FAILED --> PENDING: start/resume re-arms subgraph
    COMPLETED --> [*]
```

When a stage fails:

1.  The failing stage is stamped `FAILED` with its traceback, and its
    `rollback_action` runs if configured.
2.  Downstream dependents under `ALL_SUCCESS` are stamped `FAILED` as collateral
    (`Upstream dependency failed`), while independent parallel branches in a
    diamond continue to completion and `ALL_DONE` cleanup stages still execute.
3.  Running `lightflow resume` appends a fresh `PENDING` stamp to the failed
    stage and its collateral dependents—re-running only the broken subgraph
    while preserving completed upstream work and keeping the earlier `FAILED`
    stamp in history.

### CLI Exit Codes

| Exit Code | Meaning | Typical Next Step |
|:--------|:-----------------------------|:------------------------------|
| `0` | Workflow completed (or was already completed). | Done. |
| `1` | Stage failure, compile error, or a payload that fails the gate schema. | Inspect `lightflow status`, fix the cause, and run `lightflow resume`. |
| `2` | Suspended at an `operator_action` gate awaiting human/agent input. | Run `lightflow resume --stage=<name> --resolution=APPROVE --payload='{...}'`. |
| `3` | No saved state for the log ID (`status`, `resume`). | Check the log ID (`lightflow list`), or `start`. |
| `4` | Another process holds the run's lock (`start`, `resume`, `cleanup`, `status`). | Wait, then check `status` again. |
| `5` | Waiting: a `polling_policy` stage yielded (timeout / max attempts) before its condition was met. Nothing is broken; downstream stages are not touched. | Once the external condition holds, run `lightflow resume`. |

`lightflow status` exits with the code of the run it inspects (`0` completed,
`1` failed, rejected, or interrupted, `2` paused, `5` waiting), so a script or agent can poll
it and branch exactly as it would on `start` or `resume`. It prints one line per
stage plus any paused gate's Instructions, Payload Schema, and resume command;
`--verbose` adds the Passport payload, full stamp log, and Mermaid diagram.

--------------------------------------------------------------------------------

## Agent Ecosystem Setup

### For Antigravity (`agy`) & Skill-Based Agents

This repository ships with [`plugin.json`](./plugin.json), [`mcp_config.json`](./mcp_config.json), and two progressive-disclosure skills ([`skills/run_lightflows/SKILL.md`](./skills/run_lightflows/SKILL.md) and [`skills/create_lightflows/SKILL.md`](./skills/create_lightflows/SKILL.md)):

```bash
# Option A: Mount the entire repository as a single plugin (skills + MCP server)
ln -s "$(pwd)" ~/.gemini/config/plugins/lightflow

# Option B: Mount only the on-demand skills
ln -s "$(pwd)/skills/run_lightflows" ~/.gemini/config/skills/run_lightflows
ln -s "$(pwd)/skills/create_lightflows" ~/.gemini/config/skills/create_lightflows
```

### For Claude Code, Claude Desktop, Gemini CLI & Cursor (MCP)

Add `lightflow-mcp` to your `claude_desktop_config.json` or `.mcp.json`:

```json
{
  "mcpServers": {
    "lightflow": {
      "command": "lightflow-mcp",
      "args": []
    }
  }
}
```

Copy [`AGENTS.md`](./AGENTS.md) or [`CLAUDE.md`](./CLAUDE.md) into your
repository root (or install
[`skills/run_lightflows/SKILL.md`](./skills/run_lightflows/SKILL.md) and
[`skills/create_lightflows/SKILL.md`](./skills/create_lightflows/SKILL.md)) to
instruct your agent on executing, resuming, and authoring Lightflow workflows.
The `create_lightflow` scaffolding meta-workflow those files reference lives in
this repository under `examples/create_lightflow/`; run it from a clone of
`github.com/google/lightflow`, or pass the absolute path to that directory in
your clone (`--lightflow=/path/to/lightflow/examples/create_lightflow`).

--------------------------------------------------------------------------------

## License & Disclaimer

Licensed under the [Apache 2.0 License](./LICENSE).

This is not an officially supported Google product. Eligibility for the
[Google Open Source Software Vulnerability Rewards Program](https://bughunters.google.com/open-source-security)
is determined by the
[Google Open Source Software Vulnerability Reward Program Rules](https://bughunters.google.com/about/rules/open-source/google-open-source-software-vulnerability-reward-program-rules).
