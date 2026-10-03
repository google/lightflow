# Example 1: Hacker News Live Curate & Digest (`hn_digest`)

Demonstrates a `python_action` $\rightarrow$ `operator_action` $\rightarrow$
`python_action` pipeline tapping the official, zero-auth **Hacker News Firebase
API** (`https://hacker-news.firebaseio.com/v0/topstories.json`).

## DAG Topology

```mermaid
flowchart LR
    S1["1. fetch_top_stories<br/>(python_action + retry)<br/>GET top 5 stories from HN API"] --> S2["2. editorial_gate<br/>(operator_action)<br/>Preview #1 story & approve with editor_note"]
    S2 -->|payload.outputs.editorial_gate.approved == true| S3["3. publish_digest<br/>(python_action)<br/>Atomically write /tmp/hn_digest.md"]
```

## Quick Run

```bash
# 1. Start workflow - fetches live HN stories and suspends at 'editorial_gate' (exit code 2)
lightflow start --lightflow=examples/hn_digest --log_id=hn_run_01

# 2. Approve the gate and attach your editorial commentary
lightflow resume --lightflow=examples/hn_digest \
  --log_id=hn_run_01 \
  --stage=editorial_gate \
  --resolution=APPROVE \
  --payload='{"editor_note": "Strong systems & AI tooling threads today.", "output_path": "/tmp/hn_digest.md"}'

# 3. Inspect the generated Markdown digest
cat /tmp/hn_digest.md
```
