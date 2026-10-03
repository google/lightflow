---
name: hn-digest
description: Fetch top Hacker News stories, pause for human editorial approval, and publish a curated Markdown digest using examples/hn_digest/actions.py.
---

# Hacker News Curated Digest (`hn_digest`)

Executes the 3-stage Hacker News curation workflow using the Python helper
functions in `examples/hn_digest/actions.py`.

## Workflow Stages & Rules

1.  **Stage 1 — Fetch Top Stories (`fetch_top_stories`)**:

    -   Call `actions.fetch_top_stories(payload)` with `{"offline": True,
        "limit": 3}` (or live API if `offline=False`).
    -   Returns `(out_dict, msg)` where `out_dict` contains `{"stories": [...],
        "count": int, "top_title": str, "top_score": int, "source": str}`.
    -   Persist `out_dict` under `payload["outputs"]["fetch_top_stories"]` so
        `fetch_top_stories` is **not** called again in Stage 3.
    -   Example invocation:

        ```bash
        python3 -B -c "import json, sys; sys.path.insert(0, 'examples/hn_digest'); import actions; out, msg = actions.fetch_top_stories({'offline': True, 'limit': 3}); print(json.dumps({'outputs': {'fetch_top_stories': out}, 'message': msg}))"
        ```

2.  **Stage 2 — Human Editorial Gate (`editorial_gate`)**:

    -   **MANDATORY GATE**: Stop after Stage 1 and present `top_title`,
        `top_score`, and `count` to the human editor.
    -   Wait for their explicit approval and optional `editor_note` +
        `output_path` before executing Stage 3. Never auto-approve.

3.  **Stage 3 — Publish Digest (`publish_digest`)**:

    -   **Condition**: Run only if the editor approved Stage 2. Do **not**
        re-run `fetch_top_stories`.
    -   Call `actions.publish_digest(payload)` where `payload` contains:
        -   `"outputs": {"fetch_top_stories": <stage_1_out>, "editorial_gate":
            {"editor_note": "<note>", "output_path": "<path>"}}`
    -   Writes the curated Markdown file atomically to `output_path` and returns
        `({"digest_path": str, "story_count": int, "simulated": bool}, msg)`.
