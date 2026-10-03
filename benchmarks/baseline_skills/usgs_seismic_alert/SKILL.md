---
name: usgs-seismic-alert
description: Poll the USGS M4.5+ earthquake feed, conditionally pause for Incident Commander triage when magnitude >= 5.0, and emit an incident bulletin using examples/usgs_seismic_alert/actions.py.
---

# USGS Seismic Alert Triage (`usgs_seismic_alert`)

Executes the 3-stage USGS earthquake alert workflow using
`examples/usgs_seismic_alert/actions.py`.

## Workflow Stages & Rules

1.  **Stage 1 — Fetch USGS Feed (`fetch_usgs_feed`)**:

    -   Call `actions.fetch_usgs_feed(payload)` with `{"offline": True}` (or
        live GeoJSON feed if `offline=False`).
    -   Note: `fetch_usgs_feed` returns a single `dict` (not a tuple):
        `{"events": [...], "event_count": int, "max_magnitude": float,
        "strongest_place": str, "source": str}`.
    -   Persist this dict under `payload["outputs"]["fetch_usgs_feed"]` so Stage
        1 is **not** re-executed after triage.
    -   Example invocation:

        ```bash
        python3 -B -c "import json, sys; sys.path.insert(0, 'examples/usgs_seismic_alert'); import actions; out = actions.fetch_usgs_feed({'offline': True}); print(json.dumps({'outputs': {'fetch_usgs_feed': out}}))"
        ```

2.  **Stage 2 — Conditional Incident Commander Gate
    (`incident_commander_gate`)**:

    -   **Branch Condition (`run_if`)**: Evaluate
        `outputs.fetch_usgs_feed.max_magnitude >= 5.0`.
    -   If `max_magnitude < 5.0`, skip Stages 2 and 3.
    -   If `max_magnitude >= 5.0`, **STOP** and present `max_magnitude` and
        `strongest_place` to the human Incident Commander. Wait for their
        explicit approval and `severity` (`"ADVISORY" | "WATCH" | "WARNING"`) +
        `output_path`. Never auto-approve.

3.  **Stage 3 — Emit Bulletin (`emit_bulletin`)**:

    -   **Condition**: Run only if Stage 2 was approved. Do **not** re-call
        `fetch_usgs_feed`.
    -   Call `actions.emit_bulletin(payload)` where `payload` contains:
        -   `"outputs": {"fetch_usgs_feed": <stage_1_out>,
            "incident_commander_gate": {"severity": "<severity>", "output_path":
            "<path>"}}`
    -   Returns `({"bulletin_path": str, "severity": str, "simulated": bool},
        msg)`.
