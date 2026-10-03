---
name: async-job-watcher
description: Spawn a detached background export job, poll its status file until READY, verify the exported artifact checksum, and always clean up temporary status files using examples/async_job_watcher/actions.py.
---

# Async Background Job Watcher (`async_job_watcher`)

Executes the 4-stage asynchronous job polling and guaranteed cleanup workflow
using `examples/async_job_watcher/actions.py`.

## Workflow Stages & Rules

1.  **Stage 1 — Spawn Background Job (`spawn_background_job`)**:

    -   Call `actions.spawn_background_job(payload)` with `{"delay_seconds":
        1.0, "status_file": "<path>"}`.
    -   Returns `(out_dict, msg)` where `out_dict` is `{"status_file": str,
        "target_delay_seconds": float, "worker_pid": int}`.
    -   Store `out_dict` under `payload["outputs"]["spawn_background_job"]`.

2.  **Stage 2 — Poll Until Ready (`await_job_completion`)**:

    -   Poll `actions.check_job_status_file(payload)` in a loop every `1.0`
        second (up to `30.0` seconds timeout) until `out_dict["job_status"] ==
        "READY"`.
    -   Each tick returns `({"job_status": str, "progress_pct": int,
        "artifact_rows": int, "artifact_sha256": str}, msg)`.
    -   Store the final tick's `out_dict` under
        `payload["outputs"]["await_job_completion"]`.

3.  **Stage 3 — Verify Exported Artifact (`verify_exported_artifact`)**:

    -   Once `job_status == "READY"`, call
        `actions.verify_exported_artifact(payload)` with
        `payload["outputs"]["await_job_completion"]` populated.
    -   Raises `RuntimeError` if `artifact_rows <= 0` or `artifact_sha256` is
        empty; otherwise returns `({"verified": True, "artifact_rows": int,
        "artifact_sha256": str}, msg)`.

4.  **Stage 4 — Guaranteed Cleanup (`cleanup_job_status_file`, `ALL_DONE`)**:

    -   **Trigger Rule (`ALL_DONE`)**: Must execute inside a `finally:` block
        whether Stages 2–3 succeeded or raised an exception.
    -   Call `actions.cleanup_job_status_file(payload)` to remove `status_file`
        and `status_file + ".tmp"`.
