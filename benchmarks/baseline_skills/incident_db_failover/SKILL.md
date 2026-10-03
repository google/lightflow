---
name: incident-db-failover
description: Run a 9-stage regional primary database failover with outage confirmation, failover candidate election, conditional WAL lag replay vs. zero-loss sync verification, STONITH fencing join, Incident Commander approval gate, standby promotion, connection-pooler cutover with automatic rollback on canary write failure, surgical recovery, and failover audit ledger publication.
---

# Regional Database Failover (`SKILL.md` + `actions.py` Baseline)

Run a 9-stage regional PostgreSQL primary failover using the helper functions in
`examples/incident_db_failover/actions.py` (`python3 -c "from
examples.incident_db_failover import actions; ..."`).

Do **not** invoke the `lightflow` CLI and do **not** read `lightflow.yaml` or
`actions.py` — every function signature, payload key, `outputs` key, branch
condition, join rule, and rollback function is documented below. Every helper in
`actions.py` returns a 2-tuple `(output_dict, message_str)` — always unpack
`data, msg = actions.<fn>(payload)` and store `data` under
`payload["outputs"]["<stage_name>"]`.

--------------------------------------------------------------------------------

## Stage & Dependency Order (9 Stages)

Execute the stages in this exact dependency order, accumulating each stage's
returned dictionary under `payload["outputs"]["<stage_name>"]`:

### Stage 1: `detect_primary_outage`

-   **Function**: `actions.detect_primary_outage(payload)`
-   **Inputs**:
    -   `payload.get("failover_state_path")` (JSON file path for cluster
        failover state)
    -   `payload.get("cluster_id", "pg-orders-eu1")`
    -   `payload.get("replication_lag_bytes", 4096)`
-   **Side effects**: Initializes `failover_state_path`
    (`old_primary="pg-orders-eu1-primary-a"`,
    `candidate_replica="pg-orders-eu1-replica-b"`, `replication_lag_bytes`,
    `wal_replay_ticks=0`, `stonith_fenced=False`, `promotion_count=0`,
    `pooler_status="PRIMARY_UNREACHABLE"`, `audit_published=False`).
-   **Store return dict under**: `payload["outputs"]["detect_primary_outage"]`
    -   Keys: `cluster_id`, `old_primary`, `replication_lag_bytes`,
        `failover_state_path`.

### Stage 2: `elect_failover_candidate`

-   **Function**: `actions.elect_failover_candidate(payload)`
-   **Requires**: `payload["outputs"]["detect_primary_outage"]`
-   **Store return dict under**:
    `payload["outputs"]["elect_failover_candidate"]`
    -   Keys: `candidate_replica` (`"pg-orders-eu1-replica-b"`), `lag_mode`
        (`"wal_replay_needed"` when `replication_lag_bytes > 0`, `"zero_loss"`
        when `replication_lag_bytes == 0`), `target_lsn` (`"0/1A8F4000"`).

### Stages 3 & 4 (Mutually Exclusive Conditional Branches after Stage 2)

Inspect `payload["outputs"]["elect_failover_candidate"]["lag_mode"]`:

-   **Branch A — Stage 3: `replay_missing_wal_segments` (Run ONLY if
    `lag_mode == "wal_replay_needed"`, skip Stage 4)**:
    -   **Polling rule**: Call `actions.replay_missing_wal_segments(payload)`
        repeatedly until `result["wal_caught_up"] == True` (takes 2 ticks to
        drain `replication_lag_bytes` to `0`).
    -   **Store final return dict under**:
        `payload["outputs"]["replay_missing_wal_segments"]`
    -   Keys: `wal_caught_up` (`True`), `replayed_ticks` (`2`).
-   **Branch B — Stage 4: `verify_zero_loss_sync` (Run ONLY if `lag_mode ==
    "zero_loss"`, skip Stage 3)**:
    -   **Function**: `actions.verify_zero_loss_sync(payload)`
    -   **Store return dict under**:
        `payload["outputs"]["verify_zero_loss_sync"]`
    -   Keys: `wal_caught_up` (`True`), `sync_verified` (`True`).

### Stage 5: `fence_old_primary` (Join after Stages 3 & 4 — `ALL_DONE`)

-   **Function**: `actions.fence_old_primary(payload)`
-   **Requires**: Either `replay_missing_wal_segments.wal_caught_up == True` OR
    `verify_zero_loss_sync.wal_caught_up == True` in `payload["outputs"]`.
-   **Side effects**: Sets `stonith_fenced=True` in `failover_state_path`.
-   **Store return dict under**: `payload["outputs"]["fence_old_primary"]`
    -   Keys: `stonith_fenced` (`True`).

### Stage 6: `approve_replica_promotion` (Incident Commander Approval Gate)

-   **Pause before Stage 7**: Present the fenced old primary, candidate replica
    (`elect_failover_candidate.candidate_replica`), and target LSN
    (`elect_failover_candidate.target_lsn`) to the Incident Commander.
-   **Required approval fields**:
    -   `approved` (`bool`, must be `True` to proceed)
    -   `incident_ticket` (`str`, e.g. `"INC-2026-0841"`)
-   **Store approval dict under**:
    `payload["outputs"]["approve_replica_promotion"] = {"approved": approved,
    "incident_ticket": incident_ticket}`

### Stage 7: `promote_standby_replica` (One-Way Standby Promotion)

-   **Condition**: Only run if
    `payload["outputs"]["approve_replica_promotion"]["approved"] == True`.
-   **Function**: `actions.promote_standby_replica(payload)`
-   **Requires**: `payload["outputs"]["fence_old_primary"]["stonith_fenced"] ==
    True`.
-   **Side effects**: Promotes `candidate_replica` to read-write primary
    (`promoted_primary`, increments `promotion_count` by `1` in
    `failover_state_path`). Must execute **only once** (`promotion_count == 1`).
-   **Store return dict under**: `payload["outputs"]["promote_standby_replica"]`
    -   Keys: `promoted_primary`, `read_write` (`True`).

### Stage 8: `cutover_pooler_and_verify_writes` (PgBouncer Cutover + Write Canary + Rollback)

-   **Function**: `actions.cutover_pooler_and_verify_writes(payload)`
-   **Inputs**: Reads
    `payload["outputs"]["promote_standby_replica"]["promoted_primary"]`,
    `payload["failover_state_path"]`, and
    `payload.get("simulate_pooler_write_error", False)`.
-   **Behavior & Automatic Rollback**:
    -   Points PgBouncer (`pooler_status`) to the new primary and executes a
        write canary check.
    -   If `simulate_pooler_write_error=True`, raises `RuntimeError("Write
        canary failed on pooler cutover ...")`.
    -   **CRITICAL ROLLBACK RULE**: If
        `actions.cutover_pooler_and_verify_writes(payload)` raises an exception,
        you **must** immediately call
        `actions.revert_pooler_to_maintenance(payload)` inside your `except`
        block so `failover_state_path` places PgBouncer in `PAUSED_MAINTENANCE`
        (`pooler_status="PAUSED_MAINTENANCE"`, `pooler_rolled_back=True`).
-   **Surgical Recovery Rule**: When recovering from a Stage 8 failure (e.g.
    with `simulate_pooler_write_error=False`), do **NOT** re-run Stages 1–7
    (`detect_primary_outage`, `elect_failover_candidate`,
    `replay_missing_wal_segments`, `fence_old_primary`,
    `promote_standby_replica`). Reuse the saved `payload["outputs"]` from Stages
    1–7 and re-execute **only** from Stage 8
    (`cutover_pooler_and_verify_writes`) onward so `promotion_count` remains
    `1`.
-   **Store return dict under**:
    `payload["outputs"]["cutover_pooler_and_verify_writes"]`
    -   Keys: `pooler_status` (`"ONLINE_RW"`), `active_primary`.

### Stage 9: `publish_failover_ledger`

-   **Condition**: Only run after Stage 8 (`cutover_pooler_and_verify_writes`)
    succeeds (`pooler_status == "ONLINE_RW"`).
-   **Function**: `actions.publish_failover_ledger(payload)`
-   **Side effects**: Sets `audit_published=True` and `incident_ticket` in
    `failover_state_path`.
-   **Store return dict under**: `payload["outputs"]["publish_failover_ledger"]`
    -   Keys: `audit_published` (`True`), `incident_ticket`.
