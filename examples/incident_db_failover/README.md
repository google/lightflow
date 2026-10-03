# Incident Database Failover (`incident_db_failover`)

A 9-stage database high-availability failover workflow demonstrating:

-   **Mutually exclusive `run_if` branching** (`replay_missing_wal_segments`
    with `polling_policy` when `lag_mode == 'wal_replay_needed'` vs.
    `verify_zero_loss_sync` when `lag_mode == 'zero_loss'`),
-   **`trigger_rule: ALL_DONE` branch convergence** at `fence_old_primary`,
-   **Incident Commander approval gate (`approve_replica_promotion`)**
    validating `approved` and `incident_ticket`,
-   **One-way upstream actuation (`promote_standby_replica`)** followed by
    **pooler cutover (`cutover_pooler_and_verify_writes`)** with automatic
    `revert_pooler_to_maintenance` rollback on write-canary failure, and
-   **Surgical recovery (`lightflow resume`)** that recovers the pooler cutover
    **without** re-executing `promote_standby_replica`.
