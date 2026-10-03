# Blue-Green / Canary Release (`blue_green_release`)

An 8-stage service rollout workflow demonstrating:

-   **Parallel validation diamond** (`run_security_scan` $\parallel$
    `run_integration_suite` joining at `warm_green_environment`),
-   **Readiness polling (`polling_policy`)** warming the green slot across ticks
    until `green_ready == true`,
-   **Human/operator gate (`approve_canary_cutover`)** validating `approved` and
    `canary_percent`,
-   **Compensating rollback (`revert_canary_traffic`)** restoring 100% traffic
    to `blue` (`canary_weight: 0`) if `shift_and_verify_canary` breaches its
    error-rate SLO, and
-   **Surgical recovery (`lightflow resume`)** resuming from
    `shift_and_verify_canary` without re-running the bundle build, security
    scan, integration tests, or green slot warmup.

## Quick Start

```bash
# 1. Build, scan, test, warm green slot, and pause at approve_canary_cutover (exit 2)
lightflow start --lightflow=examples/blue_green_release --log_id=bg_demo \
  --payload='{"release_state_path": "/tmp/bg_state.json"}'

# 2. Approve canary shift with simulated SLO regression (exit 1 + automatic rollback to blue)
lightflow resume --lightflow=examples/blue_green_release --log_id=bg_demo \
  --stage=approve_canary_cutover --resolution=APPROVE \
  --payload='{"approved": true, "canary_percent": 10, "simulate_canary_regression": true}'

# 3. Recover surgically from shift_and_verify_canary without re-running stages 1-5 (exit 0)
lightflow resume --lightflow=examples/blue_green_release --log_id=bg_demo \
  --payload='{"simulate_canary_regression": false}'
```
