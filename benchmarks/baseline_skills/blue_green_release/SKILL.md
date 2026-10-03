---
name: blue-green-release
description: Run an 8-stage blue/green canary release with parallel validation, green slot warmup polling, an operator cutover gate, automatic canary traffic rollback on SLO failure, surgical recovery, green promotion, and old-slot decommission.
---

# Blue/Green Canary Release (`SKILL.md` + `actions.py` Baseline)

Run an 8-stage blue/green canary deployment using the helper functions in
`examples/blue_green_release/actions.py` (`python3 -c "from
examples.blue_green_release import actions; ..."`).

Do **not** invoke the `lightflow` CLI and do **not** read `lightflow.yaml` or
`actions.py` — every function signature, payload key, `outputs` key, branch
condition, and rollback function is documented below. Every helper in
`actions.py` returns a 2-tuple `(output_dict, message_str)` — always unpack
`data, msg = actions.<fn>(payload)` and store `data` under
`payload["outputs"]["<stage_name>"]`.

--------------------------------------------------------------------------------

## Stage & Dependency Order (8 Stages)

Execute the stages in this exact dependency order, accumulating each stage's
returned dictionary under `payload["outputs"]["<stage_name>"]`:

### Stage 1: `build_release_bundle`

-   **Function**: `actions.build_release_bundle(payload)`
-   **Inputs**:
    -   `payload.get("release_state_path")` (JSON file path for deployment slot
        state)
    -   `payload.get("service_name", "checkout-api")`
    -   `payload.get("version", "v2.4.0")`
-   **Side effects**: Initializes `release_state_path` (`active_slot="blue"`,
    `canary_weight=0`, `green_warmed_ticks=0`, `green_ready=False`,
    `old_slot_decommissioned=False`).
-   **Store return dict under**: `payload["outputs"]["build_release_bundle"]`
    -   Keys: `service_name`, `version`, `bundle_digest`, `release_state_path`.

### Stages 2 & 3 (Parallel Validation Diamond after Stage 1)

#### Stage 2: `run_security_scan`

-   **Function**: `actions.run_security_scan(payload)`
-   **Requires**: `payload["outputs"]["build_release_bundle"]`
-   **Store return dict under**: `payload["outputs"]["run_security_scan"]`
    -   Keys: `scan_passed` (`True`), `critical_cves` (`0`), `verified_digest`.

#### Stage 3: `run_integration_suite`

-   **Function**: `actions.run_integration_suite(payload)`
-   **Requires**: `payload["outputs"]["build_release_bundle"]`
-   **Store return dict under**: `payload["outputs"]["run_integration_suite"]`
    -   Keys: `tests_passed` (`True`), `total_cases` (`42`), `verified_version`.

### Stage 4: `warm_green_environment` (Join after Stages 2 & 3; Polling Loop)

-   **Function**: `actions.warm_green_environment(payload)`
-   **Requires**: Both `run_security_scan` (`scan_passed == True`) and
    `run_integration_suite` (`tests_passed == True`) present in
    `payload["outputs"]`.
-   **Polling rule**: Call `actions.warm_green_environment(payload)` repeatedly
    until the returned dictionary has `result["green_ready"] == True` (takes 2
    ticks to warm the green slot).
-   **Store final return dict under**:
    `payload["outputs"]["warm_green_environment"]`
    -   Keys: `green_ready` (`True`), `warm_ticks` (`2`).

### Stage 5: `approve_canary_cutover` (Operator Approval Gate)

-   **Pause before Stage 6**: Present the bundle digest
    (`build_release_bundle.bundle_digest`), security scan
    (`run_security_scan.scan_passed`), integration test status
    (`run_integration_suite.tests_passed`), and green slot readiness
    (`warm_green_environment.green_ready`) to the operator.
-   **Required approval fields**:
    -   `approved` (`bool`, must be `True` to proceed)
    -   `canary_percent` (`int`, e.g. `10`)
-   **Store approval dict under**:
    `payload["outputs"]["approve_canary_cutover"] = {"approved": approved,
    "canary_percent": canary_percent}`

### Stage 6: `shift_and_verify_canary` (Canary Mutation + SLO Verification + Rollback)

-   **Condition**: Only run if
    `payload["outputs"]["approve_canary_cutover"]["approved"] == True`.
-   **Function**: `actions.shift_and_verify_canary(payload)`
-   **Inputs**: Reads
    `payload["outputs"]["approve_canary_cutover"]["canary_percent"]`,
    `payload["release_state_path"]`, and
    `payload.get("simulate_canary_regression", False)`.
-   **Behavior & Automatic Rollback**:
    -   Shifts `canary_weight` (e.g. `10%`) to `green` in `release_state_path`
        (`active_slot="blue+green_canary"`).
    -   Evaluates canary 5xx error rate. If `simulate_canary_regression=True`,
        raises `RuntimeError("Canary SLO breach ...")`.
    -   **CRITICAL ROLLBACK RULE**: If
        `actions.shift_and_verify_canary(payload)` raises an exception, you
        **must** immediately call `actions.revert_canary_traffic(payload)`
        inside your `except` block so `release_state_path` is restored to
        `canary_weight=0`, `active_slot="blue"`, and `rolled_back=True`.
-   **Surgical Recovery Rule**: When recovering from a Stage 6 failure (e.g.
    with `simulate_canary_regression=False`), do **NOT** re-run Stages 1–4
    (`build_release_bundle`, `run_security_scan`, `run_integration_suite`,
    `warm_green_environment`). Reuse the saved `payload["outputs"]` from Stages
    1–5 and re-execute **only** from Stage 6 (`shift_and_verify_canary`) onward.
-   **Store return dict under**: `payload["outputs"]["shift_and_verify_canary"]`
    -   Keys: `canary_healthy` (`True`), `canary_weight`, `error_rate`.

### Stage 7: `promote_green_to_prod`

-   **Condition**: Only run after Stage 6 (`shift_and_verify_canary`) succeeds
    (`canary_healthy == True`).
-   **Function**: `actions.promote_green_to_prod(payload)`
-   **Side effects**: Updates `release_state_path` to `active_slot="green"`,
    `canary_weight=100`, `rolled_back=False`.
-   **Store return dict under**: `payload["outputs"]["promote_green_to_prod"]`
    -   Keys: `active_slot` (`"green"`), `production_weight` (`100`).

### Stage 8: `decommission_old_slot`

-   **Condition**: Only run after Stage 7 (`promote_green_to_prod`) succeeds
    (`active_slot == "green"`).
-   **Function**: `actions.decommission_old_slot(payload)`
-   **Side effects**: Sets `old_slot_decommissioned=True` in
    `release_state_path`.
-   **Store return dict under**: `payload["outputs"]["decommission_old_slot"]`
    -   Keys: `old_slot_decommissioned` (`True`), `retired_slot` (`"blue"`).
