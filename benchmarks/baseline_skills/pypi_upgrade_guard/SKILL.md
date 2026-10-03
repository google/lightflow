---
name: pypi-upgrade-guard
description: Audit pinned Python dependencies against PyPI, pause for operator approval, apply upgrades with automatic rollback on smoke-test failure, and recover without re-auditing using examples/pypi_upgrade_guard/actions.py.
---

# PyPI Upgrade Guard (`pypi_upgrade_guard`)

Executes the 3-stage dependency upgrade workflow with compensating rollback
using `examples/pypi_upgrade_guard/actions.py`.

## Workflow Stages & Rules

1.  **Stage 1 — Audit Pinned Packages (`audit_pypi_versions`)**:

    -   Call `actions.audit_pypi_versions(payload)` with `{"offline": True,
        "requirements_path": "<path>"}`.
    -   Returns `(delta_dict, msg)` where `delta_dict` contains `{"pinned":
        dict, "upgrades": list, "upgrade_count": int, "summary": str, "source":
        str}`.
    -   Persist `delta_dict` under `payload["outputs"]["audit_pypi_versions"]`
        so `audit_pypi_versions` is **never** re-executed in later steps.
    -   Example invocation:

        ```bash
        python3 -B -c "import json, sys; sys.path.insert(0, 'examples/pypi_upgrade_guard'); import actions; d, m = actions.audit_pypi_versions({'offline': True}); print(json.dumps({'delta': d, 'message': m}))"
        ```

2.  **Stage 2 — Human Approval Gate (`approve_upgrades`)**:

    -   **MANDATORY GATE**: Stop after Stage 1 and present
        `delta_dict["summary"]` (`upgrade_count` packages) to the human
        operator.
    -   Wait for their explicit approval (`{"approved": True, "approved_by":
        "<user>", "smoke_test_cmd": "<cmd>"}`). Never auto-approve.

3.  **Stage 3 — Apply Upgrades, Smoke-Test & Compensating Rollback
    (`apply_and_smoke_test`)**:

    -   **Condition**: Run only after Stage 2 is approved. Do **not** call
        `audit_pypi_versions` again.
    -   Construct `payload` with `"requirements_path"`,
        `"simulate_smoke_failure"` (`bool`), `"outputs": {"audit_pypi_versions":
        <delta_dict>, "approve_upgrades": {"approved": True}}`.
    -   Call `actions.apply_and_smoke_test(payload)` inside a `try / except
        Exception:` block:
        -   If `apply_and_smoke_test(payload)` raises an exception,
            **immediately** call `actions.restore_requirements_backup(payload)`
            to restore `requirements_path` from `requirements_path + ".bak"` and
            remove `.bak`, then exit non-zero.
    -   **Failure Recovery**: When recovering from a failed smoke test (e.g.,
        with `"simulate_smoke_failure": False`), re-run **only** Stage 3 using
        the saved `outputs.audit_pypi_versions` dict without re-running Stage 1.
