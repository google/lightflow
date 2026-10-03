---
name: tenant-gitops-onboarding
description: Execute the 22-stage multi-branch Kubernetes, Cloud IAM, KMS, and ArgoCD GitOps tenant onboarding workflow using examples/tenant_gitops_onboarding/actions.py.
---

# Enterprise Tenant GitOps Onboarding (`tenant_gitops_onboarding`)

Executes the 22-stage multi-branch tenant onboarding pipeline across 4 phases
using `examples/tenant_gitops_onboarding/actions.py`. Persist a single state
dictionary (`payload`) across turns (e.g., `/tmp/tenant_state.json`) so every
stage's return dictionary is recorded under
`payload["outputs"]["<stage_name>"]`.

## Phase 1 — Spec Resolution, RBAC Check & Change Advisory Gate (6 stages)

1.  **`init_spec`**: Call `actions.initialize_spec(payload)` (`alias`,
    `compliance_tier` in `{"standard", "pci", "hipaa"}`, `sandbox_dir`). Store
    under `outputs["init_spec"]`.
2.  **`check_admin`**: Call `actions.check_operator_is_admin(payload)`. Store
    under `outputs["check_admin"]`.
3.  **`verify_spec` (Human/Operator Gate 1)**:
    -   **STOP** and present `outputs["init_spec"]` (`alias`, `k8s_namespace`,
        `compliance_tier`) to the requester.
    -   Wait for explicit `{"approved": True}`. Store under
        `outputs["verify_spec"]`.
4.  **Branch Pattern #1 — Role-Based Early Handoff
    (`outputs.check_admin.is_admin`)**:
    -   **`create_ticket`** (run if `outputs.verify_spec.approved == True`):
        Call `actions.create_provisioning_request(payload)` ->
        `outputs["create_ticket"]`.
    -   **`stop_non_admin_stage`** (run if `is_admin == False`): Call
        `actions.stop_non_admin(payload)` -> `outputs["stop_non_admin_stage"]`
        and exit early.
    -   **`wait_governance_approval`** (run if `is_admin == True`): Poll
        `actions.check_governance_approval(payload)` until
        `d["governance_approved"] == True`. Store under
        `outputs["wait_governance_approval"]`.

## Phase 2 — IdP Groups, SCIM 2.0 Push & Cloud IAM Workload Identity (6 stages)

5.  **`provision_identity_groups`**: Call
    `actions.provision_identity_groups(payload)` ->
    `outputs["provision_identity_groups"]`.
6.  **`wait_scim_sync`**: Poll `actions.check_scim_ready(payload)` until
    `d["scim_ready"] == True` -> `outputs["wait_scim_sync"]`.
7.  **`create_workload_iam_role`**: Call
    `actions.create_workload_iam_role(payload)` ->
    `outputs["create_workload_iam_role"]`. Inspect `iam_status =
    outputs["create_workload_iam_role"]["iam_status"]`:
    -   **Branch Pattern #2 — 3-Way Status Fan-Out + `ALL_DONE` Convergence**:
        -   **`fail_on_iam_failed`** (run if `iam_status == "FAILED"`): Call
            `actions.fail_workflow(payload)` and abort.
        -   **`prompt_iam_quota_override` (Human/Operator Gate 2)** (run if
            `iam_status == "QUOTA_EXCEEDED"`): **STOP** and ask the security
            operator for `{"approved": True, "quota_override_approved": True}`.
            Store under `outputs["prompt_iam_quota_override"]`.
        -   **`wait_iam_role_ready`** (run if `iam_status == "SUCCESS"` OR
            (`iam_status == "QUOTA_EXCEEDED"` and `quota_override_approved ==
            True`)): Poll `actions.check_iam_role_ready(payload)` until
            `d["iam_role_ready"] == True` -> `outputs["wait_iam_role_ready"]`.

## Phase 3 — Backing-Resource Diamond & Compliance-Tier KMS Branch (4 stages)

8.  **`provision_external_resources`**: Call
    `actions.provision_external_resources(payload)` (retry up to 2 attempts; on
    failure call `actions.rollback_external_resources(payload)`) ->
    `outputs["provision_external_resources"]`.
9.  **Branch Pattern #3 — Compliance-Tier KMS Mutual Exclusion
    (`outputs.init_spec.compliance_tier`)**:
    -   **`provision_dedicated_kms_key`** (run if `compliance_tier == "pci"`):
        Call `actions.provision_dedicated_kms_key(payload)` ->
        `outputs["provision_dedicated_kms_key"]`.
    -   **`apply_shared_kms_policy`** (run if `compliance_tier != "pci"`): Call
        `actions.apply_shared_kms_policy(payload)` ->
        `outputs["apply_shared_kms_policy"]`.
10. **`checkpoint_catalog`** (join after storage + KMS complete): Call
    `actions.checkpoint_registry(payload)` -> `outputs["checkpoint_catalog"]`.

## Phase 4 — Declarative GitOps PRs & ArgoCD Sync (6 stages)

11. **`scaffold_k8s_gitops_pr`**: Call `actions.scaffold_k8s_gitops_pr(payload)`
    -> `outputs["scaffold_k8s_gitops_pr"]`.
12. **`wait_k8s_gitops_pr`**: Poll `actions.check_pr_merged(payload,
    target_stage="scaffold_k8s_gitops_pr", target_pr_key="k8s_pr_id")` until
    `d["pr_merged"] == True` -> `outputs["wait_k8s_gitops_pr"]`.
13. **`wait_argocd_namespace_sync`**: Poll `actions.check_argocd_synced(payload,
    app_name="tenant-namespace", required_pr_stage="wait_k8s_gitops_pr")` until
    `d["app_synced"] == True` -> `outputs["wait_argocd_namespace_sync"]`.
14. **`package_mesh_and_catalog_pr`**: Call
    `actions.package_mesh_and_catalog_pr(payload)` ->
    `outputs["package_mesh_and_catalog_pr"]`.
15. **`wait_mesh_and_catalog_pr`**: Poll `actions.check_pr_merged(payload,
    target_stage="package_mesh_and_catalog_pr", target_pr_key="catalog_pr_id")`
    until `d["pr_merged"] == True` -> `outputs["wait_mesh_and_catalog_pr"]`.
16. **`wait_argocd_mesh_sync`**: Poll `actions.check_argocd_synced(payload,
    app_name="mesh-and-catalog", required_pr_stage="wait_mesh_and_catalog_pr")`
    until `d["app_synced"] == True` -> `outputs["wait_argocd_mesh_sync"]`.
