# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Mocked platform actions for the 39-stage GitOps tenant onboarding workflow.

All external infrastructure systems (ticketing, GitOps pull requests, OIDC
identity reconciler, SCIM group sync, workload ServiceAccount portal, cloud
storage/Vault artifacts, and FinOps cost-center catalog) are simulated against
a local JSON state file inside `sandbox_dir` so the full 39-stage enterprise
pipeline runs hermetically in under 2 seconds without cloud credentials.
"""

from __future__ import annotations

import json
import os
import shutil
import tempfile
from typing import Any


def _resolve_sandbox(payload: dict[str, Any]) -> str:
  """Returns the sandbox directory for the current onboarding run."""
  sandbox = str(payload.get("sandbox_dir", "")).strip()
  if not sandbox or sandbox.startswith("<mock_"):
    alias = str(payload.get("alias", "payments-eu")).strip() or "payments-eu"
    sandbox = os.path.join(
        tempfile.gettempdir(), f"lightflow_tenant_onboarding_{alias}"
    )
  return os.path.abspath(os.path.expanduser(sandbox))


def _state_file(sandbox: str) -> str:
  return os.path.join(sandbox, "control_plane_state.json")


def _load_state(sandbox: str) -> dict[str, Any]:
  path = _state_file(sandbox)
  if os.path.isfile(path):
    with open(path, "r", encoding="utf-8") as f:
      return json.load(f)
  return {
      "prs": {},
      "groups": [],
      "admins": [],
      "checkpoints": [],
      "files": [],
      "cloud_artifacts": {},
  }


def _save_state(sandbox: str, state: dict[str, Any]) -> None:
  os.makedirs(sandbox, exist_ok=True)
  with open(_state_file(sandbox), "w", encoding="utf-8") as f:
    json.dump(state, f, indent=2, sort_keys=True)


def initialize_spec(
    payload: dict[str, Any], dry_run: bool = False, **kwargs: Any
) -> tuple[dict[str, Any], str]:
  """Normalizes tenant onboarding parameters and initializes local state."""
  del kwargs
  alias = str(payload.get("alias", "payments-eu")).strip() or "payments-eu"
  spec_name = (
      str(payload.get("spec_name", "Payments EU Checkout")).strip()
      or "Payments EU Checkout"
  )
  spec_rbac_root = (
      str(payload.get("spec_rbac_root", f"oidc-{alias}")).strip()
      or f"oidc-{alias}"
  )
  spec_primary_admin = (
      str(payload.get("spec_primary_admin", "alice@example.com")).strip()
      or "alice@example.com"
  )
  spec_secondary_admin = (
      str(payload.get("spec_secondary_admin", "bob@example.com")).strip()
      or "bob@example.com"
  )
  spec_cost_center_id = (
      str(payload.get("spec_cost_center_id", "CC-4820")).strip() or "CC-4820"
  )
  sandbox_dir = _resolve_sandbox({**payload, "alias": alias})

  out = {
      "alias": alias,
      "spec_name": spec_name,
      "spec_rbac_root": spec_rbac_root,
      "spec_primary_admin": spec_primary_admin,
      "spec_secondary_admin": spec_secondary_admin,
      "spec_cost_center_id": spec_cost_center_id,
      "sandbox_dir": sandbox_dir,
  }
  if dry_run:
    return (
        out,
        (
            f"[DRY RUN] Would initialize tenant spec for '{alias}' in"
            f" '{sandbox_dir}'."
        ),
    )

  shutil.rmtree(sandbox_dir, ignore_errors=True)
  state = _load_state(sandbox_dir)
  state["spec"] = dict(out)
  _save_state(sandbox_dir, state)
  return (
      out,
      f"Initialized tenant specification for '{alias}' ({spec_name}).",
  )


def check_operator_is_admin(
    payload: dict[str, Any], dry_run: bool = False, **kwargs: Any
) -> tuple[dict[str, Any], str]:
  """Checks whether the active operator holds platform-admin privileges."""
  del dry_run, kwargs
  is_admin = not bool(payload.get("simulate_non_admin", False))
  role_label = "platform-admin" if is_admin else "standard-requester"
  return (
      {"is_admin": is_admin, "operator_role": role_label},
      f"Verified operator role: {role_label} (is_admin={is_admin}).",
  )


def create_provisioning_request(
    payload: dict[str, Any], dry_run: bool = False, **kwargs: Any
) -> tuple[dict[str, Any], str]:
  """Creates a mocked change-management onboarding ticket."""
  del kwargs
  alias = str(payload.get("alias", "payments-eu"))
  code = sum(ord(ch) * (idx + 1) for idx, ch in enumerate(alias)) % 9000 + 1000
  ticket_id = f"ONBOARD-{code}"
  if dry_run:
    return (
        {"ticket_id": ticket_id},
        f"[DRY RUN] Would create change-management ticket {ticket_id}.",
    )
  sandbox = _resolve_sandbox(payload)
  state = _load_state(sandbox)
  state["ticket_id"] = ticket_id
  _save_state(sandbox, state)
  return (
      {"ticket_id": ticket_id},
      f"Created onboarding change-management ticket {ticket_id}.",
  )


def checkpoint_registry(
    payload: dict[str, Any], dry_run: bool = False, **kwargs: Any
) -> tuple[dict[str, Any], str]:
  """Writes a progress checkpoint into the mocked platform catalog."""
  del kwargs
  alias = str(payload.get("alias", "payments-eu"))
  ticket_id = str(payload.get("ticket_id", "ONBOARD-1000"))
  if dry_run:
    return (
        {"registry_checkpointed": True},
        f"[DRY RUN] Would checkpoint catalog state for '{alias}'.",
    )
  sandbox = _resolve_sandbox(payload)
  state = _load_state(sandbox)
  checkpoints = list(state.get("checkpoints", []))
  checkpoints.append({
      "alias": alias,
      "ticket_id": ticket_id,
      "has_cloud_artifacts": bool(payload.get("state_bucket_uri")),
  })
  state["checkpoints"] = checkpoints
  _save_state(sandbox, state)
  return (
      {"registry_checkpointed": True, "checkpoint_count": len(checkpoints)},
      (
          f"Checkpointed tenant '{alias}' in platform catalog"
          f" (#{len(checkpoints)})."
      ),
  )


def stop_non_admin(
    payload: dict[str, Any], dry_run: bool = False, **kwargs: Any
) -> tuple[dict[str, Any], str]:
  """Records early handoff when initiated by a non-admin requester."""
  del dry_run, kwargs
  ticket_id = str(payload.get("ticket_id", "ONBOARD-1000"))
  return (
      {"non_admin_handoff": True},
      (
          f"Non-admin request recorded under {ticket_id}; platform admins will"
          " complete actuation."
      ),
  )


def check_governance_approval(
    payload: dict[str, Any], dry_run: bool = False, **kwargs: Any
) -> tuple[dict[str, Any], str]:
  """Polls change-advisory approval status on the onboarding ticket."""
  del dry_run, kwargs
  ticket_id = str(payload.get("ticket_id", "ONBOARD-1000"))
  return (
      {"governance_approved": True},
      f"Change-advisory board approved ticket {ticket_id}.",
  )


def write_oidc_rbac_manifest(
    payload: dict[str, Any], dry_run: bool = False, **kwargs: Any
) -> tuple[dict[str, Any], str]:
  """Generates the declarative OIDC RBAC group manifest in the GitOps repo."""
  del kwargs
  alias = str(payload.get("alias", "payments-eu"))
  rel_path = f"gitops/rbac/{alias}/groups.yaml"
  if dry_run:
    return (
        {"rbac_manifest_path": rel_path},
        f"[DRY RUN] Would write OIDC RBAC manifest '{rel_path}'.",
    )
  sandbox = _resolve_sandbox(payload)
  full_path = os.path.join(sandbox, rel_path)
  os.makedirs(os.path.dirname(full_path), exist_ok=True)
  with open(full_path, "w", encoding="utf-8") as f:
    f.write(
        "apiVersion: identity.example.io/v1alpha1\nkind: GroupSet\n"
        f"metadata:\n  name: {payload.get('spec_rbac_root', f'oidc-{alias}')}\n"
    )
  state = _load_state(sandbox)
  files = state.setdefault("files", [])
  if rel_path not in files:
    files.append(rel_path)
  _save_state(sandbox, state)
  return (
      {"rbac_manifest_path": rel_path},
      f"Generated OIDC RBAC manifest at '{rel_path}'.",
  )


def register_in_kustomize(
    payload: dict[str, Any], dry_run: bool = False, **kwargs: Any
) -> tuple[dict[str, Any], str]:
  """Registers the tenant RBAC directory in the root kustomization.yaml."""
  del kwargs
  alias = str(payload.get("alias", "payments-eu"))
  kustomize_path = "gitops/rbac/kustomization.yaml"
  if dry_run:
    return (
        {"kustomize_registered": True},
        f"[DRY RUN] Would register '{alias}' in '{kustomize_path}'.",
    )
  sandbox = _resolve_sandbox(payload)
  full_path = os.path.join(sandbox, kustomize_path)
  os.makedirs(os.path.dirname(full_path), exist_ok=True)
  entry = f"- ./{alias}/groups.yaml\n"
  existing = ""
  if os.path.isfile(full_path):
    with open(full_path, "r", encoding="utf-8") as f:
      existing = f.read()
  if entry not in existing:
    with open(full_path, "a", encoding="utf-8") as f:
      f.write(entry)
  return (
      {"kustomize_registered": True},
      f"Registered '{alias}/groups.yaml' in '{kustomize_path}'.",
  )


def _open_mock_pr(
    payload: dict[str, Any],
    pr_key: str,
    pr_number: int,
    title: str,
    dry_run: bool,
) -> tuple[dict[str, Any], str]:
  """Opens and records a simulated GitOps pull request in local state."""
  pr_id = f"PR-{pr_number}"
  if dry_run:
    return (
        {pr_key: pr_id},
        f"[DRY RUN] Would open GitOps {pr_id} ({title}).",
    )
  sandbox = _resolve_sandbox(payload)
  state = _load_state(sandbox)
  state.setdefault("prs", {})[pr_id] = {"title": title, "status": "MERGED"}
  _save_state(sandbox, state)
  return (
      {pr_key: pr_id},
      f"Opened and queued GitOps {pr_id}: '{title}'.",
  )


def package_rbac_pr(
    payload: dict[str, Any], dry_run: bool = False, **kwargs: Any
) -> tuple[dict[str, Any], str]:
  """Packages the OIDC RBAC manifest changes into a GitOps pull request."""
  del kwargs
  alias = str(payload.get("alias", "payments-eu"))
  return _open_mock_pr(
      payload,
      "rbac_pr_id",
      101,
      f"feat(rbac): onboard OIDC group set for {alias}",
      dry_run,
  )


def check_pr_merged(
    payload: dict[str, Any],
    target_pr_key: str = "rbac_pr_id",
    dry_run: bool = False,
    **kwargs: Any,
) -> tuple[dict[str, Any], str]:
  """Reusable polling action that checks whether a GitOps PR has merged."""
  del dry_run, kwargs
  pr_id = str(payload.get(target_pr_key, f"<{target_pr_key}>"))
  merged_key = f"{target_pr_key}_merged"
  return (
      {merged_key: True},
      f"GitOps pull request {pr_id} ({target_pr_key}) merged into main.",
  )


def check_oidc_absorbed(
    payload: dict[str, Any], dry_run: bool = False, **kwargs: Any
) -> tuple[dict[str, Any], str]:
  """Polls the OIDC identity reconciler until the RBAC PR commit is absorbed."""
  del dry_run, kwargs
  root = str(payload.get("spec_rbac_root", "oidc-payments-eu"))
  return (
      {"oidc_absorbed": True},
      f"OIDC identity reconciler absorbed spec for '{root}'.",
  )


def create_identity_groups(
    payload: dict[str, Any], dry_run: bool = False, **kwargs: Any
) -> tuple[dict[str, Any], str]:
  """Actuates the tenant OIDC groups in the identity provider."""
  del kwargs
  root = str(payload.get("spec_rbac_root", "oidc-payments-eu"))
  groups = [f"{root}-admins", f"{root}-devs", f"{root}-automation"]
  if dry_run:
    return (
        {"actuated_groups": groups},
        f"[DRY RUN] Would actuate {len(groups)} OIDC groups under '{root}'.",
    )
  sandbox = _resolve_sandbox(payload)
  state = _load_state(sandbox)
  state["groups"] = groups
  _save_state(sandbox, state)
  return (
      {"actuated_groups": groups},
      f"Actuated {len(groups)} OIDC groups: {', '.join(groups)}.",
  )


def add_admins(
    payload: dict[str, Any], dry_run: bool = False, **kwargs: Any
) -> tuple[dict[str, Any], str]:
  """Binds primary and secondary tenant admins to the admin group."""
  del kwargs
  primary = str(payload.get("spec_primary_admin", "alice@example.com"))
  secondary = str(payload.get("spec_secondary_admin", "bob@example.com"))
  admins = [primary, secondary]
  if dry_run:
    return (
        {"bound_admins": admins},
        f"[DRY RUN] Would bind {admins} to tenant admin group.",
    )
  sandbox = _resolve_sandbox(payload)
  state = _load_state(sandbox)
  state["admins"] = admins
  _save_state(sandbox, state)
  return (
      {"bound_admins": admins},
      f"Bound tenant administrators: {', '.join(admins)}.",
  )


def propose_trusted_issuer(
    payload: dict[str, Any], dry_run: bool = False, **kwargs: Any
) -> tuple[dict[str, Any], str]:
  """Registers the tenant automation group as a trusted OIDC issuer."""
  del dry_run, kwargs
  root = str(payload.get("spec_rbac_root", "oidc-payments-eu"))
  return (
      {"trusted_issuer_proposed": True},
      f"Proposed '{root}-automation' as a trusted workload OIDC issuer.",
  )


def check_trusted_issuer_ready(
    payload: dict[str, Any], dry_run: bool = False, **kwargs: Any
) -> tuple[dict[str, Any], str]:
  """Polls until the OIDC trusted-issuer registration is active."""
  del dry_run, kwargs
  root = str(payload.get("spec_rbac_root", "oidc-payments-eu"))
  return (
      {"trusted_issuer_ready": True},
      f"Trusted OIDC issuer '{root}-automation' is active.",
  )


def enable_automated_management(
    payload: dict[str, Any], dry_run: bool = False, **kwargs: Any
) -> tuple[dict[str, Any], str]:
  """Enables automated SCIM lifecycle management on the tenant groups."""
  del dry_run, kwargs
  root = str(payload.get("spec_rbac_root", "oidc-payments-eu"))
  return (
      {"automated_management_enabled": True},
      f"Enabled automated SCIM lifecycle management for '{root}'.",
  )


def trigger_group_reconciliation(
    payload: dict[str, Any], dry_run: bool = False, **kwargs: Any
) -> tuple[dict[str, Any], str]:
  """Triggers an immediate SCIM group membership reconciliation pass."""
  del dry_run, kwargs
  root = str(payload.get("spec_rbac_root", "oidc-payments-eu"))
  return (
      {"reconciliation_triggered": True},
      f"Triggered SCIM group membership reconciliation for '{root}'.",
  )


def check_scim_ready(
    payload: dict[str, Any], dry_run: bool = False, **kwargs: Any
) -> tuple[dict[str, Any], str]:
  """Polls until two-way SCIM directory synchronization converges."""
  del dry_run, kwargs
  root = str(payload.get("spec_rbac_root", "oidc-payments-eu"))
  return (
      {"scim_ready": True},
      f"SCIM directory synchronization converged for '{root}'.",
  )


def create_service_account(
    payload: dict[str, Any], dry_run: bool = False, **kwargs: Any
) -> tuple[dict[str, Any], str]:
  """Creates the workload identity ServiceAccount (`<root>-jobs.svc`)."""
  # Supports simulated outcomes via `sa_creation_mode` in payload:
  # - 'PERMISSION_DENIED' (when require_manual_sa_portal=True): triggers Gate 2
  # - 'SUCCESS' (default): auto-creates without human intervention
  # - 'FAILED': triggers fail_on_sa_failed branch
  del dry_run, kwargs
  root = str(payload.get("spec_rbac_root", "oidc-payments-eu"))
  sa_name = f"{root}-jobs.svc"
  mode = str(payload.get("sa_creation_mode", "")).strip().upper()
  if not mode:
    mode = (
        "PERMISSION_DENIED"
        if payload.get("require_manual_sa_portal", False)
        else "SUCCESS"
    )
  if mode not in ("SUCCESS", "PERMISSION_DENIED", "FAILED"):
    mode = "SUCCESS"
  return (
      {"sa_creation_status": mode, "service_account_name": sa_name},
      f"ServiceAccount '{sa_name}' provisioning status: {mode}.",
  )


def fail_workflow(
    payload: dict[str, Any], dry_run: bool = False, **kwargs: Any
) -> tuple[dict[str, Any], str]:
  """Raises an error when ServiceAccount creation fails terminally."""
  del dry_run, kwargs
  sa_name = str(payload.get("service_account_name", "jobs.svc"))
  raise RuntimeError(
      f"Fatal error creating workload ServiceAccount '{sa_name}'."
  )


def check_sa_ready(
    payload: dict[str, Any], dry_run: bool = False, **kwargs: Any
) -> tuple[dict[str, Any], str]:
  """Polls until the workload ServiceAccount is active (auto or manual)."""
  del dry_run, kwargs
  sa_name = str(
      payload.get("service_account_name", "oidc-payments-eu-jobs.svc")
  )
  via = (
      "manual portal confirmation"
      if payload.get("manually_activated")
      else "automated provisioning"
  )
  return (
      {"sa_ready": True},
      f"Workload ServiceAccount '{sa_name}' is active (via {via}).",
  )


def provision_external_resources(
    payload: dict[str, Any], dry_run: bool = False, **kwargs: Any
) -> tuple[dict[str, Any], str]:
  """Provisions the tenant Terraform state bucket and Vault secret mount."""
  del kwargs
  alias = str(payload.get("alias", "payments-eu"))
  bucket_uri = f"s3://platform-tfstate-{alias}"
  vault_mount = f"secret/tenants/{alias}"
  out = {
      "state_bucket_uri": bucket_uri,
      "vault_mount_path": vault_mount,
  }
  if dry_run:
    return (
        out,
        (
            f"[DRY RUN] Would provision '{bucket_uri}' and Vault mount"
            f" '{vault_mount}'."
        ),
    )

  sandbox = _resolve_sandbox(payload)
  state = _load_state(sandbox)
  state["cloud_artifacts"] = dict(out)
  _save_state(sandbox, state)

  if payload.get("simulate_artifact_failure", False):
    raise RuntimeError(
        f"Transient Vault mount failure while initializing '{vault_mount}'."
    )

  return (
      out,
      (
          f"Provisioned state bucket '{bucket_uri}' and Vault mount"
          f" '{vault_mount}'."
      ),
  )


def rollback_external_resources(
    payload: dict[str, Any], dry_run: bool = False, **kwargs: Any
) -> tuple[dict[str, Any], str]:
  """Rollback action that cleans up partially created cloud artifacts."""
  del dry_run, kwargs
  sandbox = _resolve_sandbox(payload)
  state = _load_state(sandbox)
  state["cloud_artifacts"] = {}
  _save_state(sandbox, state)
  return (
      {"cloud_artifacts_rolled_back": True},
      "Rolled back partial cloud storage and Vault mount artifacts.",
  )


def scaffold_k8s_namespace(
    payload: dict[str, Any], dry_run: bool = False, **kwargs: Any
) -> tuple[dict[str, Any], str]:
  """Scaffolds Kubernetes Namespace, NetworkPolicy, and ResourceQuota YAML."""
  del kwargs
  alias = str(payload.get("alias", "payments-eu"))
  ns_dir = f"gitops/namespaces/{alias}"
  if dry_run:
    return (
        {"namespace_manifest_dir": ns_dir},
        f"[DRY RUN] Would scaffold Kubernetes manifests in '{ns_dir}'.",
    )
  sandbox = _resolve_sandbox(payload)
  full_dir = os.path.join(sandbox, ns_dir)
  os.makedirs(full_dir, exist_ok=True)
  for fname in ("namespace.yaml", "networkpolicy.yaml", "resourcequota.yaml"):
    with open(os.path.join(full_dir, fname), "w", encoding="utf-8") as f:
      f.write(f"# Generated for tenant {alias}\n")
  return (
      {"namespace_manifest_dir": ns_dir},
      f"Scaffolded Namespace, NetworkPolicy, and ResourceQuota in '{ns_dir}'.",
  )


def package_scaffolding_pr(
    payload: dict[str, Any], dry_run: bool = False, **kwargs: Any
) -> tuple[dict[str, Any], str]:
  """Opens a GitOps PR for the tenant Kubernetes namespace manifests."""
  del kwargs
  alias = str(payload.get("alias", "payments-eu"))
  return _open_mock_pr(
      payload,
      "scaffolding_pr_id",
      102,
      f"feat(k8s): scaffold namespace and quotas for {alias}",
      dry_run,
  )


def bootstrap_cronjob_rbac(
    payload: dict[str, Any], dry_run: bool = False, **kwargs: Any
) -> tuple[dict[str, Any], str]:
  """Generates CronJob RBAC RoleBinding for the tenant workload identity."""
  del kwargs
  alias = str(payload.get("alias", "payments-eu"))
  cron_path = f"gitops/namespaces/{alias}/cronjob-rbac.yaml"
  if dry_run:
    return (
        {"cronjob_rbac_path": cron_path},
        f"[DRY RUN] Would write CronJob RBAC binding '{cron_path}'.",
    )
  sandbox = _resolve_sandbox(payload)
  full_path = os.path.join(sandbox, cron_path)
  os.makedirs(os.path.dirname(full_path), exist_ok=True)
  with open(full_path, "w", encoding="utf-8") as f:
    f.write(f"# CronJob RoleBinding for {alias}\n")
  return (
      {"cronjob_rbac_path": cron_path},
      f"Bootstrapped CronJob RBAC binding at '{cron_path}'.",
  )


def package_cronjob_rbac_pr(
    payload: dict[str, Any], dry_run: bool = False, **kwargs: Any
) -> tuple[dict[str, Any], str]:
  """Opens a GitOps PR for the CronJob RBAC RoleBinding."""
  del kwargs
  alias = str(payload.get("alias", "payments-eu"))
  return _open_mock_pr(
      payload,
      "cronjob_pr_id",
      103,
      f"feat(cron): bind workload identity for {alias} scheduled jobs",
      dry_run,
  )


def nest_in_root_groups(
    payload: dict[str, Any], dry_run: bool = False, **kwargs: Any
) -> tuple[dict[str, Any], str]:
  """Nests the tenant OIDC groups into the cluster-wide mesh routing policy."""
  del kwargs
  alias = str(payload.get("alias", "payments-eu"))
  wiring_path = "gitops/mesh/tenant-bindings.yaml"
  if dry_run:
    return (
        {"mesh_wiring_path": wiring_path},
        f"[DRY RUN] Would nest '{alias}' groups in '{wiring_path}'.",
    )
  sandbox = _resolve_sandbox(payload)
  full_path = os.path.join(sandbox, wiring_path)
  os.makedirs(os.path.dirname(full_path), exist_ok=True)
  entry = f"- tenant: {alias}\n"
  existing = ""
  if os.path.isfile(full_path):
    with open(full_path, "r", encoding="utf-8") as f:
      existing = f.read()
  if entry not in existing:
    with open(full_path, "a", encoding="utf-8") as f:
      f.write(entry)
  return (
      {"mesh_wiring_path": wiring_path},
      f"Nested '{alias}' OIDC groups into '{wiring_path}'.",
  )


def package_wiring_pr(
    payload: dict[str, Any], dry_run: bool = False, **kwargs: Any
) -> tuple[dict[str, Any], str]:
  """Opens a GitOps PR for the root mesh and group wiring."""
  del kwargs
  alias = str(payload.get("alias", "payments-eu"))
  return _open_mock_pr(
      payload,
      "wiring_pr_id",
      104,
      f"feat(mesh): wire {alias} into root gateway and group hierarchy",
      dry_run,
  )


def check_wiring_oidc_absorbed(
    payload: dict[str, Any], dry_run: bool = False, **kwargs: Any
) -> tuple[dict[str, Any], str]:
  """Polls until the root mesh and OIDC hierarchy reconciler absorbs wiring."""
  del dry_run, kwargs
  alias = str(payload.get("alias", "payments-eu"))
  return (
      {"wiring_oidc_absorbed": True},
      f"Root mesh and OIDC reconciler absorbed wiring for '{alias}'.",
  )


def propose_cost_center_association(
    payload: dict[str, Any], dry_run: bool = False, **kwargs: Any
) -> tuple[dict[str, Any], str]:
  """Associates the tenant namespace with its FinOps cost center ID."""
  del dry_run, kwargs
  alias = str(payload.get("alias", "payments-eu"))
  cc_id = str(payload.get("spec_cost_center_id", "CC-4820"))
  return (
      {"cost_center_associated": True},
      f"Associated tenant '{alias}' with FinOps cost center '{cc_id}'.",
  )


def package_catalog_pr(
    payload: dict[str, Any], dry_run: bool = False, **kwargs: Any
) -> tuple[dict[str, Any], str]:
  """Opens the final GitOps PR registering the tenant in the service catalog."""
  del kwargs
  alias = str(payload.get("alias", "payments-eu"))
  return _open_mock_pr(
      payload,
      "catalog_pr_id",
      105,
      f"feat(catalog): finalize active tenant registration for {alias}",
      dry_run,
  )
