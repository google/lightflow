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

"""Mocked cloud-native actions for the 22-stage GitOps tenant onboarding DAG.

All control-plane systems (change-advisory ticketing, Identity Provider groups,
SCIM 2.0 push, Cloud IAM OIDC Workload Identity federation, AWS KMS / Vault /
Terraform state storage, GitOps pull requests, and ArgoCD application sync)
read and mutate a local JSON state ledger (`control_plane_state.json`) inside
`sandbox_dir` so the entire multi-branch enterprise pipeline runs hermetically
in under 2 seconds without cloud credentials.
"""

from __future__ import annotations

import json
import os
import shutil
import tempfile
from typing import Any


def _resolve_sandbox(payload: dict[str, Any]) -> str:
  """Returns the sandbox directory for the current onboarding run."""
  init_out = payload.get("outputs", {}).get("init_spec", {})
  sandbox = str(
      init_out.get("sandbox_dir") or payload.get("sandbox_dir") or ""
  ).strip()
  if not sandbox or sandbox.startswith("<mock_"):
    alias = (
        str(
            init_out.get("alias") or payload.get("alias") or "payments-eu"
        ).strip()
        or "payments-eu"
    )
    sandbox = os.path.join(
        tempfile.gettempdir(), f"lightflow_tenant_onboarding_{alias}"
    )
  return os.path.abspath(os.path.expanduser(sandbox))


def _resolve_alias(payload: dict[str, Any]) -> str:
  init_out = payload.get("outputs", {}).get("init_spec", {})
  return (
      str(
          init_out.get("alias") or payload.get("alias") or "payments-eu"
      ).strip()
      or "payments-eu"
  )


def _state_file(sandbox: str) -> str:
  return os.path.join(sandbox, "control_plane_state.json")


def _load_state(sandbox: str) -> dict[str, Any]:
  path = _state_file(sandbox)
  if os.path.isfile(path):
    with open(path, "r", encoding="utf-8") as f:
      return json.load(f)
  return {
      "prs": {},
      "argocd_apps": {},
      "groups": [],
      "admins": [],
      "scim_status": "UNINITIALIZED",
      "iam_role": {},
      "kms": {},
      "cloud_artifacts": {},
      "checkpoints": [],
      "files": [],
  }


def _save_state(sandbox: str, state: dict[str, Any]) -> None:
  os.makedirs(sandbox, exist_ok=True)
  with open(_state_file(sandbox), "w", encoding="utf-8") as f:
    json.dump(state, f, indent=2, sort_keys=True)


def initialize_spec(
    payload: dict[str, Any], dry_run: bool = False, **kwargs: Any
) -> tuple[dict[str, Any], str]:
  """Normalizes tenant onboarding parameters and resets the local sandbox."""
  del kwargs
  alias = str(payload.get("alias", "payments-eu")).strip() or "payments-eu"
  display_name = (
      str(payload.get("display_name", "Payments EU Checkout")).strip()
      or "Payments EU Checkout"
  )
  idp_group_prefix = (
      str(payload.get("idp_group_prefix", f"idp-{alias}")).strip()
      or f"idp-{alias}"
  )
  primary_admin = (
      str(payload.get("primary_admin", "alice@example.com")).strip()
      or "alice@example.com"
  )
  secondary_admin = (
      str(payload.get("secondary_admin", "bob@example.com")).strip()
      or "bob@example.com"
  )
  cost_center_id = (
      str(payload.get("cost_center_id", "CC-4820")).strip() or "CC-4820"
  )
  compliance_tier = (
      str(payload.get("compliance_tier", "pci")).strip().lower() or "pci"
  )
  sandbox_dir = _resolve_sandbox({**payload, "alias": alias})

  out = {
      "alias": alias,
      "display_name": display_name,
      "k8s_namespace": f"tenant-{alias}",
      "idp_group_prefix": idp_group_prefix,
      "primary_admin": primary_admin,
      "secondary_admin": secondary_admin,
      "cost_center_id": cost_center_id,
      "compliance_tier": compliance_tier,
      "sandbox_dir": sandbox_dir,
  }
  if dry_run:
    return (
        out,
        (
            f"[DRY RUN] Would initialize tenant spec for '{alias}'"
            f" (tier={compliance_tier}) in '{sandbox_dir}'."
        ),
    )

  shutil.rmtree(sandbox_dir, ignore_errors=True)
  state = _load_state(sandbox_dir)
  state["spec"] = dict(out)
  _save_state(sandbox_dir, state)
  return (
      out,
      (
          f"Initialized tenant specification for '{alias}' ({display_name},"
          f" tier={compliance_tier})."
      ),
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
  """Creates a change-management onboarding request ticket in local state."""
  del kwargs
  alias = _resolve_alias(payload)
  code = sum(ord(ch) * (idx + 1) for idx, ch in enumerate(alias)) % 9000 + 1000
  ticket_id = f"CAB-{code}"
  if dry_run:
    return (
        {"ticket_id": ticket_id, "ticket_status": "PENDING_CAB"},
        f"[DRY RUN] Would create change-advisory ticket {ticket_id}.",
    )
  sandbox = _resolve_sandbox(payload)
  state = _load_state(sandbox)
  state["ticket"] = {"ticket_id": ticket_id, "status": "PENDING_CAB"}
  _save_state(sandbox, state)
  return (
      {"ticket_id": ticket_id, "ticket_status": "PENDING_CAB"},
      f"Created change-advisory onboarding ticket {ticket_id}.",
  )


def stop_non_admin(
    payload: dict[str, Any], dry_run: bool = False, **kwargs: Any
) -> tuple[dict[str, Any], str]:
  """Records early ticket handoff when initiated by a non-admin requester."""
  del dry_run, kwargs
  ticket_id = str(
      payload.get("outputs", {})
      .get("create_ticket", {})
      .get("ticket_id", "CAB-1000")
  )
  return (
      {"non_admin_handoff": True, "ticket_id": ticket_id},
      (
          f"Non-admin request recorded under {ticket_id}; platform admins will"
          " complete infrastructure actuation."
      ),
  )


def check_governance_approval(
    payload: dict[str, Any], dry_run: bool = False, **kwargs: Any
) -> tuple[dict[str, Any], str]:
  """Polls change-advisory board approval and transitions ticket status."""
  del kwargs
  ticket_id = str(
      payload.get("outputs", {})
      .get("create_ticket", {})
      .get("ticket_id", "CAB-1000")
  )
  if dry_run:
    return (
        {"governance_approved": True, "ticket_id": ticket_id},
        f"Change-advisory board approved ticket {ticket_id}.",
    )
  sandbox = _resolve_sandbox(payload)
  state = _load_state(sandbox)
  ticket = state.setdefault(
      "ticket", {"ticket_id": ticket_id, "status": "PENDING_CAB"}
  )
  ticket["status"] = "APPROVED"
  _save_state(sandbox, state)
  return (
      {"governance_approved": True, "ticket_id": ticket["ticket_id"]},
      f"Change-advisory board approved ticket {ticket['ticket_id']}.",
  )


def provision_identity_groups(
    payload: dict[str, Any], dry_run: bool = False, **kwargs: Any
) -> tuple[dict[str, Any], str]:
  """Creates IdP groups, binds tenant admins, and initiates SCIM push."""
  del kwargs
  init_out = payload.get("outputs", {}).get("init_spec", {})
  prefix = str(init_out.get("idp_group_prefix") or "idp-payments-eu")
  admins = [
      str(init_out.get("primary_admin") or "alice@example.com"),
      str(init_out.get("secondary_admin") or "bob@example.com"),
  ]
  groups = [f"{prefix}-admins", f"{prefix}-devs", f"{prefix}-workloads"]
  out = {
      "created_groups": groups,
      "bound_admins": admins,
      "scim_push_initiated": True,
  }
  if dry_run:
    return (
        out,
        (
            f"[DRY RUN] Would create IdP groups {', '.join(groups)} and bind"
            f" {', '.join(admins)}."
        ),
    )
  sandbox = _resolve_sandbox(payload)
  state = _load_state(sandbox)
  state["groups"] = groups
  state["admins"] = admins
  state["scim_status"] = "SYNCING"
  _save_state(sandbox, state)
  return (
      out,
      (
          f"Provisioned IdP groups ({', '.join(groups)}), bound admins"
          f" ({', '.join(admins)}), and initiated SCIM push."
      ),
  )


def check_scim_ready(
    payload: dict[str, Any], dry_run: bool = False, **kwargs: Any
) -> tuple[dict[str, Any], str]:
  """Polls SCIM directory push status and transitions SYNCING -> SYNCED."""
  del kwargs
  init_out = payload.get("outputs", {}).get("init_spec", {})
  prefix = str(init_out.get("idp_group_prefix") or "idp-payments-eu")
  if dry_run:
    return (
        {"scim_ready": True, "synced_group_count": 3},
        f"SCIM 2.0 push converged for '{prefix}' (3 groups synced).",
    )
  sandbox = _resolve_sandbox(payload)
  state = _load_state(sandbox)
  groups = state.get("groups", [])
  if not groups:
    raise RuntimeError("SCIM sync failed: no IdP groups found in state.")
  state["scim_status"] = "SYNCED"
  _save_state(sandbox, state)
  return (
      {"scim_ready": True, "synced_group_count": len(groups)},
      f"SCIM 2.0 push converged for '{prefix}' ({len(groups)} groups synced).",
  )


def create_workload_iam_role(
    payload: dict[str, Any], dry_run: bool = False, **kwargs: Any
) -> tuple[dict[str, Any], str]:
  """Creates the Cloud IAM OIDC Workload Identity Role for the tenant."""
  # Supports simulated outcomes via `iam_creation_mode` or
  # `require_manual_quota_override` in payload:
  # - 'QUOTA_EXCEEDED' (when require_manual_quota_override=True): triggers
  #   Gate #2 (prompt_iam_quota_override)
  # - 'SUCCESS' (default): auto-provisions the IAM role without human action
  # - 'FAILED': triggers fail_on_iam_failed
  del kwargs
  alias = _resolve_alias(payload)
  role_arn = f"arn:aws:iam::123456789012:role/tenant-{alias}-workload"
  mode = str(payload.get("iam_creation_mode", "")).strip().upper()
  if not mode:
    if payload.get("require_manual_quota_override", False):
      mode = "QUOTA_EXCEEDED"
    else:
      mode = "SUCCESS"
  if mode not in ("SUCCESS", "QUOTA_EXCEEDED", "FAILED"):
    mode = "SUCCESS"

  if not dry_run:
    sandbox = _resolve_sandbox(payload)
    state = _load_state(sandbox)
    state["iam_role"] = {
        "role_arn": role_arn,
        "status": "ACTIVE" if mode == "SUCCESS" else mode,
        "oidc_subject": (
            f"system:serviceaccount:tenant-{alias}:{alias}-workload"
        ),
    }
    _save_state(sandbox, state)

  return (
      {"iam_status": mode, "iam_role_arn": role_arn},
      f"Cloud IAM Workload Identity role '{role_arn}' status: {mode}.",
  )


def fail_workflow(
    payload: dict[str, Any], dry_run: bool = False, **kwargs: Any
) -> tuple[dict[str, Any], str]:
  """Raises a terminal error when Cloud IAM role creation fails."""
  del dry_run, kwargs
  role_arn = str(
      payload.get("outputs", {})
      .get("create_workload_iam_role", {})
      .get("iam_role_arn", "unknown-role")
  )
  raise RuntimeError(
      f"Cloud IAM role creation failed terminally for '{role_arn}'."
  )


def check_iam_role_ready(
    payload: dict[str, Any], dry_run: bool = False, **kwargs: Any
) -> tuple[dict[str, Any], str]:
  """Polls until the Cloud IAM OIDC Workload Identity role is active."""
  del kwargs
  iam_out = payload.get("outputs", {}).get("create_workload_iam_role", {})
  gate_out = payload.get("outputs", {}).get("prompt_iam_quota_override", {})
  role_arn = str(
      iam_out.get("iam_role_arn")
      or "arn:aws:iam::123456789012:role/tenant-payments-eu-workload"
  )
  via = (
      "quota override approval"
      if gate_out.get("quota_override_approved")
      else "automated provisioning"
  )
  if not dry_run:
    sandbox = _resolve_sandbox(payload)
    state = _load_state(sandbox)
    state.setdefault("iam_role", {})["status"] = "ACTIVE"
    _save_state(sandbox, state)
  return (
      {"iam_role_ready": True, "iam_role_arn": role_arn},
      f"Cloud IAM role '{role_arn}' OIDC trust policy is active (via {via}).",
  )


def provision_external_resources(
    payload: dict[str, Any], dry_run: bool = False, **kwargs: Any
) -> tuple[dict[str, Any], str]:
  """Provisions the tenant Terraform state bucket and Vault KV mount."""
  del kwargs
  alias = _resolve_alias(payload)
  bucket_uri = f"s3://platform-tfstate-{alias}"
  vault_mount = f"secret/tenants/{alias}"
  out = {
      "storage_provisioned": True,
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

  if payload.get("simulate_artifact_failure", False) or payload.get(
      "simulate_storage_failure", False
  ):
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
  """Compensating rollback action that cleans up partial cloud artifacts."""
  del dry_run, kwargs
  sandbox = _resolve_sandbox(payload)
  state = _load_state(sandbox)
  state["cloud_artifacts"] = {}
  _save_state(sandbox, state)
  return (
      {"cloud_artifacts_rolled_back": True},
      "Rolled back partial Terraform state bucket and Vault mount artifacts.",
  )


def provision_dedicated_kms_key(
    payload: dict[str, Any], dry_run: bool = False, **kwargs: Any
) -> tuple[dict[str, Any], str]:
  """Provisions a dedicated HSM-backed KMS CMK for PCI-regulated tenants."""
  del kwargs
  alias = _resolve_alias(payload)
  kms_key_arn = f"arn:aws:kms:eu-west-1:123456789012:key/cmk-pci-{alias}"
  out = {
      "kms_ready": True,
      "kms_mode": "dedicated_hsm_cmk",
      "kms_key_arn": kms_key_arn,
  }
  if dry_run:
    return (
        out,
        f"[DRY RUN] Would provision dedicated PCI HSM KMS key '{kms_key_arn}'.",
    )
  sandbox = _resolve_sandbox(payload)
  state = _load_state(sandbox)
  state["kms"] = dict(out)
  _save_state(sandbox, state)
  return (
      out,
      f"Provisioned dedicated PCI HSM KMS key '{kms_key_arn}'.",
  )


def apply_shared_kms_policy(
    payload: dict[str, Any], dry_run: bool = False, **kwargs: Any
) -> tuple[dict[str, Any], str]:
  """Binds non-PCI standard tenants to the shared cluster KMS encryption key."""
  del kwargs
  alias = _resolve_alias(payload)
  kms_key_arn = "arn:aws:kms:eu-west-1:123456789012:key/cmk-shared-cluster"
  out = {
      "kms_ready": True,
      "kms_mode": "shared_cluster_key",
      "kms_key_arn": kms_key_arn,
  }
  if dry_run:
    return (
        out,
        f"[DRY RUN] Would bind '{alias}' to shared KMS key '{kms_key_arn}'.",
    )
  sandbox = _resolve_sandbox(payload)
  state = _load_state(sandbox)
  state["kms"] = dict(out)
  _save_state(sandbox, state)
  return (
      out,
      f"Bound standard tenant '{alias}' to shared KMS key '{kms_key_arn}'.",
  )


def checkpoint_registry(
    payload: dict[str, Any], dry_run: bool = False, **kwargs: Any
) -> tuple[dict[str, Any], str]:
  """Joins the parallel storage + KMS tracks and checkpoints the catalog."""
  del kwargs
  alias = _resolve_alias(payload)
  ticket_id = str(
      payload.get("outputs", {})
      .get("create_ticket", {})
      .get("ticket_id", "CAB-1000")
  )
  ext_out = payload.get("outputs", {}).get("provision_external_resources", {})
  pci_kms = payload.get("outputs", {}).get("provision_dedicated_kms_key", {})
  std_kms = payload.get("outputs", {}).get("apply_shared_kms_policy", {})
  kms_arn = str(
      pci_kms.get("kms_key_arn")
      or std_kms.get("kms_key_arn")
      or "arn:aws:kms:eu-west-1:123456789012:key/cmk-pci-payments-eu"
  )
  kms_mode = str(
      pci_kms.get("kms_mode") or std_kms.get("kms_mode") or "dedicated_hsm_cmk"
  )
  out = {
      "catalog_checkpointed": True,
      "kms_key_arn": kms_arn,
      "kms_mode": kms_mode,
  }
  if dry_run:
    return (
        out,
        (
            f"[DRY RUN] Would checkpoint backing resources for '{alias}'"
            f" (kms={kms_mode})."
        ),
    )
  sandbox = _resolve_sandbox(payload)
  state = _load_state(sandbox)
  checkpoints = list(state.get("checkpoints", []))
  checkpoints.append({
      "alias": alias,
      "ticket_id": ticket_id,
      "state_bucket_uri": ext_out.get("state_bucket_uri"),
      "vault_mount_path": ext_out.get("vault_mount_path"),
      "kms_key_arn": kms_arn,
      "kms_mode": kms_mode,
  })
  state["checkpoints"] = checkpoints
  _save_state(sandbox, state)
  return (
      out,
      (
          f"Checkpointed backing storage and {kms_mode} encryption for"
          f" '{alias}' in platform catalog."
      ),
  )


def _open_mock_pr(
    payload: dict[str, Any],
    pr_key: str,
    pr_number: int,
    title: str,
    files: list[str],
    dry_run: bool,
) -> tuple[dict[str, Any], str]:
  """Opens a simulated GitOps pull request in OPEN state with file list."""
  pr_id = f"PR-{pr_number}"
  if dry_run:
    return (
        {pr_key: pr_id, "pr_id": pr_id, "files": files},
        f"[DRY RUN] Would open GitOps {pr_id} ({title}).",
    )
  sandbox = _resolve_sandbox(payload)
  state = _load_state(sandbox)
  tracked = state.setdefault("files", [])
  for rel in files:
    if rel not in tracked:
      tracked.append(rel)
  state.setdefault("prs", {})[pr_id] = {
      "title": title,
      "status": "OPEN",
      "files": files,
  }
  _save_state(sandbox, state)
  return (
      {pr_key: pr_id, "pr_id": pr_id, "files": files},
      f"Opened GitOps {pr_id} ({len(files)} files): '{title}'.",
  )


def scaffold_k8s_gitops_pr(
    payload: dict[str, Any], dry_run: bool = False, **kwargs: Any
) -> tuple[dict[str, Any], str]:
  """Writes tenant K8s overlay manifests, updates kustomize, and opens PR-101."""
  del kwargs
  alias = _resolve_alias(payload)
  iam_arn = str(
      payload.get("outputs", {})
      .get("wait_iam_role_ready", {})
      .get(
          "iam_role_arn",
          f"arn:aws:iam::123456789012:role/tenant-{alias}-workload",
      )
  )
  kms_arn = str(
      payload.get("outputs", {})
      .get("checkpoint_catalog", {})
      .get("kms_key_arn", "")
  )
  ns_dir = f"gitops/tenants/{alias}"
  kustomize_rel = "gitops/tenants/kustomization.yaml"
  manifest_files = [
      f"{ns_dir}/namespace.yaml",
      f"{ns_dir}/networkpolicy.yaml",
      f"{ns_dir}/resourcequota.yaml",
      f"{ns_dir}/serviceaccount.yaml",
      f"{ns_dir}/cronjob-rbac.yaml",
      kustomize_rel,
  ]
  if not dry_run:
    sandbox = _resolve_sandbox(payload)
    full_dir = os.path.join(sandbox, ns_dir)
    os.makedirs(full_dir, exist_ok=True)
    for fname in (
        "namespace.yaml",
        "networkpolicy.yaml",
        "resourcequota.yaml",
        "cronjob-rbac.yaml",
    ):
      with open(os.path.join(full_dir, fname), "w", encoding="utf-8") as f:
        f.write(f"# {fname} for tenant {alias}\n")
    with open(
        os.path.join(full_dir, "serviceaccount.yaml"), "w", encoding="utf-8"
    ) as f:
      f.write(
          "apiVersion: v1\nkind: ServiceAccount\nmetadata:\n"
          f"  name: {alias}-workload\n  namespace: tenant-{alias}\n"
          "  annotations:\n"
          f"    eks.amazonaws.com/role-arn: {iam_arn}\n"
          f"    platform.example.io/kms-key-arn: {kms_arn}\n"
      )
    kust_full = os.path.join(sandbox, kustomize_rel)
    os.makedirs(os.path.dirname(kust_full), exist_ok=True)
    entry = f"- ./{alias}\n"
    existing = ""
    if os.path.isfile(kust_full):
      with open(kust_full, "r", encoding="utf-8") as f:
        existing = f.read()
    if entry not in existing:
      with open(kust_full, "a", encoding="utf-8") as f:
        f.write(entry)

  return _open_mock_pr(
      payload,
      "k8s_pr_id",
      101,
      f"feat(k8s): scaffold namespace, ServiceAccount, and quotas for {alias}",
      manifest_files,
      dry_run,
  )


def package_mesh_and_catalog_pr(
    payload: dict[str, Any], dry_run: bool = False, **kwargs: Any
) -> tuple[dict[str, Any], str]:
  """Writes Gateway API route and FinOps catalog manifest and opens PR-102."""
  del kwargs
  alias = _resolve_alias(payload)
  init_out = payload.get("outputs", {}).get("init_spec", {})
  ext_out = payload.get("outputs", {}).get("provision_external_resources", {})
  ckpt_out = payload.get("outputs", {}).get("checkpoint_catalog", {})
  cost_center = str(init_out.get("cost_center_id") or "CC-4820")
  route_rel = f"gitops/mesh/httproutes/{alias}.yaml"
  catalog_rel = f"gitops/catalog/tenants/{alias}.yaml"

  if not dry_run:
    sandbox = _resolve_sandbox(payload)
    route_full = os.path.join(sandbox, route_rel)
    cat_full = os.path.join(sandbox, catalog_rel)
    os.makedirs(os.path.dirname(route_full), exist_ok=True)
    os.makedirs(os.path.dirname(cat_full), exist_ok=True)
    with open(route_full, "w", encoding="utf-8") as f:
      f.write(
          "apiVersion: gateway.networking.k8s.io/v1\nkind: HTTPRoute\n"
          f"metadata:\n  name: {alias}-ingress\n"
      )
    with open(cat_full, "w", encoding="utf-8") as f:
      f.write(
          "apiVersion: catalog.example.io/v1\nkind: TenantRegistration\n"
          f"metadata:\n  name: {alias}\nspec:\n"
          f"  costCenter: {cost_center}\n"
          f"  stateBucket: {ext_out.get('state_bucket_uri', '')}\n"
          f"  vaultMount: {ext_out.get('vault_mount_path', '')}\n"
          f"  kmsMode: {ckpt_out.get('kms_mode', '')}\n"
      )

  return _open_mock_pr(
      payload,
      "catalog_pr_id",
      102,
      (
          f"feat(mesh,catalog): wire {alias} ingress route and FinOps cost"
          f" center {cost_center}"
      ),
      [route_rel, catalog_rel],
      dry_run,
  )


def check_pr_merged(
    payload: dict[str, Any],
    target_stage: str = "scaffold_k8s_gitops_pr",
    target_pr_key: str = "k8s_pr_id",
    dry_run: bool = False,
    **kwargs: Any,
) -> tuple[dict[str, Any], str]:
  """Verifies the target GitOps PR in state and transitions OPEN -> MERGED."""
  del kwargs
  stage_out = payload.get("outputs", {}).get(target_stage, {})
  pr_id = str(stage_out.get(target_pr_key) or "PR-100")
  if dry_run:
    return (
        {"pr_id": pr_id, "pr_merged": True},
        f"GitOps pull request {pr_id} ({target_pr_key}) merged into main.",
    )
  sandbox = _resolve_sandbox(payload)
  state = _load_state(sandbox)
  pr_record = state.get("prs", {}).get(pr_id)
  if not pr_record:
    raise RuntimeError(f"GitOps PR '{pr_id}' not found in control-plane state.")
  pr_record["status"] = "MERGED"
  _save_state(sandbox, state)
  return (
      {"pr_id": pr_id, "pr_merged": True},
      f"GitOps pull request {pr_id} ({target_pr_key}) merged into main.",
  )


def check_argocd_synced(
    payload: dict[str, Any],
    app_name: str = "tenant-namespace",
    required_pr_stage: str = "wait_k8s_gitops_pr",
    dry_run: bool = False,
    **kwargs: Any,
) -> tuple[dict[str, Any], str]:
  """Polls until ArgoCD reconciles the merged GitOps commit into the cluster."""
  del kwargs
  alias = _resolve_alias(payload)
  pr_out = payload.get("outputs", {}).get(required_pr_stage, {})
  pr_id = str(pr_out.get("pr_id") or "PR-100")
  if dry_run:
    return (
        {"app_name": app_name, "app_synced": True},
        (
            f"ArgoCD application '{app_name}' ({alias}) is Synced & Healthy"
            f" at {pr_id}."
        ),
    )
  sandbox = _resolve_sandbox(payload)
  state = _load_state(sandbox)
  pr_status = state.get("prs", {}).get(pr_id, {}).get("status")
  if pr_status != "MERGED":
    raise RuntimeError(
        f"ArgoCD cannot sync '{app_name}': {pr_id} is {pr_status!r}."
    )
  state.setdefault("argocd_apps", {})[app_name] = {
      "status": "Synced",
      "health": "Healthy",
      "synced_pr": pr_id,
  }
  _save_state(sandbox, state)
  return (
      {"app_name": app_name, "app_synced": True},
      (
          f"ArgoCD application '{app_name}' ({alias}) is Synced & Healthy"
          f" at {pr_id}."
      ),
  )
