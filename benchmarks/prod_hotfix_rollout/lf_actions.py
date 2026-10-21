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

"""Lightflow Python Action Adapters for Production Hotfix Rollout.

Each function extracts the upstream stage outputs from `payload["outputs"]`
(and operator checkpoint parameters) and delegates to `ops_steps.step_*`.
"""

from __future__ import annotations

# Benchmark material: the step functions are specified by the runbook the
# agents read, so per-function docstrings are intentionally omitted.
# pylint: disable=missing-function-docstring

from typing import Any

import ops_steps


def ingest_incident_telemetry(
    payload: dict[str, Any],
) -> tuple[dict[str, Any], str]:
  del payload
  res = ops_steps.step_ingest_incident_telemetry()
  msg = (
      f"Ingested incident {res['incident_id']} in {res['target_region']} "
      f"({res['active_shard_count']} active shards)."
  )
  return res, msg


def verify_freeze_window(payload: dict[str, Any]) -> tuple[dict[str, Any], str]:
  outputs = payload.get("outputs", {})
  inc_id = outputs["ingest_incident_telemetry"]["incident_id"]
  res = ops_steps.step_verify_freeze_window(inc_id)
  return res, f"Verified freeze window token {res['freeze_token']}."


def reserve_failover_capacity(
    payload: dict[str, Any],
) -> tuple[dict[str, Any], str]:
  outputs = payload.get("outputs", {})
  freeze_tok = outputs["verify_freeze_window"]["freeze_token"]
  res = ops_steps.step_reserve_failover_capacity(freeze_tok)
  return res, f"Reserved failover capacity {res['reservation_id']}."


def compute_blast_radius(payload: dict[str, Any]) -> tuple[dict[str, Any], str]:
  outputs = payload.get("outputs", {})
  inc_id = outputs["ingest_incident_telemetry"]["incident_id"]
  res = ops_steps.step_compute_blast_radius(inc_id)
  msg = (
      f"Blast radius snapshot {res['snapshot_id']}: "
      f"eu_regulated_tenants={res['eu_regulated_tenants']}, "
      f"payment_path_touched={res['payment_path_touched']}, "
      f"arr_at_risk_usd={res['arr_at_risk_usd']}."
  )
  return res, msg


def run_deep_compliance_audit(
    payload: dict[str, Any],
) -> tuple[dict[str, Any], str]:
  outputs = payload.get("outputs", {})
  snap_id = outputs["compute_blast_radius"]["snapshot_id"]
  res = ops_steps.step_run_deep_compliance_audit(snap_id)
  return res, f"Deep compliance audit complete ({res['compliance_hash']})."


def run_standard_preflight(
    payload: dict[str, Any],
) -> tuple[dict[str, Any], str]:
  outputs = payload.get("outputs", {})
  snap_id = outputs["compute_blast_radius"]["snapshot_id"]
  res = ops_steps.step_run_standard_preflight(snap_id)
  return res, f"Standard preflight complete ({res['compliance_hash']})."


def stage_canary_artifact(
    payload: dict[str, Any],
) -> tuple[dict[str, Any], str]:
  outputs = payload.get("outputs", {})
  freeze_tok = outputs["verify_freeze_window"]["freeze_token"]
  rsv_id = outputs["reserve_failover_capacity"]["reservation_id"]
  snap_id = outputs["compute_blast_radius"]["snapshot_id"]
  comp_out = (
      outputs.get("run_deep_compliance_audit")
      or outputs.get("run_standard_preflight")
      or {}
  )
  comp_hash = comp_out["compliance_hash"]
  res = ops_steps.step_stage_canary_artifact(
      freeze_tok, rsv_id, snap_id, comp_hash
  )
  return res, f"Staged canary artifact ({res['manifest_sha256']})."


def apply_db_schema_migration(
    payload: dict[str, Any],
) -> tuple[dict[str, Any], str]:
  outputs = payload.get("outputs", {})
  manifest_sha = outputs["stage_canary_artifact"]["manifest_sha256"]
  gate1 = outputs["gate_1_rollout_plan"]
  res = ops_steps.step_apply_db_schema_migration(
      manifest_sha256=manifest_sha,
      reviewer_ticket=str(gate1["reviewer_ticket"]),
      max_batch_size=int(gate1["max_batch_size"]),
      soak_window_minutes=int(gate1["soak_window_minutes"]),
  )
  return res, f"Applied DB schema migration {res['migration_id']}."


def rollback_staged_artifact(
    payload: dict[str, Any],
) -> tuple[dict[str, Any], str]:
  outputs = payload.get("outputs", {})
  manifest_sha = outputs.get("stage_canary_artifact", {}).get(
      "manifest_sha256", ""
  )
  res = ops_steps.step_rollback_staged_artifact(manifest_sha)
  return res, f"Rolled back staged artifact lock to {res['staging_lock']}."


def deploy_canary_region(payload: dict[str, Any]) -> tuple[dict[str, Any], str]:
  outputs = payload.get("outputs", {})
  manifest_sha = outputs["stage_canary_artifact"]["manifest_sha256"]
  mig_id = outputs["apply_db_schema_migration"]["migration_id"]
  exclude_flaky = bool(payload.get("exclude_flaky_node", False))
  res = ops_steps.step_deploy_canary_region(
      manifest_sha256=manifest_sha,
      migration_id=mig_id,
      exclude_flaky_node=exclude_flaky,
  )
  return (
      res,
      (
          f"Deployed canary {res['canary_deployment_id']}"
          f" (excluded={res['excluded_node']})."
      ),
  )


def verify_canary_slo_metrics(
    payload: dict[str, Any],
) -> tuple[dict[str, Any], str]:
  outputs = payload.get("outputs", {})
  canary_id = outputs["deploy_canary_region"]["canary_deployment_id"]
  res = ops_steps.step_verify_canary_slo_metrics(canary_id)
  msg = (
      f"Canary SLO verified ({res['slo_digest']}): "
      f"p99={res['post_canary_p99_ms']}ms, "
      f"replica_lag={res['post_canary_replica_lag_ms']}ms."
  )
  return res, msg


def run_replica_catchup_drain(
    payload: dict[str, Any],
) -> tuple[dict[str, Any], str]:
  outputs = payload.get("outputs", {})
  canary_id = outputs["deploy_canary_region"]["canary_deployment_id"]
  slo_digest = outputs["verify_canary_slo_metrics"]["slo_digest"]
  res = ops_steps.step_run_replica_catchup_drain(canary_id, slo_digest)
  return res, f"Replica catchup drain complete ({res['drain_token']})."


def dispatch_global_and_notify(
    payload: dict[str, Any],
) -> tuple[dict[str, Any], str]:
  outputs = payload.get("outputs", {})
  freeze_tok = outputs["verify_freeze_window"]["freeze_token"]
  rsv_id = outputs["reserve_failover_capacity"]["reservation_id"]
  snap_id = outputs["compute_blast_radius"]["snapshot_id"]
  comp_out = (
      outputs.get("run_deep_compliance_audit")
      or outputs.get("run_standard_preflight")
      or {}
  )
  comp_hash = comp_out["compliance_hash"]
  manifest_sha = outputs["stage_canary_artifact"]["manifest_sha256"]
  gate1 = outputs["gate_1_rollout_plan"]
  slo_digest = outputs["verify_canary_slo_metrics"]["slo_digest"]
  drain_out = outputs.get("run_replica_catchup_drain") or {}
  drain_tok = drain_out.get("drain_token", "NONE")
  gate2 = outputs["gate_2_customer_advisory"]
  batch_cap = int(gate2.get("override_batch_cap") or gate1["max_batch_size"])
  signoff = str(gate2["signoff_code"])

  res = ops_steps.step_dispatch_global_and_notify(
      freeze_token=freeze_tok,
      reservation_id=rsv_id,
      snapshot_id=snap_id,
      compliance_hash=comp_hash,
      manifest_sha256=manifest_sha,
      reviewer_ticket=str(gate1["reviewer_ticket"]),
      slo_digest=slo_digest,
      batch_cap=batch_cap,
      signoff_code=signoff,
      drain_token=drain_tok,
  )
  return (
      res,
      (
          f"Global release {res['global_release_id']} dispatched"
          f" (batch_cap={batch_cap})."
      ),
  )


def finalize_postmortem_and_cleanup(
    payload: dict[str, Any],
) -> tuple[dict[str, Any], str]:
  outputs = payload.get("outputs", {})
  rel_id = outputs["dispatch_global_and_notify"]["global_release_id"]
  snap_id = outputs["compute_blast_radius"]["snapshot_id"]
  res = ops_steps.step_finalize_postmortem_and_cleanup(rel_id, snap_id)
  return res, f"Cleanup complete ({res['completion_certificate']})."
