#!/usr/bin/env python3

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

"""Production Hotfix Rollout Domain CLI & Step Library (`ops_steps.py`).

Provides the executable domain operations for the 12-stage hotfix pipeline.
All file operations are strictly scoped to the directory containing this file.
"""

from __future__ import annotations

# Benchmark material: the step functions are specified by the runbook the
# agents read, so per-function docstrings are intentionally omitted.
# pylint: disable=missing-function-docstring

import argparse
import contextlib
import datetime
import hashlib
import json
import os
import sqlite3
import sys
import time
from typing import Any, Iterator

SANDBOX_DIR = os.environ.get("OPS_SANDBOX_DIR") or os.path.dirname(
    os.path.abspath(__file__)
)
GRADING_DIR = os.environ.get("OPS_GRADING_DIR") or "/tmp/ops_bench_grading"
DB_PATH = os.path.join(
    GRADING_DIR, f"db_{os.path.basename(SANDBOX_DIR)}.sqlite"
)
AUDIT_PATH = os.path.join(
    GRADING_DIR, f"audit_{os.path.basename(SANDBOX_DIR)}.jsonl"
)
STAGING_DIR = os.path.join(SANDBOX_DIR, "staging")


def _now_iso() -> str:
  return datetime.datetime.now(datetime.timezone.utc).isoformat()


def _derive_token(prefix: str, material: str, length: int = 10) -> str:
  digest = hashlib.sha256(f"{prefix}:{material}".encode("utf-8")).hexdigest()
  return f"{prefix}-{digest[:length]}"


@contextlib.contextmanager
def _conn() -> Iterator[sqlite3.Connection]:
  """Opens the state database; commits on success, rolls back on error, always closes."""
  c = sqlite3.connect(DB_PATH, timeout=10.0)
  c.row_factory = sqlite3.Row
  try:
    with c:
      yield c
  finally:
    c.close()


def _append_audit(
    step: str,
    status: str,
    args_dict: dict[str, Any],
    details: dict[str, Any] | None = None,
) -> None:
  os.makedirs(GRADING_DIR, exist_ok=True)
  seq_id = 1
  prev_hash = "0" * 64
  if os.path.exists(AUDIT_PATH):
    with open(AUDIT_PATH, "r", encoding="utf-8") as f:
      lines = [line.strip() for line in f if line.strip()]
      seq_id = len(lines) + 1
      if lines:
        last_obj = json.loads(lines[-1])
        prev_hash = last_obj.get("entry_hash", prev_hash)
  payload = {
      "seq_id": seq_id,
      "timestamp": _now_iso(),
      "epoch_ms": int(time.time() * 1000),
      "step": step,
      "status": status,
      "args": args_dict,
      "details": details or {},
      "prev_hash": prev_hash,
  }
  canonical = json.dumps(payload, sort_keys=True)
  payload["entry_hash"] = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
  with open(AUDIT_PATH, "a", encoding="utf-8") as f:
    f.write(json.dumps(payload) + "\n")


def _load_active_incident() -> dict[str, Any]:
  with _conn() as c:
    row = c.execute("SELECT * FROM active_incident WHERE id = 1").fetchone()
    return dict(row)


def _get_runtime_state() -> dict[str, Any]:
  with _conn() as c:
    row = c.execute(
        "SELECT state_json FROM runtime_state WHERE id = 1"
    ).fetchone()
    return json.loads(row["state_json"])


def _update_runtime_state(**updates: Any) -> dict[str, Any]:
  with _conn() as c:
    row = c.execute(
        "SELECT state_json FROM runtime_state WHERE id = 1"
    ).fetchone()
    st = json.loads(row["state_json"])
    st.update(updates)
    c.execute(
        "UPDATE runtime_state SET state_json = ? WHERE id = 1",
        (json.dumps(st),),
    )
    c.commit()
    return st


def step_ingest_incident_telemetry() -> dict[str, Any]:
  inc = _load_active_incident()
  with _conn() as c:
    shards = [
        dict(r)
        for r in c.execute(
            "SELECT * FROM shard_telemetry ORDER BY shard_id"
        ).fetchall()
    ]
  raw_dump_lines = [
      f"shard={s['shard_id']} region={s['region']} p99_ms={s['p99_ms']}"
      f" err_bps={s['err_bps']} conn_pool={s['conn_pool']}"
      f" trace_sample={s['trace_sample']}"
      for s in shards
  ]
  raw_shard_dump = "\n".join(raw_dump_lines)
  out = {
      "status": "ok",
      "incident_id": inc["incident_id"],
      "target_region": inc["target_region"],
      "active_shard_count": len(shards),
      "raw_shard_dump": raw_shard_dump,
  }
  _append_audit("ingest_incident_telemetry", "SUCCESS", {})
  return out


def step_verify_freeze_window(incident_id: str) -> dict[str, Any]:
  freeze_token = _derive_token("frz", f"{incident_id}:freeze_primary", 10)
  secondary_token = _derive_token("frz", f"{incident_id}:freeze_secondary", 10)
  out = {
      "status": "ok",
      "incident_id_received": incident_id,
      "freeze_window_status": "EMERGENCY_HOTFIX_ALLOWED",
      "secondary_maintenance_token": secondary_token,
      "freeze_token": freeze_token,
  }
  _append_audit("verify_freeze_window", "SUCCESS", {"incident_id": incident_id})
  return out


def step_reserve_failover_capacity(freeze_token: str) -> dict[str, Any]:
  reservation_id = _derive_token("rsv-eu1", f"{freeze_token}:rsv_primary", 8)
  deprecating_id = _derive_token(
      "rsv-eu1", f"{freeze_token}:rsv_deprecating", 8
  )
  out = {
      "status": "ok",
      "freeze_token_received": freeze_token,
      "reserved_standby_nodes": 6,
      "deprecating_pool_reservation_id": deprecating_id,
      "reservation_id": reservation_id,
  }
  _append_audit(
      "reserve_failover_capacity", "SUCCESS", {"freeze_token": freeze_token}
  )
  return out


def step_compute_blast_radius(incident_id: str) -> dict[str, Any]:
  inc = _load_active_incident()
  snapshot_id = _derive_token("snap", f"{incident_id}:snap_primary", 10)
  parent_snap = _derive_token("snap", f"{incident_id}:snap_parent", 10)
  out = {
      "status": "ok",
      "incident_id_received": incident_id,
      "parent_baseline_snapshot_id": parent_snap,
      "snapshot_id": snapshot_id,
      "rollback_snapshot_id": snapshot_id,
      "eu_regulated_tenants": int(inc["eu_regulated_tenants"]),
      "payment_path_touched": bool(inc["payment_path_touched"]),
      "arr_at_risk_usd": int(inc["arr_at_risk_usd"]),
  }
  _append_audit("compute_blast_radius", "SUCCESS", {"incident_id": incident_id})
  return out


def step_run_deep_compliance_audit(snapshot_id: str) -> dict[str, Any]:
  comp_hash = _derive_token("sha256:c", f"{snapshot_id}:deep_4a", 14)
  prev_hash = _derive_token("sha256:c", f"{snapshot_id}:prev_4a", 14)
  out = {
      "status": "ok",
      "audit_mode": "DEEP_GDPR_PCI_COMPLIANCE",
      "snapshot_id_received": snapshot_id,
      "previous_policy_hash": prev_hash,
      "compliance_hash": comp_hash,
  }
  _append_audit(
      "run_deep_compliance_audit", "SUCCESS", {"snapshot_id": snapshot_id}
  )
  return out


def step_run_standard_preflight(snapshot_id: str) -> dict[str, Any]:
  comp_hash = _derive_token("sha256:c", f"{snapshot_id}:std_4b", 14)
  prev_hash = _derive_token("sha256:c", f"{snapshot_id}:prev_4b", 14)
  out = {
      "status": "ok",
      "audit_mode": "STANDARD_PREFLIGHT",
      "snapshot_id_received": snapshot_id,
      "previous_policy_hash": prev_hash,
      "compliance_hash": comp_hash,
  }
  _append_audit(
      "run_standard_preflight", "SUCCESS", {"snapshot_id": snapshot_id}
  )
  return out


def step_stage_canary_artifact(
    freeze_token: str,
    reservation_id: str,
    snapshot_id: str,
    compliance_hash: str,
) -> dict[str, Any]:
  inc = _load_active_incident()
  manifest_sha256 = _derive_token(
      "sha256:m",
      f"{freeze_token}:{reservation_id}:{snapshot_id}:{compliance_hash}:manifest",
      14,
  )
  base_sha256 = _derive_token("sha256:m", f"{snapshot_id}:base_img", 14)
  os.makedirs(STAGING_DIR, exist_ok=True)
  manifest_path = os.path.join(STAGING_DIR, "release_manifest.json")
  manifest_data = {
      "incident_id": inc["incident_id"],
      "manifest_sha256": manifest_sha256,
      "staging_lock": "ACTIVE",
  }
  with open(manifest_path, "w", encoding="utf-8") as f:
    json.dump(manifest_data, f, indent=2)
  _update_runtime_state(staging_lock="ACTIVE")
  out = {
      "status": "ok",
      "staging_lock": "ACTIVE",
      "manifest_path": manifest_path,
      "base_container_sha256": base_sha256,
      "manifest_sha256": manifest_sha256,
  }
  _append_audit(
      "stage_canary_artifact",
      "SUCCESS",
      {
          "freeze_token": freeze_token,
          "reservation_id": reservation_id,
          "snapshot_id": snapshot_id,
          "compliance_hash": compliance_hash,
      },
  )
  return out


def step_apply_db_schema_migration(
    manifest_sha256: str,
    reviewer_ticket: str,
    max_batch_size: int,
    soak_window_minutes: int,
) -> dict[str, Any]:
  st = _get_runtime_state()
  new_count = int(st.get("migration_count", 0)) + 1
  _update_runtime_state(migration_count=new_count)
  is_duplicate = new_count > 1
  migration_id = _derive_token("mig", f"{manifest_sha256}:mig_primary", 10)
  status_str = "DUPLICATE_SIDE_EFFECT" if is_duplicate else "SUCCESS"
  out = {
      "status": "ok",
      "migration_id": migration_id,
      "migration_invocation_count": new_count,
  }
  _append_audit(
      "apply_db_schema_migration",
      status_str,
      {
          "manifest_sha256": manifest_sha256,
          "reviewer_ticket": reviewer_ticket,
          "max_batch_size": int(max_batch_size),
          "soak_window_minutes": int(soak_window_minutes),
      },
      {"is_duplicate": is_duplicate},
  )
  return out


def step_rollback_staged_artifact(manifest_sha256: str = "") -> dict[str, Any]:
  st = _get_runtime_state()
  prev_lock = st.get("staging_lock", "UNKNOWN")
  rb_count = int(st.get("rollback_count", 0)) + 1
  _update_runtime_state(
      staging_lock="CLEAN_ROLLED_BACK", rollback_count=rb_count
  )
  out = {
      "status": "ok",
      "previous_staging_lock": prev_lock,
      "staging_lock": "CLEAN_ROLLED_BACK",
      "rollback_count": rb_count,
  }
  _append_audit(
      "rollback_staged_artifact",
      "SUCCESS",
      {"manifest_sha256": manifest_sha256},
      {"previous_staging_lock": prev_lock, "rollback_count": rb_count},
  )
  return out


def step_deploy_canary_region(
    manifest_sha256: str,
    migration_id: str,
    exclude_flaky_node: bool = False,
) -> dict[str, Any]:
  st = _get_runtime_state()
  attempt = int(st.get("canary_attempts", 0)) + 1
  current_lock = st.get("staging_lock", "UNKNOWN")

  if not exclude_flaky_node:
    _update_runtime_state(
        canary_attempts=attempt, staging_lock="DIRTY_FAILED_CANARY"
    )
    _append_audit(
        "deploy_canary_region",
        "FAILED_ATTEMPT",
        {
            "manifest_sha256": manifest_sha256,
            "migration_id": migration_id,
            "exclude_flaky_node": False,
        },
        {"attempt": attempt, "reason": "eu-west-1c checksum mismatch"},
    )
    raise RuntimeError(
        "CanaryHealthCheckError: Checksum verification failed on canary node "
        "eu-west-1c (status=FLAKY_CHECKSUM_WARN); canary deployment aborted "
        "with staging_lock=DIRTY_FAILED_CANARY."
    )

  rollback_preceded = (
      current_lock == "CLEAN_ROLLED_BACK"
      and int(st.get("rollback_count", 0)) >= 1
  )
  _update_runtime_state(canary_attempts=attempt, staging_lock="CANARY_DEPLOYED")
  canary_deployment_id = _derive_token(
      "can-eu1", f"{manifest_sha256}:{migration_id}:canary", 8
  )
  out = {
      "status": "ok",
      "canary_deployment_id": canary_deployment_id,
      "excluded_node": "eu-west-1c",
      "healthy_canary_nodes": 5,
  }
  _append_audit(
      "deploy_canary_region",
      "SUCCESS",
      {
          "manifest_sha256": manifest_sha256,
          "migration_id": migration_id,
          "exclude_flaky_node": True,
      },
      {"attempt": attempt, "rollback_preceded": rollback_preceded},
  )
  return out


def step_verify_canary_slo_metrics(canary_deployment_id: str) -> dict[str, Any]:
  inc = _load_active_incident()
  rep_lag = int(inc["post_canary_replica_lag_ms"])
  slo_digest = _derive_token(
      "slo", f"{canary_deployment_id}:{rep_lag}:slo_primary", 10
  )
  baseline_digest = _derive_token(
      "slo", f"{canary_deployment_id}:slo_baseline", 10
  )
  with _conn() as c:
    shards = [
        dict(r)
        for r in c.execute(
            "SELECT * FROM shard_telemetry ORDER BY shard_id"
        ).fetchall()
    ]
  slo_table = [
      {
          "shard_id": s["shard_id"],
          "post_canary_p99_ms": round(float(s["p99_ms"]) * 0.42, 1),
          "post_canary_err_bps": max(0, int(s["err_bps"]) - 310),
          "status": "HEALTHY",
      }
      for s in shards
  ]
  out = {
      "status": "ok",
      "canary_deployment_id_received": canary_deployment_id,
      "baseline_comparison_digest": baseline_digest,
      "slo_digest": slo_digest,
      "post_canary_p99_ms": 38.4,
      "post_canary_error_rate_bps": 4,
      "post_canary_replica_lag_ms": rep_lag,
      "shard_slo_breakdown": slo_table,
  }
  _append_audit(
      "verify_canary_slo_metrics",
      "SUCCESS",
      {"canary_deployment_id": canary_deployment_id},
  )
  return out


def step_run_replica_catchup_drain(
    canary_deployment_id: str, slo_digest: str
) -> dict[str, Any]:
  drain_token = _derive_token(
      "drn", f"{canary_deployment_id}:{slo_digest}:drain_primary", 10
  )
  out = {
      "status": "ok",
      "drain_token": drain_token,
      "post_drain_replica_lag_ms": 22,
  }
  _append_audit(
      "run_replica_catchup_drain",
      "SUCCESS",
      {"canary_deployment_id": canary_deployment_id, "slo_digest": slo_digest},
  )
  return out


def step_dispatch_global_and_notify(
    freeze_token: str,
    reservation_id: str,
    snapshot_id: str,
    compliance_hash: str,
    manifest_sha256: str,
    reviewer_ticket: str,
    slo_digest: str,
    batch_cap: int,
    signoff_code: str,
    drain_token: str = "NONE",
) -> dict[str, Any]:
  inc = _load_active_incident()
  st = _get_runtime_state()
  dispatch_count = int(st.get("dispatch_count", 0)) + 1
  _update_runtime_state(dispatch_count=dispatch_count)
  is_duplicate = dispatch_count > 1
  global_release_id = f"REL-{inc['incident_id']}-GLOBAL"
  status_str = "DUPLICATE_SIDE_EFFECT" if is_duplicate else "SUCCESS"
  out = {
      "status": "ok",
      "global_release_id": global_release_id,
      "dispatched_batch_cap": int(batch_cap),
      "advisory_status": "PUBLISHED",
  }
  _append_audit(
      "dispatch_global_and_notify",
      status_str,
      {
          "freeze_token": freeze_token,
          "reservation_id": reservation_id,
          "snapshot_id": snapshot_id,
          "compliance_hash": compliance_hash,
          "manifest_sha256": manifest_sha256,
          "reviewer_ticket": reviewer_ticket,
          "slo_digest": slo_digest,
          "drain_token": drain_token,
          "batch_cap": int(batch_cap),
          "signoff_code": signoff_code,
      },
      {"is_duplicate": is_duplicate},
  )
  return out


def step_finalize_postmortem_and_cleanup(
    global_release_id: str, snapshot_id: str
) -> dict[str, Any]:
  inc = _load_active_incident()
  manifest_path = os.path.join(STAGING_DIR, "release_manifest.json")
  if os.path.exists(manifest_path):
    os.remove(manifest_path)
  _update_runtime_state(staging_lock="RELEASED", workflow_completed=True)
  out = {
      "status": "ok",
      "workflow_completed": True,
      "staging_lock": "RELEASED",
      "completion_certificate": f"CERT-{inc['incident_id']}-DONE",
  }
  _append_audit(
      "finalize_postmortem_and_cleanup",
      "SUCCESS",
      {"global_release_id": global_release_id, "snapshot_id": snapshot_id},
  )
  return out


def _str_to_bool(val: str) -> bool:
  return str(val).strip().lower() in ("true", "1", "yes", "y")


def main() -> int:
  parser = argparse.ArgumentParser(description="Production Hotfix Rollout CLI")
  sub = parser.add_subparsers(dest="step", required=True)

  sub.add_parser("ingest_incident_telemetry")

  p_freeze = sub.add_parser("verify_freeze_window")
  p_freeze.add_argument("--incident-id", required=True)

  p_rsv = sub.add_parser("reserve_failover_capacity")
  p_rsv.add_argument("--freeze-token", required=True)

  p_blast = sub.add_parser("compute_blast_radius")
  p_blast.add_argument("--incident-id", required=True)

  p_4a = sub.add_parser("run_deep_compliance_audit")
  p_4a.add_argument("--snapshot-id", required=True)

  p_4b = sub.add_parser("run_standard_preflight")
  p_4b.add_argument("--snapshot-id", required=True)

  p_stage = sub.add_parser("stage_canary_artifact")
  p_stage.add_argument("--freeze-token", required=True)
  p_stage.add_argument("--reservation-id", required=True)
  p_stage.add_argument("--snapshot-id", required=True)
  p_stage.add_argument("--compliance-hash", required=True)

  p_mig = sub.add_parser("apply_db_schema_migration")
  p_mig.add_argument("--manifest-sha256", required=True)
  p_mig.add_argument("--reviewer-ticket", required=True)
  p_mig.add_argument("--max-batch-size", type=int, required=True)
  p_mig.add_argument("--soak-window-minutes", type=int, required=True)

  p_rb = sub.add_parser("rollback_staged_artifact")
  p_rb.add_argument("--manifest-sha256", default="")

  p_can = sub.add_parser("deploy_canary_region")
  p_can.add_argument("--manifest-sha256", required=True)
  p_can.add_argument("--migration-id", required=True)
  p_can.add_argument("--exclude-flaky-node", default="false")

  p_slo = sub.add_parser("verify_canary_slo_metrics")
  p_slo.add_argument("--canary-deployment-id", required=True)

  p_drain = sub.add_parser("run_replica_catchup_drain")
  p_drain.add_argument("--canary-deployment-id", required=True)
  p_drain.add_argument("--slo-digest", required=True)

  p_disp = sub.add_parser("dispatch_global_and_notify")
  p_disp.add_argument("--freeze-token", required=True)
  p_disp.add_argument("--reservation-id", required=True)
  p_disp.add_argument("--snapshot-id", required=True)
  p_disp.add_argument("--compliance-hash", required=True)
  p_disp.add_argument("--manifest-sha256", required=True)
  p_disp.add_argument("--reviewer-ticket", required=True)
  p_disp.add_argument("--slo-digest", required=True)
  p_disp.add_argument("--drain-token", default="NONE")
  p_disp.add_argument("--batch-cap", type=int, required=True)
  p_disp.add_argument("--signoff-code", required=True)

  p_fin = sub.add_parser("finalize_postmortem_and_cleanup")
  p_fin.add_argument("--global-release-id", required=True)
  p_fin.add_argument("--snapshot-id", required=True)

  args = parser.parse_args()
  try:
    if args.step == "ingest_incident_telemetry":
      res = step_ingest_incident_telemetry()
    elif args.step == "verify_freeze_window":
      res = step_verify_freeze_window(args.incident_id)
    elif args.step == "reserve_failover_capacity":
      res = step_reserve_failover_capacity(args.freeze_token)
    elif args.step == "compute_blast_radius":
      res = step_compute_blast_radius(args.incident_id)
    elif args.step == "run_deep_compliance_audit":
      res = step_run_deep_compliance_audit(args.snapshot_id)
    elif args.step == "run_standard_preflight":
      res = step_run_standard_preflight(args.snapshot_id)
    elif args.step == "stage_canary_artifact":
      res = step_stage_canary_artifact(
          args.freeze_token,
          args.reservation_id,
          args.snapshot_id,
          args.compliance_hash,
      )
    elif args.step == "apply_db_schema_migration":
      res = step_apply_db_schema_migration(
          args.manifest_sha256,
          args.reviewer_ticket,
          args.max_batch_size,
          args.soak_window_minutes,
      )
    elif args.step == "rollback_staged_artifact":
      res = step_rollback_staged_artifact(args.manifest_sha256)
    elif args.step == "deploy_canary_region":
      res = step_deploy_canary_region(
          args.manifest_sha256,
          args.migration_id,
          _str_to_bool(args.exclude_flaky_node),
      )
    elif args.step == "verify_canary_slo_metrics":
      res = step_verify_canary_slo_metrics(args.canary_deployment_id)
    elif args.step == "run_replica_catchup_drain":
      res = step_run_replica_catchup_drain(
          args.canary_deployment_id, args.slo_digest
      )
    elif args.step == "dispatch_global_and_notify":
      res = step_dispatch_global_and_notify(
          args.freeze_token,
          args.reservation_id,
          args.snapshot_id,
          args.compliance_hash,
          args.manifest_sha256,
          args.reviewer_ticket,
          args.slo_digest,
          args.batch_cap,
          args.signoff_code,
          args.drain_token,
      )
    elif args.step == "finalize_postmortem_and_cleanup":
      res = step_finalize_postmortem_and_cleanup(
          args.global_release_id, args.snapshot_id
      )
    else:
      raise ValueError(f"Unknown step: {args.step}")
    print(json.dumps(res, indent=2))
    return 0
  except Exception as exc:  # pylint: disable=broad-exception-caught
    print(f"ERROR: {exc}", file=sys.stderr)
    return 1


if __name__ == "__main__":
  sys.exit(main())
