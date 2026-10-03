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

"""Actions for the 9-stage Incident Database Failover workflow."""

from __future__ import annotations

import json
import os
import tempfile
from typing import Any


def _resolve_state_file(payload: dict[str, Any]) -> str:
  raw = payload.get("failover_state_path")
  if isinstance(raw, str) and raw.strip():
    return os.path.abspath(os.path.expanduser(raw.strip()))
  return os.path.join(tempfile.gettempdir(), "lightflow_db_failover_state.json")


def _load_state(path: str) -> dict[str, Any]:
  if os.path.isfile(path):
    with open(path, "r", encoding="utf-8") as f:
      return json.load(f)
  return {}


def _save_state(path: str, state: dict[str, Any]) -> None:
  os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
  tmp = f"{path}.tmp.{os.getpid()}"
  with open(tmp, "w", encoding="utf-8") as f:
    json.dump(state, f, indent=2, sort_keys=True)
  os.replace(tmp, path)


def detect_primary_outage(
    payload: dict[str, Any], dry_run: bool = False, **kwargs: Any
) -> tuple[dict[str, Any], str]:
  """Probes primary database health and records replication lag."""
  del kwargs
  is_dry = bool(dry_run or payload.get("dry_run"))
  cluster = str(payload.get("cluster_id") or "pg-orders-eu1")
  lag_bytes = int(payload.get("replication_lag_bytes", 4096))
  state_path = _resolve_state_file(payload)
  if not is_dry:
    _save_state(
        state_path,
        {
            "cluster_id": cluster,
            "old_primary": f"{cluster}-primary-a",
            "candidate_replica": f"{cluster}-replica-b",
            "replication_lag_bytes": lag_bytes,
            "wal_replay_ticks": 0,
            "wal_caught_up": lag_bytes == 0,
            "stonith_fenced": False,
            "promoted_primary": None,
            "promotion_count": 0,
            "pooler_status": "PRIMARY_UNREACHABLE",
            "audit_published": False,
        },
    )
  out = {
      "cluster_id": cluster,
      "old_primary": f"{cluster}-primary-a",
      "replication_lag_bytes": lag_bytes,
      "failover_state_path": state_path,
  }
  return out, f"Detected outage on {cluster}-primary-a (lag={lag_bytes}B)."


def elect_failover_candidate(
    payload: dict[str, Any], dry_run: bool = False, **kwargs: Any
) -> tuple[dict[str, Any], str]:
  """Selects standby replica and determines zero_loss vs wal_replay_needed."""
  del dry_run, kwargs
  outputs = payload.get("outputs") or {}
  outage = outputs.get("detect_primary_outage") or {}
  cluster = str(
      outage.get("cluster_id") or payload.get("cluster_id") or "pg-orders-eu1"
  )
  lag_bytes = int(
      outage.get(
          "replication_lag_bytes", payload.get("replication_lag_bytes", 4096)
      )
  )
  lag_mode = "wal_replay_needed" if lag_bytes > 0 else "zero_loss"
  candidate = f"{cluster}-replica-b"
  out = {
      "candidate_replica": candidate,
      "lag_mode": lag_mode,
      "target_lsn": "0/1A8F4000",
  }
  return out, f"Elected {candidate} (lag_mode={lag_mode}, LSN=0/1A8F4000)."


def replay_missing_wal_segments(
    payload: dict[str, Any], dry_run: bool = False, **kwargs: Any
) -> tuple[dict[str, Any], str]:
  """Conditional Branch A (lag > 0): Polls WAL replay across 2 ticks."""
  del kwargs
  is_dry = bool(dry_run or payload.get("dry_run"))
  if is_dry:
    return (
        {"wal_caught_up": True, "replayed_ticks": 2},
        "[DRY RUN] WAL segments replayed to 0/1A8F4000.",
    )
  state_path = _resolve_state_file(payload)
  state = _load_state(state_path)
  ticks = int(state.get("wal_replay_ticks", 0)) + 1
  caught_up = ticks >= 2
  state["wal_replay_ticks"] = ticks
  state["wal_caught_up"] = caught_up
  if caught_up:
    state["replication_lag_bytes"] = 0
  _save_state(state_path, state)
  return (
      {"wal_caught_up": caught_up, "replayed_ticks": ticks},
      f"WAL replay tick {ticks}/2 (caught_up={caught_up}).",
  )


def verify_zero_loss_sync(
    payload: dict[str, Any], dry_run: bool = False, **kwargs: Any
) -> tuple[dict[str, Any], str]:
  """Conditional Branch B (lag == 0): Confirms synchronous LSN parity."""
  del kwargs
  is_dry = bool(dry_run or payload.get("dry_run"))
  state_path = _resolve_state_file(payload)
  if not is_dry:
    state = _load_state(state_path)
    state["wal_caught_up"] = True
    _save_state(state_path, state)
  return (
      {"wal_caught_up": True, "sync_verified": True},
      "Verified synchronous zero-loss LSN parity.",
  )


def fence_old_primary(
    payload: dict[str, Any], dry_run: bool = False, **kwargs: Any
) -> tuple[dict[str, Any], str]:
  """ALL_DONE join after WAL branch: fences old primary to prevent split-brain."""
  del kwargs
  is_dry = bool(dry_run or payload.get("dry_run"))
  outputs = payload.get("outputs") or {}
  replay = outputs.get("replay_missing_wal_segments") or {}
  zero_sync = outputs.get("verify_zero_loss_sync") or {}
  if not is_dry and not (
      replay.get("wal_caught_up") or zero_sync.get("wal_caught_up")
  ):
    raise RuntimeError(
        "Cannot fence old primary before replica WAL catches up."
    )
  state_path = _resolve_state_file(payload)
  if not is_dry:
    state = _load_state(state_path)
    state["stonith_fenced"] = True
    _save_state(state_path, state)
  return (
      {"stonith_fenced": True},
      "Applied STONITH network/storage fence to old primary.",
  )


def promote_standby_replica(
    payload: dict[str, Any], dry_run: bool = False, **kwargs: Any
) -> tuple[dict[str, Any], str]:
  """Promotes standby replica to read-write primary (must execute only once)."""
  del kwargs
  is_dry = bool(dry_run or payload.get("dry_run"))
  outputs = payload.get("outputs") or {}
  cand = outputs.get("elect_failover_candidate") or {}
  fence = outputs.get("fence_old_primary") or {}
  if not is_dry and not fence.get("stonith_fenced"):
    raise RuntimeError("Cannot promote standby before old primary is fenced.")
  candidate = str(cand.get("candidate_replica") or "pg-orders-eu1-replica-b")
  state_path = _resolve_state_file(payload)
  if not is_dry:
    state = _load_state(state_path)
    state["promoted_primary"] = candidate
    state["promotion_count"] = int(state.get("promotion_count", 0)) + 1
    _save_state(state_path, state)
  return (
      {"promoted_primary": candidate, "read_write": True},
      f"Promoted {candidate} to read-write primary.",
  )


def cutover_pooler_and_verify_writes(
    payload: dict[str, Any], dry_run: bool = False, **kwargs: Any
) -> tuple[dict[str, Any], str]:
  """Repoints PgBouncer pooler to new primary and executes write-canary check."""
  del kwargs
  is_dry = bool(dry_run or payload.get("dry_run"))
  outputs = payload.get("outputs") or {}
  promo = outputs.get("promote_standby_replica") or {}
  new_primary = str(promo.get("promoted_primary") or "")
  if not is_dry and not new_primary:
    raise RuntimeError("Missing promoted_primary from promote_standby_replica.")
  state_path = _resolve_state_file(payload)
  if is_dry:
    return (
        {"pooler_status": "ONLINE_RW", "active_primary": new_primary},
        f"[DRY RUN] Would repoint pooler to {new_primary} and verify writes.",
    )
  state = _load_state(state_path)
  state["pooler_status"] = "ROUTING_TO_NEW_PRIMARY"
  _save_state(state_path, state)

  if payload.get("simulate_pooler_write_error"):
    raise RuntimeError(
        f"Write canary failed on pooler cutover to {new_primary}: stale"
        " read-only backend."
    )
  state["pooler_status"] = "ONLINE_RW"
  state["pooler_rolled_back"] = False
  _save_state(state_path, state)
  return (
      {"pooler_status": "ONLINE_RW", "active_primary": new_primary},
      f"Pooler cutover to {new_primary} verified (write canary passed).",
  )


def revert_pooler_to_maintenance(
    payload: dict[str, Any], dry_run: bool = False, **kwargs: Any
) -> tuple[dict[str, Any], str]:
  """Compensating rollback action placing pooler in safe PAUSED_MAINTENANCE."""
  del kwargs
  is_dry = bool(dry_run or payload.get("dry_run"))
  state_path = _resolve_state_file(payload)
  if not is_dry:
    state = _load_state(state_path)
    state["pooler_status"] = "PAUSED_MAINTENANCE"
    state["pooler_rolled_back"] = True
    _save_state(state_path, state)
  return (
      {"pooler_status": "PAUSED_MAINTENANCE", "pooler_rolled_back": True},
      (
          "Reverted PgBouncer pooler to PAUSED_MAINTENANCE after canary write"
          " failure."
      ),
  )


def publish_failover_ledger(
    payload: dict[str, Any], dry_run: bool = False, **kwargs: Any
) -> tuple[dict[str, Any], str]:
  """Publishes the final post-failover audit record once pooler is ONLINE_RW."""
  del kwargs
  is_dry = bool(dry_run or payload.get("dry_run"))
  outputs = payload.get("outputs") or {}
  cutover = outputs.get("cutover_pooler_and_verify_writes") or {}
  gate = outputs.get("approve_replica_promotion") or payload
  if not is_dry and cutover.get("pooler_status") != "ONLINE_RW":
    raise RuntimeError(
        "Cannot publish failover ledger before pooler is ONLINE_RW."
    )
  ticket = str(gate.get("incident_ticket") or "INC-404")
  state_path = _resolve_state_file(payload)
  if not is_dry:
    state = _load_state(state_path)
    state["audit_published"] = True
    state["incident_ticket"] = ticket
    _save_state(state_path, state)
  return (
      {"audit_published": True, "incident_ticket": ticket},
      f"Published failover audit ledger for {ticket}.",
  )
