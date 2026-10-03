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

"""Actions for the 8-stage Blue-Green / Canary Release workflow."""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
from typing import Any


def _resolve_state_file(payload: dict[str, Any]) -> str:
  raw = payload.get("release_state_path")
  if isinstance(raw, str) and raw.strip():
    return os.path.abspath(os.path.expanduser(raw.strip()))
  return os.path.join(tempfile.gettempdir(), "lightflow_blue_green_state.json")


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


def build_release_bundle(
    payload: dict[str, Any], dry_run: bool = False, **kwargs: Any
) -> tuple[dict[str, Any], str]:
  """Builds the release bundle digest and initializes the deployment slot state."""
  del kwargs
  is_dry = bool(dry_run or payload.get("dry_run"))
  service = str(payload.get("service_name") or "checkout-api")
  version = str(payload.get("version") or "v2.4.0")
  digest = (
      "sha256:"
      + hashlib.sha256(f"{service}:{version}".encode("utf-8")).hexdigest()[:16]
  )
  state_path = _resolve_state_file(payload)
  if not is_dry:
    _save_state(
        state_path,
        {
            "service_name": service,
            "version": version,
            "bundle_digest": digest,
            "active_slot": "blue",
            "canary_weight": 0,
            "green_warmed_ticks": 0,
            "green_ready": False,
            "old_slot_decommissioned": False,
        },
    )
  out = {
      "service_name": service,
      "version": version,
      "bundle_digest": digest,
      "release_state_path": state_path,
  }
  return out, f"Built release bundle {service}@{version} ({digest})."


def run_security_scan(
    payload: dict[str, Any], dry_run: bool = False, **kwargs: Any
) -> tuple[dict[str, Any], str]:
  """Parallel Track A: Runs container CVE and SBOM policy verification."""
  del dry_run, kwargs
  outputs = payload.get("outputs") or {}
  bundle = outputs.get("build_release_bundle") or {}
  digest = str(bundle.get("bundle_digest") or "sha256:unknown")
  out = {
      "scan_passed": True,
      "critical_cves": 0,
      "verified_digest": digest,
  }
  return out, f"Security scan passed for {digest} (0 critical CVEs)."


def run_integration_suite(
    payload: dict[str, Any], dry_run: bool = False, **kwargs: Any
) -> tuple[dict[str, Any], str]:
  """Parallel Track B: Runs hermetic API contract and regression tests."""
  del dry_run, kwargs
  outputs = payload.get("outputs") or {}
  bundle = outputs.get("build_release_bundle") or {}
  version = str(bundle.get("version") or "v2.4.0")
  out = {
      "tests_passed": True,
      "total_cases": 42,
      "verified_version": version,
  }
  return out, f"Integration suite passed for {version} (42/42 cases)."


def warm_green_environment(
    payload: dict[str, Any], dry_run: bool = False, **kwargs: Any
) -> tuple[dict[str, Any], str]:
  """Diamond join + poll: Warms green slot across 2 ticks until green_ready."""
  del kwargs
  is_dry = bool(dry_run or payload.get("dry_run"))
  outputs = payload.get("outputs") or {}
  scan = outputs.get("run_security_scan") or {}
  integ = outputs.get("run_integration_suite") or {}
  if not is_dry and (
      not scan.get("scan_passed") or not integ.get("tests_passed")
  ):
    raise RuntimeError(
        "Cannot warm green slot before security scan and integration tests"
        " pass."
    )
  state_path = _resolve_state_file(payload)
  if is_dry:
    return (
        {"green_ready": True, "warm_ticks": 2},
        "[DRY RUN] Green environment ready.",
    )
  state = _load_state(state_path)
  ticks = int(state.get("green_warmed_ticks", 0)) + 1
  ready = ticks >= 2
  state["green_warmed_ticks"] = ticks
  state["green_ready"] = ready
  _save_state(state_path, state)
  return (
      {"green_ready": ready, "warm_ticks": ticks},
      f"Green slot warmup tick {ticks}/2 (ready={ready}).",
  )


def shift_and_verify_canary(
    payload: dict[str, Any], dry_run: bool = False, **kwargs: Any
) -> tuple[dict[str, Any], str]:
  """Shifts canary traffic to green and verifies error-rate and latency SLOs."""
  del kwargs
  is_dry = bool(dry_run or payload.get("dry_run"))
  outputs = payload.get("outputs") or {}
  gate = outputs.get("approve_canary_cutover") or payload
  canary_pct = int(gate.get("canary_percent", 10))
  state_path = _resolve_state_file(payload)
  if is_dry:
    return (
        {"canary_healthy": True, "canary_weight": canary_pct},
        f"[DRY RUN] Would shift {canary_pct}% canary traffic and verify SLOs.",
    )
  state = _load_state(state_path)
  state["canary_weight"] = canary_pct
  state["active_slot"] = "blue+green_canary"
  _save_state(state_path, state)

  if payload.get("simulate_canary_regression"):
    raise RuntimeError(
        f"Canary SLO breach at {canary_pct}% weight: 5xx rate 4.8% > 1.0%"
        " threshold."
    )
  return (
      {
          "canary_healthy": True,
          "canary_weight": canary_pct,
          "error_rate": 0.001,
      },
      f"Canary healthy at {canary_pct}% weight (5xx rate 0.1%).",
  )


def revert_canary_traffic(
    payload: dict[str, Any], dry_run: bool = False, **kwargs: Any
) -> tuple[dict[str, Any], str]:
  """Compensating rollback action that restores 100% traffic to the blue slot."""
  del kwargs
  is_dry = bool(dry_run or payload.get("dry_run"))
  state_path = _resolve_state_file(payload)
  if not is_dry:
    state = _load_state(state_path)
    state["canary_weight"] = 0
    state["active_slot"] = "blue"
    state["rolled_back"] = True
    _save_state(state_path, state)
  return (
      {"rolled_back": True, "active_slot": "blue", "canary_weight": 0},
      "Reverted canary traffic to 0% (100% blue).",
  )


def promote_green_to_prod(
    payload: dict[str, Any], dry_run: bool = False, **kwargs: Any
) -> tuple[dict[str, Any], str]:
  """Promotes the verified green slot to receive 100% of production traffic."""
  del kwargs
  is_dry = bool(dry_run or payload.get("dry_run"))
  outputs = payload.get("outputs") or {}
  canary = outputs.get("shift_and_verify_canary") or {}
  if not is_dry and not canary.get("canary_healthy"):
    raise RuntimeError(
        "Cannot promote green slot without healthy canary check."
    )
  state_path = _resolve_state_file(payload)
  if not is_dry:
    state = _load_state(state_path)
    state["active_slot"] = "green"
    state["canary_weight"] = 100
    state["rolled_back"] = False
    _save_state(state_path, state)
  return (
      {"active_slot": "green", "production_weight": 100},
      "Promoted green slot to 100% production traffic.",
  )


def decommission_old_slot(
    payload: dict[str, Any], dry_run: bool = False, **kwargs: Any
) -> tuple[dict[str, Any], str]:
  """Drains the retiring blue slot and writes the completed release record."""
  del kwargs
  is_dry = bool(dry_run or payload.get("dry_run"))
  outputs = payload.get("outputs") or {}
  promo = outputs.get("promote_green_to_prod") or {}
  if not is_dry and promo.get("active_slot") != "green":
    raise RuntimeError("Cannot decommission blue slot before green is active.")
  state_path = _resolve_state_file(payload)
  if not is_dry:
    state = _load_state(state_path)
    state["old_slot_decommissioned"] = True
    _save_state(state_path, state)
  return (
      {"old_slot_decommissioned": True, "retired_slot": "blue"},
      "Drained and decommissioned retiring blue slot.",
  )
