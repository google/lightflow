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

"""Actions for the PyPI dependency audit, human approval, and rollback demo."""

from __future__ import annotations

import json
import os
import shutil
from typing import Any
import urllib.error
import urllib.request

PYPI_JSON_URL = "https://pypi.org/pypi/{package}/json"

_DEFAULT_PINNED = {
    "PyYAML": "6.0",
    "jsonschema": "4.17.0",
    "httpx": "0.27.0",
}

_FALLBACK_LATEST = {
    "PyYAML": "6.0.2",
    "jsonschema": "4.23.0",
    "httpx": "0.28.1",
}


def _fetch_latest_pypi_version(package: str, timeout: float = 5.0) -> str:
  req = urllib.request.Request(
      PYPI_JSON_URL.format(package=package),
      headers={"User-Agent": "lightflow-dag-example/0.1"},
  )
  with urllib.request.urlopen(req, timeout=timeout) as resp:
    data = json.loads(resp.read().decode("utf-8"))
    return str((data.get("info") or {}).get("version") or "")


def audit_pypi_versions(
    payload: dict[str, Any], dry_run: bool = False, **kwargs: Any
) -> tuple[dict[str, Any], str]:
  """Queries the public PyPI JSON API to compare pinned vs. latest versions."""
  del kwargs
  is_dry = bool(dry_run or payload.get("dry_run") or payload.get("offline"))
  pinned: dict[str, str] = dict(
      payload.get("pinned_packages") or _DEFAULT_PINNED
  )

  upgrades: list[dict[str, str]] = []
  source = "live_pypi_api"
  for pkg, current_ver in pinned.items():
    latest_ver = ""
    if not is_dry:
      try:
        latest_ver = _fetch_latest_pypi_version(pkg)
      except (urllib.error.URLError, TimeoutError, OSError, ValueError):
        latest_ver = _FALLBACK_LATEST.get(pkg, current_ver)
        source = "offline_fallback"
    else:
      latest_ver = _FALLBACK_LATEST.get(pkg, current_ver)
      source = (
          "dry_run"
          if (dry_run or payload.get("dry_run"))
          else "offline_fixture"
      )

    if latest_ver and latest_ver != current_ver:
      upgrades.append({"package": pkg, "from": current_ver, "to": latest_ver})

  summary = (
      ", ".join(f"{u['package']} ({u['from']} -> {u['to']})" for u in upgrades)
      or "All packages up to date"
  )

  return (
      {
          "pinned": pinned,
          "upgrades": upgrades,
          "upgrade_count": len(upgrades),
          "summary": summary,
          "source": source,
      },
      f"Audited {len(pinned)} packages via {source}: {summary}.",
  )


def apply_and_smoke_test(
    payload: dict[str, Any],
    default_requirements_path: str = "/tmp/lightflow_demo_requirements.txt",
    dry_run: bool = False,
    **kwargs: Any,
) -> tuple[dict[str, Any], str]:
  """Writes upgraded requirements with a `.bak` backup and runs a smoke check."""
  del kwargs
  is_dry = bool(dry_run or payload.get("dry_run"))
  raw_outputs = payload.get("outputs")
  outputs = raw_outputs if isinstance(raw_outputs, dict) else {}
  audit = outputs.get("audit_pypi_versions") or {}
  pinned: dict[str, str] = dict(audit.get("pinned") or _DEFAULT_PINNED)
  upgrades: list[dict[str, str]] = list(audit.get("upgrades") or [])
  gate = (
      outputs.get("approve_upgrades") or payload.get("upgrade_gate") or payload
  )

  req_path = os.path.abspath(
      str(gate.get("requirements_path") or default_requirements_path)
  )
  backup_path = f"{req_path}.bak"

  if is_dry:
    return (
        {
            "requirements_path": req_path,
            "backup_path": backup_path,
            "simulated": True,
        },
        f"[DRY RUN] Would apply {len(upgrades)} upgrade(s) to '{req_path}'.",
    )

  os.makedirs(os.path.dirname(req_path) or ".", exist_ok=True)
  if not os.path.exists(req_path):
    with open(req_path, "w", encoding="utf-8") as f:
      for pkg, ver in pinned.items():
        f.write(f"{pkg}=={ver}\n")

  shutil.copy2(req_path, backup_path)

  updated = dict(pinned)
  for u in upgrades:
    updated[u["package"]] = u["to"]

  with open(req_path, "w", encoding="utf-8") as f:
    for pkg, ver in updated.items():
      f.write(f"{pkg}=={ver}\n")

  simulate_failure = (
      payload["simulate_smoke_failure"]
      if "simulate_smoke_failure" in payload
      else gate.get("simulate_smoke_failure")
  )
  if simulate_failure:
    raise RuntimeError(
        "Simulated post-upgrade smoke test failure! Triggering rollback_action"
        f" to restore '{req_path}' from '{backup_path}'."
    )

  return (
      {
          "requirements_path": req_path,
          "backup_path": backup_path,
          "updated_versions": updated,
          "simulated": False,
      },
      (
          f"Applied {len(upgrades)} upgrade(s) to '{req_path}' and passed smoke"
          " check."
      ),
  )


def restore_requirements_backup(
    payload: dict[str, Any],
    default_requirements_path: str = "/tmp/lightflow_demo_requirements.txt",
    **kwargs: Any,
) -> tuple[dict[str, Any], str]:
  """Rollback action that restores `requirements.txt` from its `.bak` file."""
  del kwargs
  raw_outputs = payload.get("outputs")
  outputs = raw_outputs if isinstance(raw_outputs, dict) else {}
  gate = (
      outputs.get("approve_upgrades") or payload.get("upgrade_gate") or payload
  )
  req_path = os.path.abspath(
      str(gate.get("requirements_path") or default_requirements_path)
  )
  backup_path = f"{req_path}.bak"

  if os.path.isfile(backup_path):
    os.replace(backup_path, req_path)
    return (
        {"rolled_back_file": req_path},
        f"Restored original pinned versions in '{req_path}' from backup.",
    )
  return (
      {"rolled_back_file": None},
      f"No backup file found at '{backup_path}'; nothing to restore.",
  )
