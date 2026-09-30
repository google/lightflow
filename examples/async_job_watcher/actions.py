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

"""Actions for the asynchronous background job polling & ALL_DONE cleanup demo."""

from __future__ import annotations

import json
import os
import random
import subprocess
import sys
import tempfile
from typing import Any

_WORKER_SCRIPT = """
import json, os, sys, time
status_file = sys.argv[1]
total_delay = float(sys.argv[2])
steps = 4
step_sleep = max(0.01, total_delay / steps)
for i in range(1, steps):
    time.sleep(step_sleep)
    tmp_path = status_file + ".tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump({
            "status": "RUNNING",
            "progress_pct": i * 25,
            "target_delay_seconds": total_delay,
            "artifact_rows": 0,
            "artifact_sha256": "",
        }, f)
    os.replace(tmp_path, status_file)
time.sleep(step_sleep)
tmp_path = status_file + ".tmp"
with open(tmp_path, "w", encoding="utf-8") as f:
    json.dump({
        "status": "READY",
        "progress_pct": 100,
        "target_delay_seconds": total_delay,
        "artifact_rows": 12500,
        "artifact_sha256": "9f86d081884c7d659a2feaa0c55ad015",
    }, f)
os.replace(tmp_path, status_file)
"""


def _resolve_status_file(payload: dict[str, Any]) -> str:
  raw_outputs = payload.get("outputs")
  outputs = raw_outputs if isinstance(raw_outputs, dict) else {}
  spawn_out = outputs.get("spawn_background_job") or {}
  path = (
      spawn_out.get("status_file")
      or payload.get("status_file")
      or os.path.join(
          tempfile.gettempdir(), f"lightflow_async_job_{os.getpid()}.json"
      )
  )
  return os.path.abspath(os.path.expanduser(str(path)))


def spawn_background_job(
    payload: dict[str, Any], dry_run: bool = False, **kwargs: Any
) -> tuple[dict[str, Any], str]:
  """Spawns a detached background process that populates a status file after 10-20s."""
  del kwargs
  is_dry = bool(dry_run or payload.get("dry_run") or payload.get("offline"))
  status_file = _resolve_status_file(payload)

  if payload.get("timeout_demo"):
    delay = float(payload.get("delay_seconds", 40.0))
  elif "delay_seconds" in payload:
    delay = float(payload["delay_seconds"])
  else:
    min_d = float(payload.get("min_delay_seconds", 10.0))
    max_d = float(payload.get("max_delay_seconds", 20.0))
    delay = round(random.uniform(min_d, max_d), 1)

  if is_dry:
    return (
        {
            "status_file": status_file,
            "target_delay_seconds": delay,
            "worker_pid": 0,
        },
        (
            "[DRY RUN] Would spawn background export job writing to"
            f" '{status_file}' (random duration ~{delay:.1f}s)."
        ),
    )

  os.makedirs(os.path.dirname(status_file) or ".", exist_ok=True)
  initial_state = {
      "status": "RUNNING",
      "progress_pct": 0,
      "target_delay_seconds": delay,
      "artifact_rows": 0,
      "artifact_sha256": "",
  }
  with open(status_file, "w", encoding="utf-8") as f:
    json.dump(initial_state, f)

  # pylint: disable=consider-using-with
  proc = subprocess.Popen(
      [sys.executable, "-c", _WORKER_SCRIPT, status_file, str(delay)],
      stdin=subprocess.DEVNULL,
      stdout=subprocess.DEVNULL,
      stderr=subprocess.DEVNULL,
      start_new_session=True,
  )

  return (
      {
          "status_file": status_file,
          "target_delay_seconds": delay,
          "worker_pid": int(proc.pid),
      },
      (
          f"Spawned background export job (PID {proc.pid}, duration"
          f" ~{delay:.1f}s) writing to '{status_file}'."
      ),
  )


def check_job_status_file(
    payload: dict[str, Any], dry_run: bool = False, **kwargs: Any
) -> tuple[dict[str, Any], str]:
  """Poll tick action that inspects the job status file on disk."""
  del kwargs
  is_dry = bool(dry_run or payload.get("dry_run") or payload.get("offline"))
  status_file = _resolve_status_file(payload)

  if is_dry:
    return (
        {
            "job_status": "READY",
            "progress_pct": 100,
            "artifact_rows": 12500,
            "artifact_sha256": "9f86d081884c7d659a2feaa0c55ad015",
        },
        "Polled job status via dry_run: status=READY (100%, 12500 rows).",
    )

  if not os.path.isfile(status_file):
    return (
        {
            "job_status": "WAITING",
            "progress_pct": 0,
            "artifact_rows": 0,
            "artifact_sha256": "",
        },
        f"Waiting for status file '{status_file}' to appear...",
    )

  try:
    with open(status_file, "r", encoding="utf-8") as f:
      data = json.load(f)
  except (OSError, ValueError):
    return (
        {
            "job_status": "RUNNING",
            "progress_pct": 0,
            "artifact_rows": 0,
            "artifact_sha256": "",
        },
        f"Status file '{status_file}' is being updated...",
    )

  job_status = str(data.get("status") or "RUNNING")
  progress_pct = int(data.get("progress_pct") or 0)
  artifact_rows = int(data.get("artifact_rows") or 0)
  artifact_sha256 = str(data.get("artifact_sha256") or "")

  return (
      {
          "job_status": job_status,
          "progress_pct": progress_pct,
          "artifact_rows": artifact_rows,
          "artifact_sha256": artifact_sha256,
      },
      (
          f"Polled '{os.path.basename(status_file)}': status={job_status}"
          f" ({progress_pct}%)."
      ),
  )


def verify_exported_artifact(
    payload: dict[str, Any], dry_run: bool = False, **kwargs: Any
) -> tuple[dict[str, Any], str]:
  """Verifies that the polled background job produced a valid artifact."""
  del kwargs
  is_dry = bool(dry_run or payload.get("dry_run") or payload.get("offline"))
  raw_outputs = payload.get("outputs")
  outputs = raw_outputs if isinstance(raw_outputs, dict) else {}
  poll_out = outputs.get("await_job_completion") or payload

  rows = int(poll_out.get("artifact_rows") or (12500 if is_dry else 0))
  sha256 = str(
      poll_out.get("artifact_sha256")
      or ("9f86d081884c7d659a2feaa0c55ad015" if is_dry else "")
  )
  if not is_dry and (rows <= 0 or not sha256):
    raise RuntimeError(
        f"Exported artifact verification failed (rows={rows},"
        f" sha256={sha256!r})."
    )

  return (
      {
          "verified": True,
          "artifact_rows": rows,
          "artifact_sha256": sha256,
      },
      f"Verified exported artifact ({rows} rows, sha256={sha256[:12]}...).",
  )


def cleanup_job_status_file(
    payload: dict[str, Any], dry_run: bool = False, **kwargs: Any
) -> tuple[dict[str, Any], str]:
  """ALL_DONE cleanup action that removes completed temporary job files."""
  del kwargs
  is_dry = bool(dry_run or payload.get("dry_run") or payload.get("offline"))
  status_file = _resolve_status_file(payload)

  if is_dry:
    return (
        {"cleaned_up": True, "status_file": status_file},
        f"[DRY RUN] Would clean up temporary status file '{status_file}'.",
    )

  if os.path.isfile(status_file):
    try:
      with open(status_file, "r", encoding="utf-8") as f:
        data = json.load(f)
      if data.get("status") == "RUNNING":
        return (
            {"cleaned_up": False, "status_file": status_file},
            (
                f"Preserved in-flight status file '{status_file}' so"
                " `lightflow resume` can continue polling."
            ),
        )
    except (OSError, ValueError):
      pass

    try:
      os.remove(status_file)
    except OSError:
      pass
    tmp_path = status_file + ".tmp"
    if os.path.exists(tmp_path):
      try:
        os.remove(tmp_path)
      except OSError:
        pass
    return (
        {"cleaned_up": True, "status_file": status_file},
        f"Cleaned up temporary status file '{status_file}'.",
    )

  return (
      {"cleaned_up": True, "status_file": status_file},
      f"No temporary status file found at '{status_file}'.",
  )
