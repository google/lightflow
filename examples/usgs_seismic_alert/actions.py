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

"""Actions for the USGS M4.5+ earthquake GeoJSON feed triage workflow."""

from __future__ import annotations

import json
import os
from typing import Any
import urllib.error
import urllib.request

USGS_GEOJSON_URL = (
    "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/4.5_day.geojson"
)

_FALLBACK_EVENTS = [
    {
        "magnitude": 5.4,
        "place": "112 km SSE of Kokopo, Papua New Guinea",
        "url": "https://earthquake.usgs.gov/earthquakes/eventpage/us7000demo1",
    },
    {
        "magnitude": 4.8,
        "place": "45 km WSW of Coquimbo, Chile",
        "url": "https://earthquake.usgs.gov/earthquakes/eventpage/us7000demo2",
    },
]


def fetch_usgs_feed(
    payload: dict[str, Any],
    limit: int = 5,
    dry_run: bool = False,
    **kwargs: Any,
) -> dict[str, Any]:
  """Fetches the past 24 hours of M4.5+ earthquakes from the USGS GeoJSON API."""
  del kwargs
  is_dry = bool(dry_run or payload.get("dry_run") or payload.get("offline"))
  max_items = max(1, min(int(payload.get("limit", limit)), 20))

  events: list[dict[str, Any]] = []
  source = "live_usgs_geojson"
  if not is_dry:
    try:
      req = urllib.request.Request(
          USGS_GEOJSON_URL,
          headers={"User-Agent": "lightflow-dag-example/0.1"},
      )
      with urllib.request.urlopen(req, timeout=5.0) as resp:
        data = json.loads(resp.read().decode("utf-8"))
      features = data.get("features") or []
      for feat in features:
        props = (feat or {}).get("properties") or {}
        mag = props.get("mag")
        if mag is None:
          continue
        events.append({
            "magnitude": round(float(mag), 1),
            "place": str(props.get("place") or "Unknown region"),
            "url": str(props.get("url") or "https://earthquake.usgs.gov"),
        })
      events.sort(key=lambda e: e["magnitude"], reverse=True)
      events = events[:max_items]
    except (urllib.error.URLError, TimeoutError, OSError, ValueError):
      events = list(_FALLBACK_EVENTS[:max_items])
      source = "offline_fallback"
  else:
    events = list(_FALLBACK_EVENTS[:max_items])
    source = (
        "dry_run" if (dry_run or payload.get("dry_run")) else "offline_fixture"
    )

  if not events:
    events = list(_FALLBACK_EVENTS[:max_items])
    source = "offline_fallback"

  strongest = events[0]
  return {
      "events": events,
      "event_count": len(events),
      "max_magnitude": strongest["magnitude"],
      "strongest_place": strongest["place"],
      "source": source,
  }


def emit_bulletin(
    payload: dict[str, Any],
    default_output: str = "/tmp/usgs_bulletin.md",
    dry_run: bool = False,
    **kwargs: Any,
) -> tuple[dict[str, Any], str]:
  """Writes an incident bulletin Markdown file atomically."""
  del kwargs
  is_dry = bool(dry_run or payload.get("dry_run"))
  raw_outputs = payload.get("outputs")
  outputs = raw_outputs if isinstance(raw_outputs, dict) else {}
  feed = outputs.get("fetch_usgs_feed") or {}
  events = feed.get("events") or []
  dispatch = (
      outputs.get("incident_commander_gate")
      or payload.get("dispatch")
      or payload
  )
  severity = str(dispatch.get("severity", "ADVISORY")).upper()
  output_path = os.path.abspath(
      str(dispatch.get("output_path") or default_output)
  )

  lines = [
      f"# USGS Seismic Bulletin [{severity}]",
      "",
      (
          f"**Peak Event:** M{feed.get('max_magnitude')} —"
          f" {feed.get('strongest_place')}"
      ),
      "",
      "## Recorded M4.5+ Events",
  ]
  for ev in events:
    lines.append(f"- **M{ev['magnitude']}** — [{ev['place']}]({ev['url']})")
  lines.append("")

  if is_dry:
    return (
        {
            "bulletin_path": output_path,
            "severity": severity,
            "simulated": True,
        },
        f"[DRY RUN] Would emit [{severity}] bulletin to '{output_path}'.",
    )

  os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
  tmp_path = f"{output_path}.tmp.{os.getpid()}"
  with open(tmp_path, "w", encoding="utf-8") as f:
    f.write("\n".join(lines))
  os.replace(tmp_path, output_path)

  return (
      {
          "bulletin_path": output_path,
          "severity": severity,
          "simulated": False,
      },
      f"Dispatched [{severity}] seismic bulletin to '{output_path}'.",
  )
