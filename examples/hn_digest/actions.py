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

"""Actions for the Hacker News curation and Markdown digest workflow."""

from __future__ import annotations

import json
import os
from typing import Any
import urllib.error
import urllib.request

HN_TOP_STORIES_URL = "https://hacker-news.firebaseio.com/v0/topstories.json"
HN_ITEM_URL_TEMPLATE = (
    "https://hacker-news.firebaseio.com/v0/item/{item_id}.json"
)

_FALLBACK_STORIES = [
    {
        "id": 40000001,
        "title": "Show HN: Deterministic DAG Guardrails for CLI AI Agents",
        "url": "https://github.com/google/lightflow",
        "score": 342,
        "by": "pg",
    },
    {
        "id": 40000002,
        "title": "SQLite and Append-Only File Ledgers in Local-First Tooling",
        "url": "https://example.com/local-first-ledgers",
        "score": 215,
        "by": "systems_dev",
    },
    {
        "id": 40000003,
        "title": (
            "Why Human-in-the-Loop Exit Codes Matter for Autonomous Agents"
        ),
        "url": "https://example.com/hitl-exit-codes",
        "score": 178,
        "by": "agent_eng",
    },
]


def _fetch_json(url: str, timeout: float = 5.0) -> Any:
  req = urllib.request.Request(
      url, headers={"User-Agent": "lightflow-dag-example/0.1"}
  )
  with urllib.request.urlopen(req, timeout=timeout) as resp:
    return json.loads(resp.read().decode("utf-8"))


def fetch_top_stories(
    payload: dict[str, Any],
    limit: int = 5,
    dry_run: bool = False,
    **kwargs: Any,
) -> tuple[dict[str, Any], str]:
  """Fetches the top N stories from the public Hacker News Firebase API."""
  del kwargs
  is_dry = bool(dry_run or payload.get("dry_run") or payload.get("offline"))
  max_items = max(1, min(int(payload.get("limit", limit)), 15))

  stories: list[dict[str, Any]] = []
  source = "live_api"
  if not is_dry:
    try:
      story_ids = _fetch_json(HN_TOP_STORIES_URL)
      for item_id in (story_ids or [])[:max_items]:
        item = _fetch_json(HN_ITEM_URL_TEMPLATE.format(item_id=item_id))
        if isinstance(item, dict) and item.get("title"):
          stories.append({
              "id": item.get("id", item_id),
              "title": str(item.get("title", "")),
              "url": str(
                  item.get("url")
                  or f"https://news.ycombinator.com/item?id={item_id}"
              ),
              "score": int(item.get("score") or 0),
              "by": str(item.get("by") or "unknown"),
          })
    except (urllib.error.URLError, TimeoutError, OSError, ValueError):
      stories = list(_FALLBACK_STORIES[:max_items])
      source = "offline_fallback"
  else:
    stories = list(_FALLBACK_STORIES[:max_items])
    source = (
        "dry_run" if (dry_run or payload.get("dry_run")) else "offline_fixture"
    )

  if not stories:
    stories = list(_FALLBACK_STORIES[:max_items])
    source = "offline_fallback"

  top = stories[0]
  return (
      {
          "stories": stories,
          "count": len(stories),
          "top_title": top["title"],
          "top_score": top["score"],
          "source": source,
      },
      (
          f"Fetched {len(stories)} HN stories via {source}"
          f" (top: '{top['title']}' [{top['score']} pts])."
      ),
  )


def publish_digest(
    payload: dict[str, Any],
    default_output: str = "/tmp/hn_digest.md",
    dry_run: bool = False,
    **kwargs: Any,
) -> tuple[dict[str, Any], str]:
  """Writes a curated Markdown digest combining HN stories and editor notes."""
  del kwargs
  is_dry = bool(dry_run or payload.get("dry_run"))
  raw_outputs = payload.get("outputs")
  outputs = raw_outputs if isinstance(raw_outputs, dict) else {}
  fetched = outputs.get("fetch_top_stories") or {}
  stories = fetched.get("stories") or payload.get("stories") or []
  editorial = (
      outputs.get("editorial_gate") or payload.get("editorial") or payload
  )
  editor_note = str(editorial.get("editor_note", "")).strip()
  output_path = os.path.abspath(
      str(editorial.get("output_path") or default_output)
  )

  lines = ["# Hacker News Curated Briefing", ""]
  if editor_note:
    lines.extend([f"> **Editor's Note:** {editor_note}", ""])
  lines.append("## Top Stories")
  for idx, s in enumerate(stories, start=1):
    lines.append(
        f"{idx}. **[{s['title']}]({s['url']})** — {s['score']} pts"
        f" (by `{s['by']}`)"
    )
  lines.append("")
  markdown_body = "\n".join(lines)

  if is_dry:
    return (
        {
            "digest_path": output_path,
            "story_count": len(stories),
            "simulated": True,
        },
        (
            f"[DRY RUN] Would write {len(stories)}-story digest to"
            f" '{output_path}'."
        ),
    )

  os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
  tmp_path = f"{output_path}.tmp.{os.getpid()}"
  with open(tmp_path, "w", encoding="utf-8") as f:
    f.write(markdown_body)
  os.replace(tmp_path, output_path)

  return (
      {
          "digest_path": output_path,
          "story_count": len(stories),
          "simulated": False,
      },
      f"Published {len(stories)}-story Markdown digest to '{output_path}'.",
  )
