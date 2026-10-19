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

"""Actions for the 6-stage quarterly revenue brief workflow."""

from __future__ import annotations

import json
import os
import sqlite3
from typing import Any

# 12 segment rows across NA, EMEA, and APAC (values in $M):
#   NA:   Q2=5.8, Plan=6.3, Q3=6.7, Cost=4.95
#   EMEA: Q2=4.0, Plan=4.37, Q3=4.1, Cost=3.22  (-6.2% vs plan)
#   APAC: Q2=2.83, Plan=3.2, Q3=3.4, Cost=2.61
#   Total Q2=12.63, Plan=13.87, Q3=14.20 (+12.4% QoQ), Net=3.42 (24.1% margin)
_SEED_ROWS: tuple[tuple[str, str, float, float, float, float], ...] = (
    ("NA", "Enterprise", 2.40, 2.60, 2.80, 2.05),
    ("NA", "Mid-Market", 1.60, 1.75, 1.85, 1.38),
    ("NA", "Self-Serve", 1.10, 1.20, 1.25, 0.92),
    ("NA", "Channel", 0.70, 0.75, 0.80, 0.60),
    ("EMEA", "Enterprise", 1.70, 1.92, 1.75, 1.37),
    ("EMEA", "Mid-Market", 1.10, 1.20, 1.15, 0.90),
    ("EMEA", "Self-Serve", 0.75, 0.80, 0.75, 0.59),
    ("EMEA", "Channel", 0.45, 0.45, 0.45, 0.36),
    ("APAC", "Enterprise", 1.20, 1.35, 1.45, 1.11),
    ("APAC", "Mid-Market", 0.83, 0.95, 1.00, 0.77),
    ("APAC", "Self-Serve", 0.50, 0.55, 0.60, 0.46),
    ("APAC", "Channel", 0.30, 0.35, 0.35, 0.27),
)


def _seed_and_query_sqlite(db_path: str) -> tuple[list[dict[str, Any]], int]:
  """Seeds a SQLite table if empty and returns regional aggregates + row count."""
  conn = sqlite3.connect(db_path)
  try:
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS regional_revenue (
            region TEXT NOT NULL,
            segment TEXT NOT NULL,
            q2_actual_m REAL NOT NULL,
            q3_plan_m REAL NOT NULL,
            q3_actual_m REAL NOT NULL,
            q3_cost_m REAL NOT NULL
        )
        """
    )
    cur.execute("SELECT COUNT(*) FROM regional_revenue")
    existing = int(cur.fetchone()[0])
    if existing == 0:
      cur.executemany(
          "INSERT INTO regional_revenue VALUES (?, ?, ?, ?, ?, ?)",
          _SEED_ROWS,
      )
      conn.commit()
      existing = len(_SEED_ROWS)

    cur.execute(
        """
        SELECT
            region,
            ROUND(SUM(q2_actual_m), 2) AS q2_actual_m,
            ROUND(SUM(q3_plan_m), 2) AS q3_plan_m,
            ROUND(SUM(q3_actual_m), 2) AS q3_actual_m,
            ROUND(SUM(q3_cost_m), 2) AS q3_cost_m
        FROM regional_revenue
        GROUP BY region
        ORDER BY q3_actual_m DESC
        """
    )
    columns = [col[0] for col in cur.description]
    rows = [dict(zip(columns, r)) for r in cur.fetchall()]
    return rows, existing
  finally:
    conn.close()


def query_sql_metrics(
    payload: dict[str, Any],
    default_db_path: str = ":memory:",
    dry_run: bool = False,
    **kwargs: Any,
) -> tuple[dict[str, Any], str]:
  """Queries regional revenue figures from SQLite using stdlib sqlite3."""
  del kwargs
  is_dry = bool(dry_run or payload.get("dry_run"))
  db_path = str(payload.get("db_path") or default_db_path)

  rows, row_count = _seed_and_query_sqlite(":memory:" if is_dry else db_path)
  regions = ", ".join(str(r["region"]) for r in rows)
  prefix = "[DRY RUN] " if is_dry else ""
  return (
      {
          "rows": rows,
          "row_count": row_count,
          "regions": [str(r["region"]) for r in rows],
      },
      (
          f"{prefix}Queried SQLite regional_revenue ({row_count} rows across"
          f" {regions})"
      ),
  )


def analyze_variances(
    payload: dict[str, Any],
    plan_alert_threshold_pct: float = -5.0,
    dry_run: bool = False,
    **kwargs: Any,
) -> tuple[dict[str, Any], str]:
  """Computes QoQ growth, net margin, and plan variances from SQL rows."""
  del kwargs
  is_dry = bool(dry_run or payload.get("dry_run"))
  sql_out = payload.get("outputs", {}).get("query_sql_metrics", {})
  rows = list(sql_out.get("rows") or [])
  if not rows:
    rows, _ = _seed_and_query_sqlite(":memory:")

  total_q2 = sum(float(r["q2_actual_m"]) for r in rows)
  total_plan = sum(float(r["q3_plan_m"]) for r in rows)
  total_q3 = sum(float(r["q3_actual_m"]) for r in rows)
  total_cost = sum(float(r["q3_cost_m"]) for r in rows)

  qoq_pct = round(((total_q3 - total_q2) / total_q2) * 100.0, 1)
  vs_plan_pct = round(((total_q3 - total_plan) / total_plan) * 100.0, 1)
  net_margin_pct = round(((total_q3 - total_cost) / total_q3) * 100.0, 1)

  regional_breakdown: list[dict[str, Any]] = []
  flagged_items: list[str] = []
  table_lines = [
      "| Region | Q2 Actual ($M) | Q3 Plan ($M) | Q3 Actual ($M) | QoQ % | vs Plan % |",
      "| :--- | ---: | ---: | ---: | ---: | ---: |",
  ]
  for r in rows:
    region = str(r["region"])
    q2 = float(r["q2_actual_m"])
    plan = float(r["q3_plan_m"])
    q3 = float(r["q3_actual_m"])
    reg_qoq = round(((q3 - q2) / q2) * 100.0, 1)
    reg_plan = round(((q3 - plan) / plan) * 100.0, 1)
    if reg_plan <= float(plan_alert_threshold_pct):
      flagged_items.append(f"{region} {reg_plan:+.1f}% vs plan")
    regional_breakdown.append({
        "region": region,
        "q2_actual_m": round(q2, 2),
        "q3_plan_m": round(plan, 2),
        "q3_actual_m": round(q3, 2),
        "qoq_pct": reg_qoq,
        "vs_plan_pct": reg_plan,
    })
    table_lines.append(
        f"| {region} | {q2:.2f} | {plan:.2f} | {q3:.2f} |"
        f" {reg_qoq:+.1f}% | {reg_plan:+.1f}% |"
    )

  table_lines.append(
      f"| **Total** | **{total_q2:.2f}** | **{total_plan:.2f}** |"
      f" **{total_q3:.2f}** | **{qoq_pct:+.1f}%** | **{vs_plan_pct:+.1f}%** |"
  )
  flag_str = ", ".join(flagged_items) if flagged_items else "none"
  summary = (
      f"Total ${total_q3:.1f}M ({qoq_pct:+.1f}% QoQ) · Net margin"
      f" {net_margin_pct:.1f}% · Flag: {flag_str}"
  )
  prefix = "[DRY RUN] " if is_dry else ""
  return (
      {
          "total_q3_m": round(total_q3, 2),
          "qoq_pct": qoq_pct,
          "vs_plan_pct": vs_plan_pct,
          "net_margin_pct": net_margin_pct,
          "flagged_variances": flagged_items,
          "regional_breakdown": regional_breakdown,
          "table_markdown": "\n".join(table_lines),
          "summary": summary,
      },
      f"{prefix}{summary}",
  )


def render_report(
    payload: dict[str, Any],
    default_output: str = "/tmp/q3_revenue_brief.md",
    dry_run: bool = False,
    **kwargs: Any,
) -> tuple[dict[str, Any], str]:
  """Combines the computed SQL table and reviewed commentary into a Markdown file."""
  del kwargs
  is_dry = bool(dry_run or payload.get("dry_run"))
  outputs = payload.get("outputs", {})
  analysis = outputs.get("analyze_variances", {})
  gate_out = outputs.get("draft_commentary", {})

  output_path = str(
      gate_out.get("output_path")
      or payload.get("output_path")
      or default_output
  )
  commentary = str(
      gate_out.get("commentary")
      or "No commentary provided."
  ).strip()
  summary = str(analysis.get("summary") or "")
  table_md = str(analysis.get("table_markdown") or "")

  lines = [
      "# Q3 Regional Revenue Brief",
      "",
      f"**Summary:** {summary}",
      "",
      "## Commentary",
      "",
      commentary,
      "",
      "## Regional Breakdown",
      "",
      table_md,
      "",
  ]
  report_markdown = "\n".join(lines)

  if not is_dry:
    out_dir = os.path.dirname(os.path.abspath(output_path))
    if out_dir:
      os.makedirs(out_dir, exist_ok=True)
    tmp_path = f"{output_path}.tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
      f.write(report_markdown)
    os.replace(tmp_path, output_path)

  prefix = "[DRY RUN] Would render " if is_dry else "Rendered "
  return (
      {
          "report_path": output_path,
          "report_markdown": report_markdown,
      },
      f"{prefix}{output_path} (computed SQL table + reviewed commentary)",
  )


def send_report(
    payload: dict[str, Any],
    default_channel: str = "#exec-staff",
    dry_run: bool = False,
    **kwargs: Any,
) -> tuple[dict[str, Any], str]:
  """Dispatches the rendered report and records a delivery receipt JSON file."""
  del kwargs
  is_dry = bool(dry_run or payload.get("dry_run"))
  outputs = payload.get("outputs", {})
  render_out = outputs.get("render_report", {})
  approve_out = outputs.get("approve_send", {})

  report_path = str(
      render_out.get("report_path")
      or payload.get("output_path")
      or "/tmp/q3_revenue_brief.md"
  )
  channel = str(
      approve_out.get("channel")
      or payload.get("channel")
      or default_channel
  )
  receipt_path = f"{report_path}.receipt.json"
  receipt = {
      "report_path": report_path,
      "channel": channel,
      "dispatched": not is_dry,
  }

  if not is_dry:
    out_dir = os.path.dirname(os.path.abspath(receipt_path))
    if out_dir:
      os.makedirs(out_dir, exist_ok=True)
    with open(receipt_path, "w", encoding="utf-8") as f:
      json.dump(receipt, f, indent=2)

  prefix = "[DRY RUN] Would dispatch " if is_dry else "Dispatched "
  return (
      {
          "sent": not is_dry,
          "channel": channel,
          "receipt_path": receipt_path,
      },
      f"{prefix}{report_path} to {channel} (receipt recorded)",
  )
