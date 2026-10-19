<!-- mdformat global-off -->

# Quarterly Revenue Brief (`quarterly_revenue_brief`)

A six-stage workflow with two checkpoints. It uses only the standard library
(`sqlite3`) and writes to `/tmp`.

1. `query_sql_metrics` (`python_action`): creates an in-memory SQLite table
   `regional_revenue` (12 rows for NA, EMEA, and APAC) and queries per-region
   totals.
2. `analyze_variances` (`python_action`): computes quarter-over-quarter growth,
   net margin, and variance against plan from the query result, and builds the
   Markdown table used in the report.
3. `draft_commentary` (`operator_action`): exits with code 2 and shows the
   computed summary. The response must contain `approved` and `commentary`. In
   an agent session, the agent drafts the commentary from the figures above and
   the user edits or approves it before `lightflow resume` is run.
4. `render_report` (`python_action`): writes the table and the commentary to
   `/tmp/q3_revenue_brief.md`.
5. `approve_send` (`operator_action`): exits with code 2 before the report is
   sent.
6. `send_report` (`python_action`): stands in for delivery. It writes
   `/tmp/q3_revenue_brief.md.receipt.json`; nothing is sent anywhere. Replace
   this action with your own delivery code.

## Run It

```bash
# 1. Dry-run the 6-stage DAG
lightflow dry_run --lightflow=examples/quarterly_revenue_brief/lightflow.yaml

# 2. Start execution (runs stages 1-2, then pauses at 'draft_commentary' with exit code 2)
lightflow start --lightflow=examples/quarterly_revenue_brief/lightflow.yaml --log_id=q3

# 3. Supply the reviewed commentary (runs stage 4, then pauses at 'approve_send' with exit code 2)
lightflow resume --log_id=q3 \
  --stage=draft_commentary --resolution=APPROVE \
  --payload='{"approved": true, "commentary": "Q3 revenue grew +12.4% QoQ led by NA and APAC; EMEA trailed plan by 6.2% on Q4 renewal timing (closed Oct 4)."}'

# 4. Approve final delivery (runs stage 6 and exits 0)
lightflow resume --log_id=q3 \
  --stage=approve_send --resolution=APPROVE
```
