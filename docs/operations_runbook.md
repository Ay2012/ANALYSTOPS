# AnalystOps Workflow Operations Runbook

Last verified: 2026-09-27

## Scope

This runbook covers the PostgreSQL-backed Silver remediation graph, human review
API, checkpoint retention, and recovery of interrupted workflow decisions. It
does not claim a production backup SLA, multi-region recovery, or automated
failover.

## Checkpoint Retention Policy

The implemented policy is `checkpoint-retention-v1`:

| Workflow state | Retention action |
| --- | --- |
| Pending human interrupt | Never delete |
| Active or retryable node | Never delete |
| Unknown/non-terminal status | Never delete |
| Terminal but inside retention window | Keep |
| Terminal but missing workflow audit | Keep and investigate |
| Audited terminal state older than 30 days | Eligible for deletion |

Retention is tenant-scoped and dry-run by default. Deletion removes checkpoint
writes, blobs, and snapshots for the selected workflow thread. The retained
workflow audit and canonical data artifacts remain. A deleted checkpoint cannot
be resumed and must be recovered from a database backup if it was removed in
error.

Run the report first:

```bash
PYTHONPATH=src .venv/bin/python \
  -m analystops.workflows.checkpoint_retention \
  --client-id <client-uuid> \
  --retention-days 30
```

Review every `ELIGIBLE` decision in the emitted JSON report. Apply the same
policy only after that review:

```bash
PYTHONPATH=src .venv/bin/python \
  -m analystops.workflows.checkpoint_retention \
  --client-id <client-uuid> \
  --retention-days 30 \
  --apply
```

Run this weekly per active tenant. Keep reports under
`data/operations/checkpoint-retention/` as change evidence. Never automate
`--apply` until database backups and alert ownership exist outside this local
project.

## Routine Health Checks

Confirm PostgreSQL is healthy:

```bash
docker compose ps postgres
```

Confirm the review API is alive:

```bash
curl http://127.0.0.1:8000/health
```

Inspect checkpoint volume:

```sql
SELECT COUNT(DISTINCT thread_id) AS threads FROM analystops.checkpoints;
SELECT COUNT(*) AS snapshots FROM analystops.checkpoints;
SELECT COUNT(*) AS blobs FROM analystops.checkpoint_blobs;
SELECT COUNT(*) AS writes FROM analystops.checkpoint_writes;
```

List pending work through the authenticated API rather than querying serialized
checkpoint payloads directly:

```bash
curl -H "Authorization: Bearer <review-token>" \
  http://127.0.0.1:8000/v1/reviews
```

## Recovery Procedures

### API process stopped

1. Confirm PostgreSQL is healthy.
2. Reload `.env` and restart `analystops.api.human_review`.
3. Call `/health`, then list pending reviews.
4. Do not replay model calls or create replacement thread IDs. PostgreSQL
   checkpoints retain the original interrupt state.

### Human decision request failed or timed out

1. Fetch `GET /v1/reviews/<thread-id>` with the same tenant token.
2. If `pending` is still `true`, the decision was not committed and may be
   submitted again.
3. If `pending` is `false`, treat the first request as committed and inspect its
   workflow audit. A repeated decision returns HTTP 409.

### Worker stopped during deterministic remediation

Use the same client and public thread UUID:

```bash
PYTHONPATH=src .venv/bin/python \
  -m analystops.workflows.silver_remediation_graph retry \
  <thread-id> --client-id <client-uuid>
```

The retry command resumes the checkpointed node. Do not start a new thread,
approve again, or call the model again.

### Checkpoint not found

1. Verify the authenticated tenant/client UUID and public thread UUID.
2. Check the tenant-scoped storage key:

```sql
SELECT thread_id, checkpoint->>'ts' AS updated_at
FROM analystops.checkpoints
WHERE thread_id = '<client-uuid>:<thread-uuid>'
ORDER BY checkpoint->>'ts' DESC;
```

3. Inspect `data/workflows/silver-remediation/<client-uuid>/<thread-uuid>/workflow.json`.
4. Check retained checkpoint cleanup reports for the thread UUID.
5. If retention deleted the thread, stop recovery and restore PostgreSQL into a
   separate database from the latest known-good backup. Do not synthesize graph
   state from the JSON audit.

### PostgreSQL unavailable

1. Stop review decisions and retention jobs.
2. Check `docker compose ps postgres` and PostgreSQL logs.
3. Restart the database only after confirming storage is present.
4. Verify checkpoint and operational table counts before restarting the API.
5. For data loss, restore the full control-plane database into a separate
   database and validate tenant counts before any cutover.

## Backup Boundary

The local Compose volume is persistence, not a backup. Before scheduling
destructive retention, production infrastructure must provide encrypted,
off-host PostgreSQL backups, tested restore procedures, retention ownership,
and monitoring. No RPO or RTO is claimed until those controls exist and a timed
restore drill has been measured.

## Verified Recovery Drill

Run the automated PostgreSQL drill:

```bash
TEST_DATABASE_URL=postgresql://analystops:analystops_dev@127.0.0.1:5432/analystops \
PYTHONPATH=src .venv/bin/python -m unittest \
  tests.integration.test_checkpoint_retention \
  tests.integration.test_human_review_api \
  tests.integration.test_silver_remediation_graph
```

The drill verifies process restart and resume, cross-tenant isolation, one model
call across interruption and recovery, duplicate-decision rejection, dry-run
retention, preservation of pending reviews, and deletion of only an audited
terminal checkpoint.
