# AnalystOps

AnalystOps includes deterministic dataset simulation and Bronze workbook intake
before any AI or agent workflow is added.

Measured architecture, evaluation, cost, failure, human-review, and PostgreSQL
evidence is collected in [the engineering evidence record](docs/engineering_evidence.md).

## Phase One Dataset Flow

The first implemented slice reads the UCI Online Retail II source workbook and
splits it into clean country/month submission workbooks.

```text
data/source/online_retail_II.xlsx
  -> src/analystops/datasets/uci_online_retail.py
  -> src/analystops/datasets/splitter.py
  -> data/generated/clean/<month>/<country>_<month>.xlsx
```

The source workbook is treated as read-only input.

## Generate Clean Submissions

Install the project dependencies, then run:

```bash
PYTHONPATH=src python -m analystops.datasets.splitter --overwrite
```

For a small smoke run:

```bash
PYTHONPATH=src python -m analystops.datasets.splitter --max-files 3 --overwrite
```

Generated files are runtime artifacts and are ignored by git.

## Generate Seeded Corruptions

Once clean submissions exist, create seeded corrupted workbooks with:

```bash
PYTHONPATH=src python -m analystops.datasets.corruptions --seed 42 --max-files 1 --overwrite
```

By default this generates every Phase One corruption scenario for each selected
clean submission under `data/generated/corrupted/`.

## Generate Manifests

Generate clean submissions, corrupted submissions, and JSON answer keys with:

```bash
PYTHONPATH=src python -m analystops.datasets.manifests --seed 42 --max-files 1 --overwrite
```

Manifests are written under `data/manifests/`.

## Phase Two Bronze Intake

Validate one or more workbooks and write their intake evidence:

```bash
PYTHONPATH=src python -m analystops.ingestion.validate workbook.xlsx --output-dir data/ingestion/results
```

Materialize authoritative Bronze records for the complete generated corpus:

```bash
PYTHONPATH=src python -m analystops.ingestion.materialize --workers 4
```

The materializer uses Phase One manifests for workbook paths and row-count
baselines, but never reads their expected handling answer. Persisted records are
policy-versioned, hash-addressed, atomically written, and integrity-checked by
Silver before transformation. Corpus records are written under
`data/ingestion/corpus-results/`, separately from individually reviewed results.

For an `AWAITING_REVIEW` workbook, create a fillable review request:

```bash
PYTHONPATH=src python -m analystops.ingestion.review workbook.xlsx --baseline-row-count 1000
```

After completing its specific resolutions, reassess the unchanged workbook:

```bash
PYTHONPATH=src python -m analystops.ingestion.review workbook.xlsx \
  --baseline-row-count 1000 \
  --resolution data/ingestion/reviews/workbook_<hash>.review.json \
  --output-dir data/ingestion/results
```

Review resolutions are bound to the workbook hash and cannot override quarantine
findings.

## Bronze Remediation Agent

Set `OPENAI_API_KEY`, then request a bounded proposal for one persisted
`AWAITING_REVIEW` Bronze record:

```bash
PYTHONPATH=src python -m analystops.agents.bronze_remediation \
  data/ingestion/corpus-results/corrupted/<scenario>/<record>.json
```

The agent sends only compact review evidence, uses `gpt-5.6-luna` before a
bounded `gpt-5.6-terra` escalation, and writes token usage with the proposal
under `data/agents/proposals/`. Model output is checked against the approved
operation registry. Automatic findings are only labeled; this command never
changes a workbook or approves a human-only operation.

## Silver Remediation Agent

For a persisted `REVIEW_REQUIRED` Silver validation profile, request a bounded
diagnosis and human-approved remediation proposal:

```bash
PYTHONPATH=src python -m analystops.agents.silver_remediation \
  data/validation/silver/profiles/<source-file-id>.json
```

The proposal is written under `data/agents/silver-proposals/` with model
attempts, token usage, latency, and a hash of the assessed validation profile.
It can only select a finding-specific registered action or defer; it never
changes canonical data or publishes a workbook.

Run the same boundary as a persisted LangGraph workflow:

```bash
set -a; source .env; set +a
PYTHONPATH=src python -m analystops.workflows.silver_remediation_graph start \
  data/validation/silver/profiles/<source-file-id>.json \
  --client-id <client-uuid>
```

The audit prints its thread ID and pauses at a durable human interrupt. Resume
that thread in a later process after reviewing the proposed actions:

```bash
PYTHONPATH=src python -m analystops.workflows.silver_remediation_graph resume \
  <thread-id> --decision APPROVE --reviewed-by <reviewer> \
  --client-id <client-uuid>
```

`APPROVE` runs the registered deterministic executor, creates country-month
child submissions, reruns Bronze and Silver validation, and requires exact row
reconciliation. Successful children stop at `READY_FOR_PUBLICATION`; this
workflow does not add them to a publication manifest. PostgreSQL retains the
LangGraph checkpoints across process restarts. The client UUID is incorporated
into the stored thread key so equal public thread UUIDs cannot collide across
tenants. Set `CHECKPOINT_DATABASE_URL` to override the local Compose database.

If execution is interrupted by a process failure, retry the checkpointed node
without replaying approval or calling the model:

```bash
PYTHONPATH=src python -m analystops.workflows.silver_remediation_graph retry \
  <thread-id> --client-id <client-uuid>
```

Repartitioning can expose a high duplicate rate inside a small child. The graph
then pauses again for an explicit `CONFIRM_VALID_DUPLICATES`, `DEDUPLICATE`, or
`REJECT` decision using the same `resume` command.

## Human Review API

Configure one or more local reviewer tokens in `.env`. Each opaque token maps to
the tenant and reviewer identity that the server will use; neither value is
accepted from an HTTP request:

```bash
.venv/bin/python -c 'import secrets; print(secrets.token_urlsafe(32))'
```

```dotenv
ANALYSTOPS_REVIEW_TOKENS='{"<generated-token>":{"client_id":"a599f9da-35b3-5ada-89c4-67ff449294d6","reviewed_by":"ayushgaur"}}'
```

Start the local API:

```bash
set -a; source .env; set +a
PYTHONPATH=src .venv/bin/python -m analystops.api.human_review
```

Open `http://127.0.0.1:8000/docs` for the interactive API, or list pending
reviews directly:

```bash
curl -H "Authorization: Bearer <generated-token>" \
  http://127.0.0.1:8000/v1/reviews
```

The API supports pending-review listing, tenant-scoped evidence inspection, and
registered Silver decisions. Reviewer identity comes from authentication,
competing decisions are serialized with a PostgreSQL advisory lock, and every
successful decision writes the same workflow audit as the CLI. The environment
token registry is a local bootstrap mechanism; a deployed service should verify
tokens issued by an external identity provider.

## Checkpoint Retention And Recovery

Preview the tenant-scoped 30-day retention policy:

```bash
PYTHONPATH=src .venv/bin/python \
  -m analystops.workflows.checkpoint_retention \
  --client-id <client-uuid> \
  --retention-days 30
```

The command is dry-run by default and writes a JSON decision report under
`data/operations/checkpoint-retention/`. It never deletes pending, active,
retryable, unknown, recent, or audit-missing workflows. After reviewing the
report, repeat with `--apply` to delete only eligible terminal checkpoints.

See [the operations runbook](docs/operations_runbook.md) for health checks,
failure recovery, backup boundaries, and the verified recovery drill.

Publish a completed `READY_FOR_PUBLICATION` execution through the atomic gate:

```bash
PYTHONPATH=src python -m analystops.workflows.silver_publication \
  data/silver/remediation/<execution-id>/execution.json \
  --client-id 00000000-0000-0000-0000-000000000001 \
  --client-name "Meridian Retail Group"
```

The gate hashes and rechecks every canonical artifact and profile, verifies row
reconciliation, atomically updates `data/publication/silver/manifest.json`, and
writes an idempotent `publication.json` receipt. PostgreSQL persistence can be
retried from that receipt without republishing files.

## Schema Onboarding Agent

For a readable external workbook quarantined only because required fields are
missing or renamed, draft client-specific mappings and targeted BA questions:

```bash
PYTHONPATH=src python -m analystops.agents.schema_onboarding \
  data/ingestion/external-shadow/<bronze-record>.json
```

The proposal under `data/agents/schema-onboarding/` contains a fillable
`context_template`. Save the completed template separately, then request a
revised draft with:

```bash
PYTHONPATH=src python -m analystops.agents.schema_onboarding \
  data/ingestion/external-shadow/<bronze-record>.json \
  --prior-proposal data/agents/schema-onboarding/<proposal>.json \
  --context data/agents/schema-onboarding/<context>.json
```

All mappings and derivations remain non-executable and require human approval.
Security, unreadable-file, and active-content quarantines never reach this
agent. An approved deterministic adapter and Bronze re-entry are intentionally
separate from the onboarding proposal.

Generate the explicit approval artifact for a proposal that has reached
`READY_FOR_HUMAN_REVIEW`:

```bash
PYTHONPATH=src python -m analystops.workflows.schema_onboarding \
  approval-template <bronze-record.json> <proposal.onboarding.json> \
  --client-id <client-id>
```

After a data owner fills the reviewer, timezone-aware review timestamp, and
changes every intended decision from `PENDING` to `APPROVE`, compile the
contract, adapt a staging copy, and rerun Bronze with:

```bash
PYTHONPATH=src python -m analystops.workflows.schema_onboarding \
  execute <bronze-record.json> <proposal.onboarding.json> <approval.json>
```

The executor verifies the original file hash and schema, supports only the
registered `divide_columns` derivation with `on_zero: BLOCK`, and never modifies
the uploaded workbook. It writes one country-month workbook and Bronze record
per partition; each record carries the approved contract and source lineage.

## Agent Evaluation and Observability

Run a bounded, scenario-balanced evaluation against the hidden corruption
manifests:

```bash
set -a
source .env
set +a
PYTHONPATH=src python -m analystops.agents.evaluate_bronze \
  data/ingestion/corpus-results/corrupted \
  --max-records 10
```

The answer key is loaded only after each agent call. The versioned report under
`data/agents/evaluations/` includes exact-resolution accuracy, routing accuracy,
false automatic approvals, deferrals, retries, model escalations, latency, and
input, cached-input, output, and reasoning token usage. The command exits with
status `2` when any evaluated finding is incorrect or automatically approved
without matching the answer key.

Use the balanced 50-case release gate before changing the prompt, evidence
format, model, or operation policy:

```bash
PYTHONPATH=src python -m analystops.agents.evaluate_bronze \
  data/ingestion/corpus-results/corrupted \
  --release-gate
```

Run the 200-case production gate and compare it with a prior report using:

```bash
PYTHONPATH=src python -m analystops.agents.evaluate_bronze \
  data/ingestion/corpus-results/corrupted \
  --production-gate \
  --baseline-report data/agents/evaluations/<baseline>.json
```

Both gates require equal coverage across the five review scenarios and return
status `2` for any failed case, incorrect finding, or false automatic approval.
Reports include per-scenario quality, latency and token metrics plus deltas by
prompt, proposal schema, Bronze policy, and model versions. The production gate
also runs nightly or manually through
`.github/workflows/agent-production-evaluation.yml`; configure the repository
secret `OPENAI_API_KEY` before enabling it. It is intentionally not run for
ordinary pushes or pull requests.

## Bronze-to-Silver Workflow

Run one authoritative Bronze record through agent remediation, reassessment,
transformation planning, Silver canonicalization, and Silver validation:

```bash
set -a
source .env
set +a
PYTHONPATH=src python -m analystops.workflows.bronze_to_silver \
  data/ingestion/corpus-results/corrupted/<scenario>/<record>.json
```

Automatic findings continue to Silver only after deterministic validation. A
deferred or human-only finding stops with `AWAITING_HUMAN_REVIEW` and writes a
prefilled `human-review.json` beside the workflow report. After an analyst fills
its reviewer, timestamp, and resolutions, resume without another model call:

```bash
PYTHONPATH=src python -m analystops.workflows.bronze_to_silver \
  data/ingestion/corpus-results/corrupted/<scenario>/<record>.json \
  --resolution data/workflows/bronze-to-silver/<run-id>/human-review.json
```

Each run writes a versioned audit record under
`data/workflows/bronze-to-silver/<run-id>/` with the proposal, reviewed Bronze
record, transformation plan, Silver result, validation profile, and final
publication state.

Failed runs still exit nonzero, but first write `workflow.json` with the failed
stage, stable error code, retryability, and any agent attempts and token usage.
When the tenant flags are supplied, the same failure audit is persisted to
PostgreSQL before the command exits.

## Bronze-to-Silver Batch Runner

Run a bounded batch with four workers and persist each workbook independently:

```bash
set -a
source .env
set +a
PYTHONPATH=src python -m analystops.workflows.batch_bronze_to_silver \
  data/ingestion/corpus-results \
  --max-records 10 \
  --workers 4 \
  --client-id 00000000-0000-0000-0000-000000000001 \
  --client-name "Meridian Retail Group"
```

The default hard limit is 10 records. Accepted Bronze records proceed directly
to Silver, review records use the agent, and duplicate or quarantined records
are recorded and skipped. A failure does not stop the remaining workbooks. The
incremental report under `data/workflows/batches/<batch-id>/batch.json` includes
status counts, attempts, escalations, and token usage. With the tenant flags,
every checkpoint is also upserted into PostgreSQL `batch_runs` and
`batch_items`, including per-workbook status, failures, persistence state, and
token usage.

Resume the same selected inputs without rerunning completed workflows:

```bash
PYTHONPATH=src python -m analystops.workflows.batch_bronze_to_silver \
  data/ingestion/corpus-results \
  --max-records 10 \
  --resume data/workflows/batches/<batch-id>/batch.json \
  --client-id 00000000-0000-0000-0000-000000000001 \
  --client-name "Meridian Retail Group"
```

Retryable workflow failures run again on resume. Database-only failures retry
the existing workflow audit without repeating model or Silver work. The command
returns status `2` when the batch completes with failures.

## Gold Analytics

Build or incrementally refresh one tenant's local analytical database from the
authoritative Silver publication manifest:

```bash
PYTHONPATH=src python -m analystops.gold \
  --client-id 00000000-0000-0000-0000-000000000001 \
  --client-name "Meridian Retail Group"
```

The command writes tenant-scoped analytical tables to PostgreSQL. It verifies
published artifact hashes when supplied, canonical row counts, lineage, and
Silver net-revenue and gross-sales totals before committing. Changed rows are
streamed through a temporary staging table, unchanged inputs are skipped,
removed publications are deleted, and a failed refresh preserves the last
successful facts. PostgreSQL row-level security isolates every Gold table.

The database exposes `fact_sales_line`, `gold_order`, `gold_monthly_sales`,
`gold_product_monthly`, `gold_customer_monthly`, and `gold_status`. Revenue is
`quantity * unit_price`; returns remain negative in net revenue, gross sales
include positive-quantity sales, and recognized bad-debt or unrecognized
negative-price adjustments are excluded. Discounts and profit are unavailable
because the canonical Silver contract does not contain those fields.

The retained corpus verification loaded 679 publication items and 1,055,041
facts, produced 58,739 orders, 78,321 product-month rows, and 31,685
customer-month rows, and completed an unchanged rerun without reloading facts.

## Metabase Dashboard

Start PostgreSQL, apply the Gold migration, build Gold, then start the
self-hosted Metabase Open Source service:

```bash
docker compose up -d postgres
docker compose exec -T postgres psql -U analystops -d analystops \
  -f /docker-entrypoint-initdb.d/006_gold_analytics.sql
PYTHONPATH=src python -m analystops.gold \
  --client-id 00000000-0000-0000-0000-000000000001 \
  --client-name "Meridian Retail Group"
docker compose up -d metabase
```

Open `http://127.0.0.1:3000`, complete the one-time administrator setup, and
add a PostgreSQL database using host `postgres`, port `5432`, database
`analystops`, username `analystops_bi`, and password `analystops_bi_dev`.
The local BI role is read-only and its PostgreSQL session default is pinned to
the demo tenant through the same row-level security policy used by the loader.

Build dashboard cards from `gold_status`, `gold_monthly_sales`,
`gold_product_monthly`, and `gold_customer_monthly`. Metabase stores its local
questions and dashboards in the `analystops-metabase` Docker volume. Its local
H2 application database is appropriate for the portfolio environment; deploy
Metabase with its own PostgreSQL application database and secret-managed BI
credentials before treating the dashboard service as production.
Set `METABASE_PORT` or `METABASE_TIMEZONE` to override the local defaults.

## PostgreSQL Control Plane

Start the local metadata database:

```bash
colima start
docker compose up -d postgres
```

PostgreSQL stores clients, workbook identities, Bronze evidence, review
resolutions, agent attempts and token usage, transformation plans, Silver run
metadata, validation profiles, workflow audits, batch operations, and
publication receipts. Workbook and canonical data files remain external
artifacts referenced by URI.

For an existing local database, apply each new migration once:

```bash
docker compose exec -T postgres psql -U analystops -d analystops \
  -f /docker-entrypoint-initdb.d/002_agent_workflow_audit.sql
docker compose exec -T postgres psql -U analystops -d analystops \
  -f /docker-entrypoint-initdb.d/003_failure_durability.sql
docker compose exec -T postgres psql -U analystops -d analystops \
  -f /docker-entrypoint-initdb.d/004_batch_operations.sql
docker compose exec -T postgres psql -U analystops -d analystops \
  -f /docker-entrypoint-initdb.d/005_silver_publication.sql
docker compose exec -T postgres psql -U analystops -d analystops \
  -f /docker-entrypoint-initdb.d/006_gold_analytics.sql
```

Verify the schema and row-level tenant isolation:

```bash
docker compose exec -T postgres psql -U analystops -d analystops \
  < db/tests/multi_tenant_smoke.sql
```

The defaults in `compose.yaml` are for local development and can be overridden
with `POSTGRES_DB`, `POSTGRES_USER`, `POSTGRES_PASSWORD`, and `POSTGRES_PORT`.

Persist one Bronze result with a stable client UUID:

```bash
PYTHONPATH=src python -m analystops.persistence.postgres \
  data/ingestion/results/workbook_<hash>.json \
  --client-id 00000000-0000-0000-0000-000000000001 \
  --client-name "Meridian Retail Group"
```

The command also accepts directories for idempotent corpus backfills. Set
`DATABASE_URL` to override the local Compose connection.

To persist a Bronze-to-Silver run and its full audit trail in the same command,
add the stable tenant identity:

```bash
PYTHONPATH=src python -m analystops.workflows.bronze_to_silver \
  data/ingestion/corpus-results/corrupted/<scenario>/<record>.json \
  --client-id 00000000-0000-0000-0000-000000000001 \
  --client-name "Meridian Retail Group"
```
