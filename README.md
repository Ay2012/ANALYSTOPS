# AnalystOps

AnalystOps includes deterministic dataset simulation and Bronze workbook intake
before any AI or agent workflow is added.

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

## PostgreSQL Control Plane

Start the local metadata database:

```bash
colima start
docker compose up -d postgres
```

PostgreSQL stores clients, workbook identities, Bronze evidence, review
resolutions, agent attempts and token usage, transformation plans, Silver run
metadata, validation profiles, workflow audits, and batch operations. Workbook
and canonical data files remain external artifacts referenced by URI.

For an existing local database, apply each new migration once:

```bash
docker compose exec -T postgres psql -U analystops -d analystops \
  -f /docker-entrypoint-initdb.d/002_agent_workflow_audit.sql
docker compose exec -T postgres psql -U analystops -d analystops \
  -f /docker-entrypoint-initdb.d/003_failure_durability.sql
docker compose exec -T postgres psql -U analystops -d analystops \
  -f /docker-entrypoint-initdb.d/004_batch_operations.sql
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
