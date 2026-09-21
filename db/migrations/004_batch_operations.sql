SET search_path = analystops, public;

CREATE TABLE IF NOT EXISTS batch_runs (
    id uuid PRIMARY KEY,
    client_id uuid NOT NULL REFERENCES clients(id),
    batch_version text NOT NULL,
    status text NOT NULL CHECK (status IN (
        'RUNNING', 'COMPLETED', 'COMPLETED_WITH_FAILURES'
    )),
    selected_records integer NOT NULL CHECK (selected_records >= 0),
    completed_records integer NOT NULL CHECK (
        completed_records >= 0 AND completed_records <= selected_records
    ),
    configuration jsonb NOT NULL,
    summary jsonb NOT NULL,
    report_uri text NOT NULL,
    started_at timestamptz NOT NULL,
    resumed_at timestamptz,
    completed_at timestamptz,
    last_checkpoint_at timestamptz NOT NULL DEFAULT now(),
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (client_id, id)
);

CREATE TABLE IF NOT EXISTS batch_items (
    client_id uuid NOT NULL,
    batch_run_id uuid NOT NULL,
    item_index integer NOT NULL CHECK (item_index >= 0),
    bronze_result_path text NOT NULL,
    bronze_record_hash char(64) CHECK (
        bronze_record_hash IS NULL OR bronze_record_hash ~ '^[0-9a-f]{64}$'
    ),
    file_hash char(64) CHECK (
        file_hash IS NULL OR file_hash ~ '^[0-9a-f]{64}$'
    ),
    workflow_run_id uuid,
    bronze_state text,
    status text NOT NULL,
    publication_state text,
    retryable boolean NOT NULL,
    persisted boolean,
    attempts integer NOT NULL CHECK (attempts >= 0),
    failed_attempts integer NOT NULL CHECK (failed_attempts >= 0),
    escalated boolean NOT NULL,
    input_tokens bigint NOT NULL CHECK (input_tokens >= 0),
    cached_input_tokens bigint NOT NULL CHECK (cached_input_tokens >= 0),
    uncached_input_tokens bigint NOT NULL CHECK (uncached_input_tokens >= 0),
    output_tokens bigint NOT NULL CHECK (output_tokens >= 0),
    reasoning_tokens bigint NOT NULL CHECK (reasoning_tokens >= 0),
    total_tokens bigint NOT NULL CHECK (total_tokens >= 0),
    failure jsonb,
    audit jsonb NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (client_id, batch_run_id, item_index),
    UNIQUE (client_id, batch_run_id, bronze_result_path),
    FOREIGN KEY (client_id, batch_run_id)
        REFERENCES batch_runs(client_id, id) ON DELETE CASCADE,
    FOREIGN KEY (client_id, workflow_run_id)
        REFERENCES workflow_runs(client_id, id)
);

CREATE INDEX IF NOT EXISTS batch_runs_client_status_idx
    ON batch_runs (client_id, status, started_at DESC);
CREATE INDEX IF NOT EXISTS batch_items_client_status_idx
    ON batch_items (client_id, status, updated_at DESC);

ALTER TABLE batch_runs ENABLE ROW LEVEL SECURITY;
ALTER TABLE batch_runs FORCE ROW LEVEL SECURITY;
ALTER TABLE batch_items ENABLE ROW LEVEL SECURITY;
ALTER TABLE batch_items FORCE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS client_isolation ON batch_runs;
CREATE POLICY client_isolation ON batch_runs TO analystops_app
    USING (client_id = current_setting('app.client_id', true)::uuid)
    WITH CHECK (client_id = current_setting('app.client_id', true)::uuid);

DROP POLICY IF EXISTS client_isolation ON batch_items;
CREATE POLICY client_isolation ON batch_items TO analystops_app
    USING (client_id = current_setting('app.client_id', true)::uuid)
    WITH CHECK (client_id = current_setting('app.client_id', true)::uuid);

GRANT SELECT, INSERT, UPDATE, DELETE ON batch_runs, batch_items
    TO analystops_app;
