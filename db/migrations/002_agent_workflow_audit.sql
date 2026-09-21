SET search_path = analystops, public;

CREATE UNIQUE INDEX IF NOT EXISTS silver_runs_client_id_id_idx
    ON silver_runs (client_id, id);

CREATE TABLE IF NOT EXISTS agent_runs (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    client_id uuid NOT NULL,
    bronze_run_id uuid NOT NULL,
    run_id uuid NOT NULL,
    prompt_version text NOT NULL,
    schema_version text NOT NULL,
    policy_version text NOT NULL,
    status text NOT NULL CHECK (status IN ('SUCCEEDED', 'FAILED')),
    route text NOT NULL CHECK (route IN ('AUTOMATIC', 'HUMAN')),
    latency_ms numeric(14, 3) NOT NULL CHECK (latency_ms >= 0),
    input_tokens bigint NOT NULL DEFAULT 0 CHECK (input_tokens >= 0),
    cached_input_tokens bigint NOT NULL DEFAULT 0 CHECK (cached_input_tokens >= 0),
    output_tokens bigint NOT NULL DEFAULT 0 CHECK (output_tokens >= 0),
    reasoning_tokens bigint NOT NULL DEFAULT 0 CHECK (reasoning_tokens >= 0),
    proposal jsonb NOT NULL,
    started_at timestamptz NOT NULL,
    completed_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (client_id, id),
    UNIQUE (client_id, run_id),
    FOREIGN KEY (client_id, bronze_run_id) REFERENCES bronze_runs(client_id, id)
);

CREATE TABLE IF NOT EXISTS agent_attempts (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    client_id uuid NOT NULL,
    agent_run_id uuid NOT NULL,
    attempt_number integer NOT NULL CHECK (attempt_number > 0),
    model text NOT NULL,
    status text NOT NULL CHECK (status IN ('SUCCEEDED', 'FAILED')),
    latency_ms numeric(14, 3) NOT NULL CHECK (latency_ms >= 0),
    input_tokens bigint NOT NULL DEFAULT 0 CHECK (input_tokens >= 0),
    cached_input_tokens bigint NOT NULL DEFAULT 0 CHECK (cached_input_tokens >= 0),
    output_tokens bigint NOT NULL DEFAULT 0 CHECK (output_tokens >= 0),
    reasoning_tokens bigint NOT NULL DEFAULT 0 CHECK (reasoning_tokens >= 0),
    response_id text,
    error text,
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (client_id, agent_run_id, attempt_number),
    FOREIGN KEY (client_id, agent_run_id) REFERENCES agent_runs(client_id, id)
);

CREATE TABLE IF NOT EXISTS workflow_runs (
    id uuid PRIMARY KEY,
    client_id uuid NOT NULL,
    workbook_id uuid NOT NULL,
    initial_bronze_run_id uuid NOT NULL,
    resolved_bronze_run_id uuid,
    agent_run_id uuid,
    transformation_plan_id uuid,
    silver_run_id uuid,
    workflow_version text NOT NULL,
    status text NOT NULL CHECK (status IN (
        'RUNNING', 'AWAITING_HUMAN_REVIEW', 'SILVER_PUBLISHABLE',
        'SILVER_REVIEW_REQUIRED'
    )),
    publication_state text,
    validation_profile jsonb,
    audit jsonb NOT NULL,
    started_at timestamptz NOT NULL,
    completed_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (client_id, id),
    FOREIGN KEY (client_id, workbook_id) REFERENCES workbooks(client_id, id),
    FOREIGN KEY (client_id, initial_bronze_run_id)
        REFERENCES bronze_runs(client_id, id),
    FOREIGN KEY (client_id, resolved_bronze_run_id)
        REFERENCES bronze_runs(client_id, id),
    FOREIGN KEY (client_id, agent_run_id) REFERENCES agent_runs(client_id, id),
    FOREIGN KEY (client_id, transformation_plan_id)
        REFERENCES transformation_plans(client_id, id),
    FOREIGN KEY (client_id, silver_run_id) REFERENCES silver_runs(client_id, id)
);

CREATE INDEX IF NOT EXISTS agent_runs_client_created_idx
    ON agent_runs (client_id, created_at DESC);
CREATE INDEX IF NOT EXISTS agent_attempts_client_model_idx
    ON agent_attempts (client_id, model, created_at DESC);
CREATE INDEX IF NOT EXISTS workflow_runs_client_status_idx
    ON workflow_runs (client_id, status, created_at DESC);

ALTER TABLE agent_runs ENABLE ROW LEVEL SECURITY;
ALTER TABLE agent_runs FORCE ROW LEVEL SECURITY;
ALTER TABLE agent_attempts ENABLE ROW LEVEL SECURITY;
ALTER TABLE agent_attempts FORCE ROW LEVEL SECURITY;
ALTER TABLE workflow_runs ENABLE ROW LEVEL SECURITY;
ALTER TABLE workflow_runs FORCE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS client_isolation ON agent_runs;
CREATE POLICY client_isolation ON agent_runs TO analystops_app
    USING (client_id = current_setting('app.client_id', true)::uuid)
    WITH CHECK (client_id = current_setting('app.client_id', true)::uuid);

DROP POLICY IF EXISTS client_isolation ON agent_attempts;
CREATE POLICY client_isolation ON agent_attempts TO analystops_app
    USING (client_id = current_setting('app.client_id', true)::uuid)
    WITH CHECK (client_id = current_setting('app.client_id', true)::uuid);

DROP POLICY IF EXISTS client_isolation ON workflow_runs;
CREATE POLICY client_isolation ON workflow_runs TO analystops_app
    USING (client_id = current_setting('app.client_id', true)::uuid)
    WITH CHECK (client_id = current_setting('app.client_id', true)::uuid);

GRANT SELECT, INSERT, UPDATE, DELETE ON agent_runs, agent_attempts, workflow_runs
    TO analystops_app;
