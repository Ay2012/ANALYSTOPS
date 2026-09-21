CREATE EXTENSION IF NOT EXISTS pgcrypto;

DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'analystops_app') THEN
        CREATE ROLE analystops_app NOLOGIN;
    END IF;
END
$$;

CREATE SCHEMA IF NOT EXISTS analystops;
GRANT USAGE ON SCHEMA analystops TO analystops_app;
ALTER ROLE analystops_app SET search_path = analystops, public;
SET search_path = analystops, public;

CREATE TABLE IF NOT EXISTS clients (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    name text NOT NULL,
    status text NOT NULL DEFAULT 'ACTIVE'
        CHECK (status IN ('ACTIVE', 'SUSPENDED')),
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS workbooks (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    client_id uuid NOT NULL REFERENCES clients(id),
    file_hash char(64) NOT NULL CHECK (file_hash ~ '^[0-9a-f]{64}$'),
    original_filename text NOT NULL,
    object_uri text NOT NULL,
    received_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (client_id, id),
    UNIQUE (client_id, file_hash)
);

CREATE TABLE IF NOT EXISTS bronze_runs (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    client_id uuid NOT NULL,
    workbook_id uuid NOT NULL,
    policy_version text NOT NULL,
    lifecycle_state text NOT NULL
        CHECK (lifecycle_state IN (
            'BRONZE_ACCEPTED', 'AWAITING_REVIEW', 'QUARANTINED', 'DUPLICATE'
        )),
    quality_disposition text NOT NULL
        CHECK (quality_disposition IN ('PASS', 'WARN', 'REVIEW', 'BLOCK')),
    decision text NOT NULL,
    record_hash char(64) NOT NULL CHECK (record_hash ~ '^[0-9a-f]{64}$'),
    evidence jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (client_id, id),
    UNIQUE (client_id, record_hash),
    FOREIGN KEY (client_id, workbook_id) REFERENCES workbooks(client_id, id)
);

CREATE TABLE IF NOT EXISTS findings (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    client_id uuid NOT NULL,
    bronze_run_id uuid NOT NULL,
    code text NOT NULL,
    quality_disposition text NOT NULL
        CHECK (quality_disposition IN ('PASS', 'WARN', 'REVIEW', 'BLOCK')),
    evidence jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (client_id, id),
    FOREIGN KEY (client_id, bronze_run_id) REFERENCES bronze_runs(client_id, id)
);

CREATE TABLE IF NOT EXISTS review_resolutions (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    client_id uuid NOT NULL,
    finding_id uuid NOT NULL,
    action text NOT NULL,
    details jsonb NOT NULL DEFAULT '{}'::jsonb,
    note text NOT NULL DEFAULT '',
    reviewed_by text NOT NULL,
    reviewed_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    FOREIGN KEY (client_id, finding_id) REFERENCES findings(client_id, id)
);

ALTER TABLE review_resolutions
    ADD COLUMN IF NOT EXISTS note text NOT NULL DEFAULT '';

CREATE TABLE IF NOT EXISTS transformation_plans (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    client_id uuid NOT NULL,
    bronze_run_id uuid NOT NULL,
    plan_version text NOT NULL,
    plan_hash char(64) NOT NULL CHECK (plan_hash ~ '^[0-9a-f]{64}$'),
    plan jsonb NOT NULL,
    approval_status text NOT NULL DEFAULT 'PENDING'
        CHECK (approval_status IN ('PENDING', 'APPROVED', 'REJECTED')),
    approved_by text,
    approved_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (client_id, id),
    UNIQUE (client_id, plan_hash),
    FOREIGN KEY (client_id, bronze_run_id) REFERENCES bronze_runs(client_id, id),
    CHECK (
        (approval_status = 'APPROVED' AND approved_by IS NOT NULL AND approved_at IS NOT NULL)
        OR approval_status <> 'APPROVED'
    )
);

CREATE TABLE IF NOT EXISTS silver_runs (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    client_id uuid NOT NULL,
    bronze_run_id uuid NOT NULL,
    transformation_plan_id uuid,
    transformation_version text NOT NULL,
    status text NOT NULL CHECK (status IN ('RUNNING', 'SUCCEEDED', 'FAILED')),
    input_rows bigint NOT NULL DEFAULT 0 CHECK (input_rows >= 0),
    accepted_rows bigint NOT NULL DEFAULT 0 CHECK (accepted_rows >= 0),
    rejected_rows bigint NOT NULL DEFAULT 0 CHECK (rejected_rows >= 0),
    dropped_rows bigint NOT NULL DEFAULT 0 CHECK (dropped_rows >= 0),
    accepted_artifact_uri text,
    rejected_artifact_uri text,
    error_message text,
    created_at timestamptz NOT NULL DEFAULT now(),
    completed_at timestamptz,
    UNIQUE (client_id, id),
    FOREIGN KEY (client_id, bronze_run_id) REFERENCES bronze_runs(client_id, id),
    FOREIGN KEY (client_id, transformation_plan_id)
        REFERENCES transformation_plans(client_id, id),
    CHECK (
        status <> 'SUCCEEDED'
        OR input_rows = accepted_rows + rejected_rows + dropped_rows
    )
);

CREATE INDEX IF NOT EXISTS workbooks_client_received_idx
    ON workbooks (client_id, received_at DESC);
CREATE INDEX IF NOT EXISTS bronze_runs_client_state_idx
    ON bronze_runs (client_id, lifecycle_state, created_at DESC);
CREATE INDEX IF NOT EXISTS findings_client_code_idx
    ON findings (client_id, code, created_at DESC);
CREATE INDEX IF NOT EXISTS transformation_plans_client_status_idx
    ON transformation_plans (client_id, approval_status, created_at DESC);
CREATE INDEX IF NOT EXISTS silver_runs_client_status_idx
    ON silver_runs (client_id, status, created_at DESC);

ALTER TABLE clients ENABLE ROW LEVEL SECURITY;
ALTER TABLE clients FORCE ROW LEVEL SECURITY;
ALTER TABLE workbooks ENABLE ROW LEVEL SECURITY;
ALTER TABLE workbooks FORCE ROW LEVEL SECURITY;
ALTER TABLE bronze_runs ENABLE ROW LEVEL SECURITY;
ALTER TABLE bronze_runs FORCE ROW LEVEL SECURITY;
ALTER TABLE findings ENABLE ROW LEVEL SECURITY;
ALTER TABLE findings FORCE ROW LEVEL SECURITY;
ALTER TABLE review_resolutions ENABLE ROW LEVEL SECURITY;
ALTER TABLE review_resolutions FORCE ROW LEVEL SECURITY;
ALTER TABLE transformation_plans ENABLE ROW LEVEL SECURITY;
ALTER TABLE transformation_plans FORCE ROW LEVEL SECURITY;
ALTER TABLE silver_runs ENABLE ROW LEVEL SECURITY;
ALTER TABLE silver_runs FORCE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS client_isolation ON clients;
CREATE POLICY client_isolation ON clients TO analystops_app
    USING (id = current_setting('app.client_id', true)::uuid)
    WITH CHECK (id = current_setting('app.client_id', true)::uuid);

DROP POLICY IF EXISTS client_isolation ON workbooks;
CREATE POLICY client_isolation ON workbooks TO analystops_app
    USING (client_id = current_setting('app.client_id', true)::uuid)
    WITH CHECK (client_id = current_setting('app.client_id', true)::uuid);

DROP POLICY IF EXISTS client_isolation ON bronze_runs;
CREATE POLICY client_isolation ON bronze_runs TO analystops_app
    USING (client_id = current_setting('app.client_id', true)::uuid)
    WITH CHECK (client_id = current_setting('app.client_id', true)::uuid);

DROP POLICY IF EXISTS client_isolation ON findings;
CREATE POLICY client_isolation ON findings TO analystops_app
    USING (client_id = current_setting('app.client_id', true)::uuid)
    WITH CHECK (client_id = current_setting('app.client_id', true)::uuid);

DROP POLICY IF EXISTS client_isolation ON review_resolutions;
CREATE POLICY client_isolation ON review_resolutions TO analystops_app
    USING (client_id = current_setting('app.client_id', true)::uuid)
    WITH CHECK (client_id = current_setting('app.client_id', true)::uuid);

DROP POLICY IF EXISTS client_isolation ON transformation_plans;
CREATE POLICY client_isolation ON transformation_plans TO analystops_app
    USING (client_id = current_setting('app.client_id', true)::uuid)
    WITH CHECK (client_id = current_setting('app.client_id', true)::uuid);

DROP POLICY IF EXISTS client_isolation ON silver_runs;
CREATE POLICY client_isolation ON silver_runs TO analystops_app
    USING (client_id = current_setting('app.client_id', true)::uuid)
    WITH CHECK (client_id = current_setting('app.client_id', true)::uuid);

GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA analystops
    TO analystops_app;
ALTER DEFAULT PRIVILEGES IN SCHEMA analystops
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO analystops_app;
