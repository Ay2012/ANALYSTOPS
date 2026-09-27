SET search_path = analystops, public;

CREATE TABLE IF NOT EXISTS publication_runs (
    id uuid PRIMARY KEY,
    client_id uuid NOT NULL REFERENCES clients(id),
    execution_id uuid NOT NULL,
    publication_version text NOT NULL,
    status text NOT NULL CHECK (status = 'PUBLISHED'),
    published_workbooks integer NOT NULL CHECK (published_workbooks > 0),
    published_rows bigint NOT NULL CHECK (published_rows >= 0),
    dropped_rows bigint NOT NULL CHECK (dropped_rows >= 0),
    manifest_uri text NOT NULL,
    manifest_hash char(64) NOT NULL CHECK (manifest_hash ~ '^[0-9a-f]{64}$'),
    receipt jsonb NOT NULL,
    published_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (client_id, id),
    UNIQUE (client_id, execution_id)
);

CREATE TABLE IF NOT EXISTS publication_items (
    client_id uuid NOT NULL,
    publication_run_id uuid NOT NULL,
    source_file_id char(64) NOT NULL CHECK (source_file_id ~ '^[0-9a-f]{64}$'),
    parent_source_file_id char(64) NOT NULL CHECK (
        parent_source_file_id ~ '^[0-9a-f]{64}$'
    ),
    reporting_month text NOT NULL CHECK (reporting_month ~ '^[0-9]{4}-[0-9]{2}$'),
    country text NOT NULL,
    row_count integer NOT NULL CHECK (row_count >= 0),
    canonical_artifact_uri text NOT NULL,
    profile_uri text NOT NULL,
    canonical_hash char(64) NOT NULL CHECK (canonical_hash ~ '^[0-9a-f]{64}$'),
    profile_hash char(64) NOT NULL CHECK (profile_hash ~ '^[0-9a-f]{64}$'),
    PRIMARY KEY (client_id, publication_run_id, source_file_id),
    FOREIGN KEY (client_id, publication_run_id)
        REFERENCES publication_runs(client_id, id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS publication_runs_client_published_idx
    ON publication_runs (client_id, published_at DESC);
CREATE INDEX IF NOT EXISTS publication_items_client_period_idx
    ON publication_items (client_id, reporting_month, country);

ALTER TABLE publication_runs ENABLE ROW LEVEL SECURITY;
ALTER TABLE publication_runs FORCE ROW LEVEL SECURITY;
ALTER TABLE publication_items ENABLE ROW LEVEL SECURITY;
ALTER TABLE publication_items FORCE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS client_isolation ON publication_runs;
CREATE POLICY client_isolation ON publication_runs TO analystops_app
    USING (client_id = current_setting('app.client_id', true)::uuid)
    WITH CHECK (client_id = current_setting('app.client_id', true)::uuid);

DROP POLICY IF EXISTS client_isolation ON publication_items;
CREATE POLICY client_isolation ON publication_items TO analystops_app
    USING (client_id = current_setting('app.client_id', true)::uuid)
    WITH CHECK (client_id = current_setting('app.client_id', true)::uuid);

GRANT SELECT, INSERT, UPDATE, DELETE ON publication_runs, publication_items
    TO analystops_app;
