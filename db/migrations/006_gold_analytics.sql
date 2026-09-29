SET search_path = analystops, public;

DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'analystops_bi') THEN
        CREATE ROLE analystops_bi LOGIN PASSWORD 'analystops_bi_dev';
    END IF;
END
$$;

ALTER ROLE analystops_bi SET search_path = analystops, public;
ALTER ROLE analystops_bi SET app.client_id = '00000000-0000-0000-0000-000000000001';
GRANT CONNECT ON DATABASE analystops TO analystops_bi;
GRANT USAGE ON SCHEMA analystops TO analystops_bi;

CREATE TABLE IF NOT EXISTS gold_run (
    id uuid PRIMARY KEY,
    client_id uuid NOT NULL REFERENCES clients(id),
    started_at timestamptz NOT NULL,
    completed_at timestamptz,
    status text NOT NULL CHECK (status IN ('RUNNING', 'SUCCESS', 'FAILED')),
    manifest_hash char(64) CHECK (manifest_hash ~ '^[0-9a-f]{64}$'),
    manifest_items integer NOT NULL DEFAULT 0 CHECK (manifest_items >= 0),
    changed_items integer NOT NULL DEFAULT 0 CHECK (changed_items >= 0),
    removed_items integer NOT NULL DEFAULT 0 CHECK (removed_items >= 0),
    loaded_rows bigint NOT NULL DEFAULT 0 CHECK (loaded_rows >= 0),
    error_message text,
    UNIQUE (client_id, id)
);

CREATE TABLE IF NOT EXISTS gold_input_item (
    client_id uuid NOT NULL REFERENCES clients(id),
    source_file_id char(64) NOT NULL CHECK (source_file_id ~ '^[0-9a-f]{64}$'),
    canonical_path text NOT NULL,
    profile_path text NOT NULL,
    canonical_hash char(64) NOT NULL CHECK (canonical_hash ~ '^[0-9a-f]{64}$'),
    profile_hash char(64) NOT NULL CHECK (profile_hash ~ '^[0-9a-f]{64}$'),
    reporting_month text NOT NULL CHECK (reporting_month ~ '^[0-9]{4}-[0-9]{2}$'),
    country text NOT NULL,
    row_count integer NOT NULL CHECK (row_count >= 0),
    PRIMARY KEY (client_id, source_file_id)
);

CREATE TABLE IF NOT EXISTS fact_sales_line (
    client_id uuid NOT NULL,
    source_file_id char(64) NOT NULL,
    source_row_number integer NOT NULL CHECK (source_row_number >= 2),
    invoice_id text NOT NULL,
    product_id text NOT NULL,
    product_description text,
    quantity integer NOT NULL,
    transaction_timestamp timestamp NOT NULL,
    unit_price numeric NOT NULL,
    customer_id text,
    country text NOT NULL,
    PRIMARY KEY (client_id, source_file_id, source_row_number),
    FOREIGN KEY (client_id, source_file_id)
        REFERENCES gold_input_item(client_id, source_file_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS fact_sales_line_client_month_idx
    ON fact_sales_line (client_id, transaction_timestamp, country);
CREATE INDEX IF NOT EXISTS fact_sales_line_client_product_idx
    ON fact_sales_line (client_id, product_id, transaction_timestamp);
CREATE INDEX IF NOT EXISTS fact_sales_line_client_customer_idx
    ON fact_sales_line (client_id, customer_id, transaction_timestamp)
    WHERE customer_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS gold_run_client_completed_idx
    ON gold_run (client_id, completed_at DESC);

ALTER TABLE gold_run ENABLE ROW LEVEL SECURITY;
ALTER TABLE gold_run FORCE ROW LEVEL SECURITY;
ALTER TABLE gold_input_item ENABLE ROW LEVEL SECURITY;
ALTER TABLE gold_input_item FORCE ROW LEVEL SECURITY;
ALTER TABLE fact_sales_line ENABLE ROW LEVEL SECURITY;
ALTER TABLE fact_sales_line FORCE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS client_isolation ON gold_run;
CREATE POLICY client_isolation ON gold_run TO analystops_app, analystops_bi
    USING (client_id = current_setting('app.client_id', true)::uuid)
    WITH CHECK (client_id = current_setting('app.client_id', true)::uuid);
DROP POLICY IF EXISTS client_isolation ON gold_input_item;
CREATE POLICY client_isolation ON gold_input_item TO analystops_app, analystops_bi
    USING (client_id = current_setting('app.client_id', true)::uuid)
    WITH CHECK (client_id = current_setting('app.client_id', true)::uuid);
DROP POLICY IF EXISTS client_isolation ON fact_sales_line;
CREATE POLICY client_isolation ON fact_sales_line TO analystops_app, analystops_bi
    USING (client_id = current_setting('app.client_id', true)::uuid)
    WITH CHECK (client_id = current_setting('app.client_id', true)::uuid);

CREATE OR REPLACE VIEW gold_order WITH (security_invoker = true) AS
SELECT
    client_id,
    source_file_id,
    invoice_id,
    MIN(transaction_timestamp) AS order_timestamp,
    MIN(country) AS country,
    MAX(customer_id) AS customer_id,
    COUNT(*) AS line_count,
    SUM(CASE WHEN unit_price >= 0 THEN quantity * unit_price ELSE 0 END) AS net_revenue,
    SUM(CASE WHEN quantity > 0 AND unit_price >= 0
        THEN quantity * unit_price ELSE 0 END) AS gross_sales,
    COUNT(*) FILTER (WHERE quantity < 0) AS return_lines,
    SUM(CASE WHEN quantity < 0 THEN -quantity ELSE 0 END) AS return_units,
    COUNT(*) FILTER (WHERE quantity < 0 AND UPPER(invoice_id) LIKE 'C%')
        AS cancellation_lines
FROM fact_sales_line
GROUP BY client_id, source_file_id, invoice_id;

CREATE OR REPLACE VIEW gold_monthly_sales WITH (security_invoker = true) AS
SELECT
    client_id,
    TO_CHAR(transaction_timestamp, 'YYYY-MM') AS reporting_month,
    country,
    SUM(CASE WHEN unit_price >= 0 THEN quantity * unit_price ELSE 0 END) AS net_revenue,
    SUM(CASE WHEN quantity > 0 AND unit_price >= 0
        THEN quantity * unit_price ELSE 0 END) AS gross_sales,
    COUNT(DISTINCT (source_file_id, invoice_id)) AS order_count,
    COUNT(DISTINCT customer_id) AS customer_count,
    COUNT(DISTINCT product_id) AS product_count,
    COUNT(*) FILTER (WHERE quantity < 0) AS return_lines,
    SUM(CASE WHEN quantity < 0 THEN -quantity ELSE 0 END) AS return_units,
    COUNT(*) FILTER (WHERE quantity < 0 AND UPPER(invoice_id) LIKE 'C%')
        AS cancellation_lines
FROM fact_sales_line
GROUP BY client_id, TO_CHAR(transaction_timestamp, 'YYYY-MM'), country;

CREATE OR REPLACE VIEW gold_product_monthly WITH (security_invoker = true) AS
SELECT
    client_id,
    TO_CHAR(transaction_timestamp, 'YYYY-MM') AS reporting_month,
    product_id,
    MAX(product_description) AS product_description,
    SUM(CASE WHEN unit_price >= 0 THEN quantity * unit_price ELSE 0 END) AS net_revenue,
    SUM(CASE WHEN quantity > 0 AND unit_price >= 0 THEN quantity ELSE 0 END)
        AS units_sold,
    SUM(CASE WHEN quantity < 0 THEN -quantity ELSE 0 END) AS units_returned,
    COUNT(DISTINCT (source_file_id, invoice_id)) AS order_count,
    COUNT(DISTINCT customer_id) AS customer_count
FROM fact_sales_line
GROUP BY client_id, TO_CHAR(transaction_timestamp, 'YYYY-MM'), product_id;

CREATE OR REPLACE VIEW gold_customer_monthly WITH (security_invoker = true) AS
SELECT
    client_id,
    TO_CHAR(transaction_timestamp, 'YYYY-MM') AS reporting_month,
    customer_id,
    SUM(CASE WHEN unit_price >= 0 THEN quantity * unit_price ELSE 0 END) AS net_revenue,
    COUNT(DISTINCT (source_file_id, invoice_id)) AS order_count,
    COUNT(DISTINCT product_id) AS product_count,
    MAX(transaction_timestamp)::date AS last_order_date
FROM fact_sales_line
WHERE customer_id IS NOT NULL
GROUP BY client_id, TO_CHAR(transaction_timestamp, 'YYYY-MM'), customer_id;

CREATE OR REPLACE VIEW gold_status WITH (security_invoker = true) AS
SELECT
    run.client_id,
    run.id AS run_id,
    run.completed_at AS last_success_at,
    run.manifest_hash,
    run.manifest_items,
    run.changed_items,
    run.removed_items,
    run.loaded_rows,
    (SELECT COUNT(*) FROM fact_sales_line fact
        WHERE fact.client_id = run.client_id) AS current_row_count,
    EXTRACT(EPOCH FROM now() - run.completed_at)::bigint AS age_seconds
FROM gold_run run
WHERE run.status = 'SUCCESS'
  AND run.completed_at = (
      SELECT MAX(latest.completed_at)
      FROM gold_run latest
      WHERE latest.client_id = run.client_id AND latest.status = 'SUCCESS'
  );

GRANT SELECT, INSERT, UPDATE, DELETE ON gold_run, gold_input_item, fact_sales_line
    TO analystops_app;
GRANT SELECT ON gold_order, gold_monthly_sales, gold_product_monthly,
    gold_customer_monthly, gold_status TO analystops_app;
GRANT SELECT ON gold_run, gold_input_item, fact_sales_line, gold_order,
    gold_monthly_sales, gold_product_monthly, gold_customer_monthly, gold_status
    TO analystops_bi;
