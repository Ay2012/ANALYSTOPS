SET search_path = analystops, public;

ALTER TABLE agent_runs RENAME COLUMN proposal TO result;
ALTER TABLE agent_runs ALTER COLUMN route DROP NOT NULL;
ALTER TABLE agent_runs DROP CONSTRAINT agent_runs_route_check;
ALTER TABLE agent_runs ADD CONSTRAINT agent_runs_route_check CHECK (
    (status = 'SUCCEEDED' AND route IN ('AUTOMATIC', 'HUMAN'))
    OR (status = 'FAILED' AND route IS NULL)
);

ALTER TABLE workflow_runs DROP CONSTRAINT workflow_runs_status_check;
ALTER TABLE workflow_runs ADD CONSTRAINT workflow_runs_status_check CHECK (
    status IN (
        'RUNNING', 'AWAITING_HUMAN_REVIEW', 'SILVER_PUBLISHABLE',
        'SILVER_REVIEW_REQUIRED', 'FAILED'
    )
);
