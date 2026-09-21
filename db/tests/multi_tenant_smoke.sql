BEGIN;
SET LOCAL search_path = analystops, public;

INSERT INTO clients (id, name) VALUES
    ('00000000-0000-0000-0000-000000000001', 'Client One'),
    ('00000000-0000-0000-0000-000000000002', 'Client Two');

INSERT INTO workbooks (
    id, client_id, file_hash, original_filename, object_uri
) VALUES
    (
        '10000000-0000-0000-0000-000000000001',
        '00000000-0000-0000-0000-000000000001',
        repeat('a', 64),
        'client-one.xlsx',
        's3://analystops/client-one.xlsx'
    ),
    (
        '10000000-0000-0000-0000-000000000002',
        '00000000-0000-0000-0000-000000000002',
        repeat('b', 64),
        'client-two.xlsx',
        's3://analystops/client-two.xlsx'
    );

SET LOCAL ROLE analystops_app;
SET LOCAL app.client_id = '00000000-0000-0000-0000-000000000001';

DO $$
DECLARE
    visible_clients integer;
    visible_workbooks integer;
    cross_tenant_write_blocked boolean := false;
BEGIN
    SELECT count(*) INTO visible_clients FROM clients;
    SELECT count(*) INTO visible_workbooks FROM workbooks;
    IF visible_clients <> 1 OR visible_workbooks <> 1 THEN
        RAISE EXCEPTION 'tenant isolation failed: clients=%, workbooks=%',
            visible_clients, visible_workbooks;
    END IF;

    BEGIN
        INSERT INTO workbooks (
            id, client_id, file_hash, original_filename, object_uri
        ) VALUES (
            '10000000-0000-0000-0000-000000000003',
            '00000000-0000-0000-0000-000000000002',
            repeat('c', 64),
            'forbidden.xlsx',
            's3://analystops/forbidden.xlsx'
        );
    EXCEPTION
        WHEN insufficient_privilege THEN
            cross_tenant_write_blocked := true;
    END;
    IF NOT cross_tenant_write_blocked THEN
        RAISE EXCEPTION 'tenant isolation allowed a cross-client insert';
    END IF;
END
$$;

ROLLBACK;
