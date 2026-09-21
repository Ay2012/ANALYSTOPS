from __future__ import annotations

import json
import os
import tempfile
import unittest
from pathlib import Path
from uuid import uuid4

import psycopg
import pandas as pd

from tests.helpers import write_clean_submission
from tests.integration.test_end_to_end_workflow import (
    FakeClient,
    FailingClient,
    resolution,
)
from analystops.ingestion.review import reassess_workbook
from analystops.ingestion.validate import validate_workbook, write_result
from analystops.persistence.postgres import (
    persist_bronze_result,
    persist_workflow_result,
)
from analystops.workflows.bronze_to_silver import (
    BronzeToSilverWorkflowError,
    run_bronze_to_silver,
)
from analystops.workflows.batch_bronze_to_silver import (
    run_bronze_to_silver_batch,
)


TEST_DATABASE_URL = os.environ.get("TEST_DATABASE_URL")


@unittest.skipUnless(TEST_DATABASE_URL, "TEST_DATABASE_URL is not configured")
class PostgresPersistenceTests(unittest.TestCase):
    def test_persists_reviewed_bronze_result_idempotently_per_client(self) -> None:
        client_id = uuid4()
        other_client_id = uuid4()
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            workbook = root / "reviewed.xlsx"
            write_clean_submission(workbook)
            initial = validate_workbook(workbook, baseline_row_count=10)
            accepted = reassess_workbook(
                workbook,
                {
                    "file_hash": initial.file_hash,
                    "reviewed_by": "analyst@example.com",
                    "reviewed_at": "2026-09-19T12:00:00Z",
                    "resolutions": [
                        {
                            "finding_code": "unexpectedly_low_row_count",
                            "action": "confirm_expected_volume",
                            "note": "Expected for this period.",
                        }
                    ],
                },
                baseline_row_count=10,
            )
            result_path = write_result(accepted, root / "bronze")

            with psycopg.connect(TEST_DATABASE_URL, autocommit=True) as connection:
                try:
                    first = persist_bronze_result(
                        connection,
                        result_path,
                        client_id=client_id,
                        client_name="Persistence Test Client",
                    )
                    second = persist_bronze_result(
                        connection,
                        result_path,
                        client_id=client_id,
                        client_name="Persistence Test Client",
                    )
                    own_counts = self._tenant_counts(connection, client_id)
                    other_counts = self._tenant_counts(connection, other_client_id)
                finally:
                    self._delete_client(connection, client_id)

        self.assertTrue(first.inserted)
        self.assertEqual(first.findings_written, 1)
        self.assertEqual(first.resolutions_written, 1)
        self.assertFalse(second.inserted)
        self.assertEqual(second.bronze_run_id, first.bronze_run_id)
        self.assertEqual(own_counts, (1, 1, 1, 1, 1))
        self.assertEqual(other_counts, (0, 0, 0, 0, 0))

    def test_persists_complete_workflow_idempotently(self) -> None:
        client_id = uuid4()
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            workbook = (
                root
                / "generated"
                / "corrupted"
                / "renamed_columns"
                / "united_kingdom_2011-01_renamed_columns_seed1.xlsx"
            )
            write_clean_submission(workbook)
            rows = pd.read_excel(workbook, sheet_name="Transactions")
            rows.rename(columns={"Quantity": "Units"}).to_excel(
                workbook, sheet_name="Transactions", index=False
            )
            bronze_path = write_result(validate_workbook(workbook), root / "bronze")
            workflow_path = run_bronze_to_silver(
                bronze_path,
                client=FakeClient(
                    {
                        "resolutions": [
                            resolution(
                                "renamed_required_columns",
                                decision="PROPOSE",
                                action="map_columns",
                                mapping=[
                                    {"source": "Units", "target": "Quantity"}
                                ],
                            )
                        ]
                    }
                ),
                output_dir=root / "workflows",
                silver_dir=root / "silver",
                validation_dir=root / "validation",
            )

            with psycopg.connect(TEST_DATABASE_URL, autocommit=True) as connection:
                try:
                    first = persist_workflow_result(
                        connection,
                        workflow_path,
                        client_id=client_id,
                        client_name="Workflow Test Client",
                    )
                    second = persist_workflow_result(
                        connection,
                        workflow_path,
                        client_id=client_id,
                        client_name="Workflow Test Client",
                    )
                    counts = self._workflow_counts(connection, client_id)
                finally:
                    self._delete_client(connection, client_id)

        self.assertTrue(first.inserted)
        self.assertFalse(second.inserted)
        self.assertEqual(second.workflow_run_id, first.workflow_run_id)
        self.assertEqual(counts, (1, 1, 1, 1, 1))

    def test_persists_failed_workflow_and_agent_attempt(self) -> None:
        client_id = uuid4()
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            workbook = (
                root
                / "generated"
                / "corrupted"
                / "renamed_columns"
                / "united_kingdom_2011-01_renamed_columns_seed1.xlsx"
            )
            write_clean_submission(workbook)
            rows = pd.read_excel(workbook, sheet_name="Transactions")
            rows.rename(columns={"Quantity": "Units"}).to_excel(
                workbook, sheet_name="Transactions", index=False
            )
            bronze_path = write_result(validate_workbook(workbook), root / "bronze")
            with self.assertRaises(BronzeToSilverWorkflowError) as raised:
                run_bronze_to_silver(
                    bronze_path,
                    client=FailingClient(),
                    output_dir=root / "workflows",
                    silver_dir=root / "silver",
                    validation_dir=root / "validation",
                    primary_attempts=1,
                    escalation_model=None,
                )

            with psycopg.connect(TEST_DATABASE_URL, autocommit=True) as connection:
                try:
                    persisted = persist_workflow_result(
                        connection,
                        raised.exception.report_path,
                        client_id=client_id,
                        client_name="Failed Workflow Test Client",
                    )
                    counts = self._workflow_counts(connection, client_id)
                    statuses = self._failure_statuses(connection, client_id)
                finally:
                    self._delete_client(connection, client_id)

        self.assertTrue(persisted.inserted)
        self.assertEqual(counts, (1, 1, 0, 0, 1))
        self.assertEqual(statuses, ("FAILED", "FAILED", "AGENT_REMEDIATION"))

    def test_batch_persists_each_completed_or_skipped_workbook(self) -> None:
        client_id = uuid4()
        other_client_id = uuid4()
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            workbook = (
                root
                / "generated"
                / "clean"
                / "united_kingdom_2011-01.xlsx"
            )
            write_clean_submission(workbook)
            accepted = validate_workbook(workbook)
            accepted_path = write_result(accepted, root / "bronze-accepted")
            duplicate_path = write_result(
                validate_workbook(
                    workbook,
                    seen_hashes={str(accepted.file_hash)},
                ),
                root / "bronze-duplicate",
            )

            with psycopg.connect(TEST_DATABASE_URL, autocommit=True) as connection:
                try:
                    report_path = run_bronze_to_silver_batch(
                        [accepted_path, duplicate_path],
                        output_dir=root / "batches",
                        workflow_output_dir=root / "workflows",
                        silver_dir=root / "silver",
                        validation_dir=root / "validation",
                        workers=2,
                        connection=connection,
                        client_id=client_id,
                        client_name="Batch Persistence Test Client",
                    )
                    report = json.loads(report_path.read_text())
                    counts = self._workflow_counts(connection, client_id)
                    bronze_runs = self._bronze_run_count(connection, client_id)
                    batch_run, batch_items = self._batch_operations(
                        connection, client_id
                    )
                    other_operations = self._batch_operations(
                        connection, other_client_id
                    )
                finally:
                    self._delete_client(connection, client_id)

        self.assertEqual(report["status"], "COMPLETED")
        self.assertTrue(all(item["persisted"] for item in report["items"]))
        self.assertTrue(report["operational_recording"]["persisted"])
        self.assertEqual(counts, (0, 0, 0, 1, 1))
        self.assertEqual(bronze_runs, 2)
        self.assertEqual(batch_run, ("COMPLETED", 2, 2))
        self.assertEqual(
            batch_items,
            [("SILVER_PUBLISHABLE", True), ("SKIPPED_DUPLICATE", True)],
        )
        self.assertEqual(other_operations, (None, []))

    @staticmethod
    def _tenant_counts(connection, client_id) -> tuple[int, ...]:
        with connection.transaction():
            connection.execute("SET LOCAL ROLE analystops_app")
            connection.execute(
                "SELECT set_config('app.client_id', %s, true)",
                (str(client_id),),
            )
            return connection.execute(
                """
                SELECT
                    (SELECT count(*) FROM analystops.clients),
                    (SELECT count(*) FROM analystops.workbooks),
                    (SELECT count(*) FROM analystops.bronze_runs),
                    (SELECT count(*) FROM analystops.findings),
                    (SELECT count(*) FROM analystops.review_resolutions)
                """
            ).fetchone()

    @staticmethod
    def _workflow_counts(connection, client_id) -> tuple[int, ...]:
        with connection.transaction():
            connection.execute("SET LOCAL ROLE analystops_app")
            connection.execute(
                "SELECT set_config('app.client_id', %s, true)",
                (str(client_id),),
            )
            return connection.execute(
                """
                SELECT
                    (SELECT count(*) FROM analystops.agent_runs),
                    (SELECT count(*) FROM analystops.agent_attempts),
                    (SELECT count(*) FROM analystops.transformation_plans),
                    (SELECT count(*) FROM analystops.silver_runs),
                    (SELECT count(*) FROM analystops.workflow_runs)
                """
            ).fetchone()

    @staticmethod
    def _failure_statuses(connection, client_id) -> tuple[str, ...]:
        with connection.transaction():
            connection.execute("SET LOCAL ROLE analystops_app")
            connection.execute(
                "SELECT set_config('app.client_id', %s, true)",
                (str(client_id),),
            )
            return connection.execute(
                """
                SELECT agent_runs.status, workflow_runs.status,
                       workflow_runs.audit->>'failed_stage'
                FROM analystops.workflow_runs
                JOIN analystops.agent_runs
                  ON agent_runs.client_id = workflow_runs.client_id
                 AND agent_runs.id = workflow_runs.agent_run_id
                """
            ).fetchone()

    @staticmethod
    def _bronze_run_count(connection, client_id) -> int:
        with connection.transaction():
            connection.execute("SET LOCAL ROLE analystops_app")
            connection.execute(
                "SELECT set_config('app.client_id', %s, true)",
                (str(client_id),),
            )
            return connection.execute(
                "SELECT count(*) FROM analystops.bronze_runs"
            ).fetchone()[0]

    @staticmethod
    def _batch_operations(connection, client_id) -> tuple[tuple, list[tuple]]:
        with connection.transaction():
            connection.execute("SET LOCAL ROLE analystops_app")
            connection.execute(
                "SELECT set_config('app.client_id', %s, true)",
                (str(client_id),),
            )
            batch_run = connection.execute(
                """
                SELECT status, selected_records, completed_records
                FROM analystops.batch_runs
                """
            ).fetchone()
            batch_items = connection.execute(
                """
                SELECT status, persisted
                FROM analystops.batch_items
                ORDER BY item_index
                """
            ).fetchall()
            return batch_run, batch_items

    @staticmethod
    def _delete_client(connection, client_id) -> None:
        for table in (
            "batch_items",
            "batch_runs",
            "workflow_runs",
            "agent_attempts",
            "agent_runs",
            "silver_runs",
            "transformation_plans",
            "review_resolutions",
            "findings",
            "bronze_runs",
            "workbooks",
            "clients",
        ):
            connection.execute(
                f"DELETE FROM analystops.{table} WHERE client_id = %s"
                if table != "clients"
                else "DELETE FROM analystops.clients WHERE id = %s",
                (client_id,),
            )


if __name__ == "__main__":
    unittest.main()
