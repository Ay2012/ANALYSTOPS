from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

import pandas as pd

from tests.helpers import read_json, write_clean_submission
from tests.integration.test_end_to_end_workflow import (
    FakeClient,
    FailingClient,
    resolution,
)
from analystops.ingestion.validate import validate_workbook, write_result
from analystops.workflows.batch_bronze_to_silver import (
    run_bronze_to_silver_batch,
)


def write_renamed_submission(root: Path) -> Path:
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
    return workbook


class BatchWorkflowTests(unittest.TestCase):
    def test_batch_continues_after_failure_and_skips_duplicate(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            accepted_workbook = (
                root
                / "generated"
                / "clean"
                / "united_kingdom_2011-01.xlsx"
            )
            write_clean_submission(accepted_workbook)
            accepted = validate_workbook(accepted_workbook)
            accepted_path = write_result(accepted, root / "bronze-accepted")
            duplicate_path = write_result(
                validate_workbook(
                    accepted_workbook,
                    seen_hashes={str(accepted.file_hash)},
                ),
                root / "bronze-duplicate",
            )
            review_path = write_result(
                validate_workbook(write_renamed_submission(root)),
                root / "bronze-review",
            )

            report_path = run_bronze_to_silver_batch(
                [accepted_path, duplicate_path, review_path],
                client_factory=FailingClient,
                output_dir=root / "batches",
                workflow_output_dir=root / "workflows",
                silver_dir=root / "silver",
                validation_dir=root / "validation",
                workers=2,
                primary_attempts=1,
                escalation_model=None,
            )
            report = read_json(report_path)

        statuses = {item["status"] for item in report["items"]}
        self.assertEqual(report["status"], "COMPLETED_WITH_FAILURES")
        self.assertEqual(
            statuses,
            {"SILVER_PUBLISHABLE", "SKIPPED_DUPLICATE", "FAILED"},
        )
        self.assertEqual(report["summary"]["completed_records"], 3)
        self.assertEqual(report["summary"]["attempts"], 1)
        self.assertEqual(report["summary"]["failed_attempts"], 1)

    def test_resume_does_not_rerun_completed_workbook(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            bronze_path = write_result(
                validate_workbook(write_renamed_submission(root)),
                root / "bronze",
            )
            client = FakeClient(
                {
                    "resolutions": [
                        resolution(
                            "renamed_required_columns",
                            decision="PROPOSE",
                            action="map_columns",
                            mapping=[{"source": "Units", "target": "Quantity"}],
                        )
                    ]
                }
            )
            report_path = run_bronze_to_silver_batch(
                [bronze_path],
                client_factory=lambda: client,
                output_dir=root / "batches",
                workflow_output_dir=root / "workflows",
                silver_dir=root / "silver",
                validation_dir=root / "validation",
                workers=1,
            )
            resumed_path = run_bronze_to_silver_batch(
                [bronze_path],
                client_factory=lambda: client,
                output_dir=root / "batches",
                workflow_output_dir=root / "workflows",
                silver_dir=root / "silver",
                validation_dir=root / "validation",
                resume_from=report_path,
                workers=1,
            )
            report = json.loads(resumed_path.read_text())

        self.assertEqual(resumed_path, report_path)
        self.assertEqual(report["status"], "COMPLETED")
        self.assertIsNotNone(report["resumed_at"])
        self.assertEqual(client.responses.calls, 1)


if __name__ == "__main__":
    unittest.main()
