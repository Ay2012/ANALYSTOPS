from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

import pandas as pd

from tests.helpers import read_json, write_clean_submission
from analystops.ingestion.validate import read_result, validate_workbook, write_result
from analystops.workflows.bronze_to_silver import (
    BronzeToSilverWorkflowError,
    run_bronze_to_silver,
)


class FakeResponses:
    def __init__(self, document: dict[str, object]):
        self.document = document
        self.calls = 0

    def create(self, **_kwargs):
        self.calls += 1
        return SimpleNamespace(
            id=f"resp_{self.calls}",
            status="completed",
            output_text=json.dumps(self.document),
            usage=SimpleNamespace(
                input_tokens=100,
                input_tokens_details=SimpleNamespace(cached_tokens=0),
                output_tokens=30,
                output_tokens_details=SimpleNamespace(reasoning_tokens=10),
            ),
        )


class FakeClient:
    def __init__(self, document: dict[str, object]):
        self.responses = FakeResponses(document)


class FailingResponses:
    def __init__(self):
        self.calls = 0

    def create(self, **_kwargs):
        self.calls += 1
        raise TimeoutError("model timed out")


class FailingClient:
    def __init__(self):
        self.responses = FailingResponses()


def resolution(
    finding_code: str,
    *,
    decision: str,
    action: str | None,
    mapping: list[dict[str, str]] | None = None,
    defer_reason: str | None = None,
) -> dict[str, object]:
    return {
        "finding_code": finding_code,
        "decision": decision,
        "action": action,
        "mapping": mapping or [],
        "format": None,
        "sheet_name": None,
        "defer_reason": defer_reason,
    }


class EndToEndWorkflowTests(unittest.TestCase):
    def test_automatic_agent_resolution_reaches_validated_silver(self) -> None:
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
            rows = rows.rename(columns={"Quantity": "Units"})
            with pd.ExcelWriter(workbook, engine="openpyxl") as writer:
                rows.to_excel(writer, sheet_name="Transactions", index=False)
            initial = validate_workbook(workbook)
            bronze_path = write_result(initial, root / "bronze")
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

            workflow_path = run_bronze_to_silver(
                bronze_path,
                client=client,
                output_dir=root / "workflows",
                silver_dir=root / "silver",
                validation_dir=root / "validation",
            )
            workflow = read_json(workflow_path)
            resolved = read_result(workflow["resolved_bronze_record_path"])
            plan = read_json(Path(workflow["transformation_plan_path"]))
            profile = read_json(Path(workflow["validation_profile_path"]))

        self.assertEqual(initial.lifecycle_state, "AWAITING_REVIEW")
        self.assertEqual(workflow["status"], "SILVER_PUBLISHABLE")
        self.assertEqual(resolved["lifecycle_state"], "BRONZE_ACCEPTED")
        self.assertEqual(plan["operations"][0]["operation"], "map_columns")
        self.assertEqual(profile["publication_state"], "PUBLISHABLE")
        self.assertEqual(client.responses.calls, 1)

    def test_human_boundary_can_resume_without_calling_the_agent(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            workbook = (
                root
                / "generated"
                / "corrupted"
                / "duplicate_rows"
                / "united_kingdom_2011-01_duplicate_rows_seed1.xlsx"
            )
            write_clean_submission(workbook)
            rows = pd.read_excel(workbook, sheet_name="Transactions")
            rows = pd.concat([rows, rows.iloc[[0]]], ignore_index=True)
            with pd.ExcelWriter(workbook, engine="openpyxl") as writer:
                rows.to_excel(writer, sheet_name="Transactions", index=False)
            bronze_path = write_result(validate_workbook(workbook), root / "bronze")
            client = FakeClient(
                {
                    "resolutions": [
                        resolution(
                            "exact_duplicate_rows",
                            decision="DEFER",
                            action=None,
                            defer_reason="BUSINESS_KNOWLEDGE_REQUIRED",
                        )
                    ]
                }
            )

            first_path = run_bronze_to_silver(
                bronze_path,
                client=client,
                output_dir=root / "workflows",
                silver_dir=root / "silver",
                validation_dir=root / "validation",
            )
            first = read_json(first_path)
            review = read_json(Path(first["review_request_path"]))
            review["reviewed_by"] = "analyst@example.com"
            review["reviewed_at"] = "2026-09-21T12:00:00Z"
            review["resolutions"] = [
                {
                    "finding_code": "exact_duplicate_rows",
                    "action": "confirm_valid_duplicates",
                    "details": {},
                }
            ]

            resumed_path = run_bronze_to_silver(
                bronze_path,
                human_resolution=review,
                output_dir=root / "workflows",
                silver_dir=root / "silver",
                validation_dir=root / "validation",
            )
            resumed = read_json(resumed_path)
            profile = read_json(Path(resumed["validation_profile_path"]))

        self.assertEqual(first["status"], "AWAITING_HUMAN_REVIEW")
        self.assertIsNone(first["silver_result_path"])
        self.assertEqual(resumed["status"], "SILVER_PUBLISHABLE")
        self.assertEqual(profile["publication_state"], "PUBLISHABLE_WITH_WARNINGS")
        self.assertEqual(
            profile["findings"][0]["review_resolution"],
            "confirm_valid_duplicates",
        )
        self.assertEqual(client.responses.calls, 1)

    def test_agent_failure_writes_retryable_workflow_audit(self) -> None:
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
            client = FailingClient()

            with self.assertRaises(BronzeToSilverWorkflowError) as raised:
                run_bronze_to_silver(
                    bronze_path,
                    client=client,
                    output_dir=root / "workflows",
                    silver_dir=root / "silver",
                    validation_dir=root / "validation",
                    primary_attempts=1,
                    escalation_model=None,
                )

            workflow = read_json(raised.exception.report_path)
            agent_run = read_json(Path(workflow["agent_run_path"]))

        self.assertEqual(workflow["status"], "FAILED")
        self.assertEqual(workflow["failed_stage"], "AGENT_REMEDIATION")
        self.assertEqual(workflow["failure"]["code"], "AGENT_RETRY_EXHAUSTED")
        self.assertTrue(workflow["failure"]["retryable"])
        self.assertEqual(agent_run["status"], "FAILED")
        self.assertEqual(len(agent_run["attempts"]), 1)
        self.assertIsNone(workflow["silver_result_path"])
        self.assertEqual(client.responses.calls, 1)


if __name__ == "__main__":
    unittest.main()
