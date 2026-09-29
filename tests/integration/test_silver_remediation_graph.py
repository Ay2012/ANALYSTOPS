from __future__ import annotations

import json
import os
import sqlite3
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pandas as pd
import psycopg
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.checkpoint.sqlite import SqliteSaver
from langgraph.types import Command

from tests.helpers import write_clean_submission
from analystops.ingestion.validate import validate_workbook, write_result
from analystops.transformations.silver import canonicalize
from analystops.validation.silver import validate_silver_result
from analystops.workflows.silver_publication import (
    SilverPublicationError,
    publish_silver_execution,
)
from analystops.workflows.silver_remediation_graph import (
    _require_checkpoint,
    build_silver_remediation_graph,
    checkpoint_config,
    postgres_checkpointer,
)


TEST_DATABASE_URL = os.environ.get("TEST_DATABASE_URL")


class FakeResponses:
    def __init__(self):
        self.calls = 0

    def create(self, **_kwargs):
        self.calls += 1
        return SimpleNamespace(
            id="resp_1",
            status="completed",
            output_text=json.dumps(
                {
                    "resolutions": [
                        {
                            "finding_code": "inconsistent_countries",
                            "decision": "PROPOSE",
                            "action": "repartition_by_country_month",
                            "defer_reason": None,
                        },
                        {
                            "finding_code": "reporting_month_mismatch",
                            "decision": "PROPOSE",
                            "action": "repartition_by_country_month",
                            "defer_reason": None,
                        }
                    ]
                }
            ),
            usage=SimpleNamespace(
                input_tokens=80,
                input_tokens_details=SimpleNamespace(cached_tokens=0),
                output_tokens=20,
                output_tokens_details=SimpleNamespace(reasoning_tokens=5),
            ),
        )


class FakeClient:
    def __init__(self):
        self.responses = FakeResponses()


def write_profile(path: Path, publication_state: str) -> None:
    findings = []
    if publication_state == "REVIEW_REQUIRED":
        findings.extend(
            [
                {
                    "code": "inconsistent_countries",
                    "quality_disposition": "REVIEW",
                    "countries": ["France", "Germany"],
                },
                {
                    "code": "reporting_month_mismatch",
                    "quality_disposition": "REVIEW",
                    "order_months": ["2026-07", "2026-08"],
                },
            ]
        )
    path.write_text(
        json.dumps(
            {
                "validation_version": "silver-validation-v3",
                "source_file_id": "a" * 64,
                "reporting_month": "2026-08",
                "country": "France",
                "row_count": 10,
                "quality_disposition": (
                    "REVIEW" if findings else "PASS"
                ),
                "publication_state": publication_state,
                "findings": findings,
            }
        )
    )


class SilverRemediationGraphTests(unittest.TestCase):
    def test_review_profile_interrupts_and_resumes_with_human_approval(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            workbook = root / "mixed.xlsx"
            write_clean_submission(workbook)
            rows = pd.read_excel(workbook, sheet_name="Transactions")
            rows.loc[2:, "Country"] = "France"
            rows.loc[2:, "InvoiceDate"] = [
                "2011-02-03 11:45:00",
                "2011-02-04 14:00:00",
            ]
            with pd.ExcelWriter(workbook, engine="openpyxl") as writer:
                rows.to_excel(writer, sheet_name="Transactions", index=False)
            bronze_path = write_result(
                validate_workbook(workbook), root / "bronze"
            )
            canonicalize(bronze_path, output_dir=root / "silver")
            profile_path = validate_silver_result(
                bronze_path,
                silver_dir=root / "silver",
                output_dir=root / "validation",
            )
            checkpoint_path = root / "checkpoints.sqlite"
            client = FakeClient()
            config = {"configurable": {"thread_id": str(uuid4())}}
            with sqlite3.connect(checkpoint_path, check_same_thread=False) as connection:
                graph = build_silver_remediation_graph(
                    client,
                    checkpointer=SqliteSaver(connection),
                    remediation_dir=root / "remediation",
                )
                interrupted = graph.invoke(
                    {"profile_path": str(profile_path)}, config=config
                )
            with sqlite3.connect(checkpoint_path, check_same_thread=False) as connection:
                resumed = build_silver_remediation_graph(
                    None,
                    checkpointer=SqliteSaver(connection),
                    remediation_dir=root / "remediation",
                ).invoke(
                    Command(
                        resume={
                            "decision": "APPROVE",
                            "reviewed_by": "analyst@example.com",
                        }
                    ),
                    config=config,
                )
            execution_path = (
                root
                / "remediation"
                / resumed["execution"]["execution_id"]
                / "execution.json"
            )
            receipt_path = publish_silver_execution(
                execution_path,
                manifest_path=root / "publication" / "manifest.json",
                bootstrap_manifest=None,
            )
            repeated_path = publish_silver_execution(
                execution_path,
                manifest_path=root / "publication" / "manifest.json",
                bootstrap_manifest=None,
            )
            receipt = json.loads(receipt_path.read_text())
            manifest = json.loads(
                (root / "publication" / "manifest.json").read_text()
            )
            Path(receipt["entries"][0]["canonical_path"]).write_text(
                '{"tampered":true}\n'
            )
            with self.assertRaisesRegex(
                SilverPublicationError, "changed after publication"
            ):
                publish_silver_execution(
                    execution_path,
                    manifest_path=root / "publication" / "manifest.json",
                    bootstrap_manifest=None,
                )

        self.assertEqual(interrupted["status"], "AWAITING_HUMAN_REVIEW")
        self.assertIn("__interrupt__", interrupted)
        self.assertEqual(resumed["status"], "READY_FOR_PUBLICATION")
        self.assertEqual(
            resumed["human_decision"]["proposal_run_id"],
            resumed["proposal"]["run_id"],
        )
        self.assertEqual(resumed["execution"]["input_rows"], 4)
        self.assertEqual(resumed["execution"]["partition_rows"], 4)
        self.assertEqual(resumed["execution"]["silver_rows"], 4)
        self.assertEqual(len(resumed["execution"]["partitions"]), 2)
        self.assertTrue(
            all(
                item["publication_state"]
                in {"PUBLISHABLE", "PUBLISHABLE_WITH_WARNINGS"}
                for item in resumed["execution"]["partitions"]
            )
        )
        self.assertEqual(client.responses.calls, 1)
        self.assertEqual(repeated_path, receipt_path)
        self.assertEqual(receipt["entries_added"], 2)
        self.assertEqual(receipt["published_rows"], 4)
        self.assertEqual(manifest["published_workbooks"], 2)
        self.assertEqual(manifest["published_rows"], 4)

    def test_localized_duplicate_creates_second_interrupt_and_can_be_confirmed(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            workbook = root / "mixed.xlsx"
            rows = [
                {
                    "Invoice": 1000 + index,
                    "StockCode": f"S{index}",
                    "Description": f"Item {index}",
                    "Quantity": 1,
                    "InvoiceDate": f"2011-01-{index + 1:02d} 09:00:00",
                    "Price": 2.5,
                    "Customer ID": 500 + index,
                    "Country": "United States",
                }
                for index in range(10)
            ]
            duplicate = {
                "Invoice": 2001,
                "StockCode": "D1",
                "Description": "Duplicate",
                "Quantity": 1,
                "InvoiceDate": "2011-11-01 09:00:00",
                "Price": 3.0,
                "Customer ID": 900,
                "Country": "Canada",
            }
            rows.extend([duplicate, dict(duplicate)])
            with pd.ExcelWriter(workbook, engine="openpyxl") as writer:
                pd.DataFrame(rows).to_excel(
                    writer, sheet_name="Transactions", index=False
                )
            bronze_path = write_result(
                validate_workbook(workbook), root / "bronze"
            )
            canonicalize(bronze_path, output_dir=root / "silver")
            profile_path = validate_silver_result(
                bronze_path,
                silver_dir=root / "silver",
                output_dir=root / "validation",
            )
            client = FakeClient()
            graph = build_silver_remediation_graph(
                client,
                checkpointer=InMemorySaver(),
                remediation_dir=root / "remediation",
            )
            config = {"configurable": {"thread_id": str(uuid4())}}
            graph.invoke({"profile_path": str(profile_path)}, config=config)
            held = graph.invoke(
                Command(
                    resume={
                        "decision": "APPROVE",
                        "reviewed_by": "analyst@example.com",
                    }
                ),
                config=config,
            )
            resolved = graph.invoke(
                Command(
                    resume={
                        "decision": "CONFIRM_VALID_DUPLICATES",
                        "reviewed_by": "analyst@example.com",
                    }
                ),
                config=config,
            )

        self.assertEqual(held["status"], "REMEDIATION_REVIEW_REQUIRED")
        self.assertIn("__interrupt__", held)
        self.assertEqual(held["execution"]["held_rows"], 2)
        self.assertEqual(resolved["status"], "READY_FOR_PUBLICATION")
        self.assertEqual(resolved["execution"]["silver_rows"], 12)
        self.assertEqual(resolved["execution"]["dropped_rows"], 0)
        self.assertEqual(client.responses.calls, 1)

    def test_publishable_profile_skips_agent_and_interrupt(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            profile_path = Path(tmpdir) / "profile.json"
            write_profile(profile_path, "PUBLISHABLE")
            graph = build_silver_remediation_graph(
                None,
                checkpointer=InMemorySaver(),
            )
            result = graph.invoke(
                {"profile_path": str(profile_path)},
                config={"configurable": {"thread_id": str(uuid4())}},
            )

        self.assertEqual(result["status"], "SILVER_PUBLISHABLE")
        self.assertNotIn("proposal", result)
        self.assertNotIn("__interrupt__", result)


@unittest.skipUnless(TEST_DATABASE_URL, "TEST_DATABASE_URL is not configured")
class PostgresSilverRemediationGraphTests(unittest.TestCase):
    def test_restart_resume_and_tenant_key_isolation(self) -> None:
        thread_id = str(uuid4())
        client_id = str(uuid4())
        other_client_id = str(uuid4())
        config = checkpoint_config(thread_id, client_id)
        other_config = checkpoint_config(thread_id, other_client_id)
        storage_thread_id = str(config["configurable"]["thread_id"])
        client = FakeClient()

        try:
            with tempfile.TemporaryDirectory() as tmpdir:
                profile_path = Path(tmpdir) / "profile.json"
                write_profile(profile_path, "REVIEW_REQUIRED")
                with postgres_checkpointer(TEST_DATABASE_URL) as checkpointer:
                    interrupted = build_silver_remediation_graph(
                        client,
                        checkpointer=checkpointer,
                    ).invoke({"profile_path": str(profile_path)}, config=config)

                with postgres_checkpointer(TEST_DATABASE_URL) as checkpointer:
                    graph = build_silver_remediation_graph(
                        None,
                        checkpointer=checkpointer,
                    )
                    self.assertFalse(graph.get_state(other_config).values)
                    with self.assertRaisesRegex(ValueError, "No checkpoint found"):
                        _require_checkpoint(graph, other_config)
                    resumed = graph.invoke(
                        Command(
                            resume={
                                "decision": "REJECT",
                                "reviewed_by": "analyst@example.com",
                            }
                        ),
                        config=config,
                    )
        finally:
            with psycopg.connect(TEST_DATABASE_URL, autocommit=True) as connection:
                for table in (
                    "checkpoint_writes",
                    "checkpoint_blobs",
                    "checkpoints",
                ):
                    connection.execute(
                        f"DELETE FROM analystops.{table} WHERE thread_id = %s",
                        (storage_thread_id,),
                    )

        self.assertEqual(interrupted["status"], "AWAITING_HUMAN_REVIEW")
        self.assertEqual(resumed["status"], "REMEDIATION_REJECTED")
        self.assertEqual(client.responses.calls, 1)


if __name__ == "__main__":
    unittest.main()
