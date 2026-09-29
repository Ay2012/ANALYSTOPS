from __future__ import annotations

import json
import os
import tempfile
import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import psycopg

from analystops.workflows.checkpoint_retention import (
    _delete_thread,
    run_checkpoint_retention,
)
from analystops.workflows.silver_remediation_graph import (
    build_silver_remediation_graph,
    checkpoint_config,
    postgres_checkpointer,
    write_workflow_audit,
)


TEST_DATABASE_URL = os.environ.get("TEST_DATABASE_URL")


class FakeResponses:
    def __init__(self):
        self.calls = 0

    def create(self, **_kwargs):
        self.calls += 1
        return SimpleNamespace(
            id="resp_retention",
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
                        },
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
        findings = [
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
    path.write_text(
        json.dumps(
            {
                "validation_version": "silver-validation-v3",
                "source_file_id": "a" * 64,
                "reporting_month": "2026-08",
                "country": "France",
                "row_count": 10,
                "quality_disposition": "REVIEW" if findings else "PASS",
                "publication_state": publication_state,
                "findings": findings,
            }
        )
    )


@unittest.skipUnless(TEST_DATABASE_URL, "TEST_DATABASE_URL is not configured")
class CheckpointRetentionTests(unittest.TestCase):
    def test_dry_run_then_delete_only_audited_terminal_thread(self) -> None:
        client_id = uuid4()
        other_client_id = uuid4()
        terminal_thread = uuid4()
        pending_thread = uuid4()
        other_thread = uuid4()
        configs = [
            checkpoint_config(str(terminal_thread), str(client_id)),
            checkpoint_config(str(pending_thread), str(client_id)),
            checkpoint_config(str(other_thread), str(other_client_id)),
        ]
        storage_thread_ids = [
            str(config["configurable"]["thread_id"]) for config in configs
        ]
        model_client = FakeClient()

        try:
            with tempfile.TemporaryDirectory() as tmpdir:
                root = Path(tmpdir)
                terminal_profile = root / "terminal.json"
                pending_profile = root / "pending.json"
                write_profile(terminal_profile, "PUBLISHABLE")
                write_profile(pending_profile, "REVIEW_REQUIRED")

                with postgres_checkpointer(TEST_DATABASE_URL) as checkpointer:
                    terminal_graph = build_silver_remediation_graph(
                        None, checkpointer=checkpointer
                    )
                    terminal_graph.invoke(
                        {"profile_path": str(terminal_profile)}, config=configs[0]
                    )
                    write_workflow_audit(terminal_graph, configs[0], root / "audits")

                    pending_graph = build_silver_remediation_graph(
                        model_client, checkpointer=checkpointer
                    )
                    pending_graph.invoke(
                        {"profile_path": str(pending_profile)}, config=configs[1]
                    )
                    write_workflow_audit(pending_graph, configs[1], root / "audits")

                    terminal_graph.invoke(
                        {"profile_path": str(terminal_profile)}, config=configs[2]
                    )
                    write_workflow_audit(terminal_graph, configs[2], root / "audits")

                observed_at = datetime.now(UTC) + timedelta(minutes=1)
                self.assertIsNone(
                    _delete_thread(
                        TEST_DATABASE_URL,
                        storage_thread_ids[0],
                        expected_updated_at=datetime(2000, 1, 1, tzinfo=UTC),
                    )
                )
                dry_path = run_checkpoint_retention(
                    TEST_DATABASE_URL,
                    client_id=client_id,
                    retention_days=0,
                    workflow_dir=root / "audits",
                    report_dir=root / "reports",
                    now=observed_at,
                )
                dry = json.loads(dry_path.read_text())
                self.assertEqual(dry["mode"], "DRY_RUN")
                self.assertEqual(dry["threads_scanned"], 2)
                self.assertEqual(dry["threads_eligible"], 1)
                self.assertEqual(dry["threads_deleted"], 0)
                self.assertEqual(
                    {item["reason"] for item in dry["decisions"]},
                    {"ELIGIBLE", "PENDING_REVIEW"},
                )

                applied_path = run_checkpoint_retention(
                    TEST_DATABASE_URL,
                    client_id=client_id,
                    retention_days=0,
                    apply=True,
                    workflow_dir=root / "audits",
                    report_dir=root / "reports",
                    now=observed_at,
                )
                applied = json.loads(applied_path.read_text())
                self.assertEqual(applied["threads_deleted"], 1)

                with postgres_checkpointer(TEST_DATABASE_URL) as checkpointer:
                    graph = build_silver_remediation_graph(
                        None, checkpointer=checkpointer
                    )
                    self.assertFalse(graph.get_state(configs[0]).values)
                    self.assertTrue(graph.get_state(configs[1]).values)
                    self.assertTrue(graph.get_state(configs[2]).values)
        finally:
            with psycopg.connect(TEST_DATABASE_URL, autocommit=True) as connection:
                for storage_thread_id in storage_thread_ids:
                    for table in (
                        "checkpoint_writes",
                        "checkpoint_blobs",
                        "checkpoints",
                    ):
                        connection.execute(
                            f"DELETE FROM analystops.{table} WHERE thread_id = %s",
                            (storage_thread_id,),
                        )

        self.assertEqual(model_client.responses.calls, 1)


if __name__ == "__main__":
    unittest.main()
