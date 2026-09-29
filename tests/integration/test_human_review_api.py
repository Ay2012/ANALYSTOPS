from __future__ import annotations

import json
import os
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import psycopg
from fastapi.testclient import TestClient

from analystops.api.human_review import (
    ReviewPrincipal,
    ReviewService,
    TokenRegistry,
    create_app,
)
from analystops.workflows.silver_remediation_graph import (
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
            id="resp_api",
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


@unittest.skipUnless(TEST_DATABASE_URL, "TEST_DATABASE_URL is not configured")
class HumanReviewApiPostgresTests(unittest.TestCase):
    def test_authenticated_review_lifecycle(self) -> None:
        client_id = uuid4()
        other_client_id = uuid4()
        thread_id = uuid4()
        token = "correct-review-token-with-entropy"
        other_token = "other-review-token-with-entropy"
        config = checkpoint_config(str(thread_id), str(client_id))
        storage_thread_id = str(config["configurable"]["thread_id"])
        model_client = FakeClient()

        try:
            with tempfile.TemporaryDirectory() as tmpdir:
                root = Path(tmpdir)
                profile_path = root / "profile.json"
                profile_path.write_text(
                    json.dumps(
                        {
                            "validation_version": "silver-validation-v3",
                            "source_file_id": "a" * 64,
                            "reporting_month": "2026-08",
                            "country": "France",
                            "row_count": 10,
                            "quality_disposition": "REVIEW",
                            "publication_state": "REVIEW_REQUIRED",
                            "findings": [
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
                            ],
                        }
                    )
                )
                with postgres_checkpointer(TEST_DATABASE_URL) as checkpointer:
                    build_silver_remediation_graph(
                        model_client, checkpointer=checkpointer
                    ).invoke({"profile_path": str(profile_path)}, config=config)

                service = ReviewService(TEST_DATABASE_URL, root / "audits")
                registry = TokenRegistry(
                    {
                        token: ReviewPrincipal(client_id, "api-reviewer@example.com"),
                        other_token: ReviewPrincipal(
                            other_client_id, "other-reviewer@example.com"
                        ),
                    }
                )
                api = TestClient(create_app(service=service, tokens=registry))
                headers = {"Authorization": f"Bearer {token}"}
                other_headers = {"Authorization": f"Bearer {other_token}"}

                reviews = api.get("/v1/reviews", headers=headers)
                self.assertEqual(reviews.status_code, 200)
                self.assertEqual(len(reviews.json()["reviews"]), 1)
                self.assertEqual(
                    api.get("/v1/reviews", headers=other_headers).json()["reviews"],
                    [],
                )
                self.assertEqual(
                    api.get(
                        f"/v1/reviews/{thread_id}", headers=other_headers
                    ).status_code,
                    404,
                )

                decision = api.post(
                    f"/v1/reviews/{thread_id}/decisions",
                    headers=headers,
                    json={"decision": "REJECT"},
                )
                self.assertEqual(decision.status_code, 200)
                self.assertFalse(decision.json()["pending"])
                self.assertEqual(
                    decision.json()["state"]["human_decision"]["reviewed_by"],
                    "api-reviewer@example.com",
                )
                self.assertTrue(Path(decision.json()["audit_path"]).exists())
                self.assertEqual(
                    api.post(
                        f"/v1/reviews/{thread_id}/decisions",
                        headers=headers,
                        json={"decision": "REJECT"},
                    ).status_code,
                    409,
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

        self.assertEqual(model_client.responses.calls, 1)


if __name__ == "__main__":
    unittest.main()
