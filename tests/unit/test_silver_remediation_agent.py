from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from analystops.agents.silver_remediation import (
    SilverProposalError,
    build_review_payload,
    propose_silver_remediation,
)


def profile(*findings: dict[str, object]) -> dict[str, object]:
    return {
        "validation_version": "silver-validation-v3",
        "source_file_id": "a" * 64,
        "bronze_record_path": "/private/bronze.json",
        "canonical_path": "/private/silver.jsonl",
        "reporting_month": "2026-08",
        "country": None,
        "row_count": 100,
        "quality_disposition": "REVIEW",
        "publication_state": "REVIEW_REQUIRED",
        "findings": list(findings),
    }


class FakeResponses:
    def __init__(self, documents: list[dict[str, object]]):
        self.documents = iter(documents)
        self.calls: list[dict[str, object]] = []

    def create(self, **kwargs):
        self.calls.append(kwargs)
        return SimpleNamespace(
            id=f"resp_{len(self.calls)}",
            status="completed",
            output_text=json.dumps(next(self.documents)),
            usage=SimpleNamespace(
                input_tokens=80,
                input_tokens_details=SimpleNamespace(cached_tokens=20),
                output_tokens=15,
                output_tokens_details=SimpleNamespace(reasoning_tokens=5),
            ),
        )


class FakeClient:
    def __init__(self, documents: list[dict[str, object]]):
        self.responses = FakeResponses(documents)


def resolution(
    code: str,
    action: str | None,
    *,
    decision: str = "PROPOSE",
    defer_reason: str | None = None,
) -> dict[str, object]:
    return {
        "resolutions": [
            {
                "finding_code": code,
                "decision": decision,
                "action": action,
                "defer_reason": defer_reason,
            }
        ]
    }


class SilverRemediationTests(unittest.TestCase):
    def test_payload_contains_only_compact_review_evidence(self) -> None:
        value = profile(
            {
                "code": "inconsistent_countries",
                "quality_disposition": "REVIEW",
                "countries": ["France", "Germany"],
            },
            {
                "code": "missing_customer_ids",
                "quality_disposition": "WARN",
                "count": 2,
            },
        )

        payload = build_review_payload(value)

        self.assertNotIn("canonical_path", payload)
        self.assertNotIn("source_file_id", payload)
        self.assertEqual(len(payload["review_items"]), 1)
        self.assertEqual(
            payload["review_items"][0]["allowed_actions"],
            [{"name": "repartition_by_country_month", "approval": "HUMAN"}],
        )

    def test_valid_proposal_is_human_routed_and_observed(self) -> None:
        value = profile(
            {
                "code": "inconsistent_countries",
                "quality_disposition": "REVIEW",
                "countries": ["France", "Germany"],
            }
        )
        client = FakeClient(
            [resolution("inconsistent_countries", "repartition_by_country_month")]
        )

        proposal = propose_silver_remediation(value, client)

        self.assertEqual(
            proposal.human_review_findings, ("inconsistent_countries",)
        )
        self.assertEqual(proposal.attempts[0].response_id, "resp_1")
        self.assertEqual(proposal.to_dict()["token_usage"]["total_tokens"], 95)
        call = client.responses.calls[0]
        self.assertEqual(call["model"], "gpt-5.6-luna")
        self.assertFalse(call["store"])
        self.assertNotIn("/private/", call["input"])

    def test_action_must_be_registered_for_finding(self) -> None:
        value = profile(
            {
                "code": "country_file_mismatch",
                "quality_disposition": "REVIEW",
                "country": "France",
            }
        )
        client = FakeClient(
            [resolution("country_file_mismatch", "deduplicate_in_silver")]
        )

        with self.assertRaisesRegex(SilverProposalError, "not approved for"):
            propose_silver_remediation(
                value, client, primary_attempts=1, escalation_model=None
            )

    def test_unknown_finding_can_only_defer(self) -> None:
        value = profile(
            {
                "code": "future_policy_gap",
                "quality_disposition": "REVIEW",
                "count": 1,
            }
        )
        client = FakeClient(
            [
                resolution(
                    "future_policy_gap",
                    None,
                    decision="DEFER",
                    defer_reason="UNSUPPORTED_FINDING",
                )
            ]
        )

        proposal = propose_silver_remediation(value, client)

        self.assertEqual(proposal.resolutions[0]["decision"], "DEFER")
        self.assertEqual(proposal.human_review_findings, ("future_policy_gap",))

    def test_non_review_profile_is_rejected_before_model_call(self) -> None:
        value = profile()
        value["publication_state"] = "PUBLISHABLE"
        client = FakeClient([])

        with self.assertRaisesRegex(SilverProposalError, "Only REVIEW_REQUIRED"):
            propose_silver_remediation(value, client)
        self.assertEqual(client.responses.calls, [])


if __name__ == "__main__":
    unittest.main()
