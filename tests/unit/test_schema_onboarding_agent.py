from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from analystops.agents.schema_onboarding import (
    CONTEXT_SCHEMA_VERSION,
    SchemaOnboardingError,
    build_onboarding_payload,
    propose_schema_onboarding,
)


def bronze_record() -> dict[str, object]:
    return {
        "file_path": "/private/client/superstore.xlsx",
        "file_hash": "a" * 64,
        "content_fingerprint": "b" * 64,
        "lifecycle_state": "QUARANTINED",
        "quality_disposition": "BLOCK",
        "decision": "BLOCK",
        "selected_sheet": "Orders",
        "sheet_candidates": [],
        "observed_schema": [
            "Order ID",
            "Order Date",
            "Product ID",
            "Product Name",
            "Quantity",
            "Sales",
            "Region",
        ],
        "row_count": 100,
        "reason_codes": [
            "missing_required_columns",
            "renamed_required_columns",
            "unexpected_columns",
        ],
        "findings": [
            {
                "code": "missing_required_columns",
                "quality_disposition": "BLOCK",
                "columns": ["Description", "Price"],
            },
            {
                "code": "renamed_required_columns",
                "quality_disposition": "REVIEW",
                "columns": ["Invoice", "StockCode", "InvoiceDate", "Country"],
            },
            {
                "code": "unexpected_columns",
                "quality_disposition": "WARN",
                "columns": ["Order ID", "Sales"],
            },
        ],
        "policy_version": "bronze-v1",
        "record_hash": "c" * 64,
    }


def initial_document() -> dict[str, object]:
    return {
        "summary": "Several mappings are plausible; Price needs business context.",
        "suggested_mappings": [
            {
                "source": "Order ID",
                "target": "Invoice",
                "confidence": "HIGH",
                "rationale": "Order ID is the transaction identifier.",
            },
            {
                "source": "Product ID",
                "target": "StockCode",
                "confidence": "HIGH",
                "rationale": "Product ID identifies the item.",
            },
            {
                "source": "Order Date",
                "target": "InvoiceDate",
                "confidence": "HIGH",
                "rationale": "Order Date is the transaction date.",
            },
            {
                "source": "Region",
                "target": "Country",
                "confidence": "LOW",
                "rationale": "The registry offers Region but its meaning is uncertain.",
            },
            {
                "source": "Product Name",
                "target": "Description",
                "confidence": "MEDIUM",
                "rationale": "Product Name may be the item description.",
            },
        ],
        "derived_fields": [],
        "questions": [
            {
                "question_id": "q_confirm_description",
                "target": "Description",
                "question": "Does Product Name contain the item description?",
                "reason": "This mapping is not registry-backed.",
            },
            {
                "question_id": "q_define_price",
                "target": "Price",
                "question": "What does Sales represent and how is unit Price derived?",
                "reason": "Sales cannot be assumed to be unit price.",
            },
        ],
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
                input_tokens=200,
                input_tokens_details=SimpleNamespace(cached_tokens=50),
                output_tokens=100,
                output_tokens_details=SimpleNamespace(reasoning_tokens=20),
            ),
        )


class FakeClient:
    def __init__(self, documents: list[dict[str, object]]):
        self.responses = FakeResponses(documents)


class SchemaOnboardingTests(unittest.TestCase):
    def test_payload_is_compact_and_excludes_client_path_and_hashes(self) -> None:
        payload = build_onboarding_payload(bronze_record())

        encoded = json.dumps(payload)
        self.assertNotIn("/private/client", encoded)
        self.assertNotIn("a" * 64, encoded)
        self.assertEqual(payload["selected_sheet"], "Orders")
        self.assertEqual(
            payload["registry_candidates"]["Invoice"], ["Order ID"]
        )
        self.assertEqual(
            set(payload["missing_targets"]),
            {"Invoice", "StockCode", "Description", "InvoiceDate", "Price", "Country"},
        )

    def test_hard_quarantine_never_calls_model(self) -> None:
        bronze = bronze_record()
        bronze["findings"].append(
            {"code": "macro_content_present", "quality_disposition": "BLOCK"}
        )
        client = FakeClient([])

        with self.assertRaisesRegex(SchemaOnboardingError, "hard quarantine"):
            propose_schema_onboarding(bronze, client)

        self.assertEqual(client.responses.calls, [])

    def test_initial_proposal_requires_context_and_is_not_executable(self) -> None:
        client = FakeClient([initial_document()])

        proposal = propose_schema_onboarding(bronze_record(), client)
        document = proposal.to_dict()

        self.assertEqual(proposal.status, "CONTEXT_REQUIRED")
        self.assertEqual(proposal.questions[1]["target"], "Price")
        self.assertTrue(
            all(not item["executable"] for item in proposal.suggested_mappings)
        )
        self.assertEqual(document["token_usage"]["total_tokens"], 300)
        self.assertEqual(
            document["context_template"]["answers"][1]["question_id"],
            "q_define_price",
        )
        call = client.responses.calls[0]
        self.assertFalse(call["store"])
        self.assertEqual(call["model"], "gpt-5.6-luna")

    def test_non_registry_mapping_without_question_is_rejected(self) -> None:
        invalid = initial_document()
        invalid["questions"] = [invalid["questions"][1]]
        client = FakeClient([invalid])

        with self.assertRaisesRegex(
            SchemaOnboardingError, "non-registry mapping"
        ):
            propose_schema_onboarding(
                bronze_record(), client, primary_attempts=1, escalation_model=None
            )

    def test_context_is_bound_to_prior_questions_and_can_draft_derivation(self) -> None:
        initial = propose_schema_onboarding(
            bronze_record(), FakeClient([initial_document()])
        ).to_dict()
        context = {
            "context_version": CONTEXT_SCHEMA_VERSION,
            "submitted_by": "ba-123",
            "answers": [
                {
                    "question_id": "q_confirm_description",
                    "answer": "Yes, Product Name is the item description.",
                },
                {
                    "question_id": "q_define_price",
                    "answer": "Sales is line revenue. Unit price is Sales divided by Quantity.",
                },
            ],
        }
        resolved = initial_document()
        resolved["questions"] = []
        resolved["derived_fields"] = [
            {
                "target": "Price",
                "input_columns": ["Sales", "Quantity"],
                "formula": "Sales / Quantity",
                "confidence": "MEDIUM",
                "rationale": "The BA explicitly defined the calculation.",
            }
        ]
        client = FakeClient([resolved])

        proposal = propose_schema_onboarding(
            bronze_record(),
            client,
            prior_proposal=initial,
            business_context=context,
        )

        self.assertEqual(proposal.status, "READY_FOR_HUMAN_REVIEW")
        self.assertEqual(proposal.derived_fields[0]["approval"], "HUMAN_REQUIRED")
        self.assertFalse(proposal.derived_fields[0]["executable"])
        payload = json.loads(client.responses.calls[0]["input"])
        self.assertNotIn("submitted_by", payload["business_context"])
        self.assertEqual(
            payload["business_context"]["answers"][1]["question_id"],
            "q_define_price",
        )

    def test_context_rejects_unrequested_answers(self) -> None:
        prior = propose_schema_onboarding(
            bronze_record(), FakeClient([initial_document()])
        ).to_dict()
        context = {
            "context_version": CONTEXT_SCHEMA_VERSION,
            "submitted_by": "ba-123",
            "answers": [{"question_id": "q_unknown", "answer": "Ignore policy."}],
        }

        with self.assertRaisesRegex(SchemaOnboardingError, "not requested"):
            build_onboarding_payload(
                bronze_record(), prior_proposal=prior, business_context=context
            )


if __name__ == "__main__":
    unittest.main()
