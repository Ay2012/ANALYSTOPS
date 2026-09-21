from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from analystops.agents.bronze_remediation import (
    AgentProposalError,
    SYSTEM_INSTRUCTIONS,
    build_review_payload,
    propose_resolutions,
)
from analystops.ingestion.validate import validate_workbook


def bronze_record(finding: dict[str, object]) -> dict[str, object]:
    return {
        "file_path": "/private/client/workbook.xlsx",
        "file_hash": "a" * 64,
        "content_fingerprint": "b" * 64,
        "lifecycle_state": "AWAITING_REVIEW",
        "quality_disposition": "REVIEW",
        "decision": "REVIEW_REQUIRED",
        "selected_sheet": "Transactions",
        "sheet_candidates": [
            {
                "sheet": "Transactions",
                "row_count": 10,
                "score": 10,
                "matched_fields": ["invoice", "quantity"],
                "plausible": True,
            }
        ],
        "observed_schema": ["Invoice", "Units"],
        "row_count": 10,
        "reason_codes": [str(finding["code"])],
        "findings": [
            finding,
            {"code": "unexpected_columns", "quality_disposition": "WARN"},
        ],
        "policy_version": "bronze-v1",
        "record_hash": "c" * 64,
    }


def model_document(
    finding_code: str,
    action: str | None,
    *,
    decision: str = "PROPOSE",
    mapping: list[dict[str, str]] | None = None,
    format: str | None = None,
    sheet_name: str | None = None,
    defer_reason: str | None = None,
) -> dict[str, object]:
    return {
        "resolutions": [
            {
                "finding_code": finding_code,
                "decision": decision,
                "action": action,
                "mapping": mapping or [],
                "format": format,
                "sheet_name": sheet_name,
                "defer_reason": defer_reason,
            }
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
                input_tokens=120,
                input_tokens_details=SimpleNamespace(cached_tokens=80),
                output_tokens=25,
                output_tokens_details=SimpleNamespace(reasoning_tokens=10),
            ),
        )


class FakeClient:
    def __init__(self, documents: list[dict[str, object]]):
        self.responses = FakeResponses(documents)


class AgentTests(unittest.TestCase):
    def test_payload_contains_only_compact_review_evidence(self) -> None:
        bronze = bronze_record(
            {
                "code": "renamed_required_columns",
                "quality_disposition": "REVIEW",
                "columns": ["Quantity"],
            }
        )

        payload = build_review_payload(bronze)

        self.assertNotIn("file_path", payload)
        self.assertNotIn("content_fingerprint", payload)
        self.assertEqual(len(payload["review_items"]), 1)
        self.assertEqual(
            payload["review_items"][0]["allowed_actions"][0]["name"],
            "map_columns",
        )
        self.assertEqual(
            payload["review_items"][0]["deterministic_candidates"],
            {"mapping": [{"source": "Units", "target": "Quantity"}]},
        )

    def test_valid_low_risk_proposal_is_marked_automatic(self) -> None:
        bronze = bronze_record(
            {
                "code": "renamed_required_columns",
                "quality_disposition": "REVIEW",
                "columns": ["Quantity"],
            }
        )
        client = FakeClient(
            [
                model_document(
                    "renamed_required_columns",
                    "map_columns",
                    mapping=[{"source": "Units", "target": "Quantity"}],
                )
            ]
        )

        proposal = propose_resolutions(bronze, client)

        self.assertEqual(proposal.automatic_findings, ("renamed_required_columns",))
        self.assertEqual(proposal.human_review_findings, ())
        self.assertEqual(proposal.attempts[0].input_tokens, 120)
        self.assertEqual(proposal.attempts[0].cached_input_tokens, 80)
        self.assertEqual(proposal.attempts[0].reasoning_tokens, 10)
        self.assertEqual(proposal.attempts[0].response_id, "resp_1")
        self.assertGreaterEqual(proposal.attempts[0].latency_ms, 0)
        self.assertEqual(proposal.prompt_version, "bronze-remediation-v4")
        self.assertEqual(proposal.to_dict()["token_usage"]["total_tokens"], 145)
        self.assertEqual(
            proposal.to_dict()["token_usage"]["uncached_input_tokens"], 40
        )
        call = client.responses.calls[0]
        self.assertEqual(call["model"], "gpt-5.6-luna")
        self.assertEqual(call["max_output_tokens"], 500)
        self.assertFalse(call["store"])
        self.assertEqual(call["instructions"], SYSTEM_INSTRUCTIONS)
        self.assertEqual(
            call["prompt_cache_key"],
            "analystops-remediation:bronze-v1:v4",
        )
        self.assertNotIn("/private/client", call["input"])
        self.assertNotIn("a" * 64, call["input"])
        self.assertEqual(proposal.to_dict()["file_hash"], "a" * 64)

    def test_insufficient_evidence_defers_without_retry(self) -> None:
        bronze = bronze_record(
            {
                "code": "renamed_required_columns",
                "quality_disposition": "REVIEW",
                "columns": ["Quantity"],
            }
        )
        client = FakeClient(
            [
                model_document(
                    "renamed_required_columns",
                    None,
                    decision="DEFER",
                    defer_reason="INSUFFICIENT_EVIDENCE",
                )
            ]
        )

        proposal = propose_resolutions(bronze, client)

        self.assertEqual(proposal.automatic_findings, ())
        self.assertEqual(proposal.human_review_findings, ("renamed_required_columns",))
        self.assertEqual(len(client.responses.calls), 1)
        self.assertEqual(proposal.resolutions[0]["decision"], "DEFER")
        self.assertIn("deterministic policy engine", SYSTEM_INSTRUCTIONS)

    def test_automatic_mapping_must_match_deterministic_candidate(self) -> None:
        bronze = bronze_record(
            {
                "code": "renamed_required_columns",
                "quality_disposition": "REVIEW",
                "columns": ["Quantity"],
            }
        )
        bronze["observed_schema"].append("Country")
        client = FakeClient(
            [
                model_document(
                    "renamed_required_columns",
                    "map_columns",
                    mapping=[{"source": "Country", "target": "Quantity"}],
                )
            ]
        )

        with self.assertRaisesRegex(AgentProposalError, "deterministically supported"):
            propose_resolutions(
                bronze,
                client,
                primary_attempts=1,
                escalation_model=None,
            )

    def test_currency_examples_authorize_currency_format(self) -> None:
        bronze = bronze_record(
            {
                "code": "price_parse_failures",
                "quality_disposition": "REVIEW",
                "column": "Price",
                "count": 2,
                "examples": ["$12.30", "$7.00"],
            }
        )
        client = FakeClient(
            [
                model_document(
                    "price_parse_failures",
                    "confirm_numeric_format",
                    format="currency",
                )
            ]
        )

        proposal = propose_resolutions(bronze, client)

        self.assertEqual(proposal.automatic_findings, ("price_parse_failures",))
        payload = json.loads(client.responses.calls[0]["input"])
        self.assertEqual(
            payload["review_items"][0]["deterministic_candidates"],
            {"formats": ["currency"]},
        )

    def test_only_unambiguous_date_examples_produce_a_candidate(self) -> None:
        finding = {
            "code": "non_iso_date_strings",
            "quality_disposition": "REVIEW",
            "column": "InvoiceDate",
            "count": 2,
            "examples": ["13/03/2011 10:15", "22/04/2011 08:30"],
        }
        payload = build_review_payload(bronze_record(finding))

        self.assertEqual(
            payload["review_items"][0]["deterministic_candidates"],
            {"formats": ["%d/%m/%Y %H:%M"]},
        )

        finding["examples"] = ["03/04/2011 10:15", "05/06/2011 08:30"]
        payload = build_review_payload(bronze_record(finding))
        self.assertEqual(
            payload["review_items"][0]["deterministic_candidates"], {}
        )

    def test_high_risk_proposal_is_routed_to_human(self) -> None:
        bronze = bronze_record(
            {
                "code": "exact_duplicate_rows",
                "quality_disposition": "REVIEW",
                "count": 12,
                "rate": 0.12,
            }
        )
        client = FakeClient(
            [model_document("exact_duplicate_rows", "deduplicate_in_silver")]
        )

        proposal = propose_resolutions(bronze, client)

        self.assertEqual(proposal.automatic_findings, ())
        self.assertEqual(proposal.human_review_findings, ("exact_duplicate_rows",))

    def test_invalid_primary_attempts_escalate_to_terra(self) -> None:
        bronze = bronze_record(
            {
                "code": "renamed_required_columns",
                "quality_disposition": "REVIEW",
                "columns": ["Quantity"],
            }
        )
        invalid = model_document(
            "renamed_required_columns",
            "map_columns",
            mapping=[{"source": "Missing", "target": "Quantity"}],
        )
        valid = model_document(
            "renamed_required_columns",
            "map_columns",
            mapping=[{"source": "Units", "target": "Quantity"}],
        )
        client = FakeClient([invalid, invalid, valid])

        proposal = propose_resolutions(bronze, client)

        self.assertEqual(
            [call["model"] for call in client.responses.calls],
            ["gpt-5.6-luna", "gpt-5.6-luna", "gpt-5.6-terra"],
        )
        self.assertEqual(
            [attempt.status for attempt in proposal.attempts],
            ["FAILED", "FAILED", "SUCCEEDED"],
        )
        self.assertIn(
            "previous_validation_error",
            json.loads(client.responses.calls[1]["input"]),
        )

    def test_failed_attempts_report_the_last_error(self) -> None:
        bronze = bronze_record(
            {
                "code": "renamed_required_columns",
                "quality_disposition": "REVIEW",
                "columns": ["Quantity"],
            }
        )
        invalid = model_document(
            "renamed_required_columns",
            "map_columns",
            mapping=[{"source": "Missing", "target": "Quantity"}],
        )
        client = FakeClient([invalid, invalid, invalid])

        with self.assertRaisesRegex(
            AgentProposalError,
            "Last error: AgentProposalError: Agent mapping",
        ) as raised:
            propose_resolutions(bronze, client)

        self.assertEqual(len(raised.exception.attempts), 3)

    def test_non_review_record_never_calls_model(self) -> None:
        bronze = bronze_record(
            {
                "code": "unexpected_columns",
                "quality_disposition": "WARN",
            }
        )
        bronze["lifecycle_state"] = "BRONZE_ACCEPTED"
        client = FakeClient([])

        with self.assertRaisesRegex(AgentProposalError, "AWAITING_REVIEW"):
            propose_resolutions(bronze, client)

        self.assertEqual(client.responses.calls, [])

    def test_impossible_output_limit_never_calls_model(self) -> None:
        bronze = bronze_record(
            {
                "code": "exact_duplicate_rows",
                "quality_disposition": "REVIEW",
            }
        )
        client = FakeClient([])

        with self.assertRaisesRegex(ValueError, "at least 16"):
            propose_resolutions(bronze, client, max_output_tokens=15)

        self.assertEqual(client.responses.calls, [])

    def test_bronze_bounds_parse_failure_examples(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "currency.xlsx"
            rows = pd.DataFrame(
                {
                    "Invoice": range(10),
                    "StockCode": ["A"] * 10,
                    "Description": ["Item"] * 10,
                    "Quantity": [1] * 10,
                    "InvoiceDate": ["2011-01-01"] * 10,
                    "Price": [f"${value}.00" for value in range(10)],
                    "Country": ["US"] * 10,
                }
            )
            rows.to_excel(path, sheet_name="Transactions", index=False)

            result = validate_workbook(path)

        finding = next(
            item for item in result.findings if item["code"] == "price_parse_failures"
        )
        self.assertEqual(finding["count"], 10)
        self.assertEqual(len(finding["examples"]), 5)

    def test_bronze_examples_include_evidence_from_later_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "dates.xlsx"
            rows = pd.DataFrame(
                {
                    "Invoice": range(10),
                    "StockCode": ["A"] * 10,
                    "Description": ["Item"] * 10,
                    "Quantity": [1] * 10,
                    "InvoiceDate": ["04/03/2011 09:32"] * 9
                    + ["16/03/2011 13:29"],
                    "Price": [1.0] * 10,
                    "Country": ["US"] * 10,
                }
            )
            rows.to_excel(path, sheet_name="Transactions", index=False)

            result = validate_workbook(path)

        finding = next(
            item for item in result.findings if item["code"] == "non_iso_date_strings"
        )
        self.assertIn("16/03/2011 13:29", finding["examples"])


if __name__ == "__main__":
    unittest.main()
