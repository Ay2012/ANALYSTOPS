from __future__ import annotations

import json
import sys
import tempfile
import unittest
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from analystops.agents.schema_onboarding import PROPOSAL_SCHEMA_VERSION
from analystops.ingestion.validate import read_result, validate_workbook, write_result
from analystops.workflows.schema_onboarding import (
    SchemaContractError,
    _safe_name,
    create_approval_template,
    execute_onboarding,
)


def read_json(path: Path) -> dict:
    return json.loads(path.read_text())


def write_external_workbook(path: Path, *, zero_quantity: bool = False) -> None:
    rows = pd.DataFrame(
        {
            "Order ID": ["O-1", "O-2"],
            "Order Date": ["2026-01-01", "2026-01-02"],
            "Product ID": ["P-1", "P-2"],
            "Product Name": ["Desk", "Chair"],
            "Quantity": [2, 0 if zero_quantity else 4],
            "Sales": [20.0, 40.0],
            "Customer ID": [101, 102],
            "Country/Region": ["United States", "Canada"],
            "Profit": [5.0, 10.0],
        }
    )
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        rows.to_excel(writer, sheet_name="Orders", index=False)
        pd.DataFrame({"Manager": ["A"]}).to_excel(
            writer, sheet_name="People", index=False
        )


def proposal(bronze: dict[str, object]) -> dict[str, object]:
    mappings = [
        ("Order ID", "Invoice"),
        ("Product ID", "StockCode"),
        ("Product Name", "Description"),
        ("Order Date", "InvoiceDate"),
        ("Country/Region", "Country"),
    ]
    return {
        "run_id": "proposal-1",
        "prompt_version": "schema-onboarding-v2",
        "schema_version": PROPOSAL_SCHEMA_VERSION,
        "policy_version": "bronze-v1",
        "status": "READY_FOR_HUMAN_REVIEW",
        "started_at": "2026-01-01T00:00:00+00:00",
        "completed_at": "2026-01-01T00:00:01+00:00",
        "file_hash": bronze["file_hash"],
        "bronze_record_hash": bronze["record_hash"],
        "selected_sheet": "Orders",
        "context_hash": "d" * 64,
        "summary": "Ready for review.",
        "suggested_mappings": [
            {
                "source": source,
                "target": target,
                "confidence": "HIGH",
                "rationale": "Confirmed by context.",
                "approval": "HUMAN_REQUIRED",
                "executable": False,
            }
            for source, target in mappings
        ],
        "derived_fields": [
            {
                "target": "Price",
                "input_columns": ["Sales", "Quantity"],
                "formula": "Sales / Quantity",
                "confidence": "HIGH",
                "rationale": "Confirmed by context.",
                "approval": "HUMAN_REQUIRED",
                "executable": False,
            }
        ],
        "questions": [],
        "attempts": [],
        "token_usage": {},
        "latency_ms": 0,
        "context_template": {},
    }


def approve(template: dict[str, object]) -> dict[str, object]:
    template["reviewer"] = "data-owner-1"
    template["reviewed_at"] = datetime.now(UTC).isoformat()
    for item in template["mapping_decisions"]:
        item["decision"] = "APPROVE"
    for item in template["derived_field_decisions"]:
        item["decision"] = "APPROVE"
    template["drop_columns"]["decision"] = "APPROVE"
    template["ignore_unselected_sheets"]["decision"] = "APPROVE"
    return template


class SchemaOnboardingWorkflowTests(unittest.TestCase):
    def test_client_id_is_safe_at_filesystem_boundaries(self) -> None:
        self.assertEqual(_safe_name("../../client a"), "client-a")

    def test_approved_contract_adapts_and_reenters_bronze(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            workbook = root / "external.xlsx"
            write_external_workbook(workbook)
            bronze_path = write_result(
                validate_workbook(workbook), root / "initial-bronze"
            )
            bronze = read_result(bronze_path)
            proposal_document = proposal(bronze)
            proposal_path = root / "proposal.json"
            proposal_path.write_text(json.dumps(proposal_document))
            approval_document = approve(
                create_approval_template(
                    bronze, proposal_document, client_id="client-a"
                )
            )
            approval_path = root / "approval.json"
            approval_path.write_text(json.dumps(approval_document))

            audit_path = execute_onboarding(
                bronze_path,
                proposal_path,
                approval_path,
                contract_dir=root / "contracts",
                adapted_dir=root / "adapted",
                bronze_dir=root / "onboarded-bronze",
                audit_dir=root / "audits",
            )
            audit = read_json(audit_path)
            records = audit["adapted_bronze_records"]
            adapted_bronze = [read_result(item["bronze_path"]) for item in records]
            adapted = pd.concat(
                [pd.read_excel(item["workbook_path"]) for item in records],
                ignore_index=True,
            )
            contract = read_json(Path(audit["contract_path"]))

        self.assertEqual(audit["status"], "BRONZE_ACCEPTED")
        self.assertEqual(len(records), 2)
        self.assertTrue(
            all(item["lifecycle_state"] == "BRONZE_ACCEPTED" for item in adapted_bronze)
        )
        self.assertEqual(
            {Path(item["workbook_path"]).name for item in records},
            {"canada_2026-01.xlsx", "united_states_2026-01.xlsx"},
        )
        self.assertTrue(
            all(
                item["lineage"]["schema_contract_id"] == contract["contract_id"]
                for item in adapted_bronze
            )
        )
        self.assertEqual(
            adapted.columns.tolist(),
            [
                "Invoice",
                "StockCode",
                "Description",
                "Quantity",
                "InvoiceDate",
                "Price",
                "Customer ID",
                "Country",
            ],
        )
        self.assertEqual(sorted(adapted["Price"].tolist()), [10, 10])

    def test_pending_approval_cannot_compile(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            workbook = Path(tmpdir) / "external.xlsx"
            write_external_workbook(workbook)
            bronze_path = write_result(
                validate_workbook(workbook), Path(tmpdir) / "bronze"
            )
            bronze = read_result(bronze_path)
            proposal_document = proposal(bronze)
            template = create_approval_template(
                bronze, proposal_document, client_id="client-a"
            )
            template["reviewer"] = "data-owner-1"
            template["reviewed_at"] = datetime.now(UTC).isoformat()

        with self.assertRaisesRegex(SchemaContractError, "not approved"):
            from analystops.workflows.schema_onboarding import compile_contract

            compile_contract(bronze, proposal_document, template)

    def test_zero_denominator_fails_with_durable_audit(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            workbook = root / "external.xlsx"
            write_external_workbook(workbook, zero_quantity=True)
            bronze_path = write_result(
                validate_workbook(workbook), root / "initial-bronze"
            )
            bronze = read_result(bronze_path)
            proposal_document = proposal(bronze)
            proposal_path = root / "proposal.json"
            proposal_path.write_text(json.dumps(proposal_document))
            approval_path = root / "approval.json"
            approval_path.write_text(
                json.dumps(
                    approve(
                        create_approval_template(
                            bronze, proposal_document, client_id="client-a"
                        )
                    )
                )
            )

            with self.assertRaisesRegex(
                SchemaContractError, "denominator contains zero"
            ) as raised:
                execute_onboarding(
                    bronze_path,
                    proposal_path,
                    approval_path,
                    contract_dir=root / "contracts",
                    adapted_dir=root / "adapted",
                    bronze_dir=root / "onboarded-bronze",
                    audit_dir=root / "audits",
                )
            audit = read_json(raised.exception.audit_path)

        self.assertEqual(audit["status"], "FAILED")
        self.assertEqual(audit["current_stage"], "ADAPTER_EXECUTION")
        self.assertIn("denominator contains zero", audit["failure"]["message"])


if __name__ == "__main__":
    unittest.main()
