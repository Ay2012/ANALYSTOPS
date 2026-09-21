from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

import pandas as pd

from tests.helpers import read_json, write_clean_submission
from analystops.ingestion.review import reassess_workbook
from analystops.ingestion.validate import validate_workbook, write_result
from analystops.transformations.operations import PLAN_VERSION
from analystops.transformations.silver import (
    SilverCanonicalizationError,
    canonicalize,
    canonicalize_corpus,
)


def read_jsonl(path: Path) -> list[dict[str, object]]:
    return [json.loads(line) for line in path.read_text().splitlines()]


class SilverCanonicalizationTests(unittest.TestCase):
    def test_canonicalizes_accepted_workbook_with_lineage_deterministically(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            workbook = root / "clean.xlsx"
            write_clean_submission(workbook)
            bronze = write_result(validate_workbook(workbook), root / "bronze")

            result_path = canonicalize(bronze, output_dir=root / "silver")
            first_output = read_json(result_path)
            accepted_path = Path(first_output["accepted_path"])
            first_rows = accepted_path.read_text()

            second_result_path = canonicalize(bronze, output_dir=root / "silver")

            self.assertEqual(first_output, read_json(second_result_path))
            self.assertEqual(first_rows, accepted_path.read_text())

        self.assertEqual(first_output["input_rows"], 4)
        self.assertEqual(first_output["accepted_rows"], 4)
        self.assertEqual(first_output["rejected_rows"], 0)
        first = json.loads(first_rows.splitlines()[0])
        self.assertEqual(
            list(first),
            [
                "invoice_id",
                "product_id",
                "product_description",
                "quantity",
                "transaction_timestamp",
                "unit_price",
                "customer_id",
                "country",
                "source_file_id",
                "source_row_number",
            ],
        )
        self.assertEqual(first["invoice_id"], "1001")
        self.assertEqual(first["quantity"], 2)
        self.assertEqual(first["transaction_timestamp"], "2011-01-01T09:00:00")
        self.assertEqual(first["unit_price"], "2.5")
        self.assertEqual(first["customer_id"], "501")
        self.assertEqual(first["source_file_id"], first_output["source_file_id"])
        self.assertEqual(first["source_row_number"], 2)

    def test_canonicalizes_only_accepted_materialized_records(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            workbook = root / "clean.xlsx"
            write_clean_submission(workbook)
            accepted = validate_workbook(workbook)
            write_result(accepted, root / "bronze" / "clean")
            duplicate = validate_workbook(
                workbook, seen_hashes=[accepted.file_hash]
            )
            write_result(duplicate, root / "bronze" / "duplicate")

            summary = canonicalize_corpus(
                root / "bronze", output_dir=root / "silver"
            )

        self.assertEqual(summary["records"], 2)
        self.assertEqual(
            summary["states"], {"BRONZE_ACCEPTED": 1, "DUPLICATE": 1}
        )
        self.assertEqual(summary["canonicalized_workbooks"], 1)
        self.assertEqual(summary["input_rows"], 4)
        self.assertEqual(summary["accepted_rows"], 4)
        self.assertEqual(summary["rejected_rows"], 0)

    def test_handles_accepted_variants_and_reconciles_rejected_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            workbook = root / "warnings.xlsx"
            transactions = pd.DataFrame(
                [
                    {
                        "Invoice": 1001,
                        "StockCode": "A1",
                        "Description": "Valid item",
                        "Quantity": 2,
                        "InvoiceDate": "2011-01-01 09:00:00",
                        "Price": 2.5,
                        "Country": "United Kingdom",
                        "Unexpected": "ignored",
                    },
                    {
                        "Invoice": 1002,
                        "StockCode": "B2",
                        "Description": "Missing quantity",
                        "Quantity": None,
                        "InvoiceDate": "2011-01-02 10:00:00",
                        "Price": 4.0,
                        "Country": "United Kingdom",
                        "Unexpected": "ignored",
                    },
                ]
            )
            with pd.ExcelWriter(workbook, engine="openpyxl") as writer:
                transactions.to_excel(writer, sheet_name="Transactions", index=False)
                pd.DataFrame({"Code": ["UK"]}).to_excel(
                    writer, sheet_name="Lookup", index=False
                )

            bronze_result = validate_workbook(workbook)
            self.assertEqual(bronze_result.lifecycle_state, "BRONZE_ACCEPTED")
            bronze = write_result(bronze_result, root / "bronze")
            result = read_json(canonicalize(bronze, output_dir=root / "silver"))
            accepted = read_jsonl(Path(result["accepted_path"]))
            rejected = read_jsonl(Path(result["rejected_path"]))

        self.assertEqual(result["input_rows"], 2)
        self.assertEqual(result["accepted_rows"], 1)
        self.assertEqual(result["rejected_rows"], 1)
        self.assertIsNone(accepted[0]["customer_id"])
        self.assertNotIn("Unexpected", accepted[0])
        self.assertEqual(rejected[0]["source_row_number"], 3)
        self.assertEqual(rejected[0]["reason_codes"], ["invalid_quantity"])

    def test_executes_only_the_transformation_plan_approved_by_bronze(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            workbook = root / "reviewed.xlsx"
            source = root / "source.xlsx"
            write_clean_submission(source)
            rows = pd.read_excel(source, sheet_name="Transactions")
            rows = rows.rename(columns={"Quantity": "Units"})
            rows["Price"] = rows["Price"].map(lambda value: f"${value:,.2f}")
            rows["InvoiceDate"] = pd.to_datetime(rows["InvoiceDate"]).dt.strftime(
                "%d/%m/%Y %H:%M"
            )
            rows = pd.concat([rows, rows.iloc[[0]]], ignore_index=True)
            with pd.ExcelWriter(workbook, engine="openpyxl") as writer:
                rows.to_excel(writer, sheet_name="Transactions", index=False)

            initial = validate_workbook(workbook)
            resolution = {
                "file_hash": initial.file_hash,
                "reviewed_by": "analyst@example.com",
                "reviewed_at": "2026-09-19T12:00:00Z",
                "resolutions": [
                    {
                        "finding_code": "renamed_required_columns",
                        "action": "map_columns",
                        "details": {"mapping": {"Units": "Quantity"}},
                    },
                    {
                        "finding_code": "price_parse_failures",
                        "action": "confirm_numeric_format",
                        "details": {"format": "currency"},
                    },
                    {
                        "finding_code": "non_iso_date_strings",
                        "action": "confirm_date_format",
                        "details": {"format": "%d/%m/%Y %H:%M"},
                    },
                    {
                        "finding_code": "exact_duplicate_rows",
                        "action": "deduplicate_in_silver",
                    },
                ],
            }
            accepted = reassess_workbook(workbook, resolution)
            bronze_path = write_result(accepted, root / "bronze")
            bronze = read_json(bronze_path)
            plan = {
                "plan_version": PLAN_VERSION,
                "source_file_id": accepted.file_hash,
                "bronze_record_hash": bronze["record_hash"],
                "operations": [
                    {
                        "finding_code": item["finding_code"],
                        "operation": item["action"],
                        "parameters": item.get("details", {}),
                    }
                    for item in resolution["resolutions"]
                ],
            }
            plan_path = root / "plan.json"
            plan_path.write_text(json.dumps(plan))

            with self.assertRaisesRegex(
                SilverCanonicalizationError, "requires a transformation plan"
            ):
                canonicalize(bronze_path, output_dir=root / "missing-plan")

            invalid = json.loads(json.dumps(plan))
            invalid["operations"][0]["operation"] = "drop_rows"
            with self.assertRaisesRegex(
                SilverCanonicalizationError, "not approved"
            ):
                canonicalize(
                    bronze_path,
                    output_dir=root / "invalid-plan",
                    plan=invalid,
                )

            result = read_json(
                canonicalize(
                    bronze_path,
                    output_dir=root / "silver",
                    plan=plan_path,
                )
            )
            canonical_rows = read_jsonl(Path(result["accepted_path"]))

        self.assertEqual(accepted.lifecycle_state, "BRONZE_ACCEPTED")
        self.assertEqual(result["input_rows"], 5)
        self.assertEqual(result["dropped_rows"], 1)
        self.assertEqual(result["accepted_rows"], 4)
        self.assertEqual(result["rejected_rows"], 0)
        self.assertEqual(len(result["applied_operations"]), 4)
        self.assertIsNotNone(result["transformation_plan_hash"])
        self.assertEqual(
            [row["source_row_number"] for row in canonical_rows],
            [2, 3, 4, 5],
        )
        self.assertEqual(canonical_rows[0]["quantity"], 2)
        self.assertEqual(canonical_rows[0]["unit_price"], "2.5")
        self.assertEqual(
            canonical_rows[0]["transaction_timestamp"],
            "2011-01-01T09:00:00",
        )

    def test_refuses_unauthorized_or_changed_workbook(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            workbook = root / "clean.xlsx"
            write_clean_submission(workbook)
            result = validate_workbook(workbook)
            bronze = write_result(result, root / "bronze")

            payload = read_json(bronze)
            payload["lifecycle_state"] = "AWAITING_REVIEW"
            bronze.write_text(json.dumps(payload))
            with self.assertRaisesRegex(
                SilverCanonicalizationError, "record hash mismatch"
            ):
                canonicalize(bronze, output_dir=root / "silver")

            write_result(result, root / "bronze")
            with workbook.open("ab") as file:
                file.write(b"changed")
            with self.assertRaisesRegex(SilverCanonicalizationError, "hash mismatch"):
                canonicalize(bronze, output_dir=root / "silver")


if __name__ == "__main__":
    unittest.main()
