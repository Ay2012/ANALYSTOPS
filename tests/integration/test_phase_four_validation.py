from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

import pandas as pd

from tests.helpers import read_json
from analystops.ingestion.validate import validate_workbook, write_result
from analystops.transformations.silver import canonicalize
from analystops.validation.silver import SilverValidationError, validate_silver_corpus


def prepare_silver(
    root: Path,
    name: str,
    rows: list[dict[str, object]],
    *,
    cohort: str = "clean",
) -> tuple[Path, Path]:
    workbook = root / "generated" / cohort / "2011-01" / f"{name}_2011-01.xlsx"
    workbook.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(workbook, engine="openpyxl") as writer:
        pd.DataFrame(rows).to_excel(writer, sheet_name="Transactions", index=False)
    bronze = write_result(
        validate_workbook(workbook), root / "bronze" / cohort
    )
    silver = canonicalize(bronze, output_dir=root / "silver")
    return bronze, silver


class SilverValidationTests(unittest.TestCase):
    def test_profiles_clean_cohort_and_publishes_only_eligible_workbooks(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            valid_rows = [
                {
                    "Invoice": 1001,
                    "StockCode": "A1",
                    "Description": "Sale",
                    "Quantity": 2,
                    "InvoiceDate": "2011-01-01 09:00:00",
                    "Price": 2.5,
                    "Customer ID": None,
                    "Country": "United Kingdom",
                },
                {
                    "Invoice": "C1002",
                    "StockCode": "B2",
                    "Description": None,
                    "Quantity": -1,
                    "InvoiceDate": "2011-01-02 10:00:00",
                    "Price": 3.0,
                    "Customer ID": 502,
                    "Country": "United Kingdom",
                },
                {
                    "Invoice": 1003,
                    "StockCode": "C3",
                    "Description": "Free item",
                    "Quantity": 1,
                    "InvoiceDate": "2011-01-03 11:00:00",
                    "Price": 0,
                    "Customer ID": 503,
                    "Country": "United Kingdom",
                },
                {
                    "Invoice": "A1004",
                    "StockCode": "B",
                    "Description": "Adjust bad debt",
                    "Quantity": 1,
                    "InvoiceDate": "2011-01-04 12:00:00",
                    "Price": -10,
                    "Customer ID": 504,
                    "Country": "United Kingdom",
                },
            ]
            review_rows = [
                {
                    "Invoice": 1005,
                    "StockCode": "D4",
                    "Description": "Unknown adjustment",
                    "Quantity": 1,
                    "InvoiceDate": "2011-01-05 13:00:00",
                    "Price": -1,
                    "Customer ID": 505,
                    "Country": "France",
                }
            ]
            prepare_silver(root, "united_kingdom", valid_rows)
            prepare_silver(root, "france", review_rows)
            prepare_silver(
                root, "fixture", valid_rows, cohort="corrupted/unexpected_columns"
            )

            summary = validate_silver_corpus(
                root / "bronze",
                silver_dir=root / "silver",
                output_dir=root / "validation",
            )
            first_summary = (root / "validation" / "summary.json").read_text()
            first_manifest = (root / "validation" / "publish-manifest.json").read_text()
            rerun = validate_silver_corpus(
                root / "bronze",
                silver_dir=root / "silver",
                output_dir=root / "validation",
            )
            manifest = read_json(root / "validation" / "publish-manifest.json")
            profiles = [
                read_json(path)
                for path in (root / "validation" / "profiles").glob("*.json")
            ]

        self.assertEqual(summary, rerun)
        self.assertEqual(first_summary, json.dumps(summary, indent=2, sort_keys=True) + "\n")
        self.assertEqual(
            first_manifest,
            json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        )
        self.assertEqual(summary["bronze_records"], 3)
        self.assertEqual(summary["candidate_workbooks"], 2)
        self.assertEqual(summary["published_workbooks"], 1)
        self.assertEqual(summary["held_workbooks"], 1)
        self.assertEqual(summary["candidate_rows"], 5)
        self.assertEqual(summary["published_rows"], 4)
        self.assertEqual(len(manifest["entries"]), 1)
        by_country = {profile["country"]: profile for profile in profiles}
        united_kingdom = by_country["United Kingdom"]
        self.assertEqual(united_kingdom["quality_disposition"], "WARN")
        self.assertEqual(
            united_kingdom["publication_state"], "PUBLISHABLE_WITH_WARNINGS"
        )
        self.assertEqual(united_kingdom["metrics"]["net_revenue"], "2.0")
        self.assertEqual(united_kingdom["metrics"]["return_lines"], 1)
        self.assertEqual(united_kingdom["metrics"]["cancellation_lines"], 1)
        self.assertEqual(united_kingdom["metrics"]["zero_value_lines"], 1)
        self.assertEqual(united_kingdom["metrics"]["bad_debt_adjustment_lines"], 1)
        self.assertEqual(
            united_kingdom["metrics"]["bad_debt_adjustment_amount"], "-10.0"
        )
        france = by_country["France"]
        self.assertEqual(france["quality_disposition"], "REVIEW")
        self.assertEqual(france["publication_state"], "REVIEW_REQUIRED")
        self.assertIn("unrecognized_negative_unit_price", france["reason_codes"])

    def test_refuses_tampered_canonical_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            _, silver_result = prepare_silver(
                root,
                "germany",
                [
                    {
                        "Invoice": 1001,
                        "StockCode": "A1",
                        "Description": "Sale",
                        "Quantity": 2,
                        "InvoiceDate": "2011-01-01 09:00:00",
                        "Price": 2.5,
                        "Customer ID": 501,
                        "Country": "Germany",
                    }
                ],
            )
            accepted = Path(read_json(silver_result)["accepted_path"])
            row = json.loads(accepted.read_text())
            row.pop("country")
            accepted.write_text(json.dumps(row) + "\n")

            with self.assertRaisesRegex(SilverValidationError, "canonical schema"):
                validate_silver_corpus(
                    root / "bronze",
                    silver_dir=root / "silver",
                    output_dir=root / "validation",
                )


if __name__ == "__main__":
    unittest.main()
