from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import pandas as pd

from tests.helpers import write_clean_submission
from analystops.datasets.corruptions import (
    SCENARIO_NAMES,
    corrupt_clean_submissions,
    corrupt_submission,
)


class CorruptionTests(unittest.TestCase):
    def test_currency_string_corruption_is_reproducible_for_fixed_seed(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            clean_path = Path(tmpdir) / "clean" / "sample.xlsx"
            write_clean_submission(clean_path)

            first = corrupt_submission(
                clean_path,
                scenario="currency_strings",
                seed=7,
                output_dir=Path(tmpdir) / "first",
            )
            second = corrupt_submission(
                clean_path,
                scenario="currency_strings",
                seed=7,
                output_dir=Path(tmpdir) / "second",
            )

            first_rows = first.details["selected_excel_rows"]
            second_rows = second.details["selected_excel_rows"]
            first_frame = pd.read_excel(first.output_path, sheet_name="Transactions")
            second_frame = pd.read_excel(second.output_path, sheet_name="Transactions")

        self.assertEqual(first_rows, second_rows)
        pd.testing.assert_frame_equal(first_frame, second_frame)
        price_values = first_frame["Price"].astype(str).tolist()
        self.assertTrue(any(value.startswith("$") for value in price_values))

    def test_missing_required_column_drops_seeded_column(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            clean_path = Path(tmpdir) / "clean" / "sample.xlsx"
            write_clean_submission(clean_path)

            result = corrupt_submission(
                clean_path,
                scenario="missing_required_column",
                seed=11,
                output_dir=Path(tmpdir) / "corrupted",
            )
            frame = pd.read_excel(result.output_path, sheet_name="Transactions")

        missing_column = result.details["missing_column"]
        self.assertNotIn(missing_column, frame.columns)
        self.assertEqual(result.row_count_before, result.row_count_after)

    def test_missing_customer_ids_allows_files_with_no_customer_ids(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            clean_path = Path(tmpdir) / "clean" / "sample.xlsx"
            write_clean_submission(clean_path)
            frame = pd.read_excel(clean_path, sheet_name="Transactions")
            frame["Customer ID"] = pd.NA
            with pd.ExcelWriter(clean_path, engine="openpyxl") as writer:
                frame.to_excel(writer, sheet_name="Transactions", index=False)

            result = corrupt_submission(
                clean_path,
                scenario="missing_customer_ids",
                seed=5,
                output_dir=Path(tmpdir) / "corrupted",
            )

            self.assertTrue(result.output_path.exists())
            self.assertTrue(result.details["preexisting_all_missing"])
            self.assertEqual(result.details["selected_excel_rows"], [])

    def test_multi_sheet_corruption_adds_extra_sheet(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            clean_path = Path(tmpdir) / "clean" / "sample.xlsx"
            write_clean_submission(clean_path)

            result = corrupt_submission(
                clean_path,
                scenario="multi_sheet_workbook",
                seed=3,
                output_dir=Path(tmpdir) / "corrupted",
            )

            with pd.ExcelFile(result.output_path) as workbook:
                sheet_names = workbook.sheet_names

        self.assertEqual(sheet_names, ["Transactions", "Lookup"])
        self.assertEqual(result.details["extra_sheets"], ["Lookup"])

    def test_incomplete_file_removes_the_only_row(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            clean_path = Path(tmpdir) / "clean" / "sample.xlsx"
            write_clean_submission(clean_path)
            frame = pd.read_excel(clean_path, sheet_name="Transactions").head(1)
            with pd.ExcelWriter(clean_path, engine="openpyxl") as writer:
                frame.to_excel(writer, sheet_name="Transactions", index=False)

            result = corrupt_submission(
                clean_path,
                scenario="incomplete_file",
                seed=3,
                output_dir=Path(tmpdir) / "corrupted",
            )
            corrupted = pd.read_excel(result.output_path, sheet_name="Transactions")

        self.assertEqual(result.row_count_before, 1)
        self.assertEqual(result.row_count_after, 0)
        self.assertTrue(corrupted.empty)

    def test_corrupt_clean_submissions_generates_requested_scenarios(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            clean_path = Path(tmpdir) / "clean" / "sample.xlsx"
            write_clean_submission(clean_path)

            results = corrupt_clean_submissions(
                [clean_path],
                seed=42,
                output_dir=Path(tmpdir) / "corrupted",
                scenarios=["renamed_columns", "duplicate_rows"],
            )

            self.assertTrue(all(result.output_path.exists() for result in results))

        self.assertEqual(
            [result.scenario for result in results],
            ["renamed_columns", "duplicate_rows"],
        )
        self.assertEqual([result.seed for result in results], [42, 43])

    def test_all_phase_one_scenarios_can_be_generated(self) -> None:
        expected = {
            "renamed_columns",
            "currency_strings",
            "date_format_changes",
            "duplicate_rows",
            "missing_customer_ids",
            "missing_required_column",
            "unexpected_columns",
            "incomplete_file",
            "duplicate_prior_month_upload",
            "multi_sheet_workbook",
        }

        self.assertEqual(set(SCENARIO_NAMES), expected)

        with tempfile.TemporaryDirectory() as tmpdir:
            clean_path = Path(tmpdir) / "clean" / "sample.xlsx"
            write_clean_submission(clean_path)

            results = corrupt_clean_submissions(
                [clean_path],
                seed=100,
                output_dir=Path(tmpdir) / "corrupted",
                scenarios=SCENARIO_NAMES,
            )

            self.assertEqual(len(results), len(SCENARIO_NAMES))
            self.assertTrue(all(result.output_path.exists() for result in results))
