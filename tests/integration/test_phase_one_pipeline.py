from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import pandas as pd

from tests.helpers import (
    read_json,
    stable_manifests,
    workbook_columns,
    workbook_row_count,
    workbook_sheets,
    write_clean_submission,
    write_source_workbook,
)
from analystops.datasets.corruptions import corrupt_submission
from analystops.datasets.manifests import (
    generate_manifests,
    write_clean_manifest,
    write_corruption_manifest,
)
from analystops.datasets.splitter import split_by_country_month
from analystops.datasets.uci_online_retail import load_source_workbook


class ManifestTests(unittest.TestCase):
    def test_clean_manifest_matches_generated_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            source_path = Path(tmpdir) / "online_retail_II.xlsx"
            output_dir = Path(tmpdir) / "generated"
            manifest_dir = Path(tmpdir) / "manifests"
            write_source_workbook(source_path)

            transactions = load_source_workbook(source_path)
            split = split_by_country_month(transactions, output_dir=output_dir)[0]
            manifest_path = write_clean_manifest(split, manifest_dir=manifest_dir)
            manifest = read_json(manifest_path)
            generated = pd.read_excel(split.output_path, sheet_name="Transactions")

        self.assertEqual(manifest["row_count"], len(generated))
        self.assertEqual(manifest["expected_handling"], "accept")
        self.assertEqual(manifest["source_rows"], split.source_rows)

    def test_corruption_manifest_matches_result(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            clean_path = Path(tmpdir) / "clean" / "sample.xlsx"
            manifest_dir = Path(tmpdir) / "manifests"
            write_clean_submission(clean_path)

            result = corrupt_submission(
                clean_path,
                scenario="missing_required_column",
                seed=11,
                output_dir=Path(tmpdir) / "corrupted",
            )
            manifest_path = write_corruption_manifest(
                result,
                source_rows=[{"sheet": "Year 2010-2011", "row_number": 2}],
                manifest_dir=manifest_dir,
            )
            manifest = read_json(manifest_path)
            generated = pd.read_excel(result.output_path, sheet_name="Transactions")

        self.assertEqual(manifest["row_count_after"], len(generated))
        self.assertEqual(manifest["scenario"], "missing_required_column")
        self.assertEqual(manifest["expected_handling"], "quarantine")
        self.assertNotIn(manifest["details"]["missing_column"], generated.columns)

    def test_generated_manifests_match_their_workbooks(self) -> None:
        scenarios = [
            "missing_required_column",
            "duplicate_rows",
            "incomplete_file",
            "multi_sheet_workbook",
        ]
        with tempfile.TemporaryDirectory() as tmpdir:
            source_path = Path(tmpdir) / "online_retail_II.xlsx"
            clean_dir = Path(tmpdir) / "clean"
            corrupted_dir = Path(tmpdir) / "corrupted"
            write_source_workbook(source_path)

            paths = generate_manifests(
                source_path=source_path,
                clean_dir=clean_dir,
                corrupted_dir=corrupted_dir,
                manifest_dir=Path(tmpdir) / "manifests",
                seed=42,
                scenarios=scenarios,
                max_files=1,
            )
            manifests = [read_json(path) for path in paths]
            workbooks = sorted(clean_dir.rglob("*.xlsx")) + sorted(
                corrupted_dir.rglob("*.xlsx")
            )

            self.assertEqual(len(manifests), len(workbooks))
            self.assertEqual(
                {Path(item["file_path"]) for item in manifests},
                set(workbooks),
            )

            for manifest in manifests:
                workbook = Path(manifest["file_path"])
                columns = workbook_columns(workbook)
                self.assertEqual(manifest["observed_schema"], columns)

                if manifest["file_type"] == "clean_submission":
                    self.assertEqual(manifest["row_count"], workbook_row_count(workbook))
                    continue

                details = manifest["details"]
                self.assertEqual(manifest["row_count_after"], workbook_row_count(workbook))

                if manifest["scenario"] == "missing_required_column":
                    self.assertNotIn(details["missing_column"], columns)
                elif manifest["scenario"] == "duplicate_rows":
                    self.assertEqual(
                        manifest["row_count_after"],
                        manifest["row_count_before"] + details["duplicate_count"],
                    )
                elif manifest["scenario"] == "incomplete_file":
                    self.assertEqual(
                        manifest["row_count_after"],
                        details["retained_row_count"],
                    )
                    self.assertEqual(manifest["expected_handling"], "review")
                elif manifest["scenario"] == "multi_sheet_workbook":
                    self.assertTrue(
                        set(details["extra_sheets"]).issubset(workbook_sheets(workbook))
                    )

    def test_generate_manifests_is_reproducible_for_fixed_seed(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            source_path = Path(tmpdir) / "online_retail_II.xlsx"
            write_source_workbook(source_path)

            first = stable_manifests(
                generate_manifests(
                    source_path=source_path,
                    clean_dir=Path(tmpdir) / "clean-first",
                    corrupted_dir=Path(tmpdir) / "corrupted-first",
                    manifest_dir=Path(tmpdir) / "manifests-first",
                    seed=7,
                    scenarios=["currency_strings", "date_format_changes"],
                    max_files=1,
                )
            )
            second = stable_manifests(
                generate_manifests(
                    source_path=source_path,
                    clean_dir=Path(tmpdir) / "clean-second",
                    corrupted_dir=Path(tmpdir) / "corrupted-second",
                    manifest_dir=Path(tmpdir) / "manifests-second",
                    seed=7,
                    scenarios=["currency_strings", "date_format_changes"],
                    max_files=1,
                )
            )

            self.assertEqual(first, second)
