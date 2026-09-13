from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import pandas as pd

from tests.helpers import write_source_workbook
from analystops.datasets.splitter import split_by_country_month
from analystops.datasets.uci_online_retail import load_source_workbook


class DatasetLoaderTests(unittest.TestCase):
    def test_load_source_workbook_normalizes_both_sheets_and_lineage(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            source_path = Path(tmpdir) / "online_retail_II.xlsx"
            write_source_workbook(source_path)

            transactions = load_source_workbook(source_path)

        self.assertEqual(len(transactions), 4)
        self.assertEqual(
            transactions["source_sheet"].tolist(),
            ["Year 2009-2010", "Year 2009-2010", "Year 2010-2011", "Year 2010-2011"],
        )
        self.assertEqual(transactions["source_row_number"].tolist(), [2, 3, 2, 3])
        self.assertEqual(
            transactions["invoice"].tolist(),
            ["536365", "C536379", "539993", "540001"],
        )
        self.assertEqual(
            transactions["stock_code"].tolist(),
            ["85123A", "71053", "POST", "22086"],
        )
        self.assertEqual(
            transactions["customer_id"].iloc[[0, 2, 3]].tolist(),
            ["17850", "12431", "12431"],
        )
        self.assertTrue(pd.isna(transactions.loc[1, "customer_id"]))
        self.assertEqual(
            transactions["reporting_month"].iloc[[0, 1, 2]].tolist(),
            ["2009-12", "2009-12", "2010-12"],
        )
        self.assertTrue(pd.isna(transactions.loc[3, "reporting_month"]))
        self.assertAlmostEqual(transactions.loc[0, "line_revenue"], 15.3)

    def test_split_by_country_month_writes_clean_submission_files(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            source_path = Path(tmpdir) / "online_retail_II.xlsx"
            output_dir = Path(tmpdir) / "generated"
            write_source_workbook(source_path)

            transactions = load_source_workbook(source_path)
            splits = split_by_country_month(transactions, output_dir=output_dir)

            self.assertEqual(len(splits), 3)
            self.assertEqual(
                [split.reporting_month for split in splits],
                ["2009-12", "2009-12", "2010-12"],
            )
            self.assertEqual(splits[0].country, "Germany")
            self.assertTrue(splits[0].output_path.exists())
            self.assertEqual(
                splits[0].source_rows,
                [{"sheet": "Year 2009-2010", "row_number": 3}],
            )

            generated = pd.read_excel(splits[0].output_path, sheet_name="Transactions")

        self.assertEqual(
            generated.columns.tolist(),
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
        self.assertEqual(len(generated), 1)
        self.assertEqual(generated.loc[0, "Country"], "Germany")

    def test_split_by_country_month_protects_existing_files(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            source_path = Path(tmpdir) / "online_retail_II.xlsx"
            output_dir = Path(tmpdir) / "generated"
            write_source_workbook(source_path)
            transactions = load_source_workbook(source_path)

            split_by_country_month(transactions, output_dir=output_dir)

            with self.assertRaises(FileExistsError):
                split_by_country_month(transactions, output_dir=output_dir)

            splits = split_by_country_month(
                transactions,
                output_dir=output_dir,
                overwrite=True,
            )

        self.assertEqual(len(splits), 3)
