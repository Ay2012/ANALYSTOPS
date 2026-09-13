from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))


def write_source_workbook(path: Path) -> None:
    first_sheet = pd.DataFrame(
        [
            {
                "Invoice": 536365,
                "StockCode": "85123A",
                "Description": "WHITE HANGING HEART T-LIGHT HOLDER",
                "Quantity": 6,
                "InvoiceDate": "2009-12-01 07:45:00",
                "Price": 2.55,
                "Customer ID": 17850,
                "Country": "United Kingdom",
            },
            {
                "Invoice": "C536379",
                "StockCode": 71053,
                "Description": "WHITE METAL LANTERN",
                "Quantity": -1,
                "InvoiceDate": "2009-12-01 09:41:00",
                "Price": 3.39,
                "Customer ID": None,
                "Country": "Germany",
            },
        ]
    )
    second_sheet = pd.DataFrame(
        [
            {
                "Invoice": 539993,
                "StockCode": "POST",
                "Description": "POSTAGE",
                "Quantity": 1,
                "InvoiceDate": "2010-12-01 08:22:00",
                "Price": 18.0,
                "Customer ID": 12431,
                "Country": "Australia",
            },
            {
                "Invoice": 540001,
                "StockCode": 22086,
                "Description": "PAPER CHAIN KIT 50'S CHRISTMAS",
                "Quantity": 1,
                "InvoiceDate": None,
                "Price": 2.95,
                "Customer ID": 12431,
                "Country": "Australia",
            },
        ]
    )

    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        first_sheet.to_excel(writer, sheet_name="Year 2009-2010", index=False)
        second_sheet.to_excel(writer, sheet_name="Year 2010-2011", index=False)


def write_clean_submission(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    rows = [
        {
            "Invoice": 1001,
            "StockCode": "A1",
            "Description": "Test item A",
            "Quantity": 2,
            "InvoiceDate": "2011-01-01 09:00:00",
            "Price": 2.5,
            "Customer ID": 501,
            "Country": "United Kingdom",
        },
        {
            "Invoice": 1002,
            "StockCode": "B2",
            "Description": "Test item B",
            "Quantity": 3,
            "InvoiceDate": "2011-01-02 10:30:00",
            "Price": 4.0,
            "Customer ID": 502,
            "Country": "United Kingdom",
        },
        {
            "Invoice": 1003,
            "StockCode": "C3",
            "Description": "Test item C",
            "Quantity": 1,
            "InvoiceDate": "2011-01-03 11:45:00",
            "Price": 7.25,
            "Customer ID": 503,
            "Country": "United Kingdom",
        },
        {
            "Invoice": 1004,
            "StockCode": "D4",
            "Description": "Test item D",
            "Quantity": 5,
            "InvoiceDate": "2011-01-04 14:00:00",
            "Price": 1.99,
            "Customer ID": 504,
            "Country": "United Kingdom",
        },
    ]

    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        pd.DataFrame(rows).to_excel(writer, sheet_name="Transactions", index=False)


def read_json(path: Path) -> dict:
    return json.loads(path.read_text())


def stable_manifests(paths: list[Path]) -> list[dict]:
    return [
        {
            key: value
            for key, value in read_json(path).items()
            if key not in {"file_path", "source_file_path"}
        }
        for path in paths
    ]


def workbook_columns(path: Path) -> list[str]:
    return pd.read_excel(path, sheet_name="Transactions", nrows=0).columns.tolist()


def workbook_row_count(path: Path) -> int:
    return len(pd.read_excel(path, sheet_name="Transactions"))


def workbook_sheets(path: Path) -> list[str]:
    with pd.ExcelFile(path) as workbook:
        return workbook.sheet_names
