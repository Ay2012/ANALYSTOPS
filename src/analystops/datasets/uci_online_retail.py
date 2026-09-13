"""Loader for the UCI Online Retail II source workbook.

The Phase One simulator treats the original workbook as read-only source data.
This module only reads it and normalizes rows into the internal transaction
shape used by downstream splitters and corruption injectors.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_SOURCE_WORKBOOK = PROJECT_ROOT / "data" / "source" / "online_retail_II.xlsx"

SOURCE_SHEETS = ("Year 2009-2010", "Year 2010-2011")

SOURCE_TO_CANONICAL_COLUMNS = {
    "Invoice": "invoice",
    "StockCode": "stock_code",
    "Description": "description",
    "Quantity": "quantity",
    "InvoiceDate": "invoice_date",
    "Price": "unit_price",
    "Customer ID": "customer_id",
    "Country": "country",
}

NORMALIZED_COLUMNS = [
    "source_file",
    "source_sheet",
    "source_row_number",
    "invoice",
    "stock_code",
    "description",
    "quantity",
    "invoice_date",
    "unit_price",
    "customer_id",
    "country",
    "reporting_month",
    "line_revenue",
]


class SourceWorkbookError(ValueError):
    """Raised when the source workbook does not match the expected contract."""


@dataclass(frozen=True)
class WorkbookProfile:
    """Small summary of normalized transaction data."""

    row_count: int
    country_count: int
    first_invoice_date: pd.Timestamp | None
    last_invoice_date: pd.Timestamp | None


def load_source_workbook(
    source_path: Path | str = DEFAULT_SOURCE_WORKBOOK,
    *,
    sheets: Iterable[str] = SOURCE_SHEETS,
) -> pd.DataFrame:
    """Read both UCI sheets and return normalized transaction rows.

    The returned frame keeps source lineage columns so later manifests can point
    generated rows back to the original workbook sheet and row number.
    """

    path = Path(source_path)
    sheet_names = tuple(sheets)

    if not path.exists():
        raise FileNotFoundError(f"Source workbook not found: {path}")

    with pd.ExcelFile(path) as excel_file:
        missing_sheets = [
            name for name in sheet_names if name not in excel_file.sheet_names
        ]
        if missing_sheets:
            missing = ", ".join(missing_sheets)
            raise SourceWorkbookError(
                f"Source workbook missing expected sheets: {missing}"
            )

        frames = [
            _normalize_sheet(
                pd.read_excel(excel_file, sheet_name=sheet_name),
                source_file=path.name,
                source_sheet=sheet_name,
            )
            for sheet_name in sheet_names
        ]

    return pd.concat(frames, ignore_index=True)[NORMALIZED_COLUMNS]


def profile_transactions(transactions: pd.DataFrame) -> WorkbookProfile:
    """Return a compact profile for normalized transaction data."""

    if transactions.empty:
        return WorkbookProfile(
            row_count=0,
            country_count=0,
            first_invoice_date=None,
            last_invoice_date=None,
        )

    invoice_dates = pd.to_datetime(transactions["invoice_date"], errors="coerce")
    return WorkbookProfile(
        row_count=len(transactions),
        country_count=transactions["country"].dropna().nunique(),
        first_invoice_date=invoice_dates.min(),
        last_invoice_date=invoice_dates.max(),
    )


def _normalize_sheet(
    raw_sheet: pd.DataFrame,
    *,
    source_file: str,
    source_sheet: str,
) -> pd.DataFrame:
    missing_columns = [
        column for column in SOURCE_TO_CANONICAL_COLUMNS if column not in raw_sheet.columns
    ]
    if missing_columns:
        missing = ", ".join(missing_columns)
        raise SourceWorkbookError(
            f"Sheet {source_sheet!r} missing expected columns: {missing}"
        )

    normalized = raw_sheet.rename(columns=SOURCE_TO_CANONICAL_COLUMNS)
    normalized = normalized[list(SOURCE_TO_CANONICAL_COLUMNS.values())].copy()

    normalized.insert(0, "source_row_number", raw_sheet.index + 2)
    normalized.insert(0, "source_sheet", source_sheet)
    normalized.insert(0, "source_file", source_file)

    normalized["invoice"] = _normalize_identifier(normalized["invoice"])
    normalized["stock_code"] = _normalize_identifier(normalized["stock_code"])
    normalized["customer_id"] = _normalize_identifier(normalized["customer_id"])
    normalized["description"] = _normalize_text(normalized["description"])
    normalized["country"] = _normalize_text(normalized["country"])
    normalized["quantity"] = pd.to_numeric(normalized["quantity"], errors="coerce")
    normalized["invoice_date"] = pd.to_datetime(
        normalized["invoice_date"], errors="coerce"
    )
    normalized["unit_price"] = pd.to_numeric(normalized["unit_price"], errors="coerce")
    normalized["reporting_month"] = (
        normalized["invoice_date"].dt.to_period("M").astype("string")
    )
    normalized["line_revenue"] = normalized["quantity"] * normalized["unit_price"]

    return normalized


def _normalize_identifier(values: pd.Series) -> pd.Series:
    def clean(value: object) -> str | pd.NA:
        if pd.isna(value):
            return pd.NA

        if isinstance(value, float) and value.is_integer():
            return str(int(value))

        text = str(value).strip()
        if text.endswith(".0") and text[:-2].isdigit():
            return text[:-2]
        return text

    return values.map(clean).astype("string")


def _normalize_text(values: pd.Series) -> pd.Series:
    return values.astype("string").str.strip()
