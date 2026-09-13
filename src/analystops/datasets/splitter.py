"""Split normalized UCI Online Retail II rows into clean monthly submissions."""

from __future__ import annotations

import argparse
import re
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from .uci_online_retail import (
    DEFAULT_SOURCE_WORKBOOK,
    NORMALIZED_COLUMNS,
    load_source_workbook,
)


PROJECT_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_GENERATED_DIR = PROJECT_ROOT / "data" / "generated" / "clean"

SUBMISSION_COLUMNS = {
    "invoice": "Invoice",
    "stock_code": "StockCode",
    "description": "Description",
    "quantity": "Quantity",
    "invoice_date": "InvoiceDate",
    "unit_price": "Price",
    "customer_id": "Customer ID",
    "country": "Country",
}


@dataclass(frozen=True)
class SubmissionSplit:
    """Metadata for one generated clean submission workbook."""

    country: str
    reporting_month: str
    row_count: int
    output_path: Path
    source_rows: list[dict[str, int | str]]


def generate_clean_submissions(
    source_path: Path | str = DEFAULT_SOURCE_WORKBOOK,
    *,
    output_dir: Path | str = DEFAULT_GENERATED_DIR,
    max_files: int | None = None,
    min_rows: int = 1,
    overwrite: bool = False,
) -> list[SubmissionSplit]:
    """Load the source workbook and split it into clean monthly submissions."""

    transactions = load_source_workbook(source_path)
    return split_by_country_month(
        transactions,
        output_dir=output_dir,
        max_files=max_files,
        min_rows=min_rows,
        overwrite=overwrite,
    )


def split_by_country_month(
    transactions: pd.DataFrame,
    *,
    output_dir: Path | str = DEFAULT_GENERATED_DIR,
    max_files: int | None = None,
    min_rows: int = 1,
    overwrite: bool = False,
) -> list[SubmissionSplit]:
    """Write country/month Excel submissions from normalized transactions."""

    _validate_normalized_transactions(transactions)

    output_root = Path(output_dir)
    splits: list[SubmissionSplit] = []
    eligible = transactions.dropna(subset=["country", "reporting_month"])
    grouped = eligible.sort_values(
        ["reporting_month", "country", "source_sheet", "source_row_number"]
    ).groupby(["reporting_month", "country"], sort=True)

    for (reporting_month, country), group in grouped:
        if len(group) < min_rows:
            continue

        period_dir = output_root / str(reporting_month)
        file_name = f"{_slugify(country)}_{reporting_month}.xlsx"
        output_path = period_dir / file_name

        if output_path.exists() and not overwrite:
            raise FileExistsError(
                f"Refusing to overwrite existing generated file: {output_path}"
            )

        period_dir.mkdir(parents=True, exist_ok=True)
        _write_submission_workbook(group, output_path)

        splits.append(
            SubmissionSplit(
                country=str(country),
                reporting_month=str(reporting_month),
                row_count=len(group),
                output_path=output_path,
                source_rows=_source_rows(group),
            )
        )

        if max_files is not None and len(splits) >= max_files:
            break

    return splits


def to_submission_frame(transactions: pd.DataFrame) -> pd.DataFrame:
    """Return the producer-facing columns for a generated submission."""

    _validate_normalized_transactions(transactions)
    return transactions[list(SUBMISSION_COLUMNS)].rename(columns=SUBMISSION_COLUMNS)


def _write_submission_workbook(transactions: pd.DataFrame, output_path: Path) -> None:
    submission = to_submission_frame(transactions)

    with pd.ExcelWriter(
        output_path,
        engine="openpyxl",
        datetime_format="yyyy-mm-dd hh:mm:ss",
    ) as writer:
        submission.to_excel(writer, sheet_name="Transactions", index=False)
        worksheet = writer.book["Transactions"]
        worksheet.freeze_panes = "A2"
        for column_cells in worksheet.columns:
            header = str(column_cells[0].value)
            width = min(max(len(header) + 2, 12), 32)
            worksheet.column_dimensions[column_cells[0].column_letter].width = width


def _validate_normalized_transactions(transactions: pd.DataFrame) -> None:
    missing_columns = [column for column in NORMALIZED_COLUMNS if column not in transactions]
    if missing_columns:
        missing = ", ".join(missing_columns)
        raise ValueError(f"Transactions missing normalized columns: {missing}")


def _source_rows(transactions: pd.DataFrame) -> list[dict[str, int | str]]:
    return [
        {
            "sheet": str(row.source_sheet),
            "row_number": int(row.source_row_number),
        }
        for row in transactions[["source_sheet", "source_row_number"]].itertuples()
    ]


def _slugify(value: object) -> str:
    text = str(value).strip().lower()
    text = re.sub(r"[^a-z0-9]+", "_", text)
    return text.strip("_") or "unknown"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Generate clean country/month submissions from UCI Online Retail II."
    )
    parser.add_argument(
        "--source",
        default=DEFAULT_SOURCE_WORKBOOK,
        type=Path,
        help="Path to the read-only source workbook.",
    )
    parser.add_argument(
        "--output-dir",
        default=DEFAULT_GENERATED_DIR,
        type=Path,
        help="Directory for generated clean submissions.",
    )
    parser.add_argument(
        "--max-files",
        default=None,
        type=int,
        help="Optional cap for smoke-test generation.",
    )
    parser.add_argument(
        "--min-rows",
        default=1,
        type=int,
        help="Skip country/month groups smaller than this row count.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Allow replacing existing generated submissions.",
    )
    args = parser.parse_args(argv)

    splits = generate_clean_submissions(
        args.source,
        output_dir=args.output_dir,
        max_files=args.max_files,
        min_rows=args.min_rows,
        overwrite=args.overwrite,
    )

    print(f"Generated {len(splits)} clean submission workbook(s).")
    for split in splits[:10]:
        print(f"{split.reporting_month} {split.country}: {split.row_count} rows")
    if len(splits) > 10:
        print(f"...and {len(splits) - 10} more.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
