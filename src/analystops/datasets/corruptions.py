"""Seeded corruption injectors for generated submission workbooks."""

from __future__ import annotations

import argparse
import math
import random
import shutil
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Callable, Iterable

import pandas as pd

from .splitter import DEFAULT_GENERATED_DIR


PROJECT_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_CORRUPTED_DIR = PROJECT_ROOT / "data" / "generated" / "corrupted"

TRANSACTIONS_SHEET = "Transactions"
FIXED_WORKBOOK_TIMESTAMP = datetime(2000, 1, 1, 0, 0, 0)

REQUIRED_SUBMISSION_COLUMNS = (
    "Invoice",
    "StockCode",
    "Description",
    "Quantity",
    "InvoiceDate",
    "Price",
    "Country",
)

SCENARIO_NAMES = (
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
)


class CorruptionError(ValueError):
    """Raised when a requested corruption cannot be applied."""


@dataclass(frozen=True)
class CorruptionResult:
    """Metadata for one corrupted workbook."""

    scenario: str
    seed: int
    source_path: Path
    output_path: Path
    row_count_before: int
    row_count_after: int
    details: dict[str, object]


ScenarioFunction = Callable[
    [pd.DataFrame, random.Random],
    tuple[pd.DataFrame, dict[str, object]],
]


def discover_clean_submission_files(
    input_dir: Path | str = DEFAULT_GENERATED_DIR,
) -> list[Path]:
    """Return clean submission workbooks in stable path order."""

    return sorted(Path(input_dir).glob("*/*.xlsx"))


def corrupt_clean_submissions(
    input_paths: Iterable[Path | str],
    *,
    seed: int,
    output_dir: Path | str = DEFAULT_CORRUPTED_DIR,
    scenarios: Iterable[str] = SCENARIO_NAMES,
    overwrite: bool = False,
) -> list[CorruptionResult]:
    """Apply each requested scenario to each clean submission workbook."""

    results: list[CorruptionResult] = []
    scenario_names = tuple(scenarios)

    for source_index, input_path in enumerate(input_paths):
        for scenario_index, scenario in enumerate(scenario_names):
            scenario_seed = _derive_scenario_seed(seed, source_index, scenario_index)
            results.append(
                corrupt_submission(
                    input_path,
                    scenario=scenario,
                    seed=scenario_seed,
                    output_dir=output_dir,
                    overwrite=overwrite,
                )
            )

    return results


def corrupt_submission(
    input_path: Path | str,
    *,
    scenario: str,
    seed: int,
    output_dir: Path | str = DEFAULT_CORRUPTED_DIR,
    overwrite: bool = False,
) -> CorruptionResult:
    """Create one corrupted workbook from a clean submission workbook."""

    source_path = Path(input_path)
    if scenario not in SCENARIO_NAMES:
        valid = ", ".join(SCENARIO_NAMES)
        raise CorruptionError(
            f"Unknown corruption scenario {scenario!r}. Valid: {valid}"
        )
    if not source_path.exists():
        raise FileNotFoundError(f"Clean submission not found: {source_path}")

    output_path = _scenario_output_path(source_path, output_dir, scenario, seed)
    if output_path.exists() and not overwrite:
        raise FileExistsError(f"Refusing to overwrite corrupted file: {output_path}")

    transactions = _read_transactions(source_path)
    rng = random.Random(seed)

    if scenario == "duplicate_prior_month_upload":
        output_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source_path, output_path)
        return CorruptionResult(
            scenario=scenario,
            seed=seed,
            source_path=source_path,
            output_path=output_path,
            row_count_before=len(transactions),
            row_count_after=len(transactions),
            details={
                "operation": "copied_source_workbook",
                "expected_issue": "duplicate_file_content",
            },
        )

    corrupted, details = SCENARIO_REGISTRY[scenario](transactions, rng)
    extra_sheets = _extra_sheets_for_scenario(scenario, corrupted, seed)
    _write_workbook(corrupted, output_path, extra_sheets=extra_sheets)

    return CorruptionResult(
        scenario=scenario,
        seed=seed,
        source_path=source_path,
        output_path=output_path,
        row_count_before=len(transactions),
        row_count_after=len(corrupted),
        details=details,
    )


def _apply_renamed_columns(
    transactions: pd.DataFrame,
    rng: random.Random,
) -> tuple[pd.DataFrame, dict[str, object]]:
    rename_options = [
        ("InvoiceDate", "Transaction Date"),
        ("Price", "Unit Price"),
        ("StockCode", "SKU"),
        ("Customer ID", "Customer Number"),
        ("Quantity", "Units"),
    ]
    available = [
        option for option in rename_options if option[0] in transactions.columns
    ]
    if not available:
        raise CorruptionError("No renameable columns found.")

    rename_count = min(2, len(available))
    selected = sorted(rng.sample(available, rename_count))
    mapping = dict(selected)

    return transactions.rename(columns=mapping), {
        "renamed_columns": mapping,
        "expected_issue": "schema_drift",
    }


def _apply_currency_strings(
    transactions: pd.DataFrame,
    rng: random.Random,
) -> tuple[pd.DataFrame, dict[str, object]]:
    _require_columns(transactions, ["Price"])
    corrupted = transactions.copy()
    corrupted["Price"] = corrupted["Price"].astype("object")
    selected = _sample_indices(corrupted.index.tolist(), rng, fraction=0.25)

    for index in selected:
        value = corrupted.at[index, "Price"]
        if pd.notna(value):
            corrupted.at[index, "Price"] = f"${float(value):.2f}"

    return corrupted, {
        "column": "Price",
        "selected_excel_rows": _excel_rows(selected),
        "expected_issue": "numeric_value_as_currency_string",
    }


def _apply_date_format_changes(
    transactions: pd.DataFrame,
    rng: random.Random,
) -> tuple[pd.DataFrame, dict[str, object]]:
    _require_columns(transactions, ["InvoiceDate"])
    corrupted = transactions.copy()
    corrupted["InvoiceDate"] = corrupted["InvoiceDate"].astype("object")
    eligible = [
        index
        for index, value in corrupted["InvoiceDate"].items()
        if pd.notna(pd.to_datetime(value, errors="coerce"))
    ]
    selected = _sample_indices(eligible, rng, fraction=0.25)

    for index in selected:
        value = pd.to_datetime(corrupted.at[index, "InvoiceDate"])
        corrupted.at[index, "InvoiceDate"] = value.strftime("%d/%m/%Y %H:%M")

    return corrupted, {
        "column": "InvoiceDate",
        "selected_excel_rows": _excel_rows(selected),
        "format": "dd/mm/YYYY HH:MM text",
        "expected_issue": "date_value_as_text",
    }


def _apply_duplicate_rows(
    transactions: pd.DataFrame,
    rng: random.Random,
) -> tuple[pd.DataFrame, dict[str, object]]:
    selected = _sample_indices(transactions.index.tolist(), rng, fraction=0.15)
    duplicates = transactions.loc[selected]
    corrupted = pd.concat([transactions, duplicates], ignore_index=True)

    return corrupted, {
        "duplicated_source_excel_rows": _excel_rows(selected),
        "duplicate_count": len(duplicates),
        "expected_issue": "exact_duplicate_rows",
    }


def _apply_missing_customer_ids(
    transactions: pd.DataFrame,
    rng: random.Random,
) -> tuple[pd.DataFrame, dict[str, object]]:
    _require_columns(transactions, ["Customer ID"])
    corrupted = transactions.copy()
    eligible = [
        index
        for index, value in corrupted["Customer ID"].items()
        if pd.notna(value)
    ]
    selected = _sample_indices(eligible, rng, fraction=0.5) if eligible else []
    if selected:
        corrupted.loc[selected, "Customer ID"] = pd.NA

    return corrupted, {
        "column": "Customer ID",
        "selected_excel_rows": _excel_rows(selected),
        "preexisting_all_missing": not eligible,
        "expected_issue": "missing_optional_customer_ids",
    }


def _apply_missing_required_column(
    transactions: pd.DataFrame,
    rng: random.Random,
) -> tuple[pd.DataFrame, dict[str, object]]:
    available = [
        column for column in REQUIRED_SUBMISSION_COLUMNS if column in transactions.columns
    ]
    if not available:
        raise CorruptionError("No required columns found to remove.")

    missing_column = rng.choice(available)
    corrupted = transactions.drop(columns=[missing_column])

    return corrupted, {
        "missing_column": missing_column,
        "expected_issue": "missing_required_column",
    }


def _apply_unexpected_columns(
    transactions: pd.DataFrame,
    rng: random.Random,
) -> tuple[pd.DataFrame, dict[str, object]]:
    corrupted = transactions.copy()
    batch_id = f"manual-upload-{rng.randint(1000, 9999)}"
    corrupted["Upload Batch ID"] = batch_id
    corrupted["Regional Manager Notes"] = ""

    return corrupted, {
        "unexpected_columns": ["Upload Batch ID", "Regional Manager Notes"],
        "batch_id": batch_id,
        "expected_issue": "unexpected_columns",
    }


def _apply_incomplete_file(
    transactions: pd.DataFrame,
    rng: random.Random,
) -> tuple[pd.DataFrame, dict[str, object]]:
    if transactions.empty:
        raise CorruptionError("Cannot truncate an empty file.")

    remaining_count = min(
        len(transactions) - 1,
        max(1, math.floor(len(transactions) * 0.1)),
    )
    selected = sorted(rng.sample(transactions.index.tolist(), remaining_count))
    corrupted = transactions.loc[selected].reset_index(drop=True)

    return corrupted, {
        "retained_source_excel_rows": _excel_rows(selected),
        "original_row_count": len(transactions),
        "retained_row_count": len(corrupted),
        "expected_issue": "unexpectedly_low_row_count",
    }


def _apply_multi_sheet_workbook(
    transactions: pd.DataFrame,
    rng: random.Random,
) -> tuple[pd.DataFrame, dict[str, object]]:
    _ = rng
    return transactions.copy(), {
        "extra_sheets": ["Lookup"],
        "expected_issue": "unexpected_extra_sheet",
    }


def _extra_sheets_for_scenario(
    scenario: str,
    transactions: pd.DataFrame,
    seed: int,
) -> dict[str, pd.DataFrame]:
    if scenario != "multi_sheet_workbook":
        return {}

    countries = sorted(
        transactions["Country"].dropna().astype(str).unique().tolist()
    )
    lookup = pd.DataFrame(
        {
            "Country": countries or ["Unknown"],
            "Currency": ["GBP"] * max(1, len(countries)),
            "Seed": [seed] * max(1, len(countries)),
        }
    )
    return {"Lookup": lookup}


def _read_transactions(path: Path) -> pd.DataFrame:
    workbook = pd.read_excel(path, sheet_name=None)
    if TRANSACTIONS_SHEET not in workbook:
        raise CorruptionError(
            f"Workbook missing {TRANSACTIONS_SHEET!r} sheet: {path}"
        )
    return workbook[TRANSACTIONS_SHEET]


def _write_workbook(
    transactions: pd.DataFrame,
    output_path: Path,
    *,
    extra_sheets: dict[str, pd.DataFrame] | None = None,
) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with pd.ExcelWriter(
        output_path,
        engine="openpyxl",
        datetime_format="yyyy-mm-dd hh:mm:ss",
    ) as writer:
        transactions.to_excel(writer, sheet_name=TRANSACTIONS_SHEET, index=False)
        for sheet_name, frame in (extra_sheets or {}).items():
            frame.to_excel(writer, sheet_name=sheet_name, index=False)

        writer.book.properties.creator = "AnalystOps"
        writer.book.properties.created = FIXED_WORKBOOK_TIMESTAMP
        writer.book.properties.modified = FIXED_WORKBOOK_TIMESTAMP

        for worksheet in writer.book.worksheets:
            worksheet.freeze_panes = "A2"
            for column_cells in worksheet.columns:
                header = str(column_cells[0].value)
                width = min(max(len(header) + 2, 12), 36)
                worksheet.column_dimensions[column_cells[0].column_letter].width = width


def _sample_indices(
    indices: list[int],
    rng: random.Random,
    *,
    fraction: float,
) -> list[int]:
    if not indices:
        raise CorruptionError("No eligible rows found for corruption.")

    sample_size = max(1, math.ceil(len(indices) * fraction))
    sample_size = min(sample_size, len(indices))
    return sorted(rng.sample(indices, sample_size))


def _excel_rows(indices: Iterable[int]) -> list[int]:
    return [int(index) + 2 for index in indices]


def _require_columns(transactions: pd.DataFrame, columns: Iterable[str]) -> None:
    missing = [column for column in columns if column not in transactions.columns]
    if missing:
        missing_text = ", ".join(missing)
        raise CorruptionError(f"Workbook missing required columns: {missing_text}")


def _scenario_output_path(
    source_path: Path,
    output_dir: Path | str,
    scenario: str,
    seed: int,
) -> Path:
    file_name = f"{source_path.stem}_{scenario}_seed{seed}.xlsx"
    return Path(output_dir) / scenario / file_name


def _derive_scenario_seed(
    base_seed: int,
    source_index: int,
    scenario_index: int,
) -> int:
    return base_seed + (source_index * 10_000) + scenario_index


SCENARIO_REGISTRY: dict[str, ScenarioFunction] = {
    "renamed_columns": _apply_renamed_columns,
    "currency_strings": _apply_currency_strings,
    "date_format_changes": _apply_date_format_changes,
    "duplicate_rows": _apply_duplicate_rows,
    "missing_customer_ids": _apply_missing_customer_ids,
    "missing_required_column": _apply_missing_required_column,
    "unexpected_columns": _apply_unexpected_columns,
    "incomplete_file": _apply_incomplete_file,
    "multi_sheet_workbook": _apply_multi_sheet_workbook,
}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Generate seeded corrupted submissions from clean workbooks."
    )
    parser.add_argument(
        "--input-dir",
        default=DEFAULT_GENERATED_DIR,
        type=Path,
        help="Directory containing clean generated submissions.",
    )
    parser.add_argument(
        "--output-dir",
        default=DEFAULT_CORRUPTED_DIR,
        type=Path,
        help="Directory for corrupted generated submissions.",
    )
    parser.add_argument("--seed", default=42, type=int, help="Base seed.")
    parser.add_argument(
        "--scenario",
        action="append",
        choices=SCENARIO_NAMES,
        help="Scenario to generate. Repeat for multiple. Defaults to all scenarios.",
    )
    parser.add_argument(
        "--max-files",
        default=None,
        type=int,
        help="Optional number of clean submission files to corrupt.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Allow replacing existing corrupted submissions.",
    )
    args = parser.parse_args(argv)

    input_paths = discover_clean_submission_files(args.input_dir)
    if args.max_files is not None:
        input_paths = input_paths[: args.max_files]

    results = corrupt_clean_submissions(
        input_paths,
        seed=args.seed,
        output_dir=args.output_dir,
        scenarios=args.scenario or SCENARIO_NAMES,
        overwrite=args.overwrite,
    )

    print(f"Generated {len(results)} corrupted submission workbook(s).")
    for result in results[:10]:
        print(
            f"{result.scenario} seed={result.seed}: "
            f"{result.row_count_before}->{result.row_count_after} rows"
        )
    if len(results) > 10:
        print(f"...and {len(results) - 10} more.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
