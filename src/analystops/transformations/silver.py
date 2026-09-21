"""Canonicalize Bronze-approved workbooks into deterministic Silver rows."""

from __future__ import annotations

import argparse
import json
from collections import Counter
from collections.abc import Mapping
from decimal import Decimal, InvalidOperation
from pathlib import Path

import pandas as pd

from analystops.ingestion.validate import _file_hash, _normalized_cell, read_result
from analystops.transformations.operations import (
    PlanOperation,
    TransformationPlan,
    TransformationPlanError,
    load_transformation_plan,
    plan_required,
    transformation_plan_hash,
)


PROJECT_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_SILVER_DIR = PROJECT_ROOT / "data" / "silver"
TRANSFORMATION_VERSION = "silver-v2"
SOURCE_COLUMNS = (
    "Invoice",
    "StockCode",
    "Description",
    "Quantity",
    "InvoiceDate",
    "Price",
    "Country",
)


class SilverCanonicalizationError(ValueError):
    """Raised when a workbook is not authorized or safe to canonicalize."""


def canonicalize(
    bronze_result_path: Path | str,
    *,
    output_dir: Path | str = DEFAULT_SILVER_DIR,
    plan: Path | str | Mapping[str, object] | None = None,
) -> Path:
    """Canonicalize one Bronze-approved workbook and return its result path."""

    bronze_path = Path(bronze_result_path)
    try:
        bronze = read_result(bronze_path)
    except ValueError as exc:
        raise SilverCanonicalizationError(str(exc)) from exc
    if bronze.get("lifecycle_state") != "BRONZE_ACCEPTED":
        raise SilverCanonicalizationError(
            f"Workbook is not BRONZE_ACCEPTED: {bronze.get('lifecycle_state')!r}"
        )

    transformation_plan = _authorized_plan(plan, bronze)

    expected_hash = bronze.get("file_hash")
    selected_sheet = bronze.get("selected_sheet")
    file_path = bronze.get("file_path")
    required_values = (expected_hash, selected_sheet, file_path)
    if not all(isinstance(value, str) and value for value in required_values):
        raise SilverCanonicalizationError(
            "Bronze result requires file_path, file_hash, and selected_sheet."
        )

    workbook_path = Path(file_path)
    if not workbook_path.is_absolute() and not workbook_path.exists():
        workbook_path = PROJECT_ROOT / workbook_path
    if not workbook_path.is_file():
        raise SilverCanonicalizationError(f"Workbook not found: {workbook_path}")
    if _file_hash(workbook_path) != expected_hash:
        raise SilverCanonicalizationError(f"Workbook hash mismatch: {workbook_path}")

    try:
        frame = pd.read_excel(workbook_path, sheet_name=selected_sheet)
    except Exception as exc:
        raise SilverCanonicalizationError(
            f"Cannot read approved sheet {selected_sheet!r}: {exc}"
        ) from exc

    observed_schema = bronze.get("observed_schema")
    if observed_schema != [str(column) for column in frame.columns]:
        raise SilverCanonicalizationError("Bronze schema does not match approved sheet.")
    if bronze.get("row_count") != len(frame):
        raise SilverCanonicalizationError("Bronze row count does not match approved sheet.")

    input_rows = len(frame)
    frame = _apply_plan(frame, transformation_plan)
    missing = [column for column in SOURCE_COLUMNS if column not in frame]
    if missing:
        raise SilverCanonicalizationError(
            f"Approved sheet is missing required columns: {', '.join(missing)}"
        )

    output_root = Path(output_dir)
    accepted_path = output_root / "accepted" / f"{expected_hash}.jsonl"
    rejected_path = output_root / "rejected" / f"{expected_hash}.jsonl"
    result_path = output_root / "results" / f"{expected_hash}.json"
    for path in (accepted_path, rejected_path, result_path):
        path.parent.mkdir(parents=True, exist_ok=True)

    accepted_tmp = accepted_path.with_suffix(".jsonl.tmp")
    rejected_tmp = rejected_path.with_suffix(".jsonl.tmp")
    accepted_rows = 0
    rejected_rows = 0
    # ponytail: one writer per file hash; use unique temp files if concurrency is added.
    try:
        with (
            accepted_tmp.open("w") as accepted_file,
            rejected_tmp.open("w") as rejected_file,
        ):
            for source_index, row in frame.iterrows():
                source_row_number = int(source_index) + 2
                record, reason_codes = _canonical_row(
                    row, expected_hash, source_row_number
                )
                if reason_codes:
                    rejected_file.write(
                        _json_line(
                            {
                                "source_file_id": expected_hash,
                                "source_row_number": source_row_number,
                                "reason_codes": reason_codes,
                                "values": {
                                    str(column): _normalized_cell(value)
                                    for column, value in row.items()
                                },
                            }
                        )
                    )
                    rejected_rows += 1
                else:
                    accepted_file.write(_json_line(record))
                    accepted_rows += 1
        accepted_tmp.replace(accepted_path)
        rejected_tmp.replace(rejected_path)
    except Exception:
        accepted_tmp.unlink(missing_ok=True)
        rejected_tmp.unlink(missing_ok=True)
        raise

    result = {
        "transformation_version": TRANSFORMATION_VERSION,
        "source_file_id": expected_hash,
        "source_file_path": str(workbook_path),
        "selected_sheet": selected_sheet,
        "input_rows": input_rows,
        "dropped_rows": input_rows - len(frame),
        "accepted_rows": accepted_rows,
        "rejected_rows": rejected_rows,
        "transformation_plan_hash": (
            transformation_plan_hash(transformation_plan)
            if transformation_plan is not None
            else None
        ),
        "applied_operations": [
            {
                "finding_code": operation.finding_code,
                "operation": operation.operation,
                "parameters": operation.parameters,
            }
            for operation in transformation_plan.operations
        ]
        if transformation_plan is not None
        else [],
        "accepted_path": str(accepted_path.resolve()),
        "rejected_path": str(rejected_path.resolve()),
    }
    result_tmp = result_path.with_suffix(".json.tmp")
    result_tmp.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    result_tmp.replace(result_path)
    return result_path


def canonicalize_corpus(
    bronze_results_dir: Path | str,
    *,
    output_dir: Path | str = DEFAULT_SILVER_DIR,
) -> dict[str, object]:
    """Canonicalize every authorized record in a materialized Bronze corpus."""

    bronze_root = Path(bronze_results_dir)
    records = sorted(bronze_root.rglob("*.json"))
    if not records:
        raise SilverCanonicalizationError(
            f"No Bronze results found under {bronze_root}"
        )

    states: Counter[str] = Counter()
    totals = Counter()
    for record in records:
        try:
            bronze = read_result(record)
        except ValueError as exc:
            raise SilverCanonicalizationError(f"{record}: {exc}") from exc
        state = str(bronze.get("lifecycle_state"))
        states[state] += 1
        if state != "BRONZE_ACCEPTED":
            continue
        result = json.loads(canonicalize(record, output_dir=output_dir).read_text())
        totals["canonicalized_workbooks"] += 1
        totals["input_rows"] += int(result["input_rows"])
        totals["accepted_rows"] += int(result["accepted_rows"])
        totals["rejected_rows"] += int(result["rejected_rows"])

    return {
        "records": len(records),
        "states": dict(sorted(states.items())),
        "canonicalized_workbooks": totals["canonicalized_workbooks"],
        "input_rows": totals["input_rows"],
        "accepted_rows": totals["accepted_rows"],
        "rejected_rows": totals["rejected_rows"],
        "output_dir": str(Path(output_dir).resolve()),
    }


def _canonical_row(
    row: pd.Series,
    source_file_id: str,
    source_row_number: int,
) -> tuple[dict[str, object], list[str]]:
    invoice_id = _text(row["Invoice"])
    product_id = _text(row["StockCode"])
    quantity = _integer(row["Quantity"])
    timestamp = _timestamp(row["InvoiceDate"])
    unit_price = _decimal(row["Price"])
    country = _text(row["Country"])
    reasons = []
    if invoice_id is None:
        reasons.append("missing_invoice_id")
    if product_id is None:
        reasons.append("missing_product_id")
    if quantity is None:
        reasons.append("invalid_quantity")
    if timestamp is None:
        reasons.append("invalid_transaction_timestamp")
    if unit_price is None:
        reasons.append("invalid_unit_price")
    if country is None:
        reasons.append("missing_country")

    return (
        {
            "invoice_id": invoice_id,
            "product_id": product_id,
            "product_description": _text(row["Description"]),
            "quantity": quantity,
            "transaction_timestamp": timestamp,
            "unit_price": unit_price,
            "customer_id": _text(row.get("Customer ID")),
            "country": country,
            "source_file_id": source_file_id,
            "source_row_number": source_row_number,
        },
        reasons,
    )


def _authorized_plan(
    value: Path | str | Mapping[str, object] | None,
    bronze: Mapping[str, object],
) -> TransformationPlan | None:
    if value is None:
        try:
            requires_plan = plan_required(bronze)
        except TransformationPlanError as exc:
            raise SilverCanonicalizationError(str(exc)) from exc
        if requires_plan:
            raise SilverCanonicalizationError(
                "Bronze authorization requires a transformation plan."
            )
        return None
    try:
        return load_transformation_plan(value, bronze)
    except TransformationPlanError as exc:
        raise SilverCanonicalizationError(str(exc)) from exc


def _apply_plan(
    frame: pd.DataFrame,
    plan: TransformationPlan | None,
) -> pd.DataFrame:
    if plan is None:
        return frame
    transformed = frame.copy()
    order = {
        "map_columns": 0,
        "materialize_values_in_silver": 1,
        "deduplicate_in_silver": 2,
        "confirm_numeric_format": 3,
        "confirm_date_format": 4,
    }
    for operation in sorted(
        plan.operations, key=lambda item: order[item.operation]
    ):
        transformed = _apply_operation(transformed, operation)
    return transformed


def _apply_operation(frame: pd.DataFrame, operation: PlanOperation) -> pd.DataFrame:
    if operation.operation == "map_columns":
        mapping = operation.parameters["mapping"]
        assert isinstance(mapping, dict)
        sources = set(mapping)
        targets = list(mapping.values())
        if not sources.issubset(frame.columns):
            raise SilverCanonicalizationError(
                "map_columns references columns absent from the approved sheet."
            )
        if len(set(targets)) != len(targets) or any(
            target in frame.columns and target not in sources for target in targets
        ):
            raise SilverCanonicalizationError("map_columns would create duplicate columns.")
        return frame.rename(columns=mapping)
    if operation.operation == "deduplicate_in_silver":
        return frame.drop_duplicates(keep="first")
    if operation.operation == "confirm_numeric_format":
        column = {
            "quantity_parse_failures": "Quantity",
            "price_parse_failures": "Price",
        }[operation.finding_code]
        if column not in frame:
            raise SilverCanonicalizationError(f"Cannot parse missing column {column!r}.")
        normalized = frame[column].astype("string").str.strip()
        normalized = normalized.str.replace(",", "", regex=False)
        normalized = normalized.str.replace(r"^\((.+)\)$", r"-\1", regex=True)
        if operation.parameters["format"] == "currency":
            normalized = normalized.str.replace(
                r"[\u0024\u00a3\u20ac]", "", regex=True
            )
        updated = frame.copy()
        updated[column] = pd.to_numeric(normalized, errors="coerce")
        return updated
    if operation.operation == "confirm_date_format":
        if "InvoiceDate" not in frame:
            raise SilverCanonicalizationError(
                "Cannot parse missing column 'InvoiceDate'."
            )
        updated = frame.copy()
        updated["InvoiceDate"] = pd.to_datetime(
            updated["InvoiceDate"],
            format=str(operation.parameters["format"]),
            errors="coerce",
        )
        return updated
    if operation.operation == "materialize_values_in_silver":
        # pandas reads cached formula values; formulas are never evaluated here.
        return frame
    raise SilverCanonicalizationError(
        f"Silver does not implement operation {operation.operation!r}."
    )


def _text(value: object) -> str | None:
    normalized = _normalized_cell(value)
    if normalized is None:
        return None
    text = str(normalized).strip()
    return text or None


def _integer(value: object) -> int | None:
    if pd.isna(value):
        return None
    try:
        number = Decimal(str(value))
    except InvalidOperation:
        return None
    if not number.is_finite() or number != number.to_integral_value():
        return None
    return int(number)


def _decimal(value: object) -> str | None:
    if pd.isna(value):
        return None
    try:
        number = Decimal(str(value))
    except InvalidOperation:
        return None
    return format(number, "f") if number.is_finite() else None


def _timestamp(value: object) -> str | None:
    if pd.isna(value):
        return None
    try:
        timestamp = pd.Timestamp(value)
    except (TypeError, ValueError):
        return None
    return timestamp.isoformat() if not pd.isna(timestamp) else None


def _json_line(record: dict[str, object]) -> str:
    return json.dumps(record, separators=(",", ":")) + "\n"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Canonicalize Bronze-approved workbooks into Silver rows."
    )
    parser.add_argument("bronze_results", type=Path)
    parser.add_argument("--output-dir", default=DEFAULT_SILVER_DIR, type=Path)
    parser.add_argument("--plan", type=Path)
    args = parser.parse_args(argv)

    if args.bronze_results.is_dir():
        if args.plan:
            parser.error("--plan can only be used with one Bronze result")
        summary = canonicalize_corpus(
            args.bronze_results, output_dir=args.output_dir
        )
        print(json.dumps(summary, indent=2, sort_keys=True))
    else:
        print(
            canonicalize(
                args.bronze_results,
                output_dir=args.output_dir,
                plan=args.plan,
            )
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
