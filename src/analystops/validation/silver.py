"""Validate Silver artifacts and publish the trusted clean cohort."""

from __future__ import annotations

import argparse
import json
import re
from collections import Counter
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path

from analystops.ingestion.validate import read_result
from analystops.transformations.silver import TRANSFORMATION_VERSION


PROJECT_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_BRONZE_DIR = PROJECT_ROOT / "data" / "ingestion" / "corpus-results"
DEFAULT_SILVER_DIR = PROJECT_ROOT / "data" / "silver"
DEFAULT_VALIDATION_DIR = PROJECT_ROOT / "data" / "validation" / "silver"
VALIDATION_VERSION = "silver-validation-v2"
CANONICAL_FIELDS = (
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
)
QUALITY_RANK = {"PASS": 0, "WARN": 1, "REVIEW": 2, "BLOCK": 3}


class SilverValidationError(ValueError):
    """Raised when a Silver artifact violates its deterministic contract."""


def validate_silver_result(
    bronze_result_path: Path | str,
    *,
    silver_dir: Path | str = DEFAULT_SILVER_DIR,
    output_dir: Path | str = DEFAULT_VALIDATION_DIR,
) -> Path:
    """Validate one Silver result and write its profile."""

    profile = _profile_candidate(Path(bronze_result_path), Path(silver_dir))
    profile_path = Path(output_dir) / "profiles" / f"{profile['source_file_id']}.json"
    _write_json(profile_path, profile)
    return profile_path


def validate_silver_corpus(
    bronze_results_dir: Path | str = DEFAULT_BRONZE_DIR,
    *,
    silver_dir: Path | str = DEFAULT_SILVER_DIR,
    output_dir: Path | str = DEFAULT_VALIDATION_DIR,
) -> dict[str, object]:
    """Validate clean Silver candidates and write their publication evidence."""

    bronze_root = Path(bronze_results_dir)
    silver_root = Path(silver_dir)
    output_root = Path(output_dir)
    records = sorted(bronze_root.rglob("*.json"))
    if not records:
        raise SilverValidationError(f"No Bronze records found under {bronze_root}")
    candidates = [
        path
        for path in records
        if path.relative_to(bronze_root).parts[0] == "clean"
    ]
    if not candidates:
        raise SilverValidationError(f"No clean Bronze candidates under {bronze_root}")

    profiles = []
    manifest_entries = []
    dispositions: Counter[str] = Counter()
    for record_path in candidates:
        profile = _profile_candidate(record_path, silver_root)
        profile_path = output_root / "profiles" / f"{profile['source_file_id']}.json"
        _write_json(profile_path, profile)
        profiles.append(profile)
        dispositions[str(profile["quality_disposition"])] += 1
        if str(profile["quality_disposition"]) in {"PASS", "WARN"}:
            manifest_entries.append(
                {
                    "source_file_id": profile["source_file_id"],
                    "canonical_path": profile["canonical_path"],
                    "profile_path": str(profile_path.resolve()),
                    "reporting_month": profile["reporting_month"],
                    "country": profile["country"],
                    "row_count": profile["row_count"],
                }
            )

    candidate_rows = sum(int(profile["row_count"]) for profile in profiles)
    published_rows = sum(int(entry["row_count"]) for entry in manifest_entries)
    manifest = {
        "validation_version": VALIDATION_VERSION,
        "candidate_workbooks": len(candidates),
        "published_workbooks": len(manifest_entries),
        "published_rows": published_rows,
        "entries": manifest_entries,
    }
    summary = {
        "validation_version": VALIDATION_VERSION,
        "bronze_records": len(records),
        "candidate_workbooks": len(candidates),
        "candidate_rows": candidate_rows,
        "published_workbooks": len(manifest_entries),
        "published_rows": published_rows,
        "held_workbooks": len(candidates) - len(manifest_entries),
        "dispositions": dict(sorted(dispositions.items())),
    }
    _write_json(output_root / "publish-manifest.json", manifest)
    _write_json(output_root / "summary.json", summary)
    return summary


def _profile_candidate(record_path: Path, silver_root: Path) -> dict[str, object]:
    try:
        bronze = read_result(record_path)
    except ValueError as exc:
        raise SilverValidationError(f"{record_path}: {exc}") from exc
    if bronze.get("lifecycle_state") != "BRONZE_ACCEPTED":
        raise SilverValidationError(f"Clean candidate is not approved: {record_path}")

    source_file_id = bronze.get("file_hash")
    source_file_path = bronze.get("file_path")
    if not isinstance(source_file_id, str) or not isinstance(source_file_path, str):
        raise SilverValidationError(f"Invalid Bronze identity: {record_path}")
    result_path = silver_root / "results" / f"{source_file_id}.json"
    try:
        result = json.loads(result_path.read_text())
    except (OSError, json.JSONDecodeError) as exc:
        raise SilverValidationError(f"Cannot read Silver result {result_path}: {exc}") from exc
    if not isinstance(result, dict):
        raise SilverValidationError(f"Silver result must be an object: {result_path}")

    accepted_path = silver_root / "accepted" / f"{source_file_id}.jsonl"
    rejected_path = silver_root / "rejected" / f"{source_file_id}.jsonl"
    expected_result = {
        "transformation_version": TRANSFORMATION_VERSION,
        "source_file_id": source_file_id,
        "input_rows": bronze.get("row_count"),
        "accepted_path": str(accepted_path.resolve()),
        "rejected_path": str(rejected_path.resolve()),
    }
    for key, value in expected_result.items():
        if result.get(key) != value:
            raise SilverValidationError(
                f"Silver result {key} mismatch for {source_file_id}"
            )
    row_counts = {
        key: result.get(key)
        for key in ("input_rows", "accepted_rows", "rejected_rows", "dropped_rows")
    }
    if not all(
        isinstance(value, int) and not isinstance(value, bool) and value >= 0
        for value in row_counts.values()
    ):
        raise SilverValidationError(f"Invalid Silver row counts for {source_file_id}")
    if row_counts["input_rows"] != (
        row_counts["accepted_rows"]
        + row_counts["rejected_rows"]
        + row_counts["dropped_rows"]
    ):
        raise SilverValidationError(
            f"Silver rows do not reconcile for {source_file_id}"
        )
    if row_counts["rejected_rows"]:
        raise SilverValidationError(f"Silver contains rejected rows: {source_file_id}")
    if not rejected_path.is_file() or rejected_path.stat().st_size:
        raise SilverValidationError(f"Silver rejects are not empty: {rejected_path}")

    metrics = {
        "net_revenue": Decimal("0"),
        "gross_sales": Decimal("0"),
        "return_lines": 0,
        "return_units": 0,
        "cancellation_lines": 0,
        "missing_customer_ids": 0,
        "missing_descriptions": 0,
        "zero_value_lines": 0,
        "bad_debt_adjustment_lines": 0,
        "bad_debt_adjustment_amount": Decimal("0"),
        "unrecognized_negative_price_lines": 0,
        "duplicate_rows": 0,
    }
    invoices: set[str] = set()
    products: set[str] = set()
    customers: set[str] = set()
    countries: set[str] = set()
    order_months: set[str] = set()
    business_rows: set[tuple[object, ...]] = set()
    row_count = 0
    try:
        lines = accepted_path.open()
    except OSError as exc:
        raise SilverValidationError(f"Cannot read Silver rows {accepted_path}: {exc}") from exc
    with lines:
        for line_number, line in enumerate(lines, start=1):
            try:
                row = json.loads(line)
            except json.JSONDecodeError as exc:
                raise SilverValidationError(
                    f"Invalid JSON at {accepted_path}:{line_number}"
                ) from exc
            quantity, price, timestamp = _validate_row(
                row,
                source_file_id,
                line_number + 1 if not row_counts["dropped_rows"] else None,
                accepted_path,
                line_number,
            )
            row_count += 1
            invoice_id = str(row["invoice_id"])
            product_id = str(row["product_id"])
            customer_id = row["customer_id"]
            invoices.add(invoice_id)
            products.add(product_id)
            countries.add(str(row["country"]))
            order_months.add(timestamp.strftime("%Y-%m"))
            if customer_id is None:
                metrics["missing_customer_ids"] += 1
            else:
                customers.add(str(customer_id))
            if row["product_description"] is None:
                metrics["missing_descriptions"] += 1
            revenue = Decimal(quantity) * price
            price_policy = _price_policy(row, quantity, price)
            if price_policy == "ZERO_VALUE":
                metrics["zero_value_lines"] += 1
                metrics["net_revenue"] += revenue
            elif price_policy == "BAD_DEBT_ADJUSTMENT":
                metrics["bad_debt_adjustment_lines"] += 1
                metrics["bad_debt_adjustment_amount"] += revenue
            elif price_policy == "UNRECOGNIZED_NEGATIVE_PRICE":
                metrics["unrecognized_negative_price_lines"] += 1
            else:
                metrics["net_revenue"] += revenue
                if quantity > 0:
                    metrics["gross_sales"] += revenue
            if quantity < 0:
                metrics["return_lines"] += 1
                metrics["return_units"] += abs(quantity)
            if invoice_id.upper().startswith("C"):
                metrics["cancellation_lines"] += 1
            business_row = tuple(row[field] for field in CANONICAL_FIELDS[:-2])
            if business_row in business_rows:
                metrics["duplicate_rows"] += 1
            else:
                business_rows.add(business_row)

    if row_count != result["accepted_rows"]:
        raise SilverValidationError(f"Silver row count mismatch for {source_file_id}")

    workbook_path = Path(source_file_path)
    reporting_month = _reporting_month(workbook_path)
    country = next(iter(countries)) if len(countries) == 1 else None
    findings: list[dict[str, object]] = []
    _add_count_finding(
        findings, "missing_customer_ids", "WARN", metrics["missing_customer_ids"]
    )
    _add_count_finding(
        findings, "missing_descriptions", "WARN", metrics["missing_descriptions"]
    )
    _add_count_finding(
        findings,
        "zero_value_lines",
        "WARN",
        metrics["zero_value_lines"],
    )
    _add_count_finding(
        findings,
        "bad_debt_adjustments",
        "WARN",
        metrics["bad_debt_adjustment_lines"],
    )
    _add_count_finding(
        findings,
        "unrecognized_negative_unit_price",
        "REVIEW",
        metrics["unrecognized_negative_price_lines"],
    )
    duplicate_rows = int(metrics["duplicate_rows"])
    if duplicate_rows:
        duplicate_rate = duplicate_rows / row_count
        duplicate_action = _review_action(bronze, "exact_duplicate_rows")
        findings.append(
            {
                "code": "exact_duplicate_rows",
                "quality_disposition": (
                    "WARN"
                    if duplicate_action == "confirm_valid_duplicates"
                    else "REVIEW" if duplicate_rate > 0.10 else "WARN"
                ),
                "count": duplicate_rows,
                "rate": round(duplicate_rate, 6),
                "review_resolution": duplicate_action,
            }
        )
    if len(countries) != 1:
        findings.append(
            {
                "code": "inconsistent_countries",
                "quality_disposition": "REVIEW",
                "countries": sorted(countries),
            }
        )
    elif not _matches_workbook_identity(workbook_path, country, reporting_month):
        findings.append(
            {
                "code": "country_file_mismatch",
                "quality_disposition": "REVIEW",
                "country": country,
            }
        )
    if not re.fullmatch(r"\d{4}-\d{2}", reporting_month) or order_months != {
        reporting_month
    }:
        findings.append(
            {
                "code": "reporting_month_mismatch",
                "quality_disposition": "REVIEW",
                "order_months": sorted(order_months),
            }
        )

    disposition = max(
        (str(item["quality_disposition"]) for item in findings),
        key=QUALITY_RANK.get,
        default="PASS",
    )
    publication_state = {
        "PASS": "PUBLISHABLE",
        "WARN": "PUBLISHABLE_WITH_WARNINGS",
        "REVIEW": "REVIEW_REQUIRED",
        "BLOCK": "BLOCKED",
    }[disposition]
    return {
        "validation_version": VALIDATION_VERSION,
        "source_file_id": source_file_id,
        "bronze_record_path": str(record_path.resolve()),
        "silver_result_path": str(result_path.resolve()),
        "canonical_path": str(accepted_path.resolve()),
        "reporting_month": reporting_month,
        "country": country,
        "row_count": row_count,
        "quality_disposition": disposition,
        "publication_state": publication_state,
        "reason_codes": [str(item["code"]) for item in findings],
        "findings": findings,
        "metrics": {
            "net_revenue": format(metrics["net_revenue"], "f"),
            "gross_sales": format(metrics["gross_sales"], "f"),
            "invoice_count": len(invoices),
            "product_count": len(products),
            "known_customer_count": len(customers),
            "return_lines": metrics["return_lines"],
            "return_units": metrics["return_units"],
            "cancellation_lines": metrics["cancellation_lines"],
            "missing_customer_ids": metrics["missing_customer_ids"],
            "missing_descriptions": metrics["missing_descriptions"],
            "zero_value_lines": metrics["zero_value_lines"],
            "bad_debt_adjustment_lines": metrics["bad_debt_adjustment_lines"],
            "bad_debt_adjustment_amount": format(
                metrics["bad_debt_adjustment_amount"], "f"
            ),
            "unrecognized_negative_price_lines": metrics[
                "unrecognized_negative_price_lines"
            ],
            "duplicate_rows": duplicate_rows,
        },
    }


def _price_policy(row: dict[str, object], quantity: int, price: Decimal) -> str | None:
    if price == 0:
        return "ZERO_VALUE"
    if price >= 0:
        return None
    description = str(row["product_description"] or "").strip().casefold()
    if (
        quantity == 1
        and str(row["invoice_id"]).upper().startswith("A")
        and str(row["product_id"]).upper() == "B"
        and description == "adjust bad debt"
        and row["country"] == "United Kingdom"
    ):
        return "BAD_DEBT_ADJUSTMENT"
    return "UNRECOGNIZED_NEGATIVE_PRICE"


def _validate_row(
    row: object,
    source_file_id: str,
    expected_source_row: int | None,
    path: Path,
    line_number: int,
) -> tuple[int, Decimal, datetime]:
    if not isinstance(row, dict) or set(row) != set(CANONICAL_FIELDS):
        raise SilverValidationError(
            f"Silver canonical schema mismatch at {path}:{line_number}"
        )
    for field in ("invoice_id", "product_id", "country"):
        if not isinstance(row[field], str) or not row[field]:
            raise SilverValidationError(f"Invalid {field} at {path}:{line_number}")
    for field in ("product_description", "customer_id"):
        if row[field] is not None and not isinstance(row[field], str):
            raise SilverValidationError(f"Invalid {field} at {path}:{line_number}")
    quantity = row["quantity"]
    if not isinstance(quantity, int) or isinstance(quantity, bool):
        raise SilverValidationError(f"Invalid quantity at {path}:{line_number}")
    unit_price = row["unit_price"]
    if not isinstance(unit_price, str):
        raise SilverValidationError(f"Invalid unit_price at {path}:{line_number}")
    try:
        price = Decimal(unit_price)
    except (InvalidOperation, TypeError):
        raise SilverValidationError(f"Invalid unit_price at {path}:{line_number}")
    if not price.is_finite():
        raise SilverValidationError(f"Invalid unit_price at {path}:{line_number}")
    try:
        timestamp = datetime.fromisoformat(row["transaction_timestamp"])
    except (TypeError, ValueError):
        raise SilverValidationError(
            f"Invalid transaction_timestamp at {path}:{line_number}"
        )
    if row["source_file_id"] != source_file_id:
        raise SilverValidationError(f"Lineage file mismatch at {path}:{line_number}")
    source_row_number = row["source_row_number"]
    if not isinstance(source_row_number, int) or source_row_number < 2:
        raise SilverValidationError(f"Invalid lineage row at {path}:{line_number}")
    if expected_source_row is not None and source_row_number != expected_source_row:
        raise SilverValidationError(f"Lineage row mismatch at {path}:{line_number}")
    return quantity, price, timestamp


def _review_action(bronze: dict[str, object], finding_code: str) -> str | None:
    findings = bronze.get("findings", [])
    if not isinstance(findings, list):
        return None
    for finding in findings:
        if not isinstance(finding, dict) or finding.get("code") != finding_code:
            continue
        resolution = finding.get("review_resolution")
        if isinstance(resolution, dict) and isinstance(resolution.get("action"), str):
            return str(resolution["action"])
    return None


def _add_count_finding(
    findings: list[dict[str, object]],
    code: str,
    disposition: str,
    count: object,
) -> None:
    if int(count):
        findings.append(
            {"code": code, "quality_disposition": disposition, "count": int(count)}
        )


def _slugify(value: object) -> str:
    text = re.sub(r"[^a-z0-9]+", "_", str(value).strip().lower())
    return text.strip("_") or "unknown"


def _reporting_month(workbook_path: Path) -> str:
    if re.fullmatch(r"\d{4}-\d{2}", workbook_path.parent.name):
        return workbook_path.parent.name
    match = re.search(r"(?:^|_)(\d{4}-\d{2})(?:_|$)", workbook_path.stem)
    return match.group(1) if match else workbook_path.parent.name


def _matches_workbook_identity(
    workbook_path: Path, country: object, reporting_month: str
) -> bool:
    expected = f"{_slugify(country)}_{reporting_month}"
    return workbook_path.stem == expected or workbook_path.stem.startswith(
        f"{expected}_"
    )


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    temporary.replace(path)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Validate Silver artifacts and publish the trusted clean cohort."
    )
    parser.add_argument("--bronze-dir", type=Path, default=DEFAULT_BRONZE_DIR)
    parser.add_argument("--silver-dir", type=Path, default=DEFAULT_SILVER_DIR)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_VALIDATION_DIR)
    args = parser.parse_args(argv)
    summary = validate_silver_corpus(
        args.bronze_dir,
        silver_dir=args.silver_dir,
        output_dir=args.output_dir,
    )
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
