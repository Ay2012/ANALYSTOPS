"""Human resolution and reassessment for Bronze review findings."""

from __future__ import annotations

import argparse
import json
from collections.abc import Iterable, Mapping
from datetime import datetime
from pathlib import Path
from typing import Any

from .validate import IntakeResult, _result, validate_workbook, write_result


PROJECT_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_REVIEW_DIR = PROJECT_ROOT / "data" / "ingestion" / "reviews"

REVIEW_ACTIONS = {
    "ambiguous_transaction_sheets": ("select_sheet",),
    "renamed_required_columns": ("map_columns",),
    "quantity_parse_failures": ("confirm_numeric_format",),
    "price_parse_failures": ("confirm_numeric_format",),
    "date_parse_failures": ("confirm_date_format",),
    "non_iso_date_strings": ("confirm_date_format",),
    "exact_duplicate_rows": ("confirm_valid_duplicates", "deduplicate_in_silver"),
    "empty_transaction_sheet": ("confirm_zero_activity",),
    "unexpectedly_low_row_count": ("confirm_expected_volume",),
    "formula_cells": ("materialize_values_in_silver",),
}

REVIEW_QUESTIONS = {
    "ambiguous_transaction_sheets": "Which plausible sheet contains the transactions?",
    "renamed_required_columns": "How do the renamed columns map to canonical fields?",
    "quantity_parse_failures": "What numeric format does Quantity use?",
    "price_parse_failures": "What numeric or currency format does Price use?",
    "date_parse_failures": "What date format does InvoiceDate use?",
    "non_iso_date_strings": "What date format does InvoiceDate use?",
    "exact_duplicate_rows": "Are these valid repeated lines or should Silver deduplicate them?",
    "empty_transaction_sheet": "Does this period genuinely contain zero activity?",
    "unexpectedly_low_row_count": "Is this row volume correct for the reporting period?",
    "formula_cells": "Should Silver materialize the stored formula values?",
}


class ReviewResolutionError(ValueError):
    """Raised when a review resolution is invalid or unsafe to apply."""


def write_review_request(
    result: IntakeResult,
    output_dir: Path | str = DEFAULT_REVIEW_DIR,
) -> Path:
    """Write a fillable JSON request for an awaiting-review result."""

    if result.lifecycle_state != "AWAITING_REVIEW":
        raise ReviewResolutionError("Only AWAITING_REVIEW results need review requests.")

    review_items = []
    for finding in result.findings:
        if finding["quality_disposition"] != "REVIEW":
            continue
        code = str(finding["code"])
        review_items.append(
            {
                "finding": finding,
                "question": REVIEW_QUESTIONS.get(code, "Resolve this finding."),
                "allowed_actions": list(REVIEW_ACTIONS.get(code, ())),
            }
        )

    request = {
        "file_path": result.file_path,
        "file_hash": result.file_hash,
        "content_fingerprint": result.content_fingerprint,
        "status": result.lifecycle_state,
        "reviewed_by": "",
        "reviewed_at": "",
        "resolutions": [],
        "review_items": review_items,
    }
    output_path = Path(output_dir) / (
        f"{Path(result.file_path).stem}_{str(result.file_hash)[:12]}.review.json"
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(request, indent=2, sort_keys=True) + "\n")
    return output_path


def reassess_workbook(
    path: Path | str,
    resolution: Path | str | Mapping[str, Any],
    *,
    seen_hashes: Iterable[str] = (),
    seen_content_fingerprints: Iterable[str] = (),
    baseline_row_count: int | None = None,
) -> IntakeResult:
    """Revalidate a workbook and apply its verified human resolutions."""

    seen_hashes = tuple(seen_hashes)
    seen_content_fingerprints = tuple(seen_content_fingerprints)
    validation_kwargs = {
        "seen_hashes": seen_hashes,
        "seen_content_fingerprints": seen_content_fingerprints,
        "baseline_row_count": baseline_row_count,
    }
    initial = validate_workbook(path, **validation_kwargs)
    document = _load_resolution(resolution)
    resolutions = _validate_resolution_document(initial, document)

    selected = resolutions.get("ambiguous_transaction_sheets")
    if selected:
        selected_sheet = str(selected["details"]["sheet_name"])
        base = validate_workbook(
            path,
            selected_sheet=selected_sheet,
            **validation_kwargs,
        )
        ambiguous = next(
            finding
            for finding in initial.findings
            if finding["code"] == "ambiguous_transaction_sheets"
        )
        base = _result(
            file_path=base.file_path,
            file_hash=base.file_hash,
            content_fingerprint=base.content_fingerprint,
            selected_sheet=base.selected_sheet,
            sheet_candidates=base.sheet_candidates,
            observed_schema=base.observed_schema,
            row_count=base.row_count,
            findings=[*base.findings, ambiguous],
        )
    else:
        base = initial

    findings = []
    for finding in base.findings:
        item = resolutions.get(str(finding["code"]))
        if finding["quality_disposition"] != "REVIEW" or item is None:
            findings.append(finding)
            continue
        resolved = dict(finding)
        resolved["quality_disposition"] = "WARN"
        resolved["review_resolution"] = {
            "action": item["action"],
            "details": item["details"],
            "note": item.get("note", ""),
            "reviewed_by": document["reviewed_by"],
            "reviewed_at": document["reviewed_at"],
        }
        findings.append(resolved)

    return _result(
        file_path=base.file_path,
        file_hash=base.file_hash,
        content_fingerprint=base.content_fingerprint,
        selected_sheet=base.selected_sheet,
        sheet_candidates=base.sheet_candidates,
        observed_schema=base.observed_schema,
        row_count=base.row_count,
        findings=findings,
    )


def _load_resolution(value: Path | str | Mapping[str, Any]) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    try:
        document = json.loads(Path(value).read_text())
    except (OSError, json.JSONDecodeError) as exc:
        raise ReviewResolutionError(f"Cannot read review resolution: {exc}") from exc
    if not isinstance(document, dict):
        raise ReviewResolutionError("Review resolution must be a JSON object.")
    return document


def _validate_resolution_document(
    result: IntakeResult,
    document: dict[str, Any],
) -> dict[str, dict[str, Any]]:
    if document.get("file_hash") != result.file_hash:
        raise ReviewResolutionError("Resolution file hash does not match the workbook.")
    if result.lifecycle_state == "QUARANTINED":
        raise ReviewResolutionError("Quarantined workbooks cannot be approved by review.")
    if result.lifecycle_state != "AWAITING_REVIEW":
        raise ReviewResolutionError("Workbook is not awaiting review.")

    reviewed_by = document.get("reviewed_by")
    reviewed_at = document.get("reviewed_at")
    if not isinstance(reviewed_by, str) or not reviewed_by.strip():
        raise ReviewResolutionError("reviewed_by must be a non-empty string.")
    if not isinstance(reviewed_at, str) or not reviewed_at.strip():
        raise ReviewResolutionError("reviewed_at must be an ISO-8601 timestamp.")
    try:
        timestamp = datetime.fromisoformat(reviewed_at.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ReviewResolutionError("reviewed_at must be an ISO-8601 timestamp.") from exc
    if timestamp.tzinfo is None:
        raise ReviewResolutionError("reviewed_at must include a timezone.")

    items = document.get("resolutions")
    if not isinstance(items, list):
        raise ReviewResolutionError("resolutions must be a list.")
    review_findings = {
        str(finding["code"]): finding
        for finding in result.findings
        if finding["quality_disposition"] == "REVIEW"
    }
    validated: dict[str, dict[str, Any]] = {}
    for item in items:
        if not isinstance(item, dict):
            raise ReviewResolutionError("Each resolution must be an object.")
        code = item.get("finding_code")
        action = item.get("action")
        if not isinstance(code, str) or code not in review_findings:
            raise ReviewResolutionError(f"{code!r} is not an active review finding.")
        if code in validated:
            raise ReviewResolutionError(f"Duplicate resolution for {code!r}.")
        if action not in REVIEW_ACTIONS.get(code, ()):
            raise ReviewResolutionError(f"Action {action!r} is not allowed for {code!r}.")
        details = item.get("details", {})
        if not isinstance(details, dict):
            raise ReviewResolutionError(f"details for {code!r} must be an object.")
        _validate_action_details(str(action), details, review_findings[code], result)
        note = item.get("note", "")
        if not isinstance(note, str):
            raise ReviewResolutionError(f"note for {code!r} must be a string.")
        validated[code] = {
            "action": action,
            "details": details,
            "note": note,
        }
    return validated


def _validate_action_details(
    action: str,
    details: dict[str, Any],
    finding: dict[str, object],
    result: IntakeResult,
) -> None:
    if action == "map_columns":
        mapping = details.get("mapping")
        if not isinstance(mapping, dict) or not mapping:
            raise ReviewResolutionError("map_columns requires a non-empty mapping.")
        if not set(mapping).issubset(result.observed_schema):
            raise ReviewResolutionError("Mapped source columns are not in the workbook.")
        if not set(finding["columns"]).issubset(set(mapping.values())):
            raise ReviewResolutionError("Mapping does not resolve every renamed column.")
    elif action == "select_sheet":
        sheet_name = details.get("sheet_name")
        plausible = {
            str(candidate["sheet"])
            for candidate in result.sheet_candidates
            if candidate["plausible"]
        }
        if not isinstance(sheet_name, str) or sheet_name not in plausible:
            raise ReviewResolutionError("select_sheet requires a plausible sheet name.")
    elif action in {"confirm_numeric_format", "confirm_date_format"}:
        value = details.get("format")
        if not isinstance(value, str) or not value.strip():
            raise ReviewResolutionError(f"{action} requires a non-empty format.")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Create or apply a Bronze review.")
    parser.add_argument("workbook", type=Path)
    parser.add_argument("--baseline-row-count", type=int)
    parser.add_argument("--resolution", type=Path)
    parser.add_argument("--review-dir", type=Path, default=DEFAULT_REVIEW_DIR)
    parser.add_argument("--output-dir", type=Path)
    args = parser.parse_args(argv)

    if args.resolution:
        result = reassess_workbook(
            args.workbook,
            args.resolution,
            baseline_row_count=args.baseline_row_count,
        )
        if args.output_dir:
            print(write_result(result, args.output_dir))
        else:
            print(json.dumps(result.to_dict(), indent=2, sort_keys=True))
        return 0

    result = validate_workbook(
        args.workbook,
        baseline_row_count=args.baseline_row_count,
    )
    if result.lifecycle_state == "AWAITING_REVIEW":
        print(write_review_request(result, args.review_dir))
    else:
        print(json.dumps(result.to_dict(), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
