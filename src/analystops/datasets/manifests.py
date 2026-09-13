"""Write JSON manifests for generated submission workbooks."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import pandas as pd

from .corruptions import (
    DEFAULT_CORRUPTED_DIR,
    SCENARIO_NAMES,
    TRANSACTIONS_SHEET,
    CorruptionResult,
    corrupt_clean_submissions,
)
from .splitter import (
    DEFAULT_GENERATED_DIR,
    SUBMISSION_COLUMNS,
    SubmissionSplit,
    generate_clean_submissions,
)
from .uci_online_retail import DEFAULT_SOURCE_WORKBOOK


PROJECT_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_MANIFEST_DIR = PROJECT_ROOT / "data" / "manifests"

EXPECTED_SCHEMA = list(SUBMISSION_COLUMNS.values())
EXPECTED_HANDLING = {
    "clean": "accept",
    "renamed_columns": "review",
    "currency_strings": "review",
    "date_format_changes": "review",
    "duplicate_rows": "review",
    "missing_customer_ids": "review",
    "missing_required_column": "quarantine",
    "unexpected_columns": "review",
    "incomplete_file": "quarantine",
    "duplicate_prior_month_upload": "quarantine",
    "multi_sheet_workbook": "review",
}


def write_clean_manifest(
    split: SubmissionSplit,
    *,
    manifest_dir: Path | str = DEFAULT_MANIFEST_DIR,
) -> Path:
    manifest = {
        "file_path": str(split.output_path),
        "file_type": "clean_submission",
        "country": split.country,
        "reporting_month": split.reporting_month,
        "expected_schema": EXPECTED_SCHEMA,
        "observed_schema": _sheet_columns(split.output_path),
        "source_rows": split.source_rows,
        "row_count": split.row_count,
        "injected_failures": [],
        "expected_handling": EXPECTED_HANDLING["clean"],
    }
    return _write_json(manifest, _manifest_path(split.output_path, manifest_dir, "clean"))


def write_corruption_manifest(
    result: CorruptionResult,
    *,
    source_rows: list[dict[str, int | str]],
    manifest_dir: Path | str = DEFAULT_MANIFEST_DIR,
) -> Path:
    manifest = {
        "file_path": str(result.output_path),
        "source_file_path": str(result.source_path),
        "file_type": "corrupted_submission",
        "scenario": result.scenario,
        "seed": result.seed,
        "expected_schema": EXPECTED_SCHEMA,
        "observed_schema": _sheet_columns(result.output_path),
        "source_rows": source_rows,
        "row_count_before": result.row_count_before,
        "row_count_after": result.row_count_after,
        "injected_failures": [result.scenario],
        "details": result.details,
        "expected_handling": EXPECTED_HANDLING[result.scenario],
    }
    return _write_json(
        manifest,
        _manifest_path(result.output_path, manifest_dir, f"corrupted/{result.scenario}"),
    )


def generate_manifests(
    *,
    source_path: Path | str = DEFAULT_SOURCE_WORKBOOK,
    clean_dir: Path | str = DEFAULT_GENERATED_DIR,
    corrupted_dir: Path | str = DEFAULT_CORRUPTED_DIR,
    manifest_dir: Path | str = DEFAULT_MANIFEST_DIR,
    seed: int = 42,
    scenarios: list[str] | None = None,
    max_files: int | None = None,
    overwrite: bool = False,
) -> list[Path]:
    splits = generate_clean_submissions(
        source_path,
        output_dir=clean_dir,
        max_files=max_files,
        overwrite=overwrite,
    )
    paths = [write_clean_manifest(split, manifest_dir=manifest_dir) for split in splits]
    source_rows = {split.output_path: split.source_rows for split in splits}
    corruptions = corrupt_clean_submissions(
        [split.output_path for split in splits],
        seed=seed,
        output_dir=corrupted_dir,
        scenarios=scenarios or list(SCENARIO_NAMES),
        overwrite=overwrite,
    )
    paths.extend(
        write_corruption_manifest(
            result,
            source_rows=source_rows[result.source_path],
            manifest_dir=manifest_dir,
        )
        for result in corruptions
    )
    return paths


def _sheet_columns(path: Path) -> list[str]:
    return pd.read_excel(path, sheet_name=TRANSACTIONS_SHEET, nrows=0).columns.tolist()


def _manifest_path(file_path: Path, manifest_dir: Path | str, kind: str) -> Path:
    return Path(manifest_dir) / kind / f"{file_path.stem}.json"


def _write_json(data: dict[str, Any], path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n")
    return path


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Generate clean and corrupted submission manifests."
    )
    parser.add_argument("--source", default=DEFAULT_SOURCE_WORKBOOK, type=Path)
    parser.add_argument("--clean-dir", default=DEFAULT_GENERATED_DIR, type=Path)
    parser.add_argument("--corrupted-dir", default=DEFAULT_CORRUPTED_DIR, type=Path)
    parser.add_argument("--manifest-dir", default=DEFAULT_MANIFEST_DIR, type=Path)
    parser.add_argument("--seed", default=42, type=int)
    parser.add_argument("--scenario", action="append", choices=SCENARIO_NAMES)
    parser.add_argument("--max-files", default=None, type=int)
    parser.add_argument("--overwrite", action="store_true")
    args = parser.parse_args(argv)

    paths = generate_manifests(
        source_path=args.source,
        clean_dir=args.clean_dir,
        corrupted_dir=args.corrupted_dir,
        manifest_dir=args.manifest_dir,
        seed=args.seed,
        scenarios=args.scenario,
        max_files=args.max_files,
        overwrite=args.overwrite,
    )
    print(f"Generated {len(paths)} manifest(s).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
