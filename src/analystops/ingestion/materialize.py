"""Materialize authoritative Bronze records for a generated corpus."""

from __future__ import annotations

import argparse
import json
from collections import Counter
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

from .validate import _file_hash, validate_workbook, write_result


PROJECT_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_MANIFEST_DIR = PROJECT_ROOT / "data" / "manifests"
DEFAULT_RESULTS_DIR = PROJECT_ROOT / "data" / "ingestion" / "corpus-results"


def materialize_corpus(
    manifest_dir: Path | str = DEFAULT_MANIFEST_DIR,
    *,
    output_dir: Path | str = DEFAULT_RESULTS_DIR,
    workers: int = 1,
) -> dict[str, object]:
    """Validate every manifested workbook and persist its Bronze record."""

    manifest_root = Path(manifest_dir)
    output_root = Path(output_dir)
    manifests = sorted(manifest_root.rglob("*.json"))
    if not manifests:
        raise ValueError(f"No manifests found under {manifest_root}")
    tasks = [(path, manifest_root, output_root) for path in manifests]
    if workers > 1:
        with ProcessPoolExecutor(max_workers=workers) as executor:
            completed = list(executor.map(_materialize_manifest, tasks))
    else:
        completed = [_materialize_manifest(task) for task in tasks]

    states = Counter(state for state, _ in completed)
    return {
        "total": len(completed),
        "states": dict(sorted(states.items())),
        "output_dir": str(output_root.resolve()),
    }


def _materialize_manifest(
    task: tuple[Path, Path, Path],
) -> tuple[str, str]:
    manifest_path, manifest_root, output_root = task
    manifest = json.loads(manifest_path.read_text())
    workbook = Path(manifest["file_path"])
    baseline = manifest.get("row_count_before")
    kwargs: dict[str, object] = {}
    if isinstance(baseline, int) and not isinstance(baseline, bool):
        kwargs["baseline_row_count"] = baseline

    source_path = manifest.get("source_file_path")
    if isinstance(source_path, str):
        source = Path(source_path)
        if workbook.is_file() and source.is_file() and _file_hash(workbook) == _file_hash(source):
            source_result = validate_workbook(source)
            kwargs["seen_hashes"] = (source_result.file_hash,)
            kwargs["seen_content_fingerprints"] = (
                source_result.content_fingerprint,
            )

    result = validate_workbook(workbook, **kwargs)
    relative_dir = manifest_path.relative_to(manifest_root).parent
    record = write_result(result, output_root / relative_dir)
    return result.lifecycle_state, str(record)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Materialize authoritative Bronze records from Phase 1 manifests."
    )
    parser.add_argument("--manifest-dir", type=Path, default=DEFAULT_MANIFEST_DIR)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_RESULTS_DIR)
    parser.add_argument("--workers", type=int, default=4)
    args = parser.parse_args(argv)
    if args.workers < 1:
        parser.error("--workers must be at least 1")
    summary = materialize_corpus(
        args.manifest_dir,
        output_dir=args.output_dir,
        workers=args.workers,
    )
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
