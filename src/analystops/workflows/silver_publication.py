"""Atomically publish a completed Silver remediation execution."""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID


PROJECT_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_MANIFEST = PROJECT_ROOT / "data" / "publication" / "silver" / "manifest.json"
DEFAULT_BOOTSTRAP_MANIFEST = (
    PROJECT_ROOT / "data" / "validation" / "silver" / "publish-manifest.json"
)
PUBLICATION_VERSION = "silver-publication-v1"
MANIFEST_VERSION = "silver-publication-manifest-v1"


class SilverPublicationError(RuntimeError):
    """Raised when execution evidence is not safe to publish."""


def publish_silver_execution(
    execution_path: Path | str,
    *,
    manifest_path: Path | str = DEFAULT_MANIFEST,
    bootstrap_manifest: Path | str | None = DEFAULT_BOOTSTRAP_MANIFEST,
) -> Path:
    """Verify and atomically merge one remediation into the final manifest."""

    source_path = Path(execution_path)
    execution = _read_object(source_path)
    entries = _publication_entries(execution)
    execution_id = _uuid_text(execution, "execution_id")
    execution_hash = _document_hash(execution)
    destination = Path(manifest_path)
    receipt_path = source_path.with_name("publication.json")
    if receipt_path.exists():
        receipt = _read_object(receipt_path)
        if (
            receipt.get("execution_id") != execution_id
            or receipt.get("execution_hash") != execution_hash
            or receipt.get("manifest_path") != str(destination.resolve())
        ):
            raise SilverPublicationError("Existing publication receipt conflicts.")
        _validate_existing_receipt(receipt)
        return receipt_path

    destination.parent.mkdir(parents=True, exist_ok=True)
    lock_path = destination.with_suffix(destination.suffix + ".lock")
    with lock_path.open("a+") as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
        manifest = _load_manifest(destination, bootstrap_manifest)
        manifest_entries = _entry_list(manifest)
        existing = {
            _required_text(item, "source_file_id"): item for item in manifest_entries
        }
        if len(existing) != len(manifest_entries):
            raise SilverPublicationError("Publication manifest has duplicate identities.")
        added = []
        already_present = []
        for entry in entries:
            source_id = str(entry["source_file_id"])
            prior = existing.get(source_id)
            if prior is None:
                existing[source_id] = entry
                added.append(source_id)
            elif _entry_identity(prior) == _entry_identity(entry):
                already_present.append(source_id)
            else:
                raise SilverPublicationError(
                    f"Publication conflict for source_file_id {source_id}."
                )
        merged = sorted(
            existing.values(), key=lambda item: str(item["source_file_id"])
        )
        now = datetime.now(UTC).isoformat()
        updated = {
            "manifest_version": MANIFEST_VERSION,
            "updated_at": now,
            "published_workbooks": len(merged),
            "published_rows": sum(int(item["row_count"]) for item in merged),
            "entries": merged,
        }
        _write_json(destination, updated)
        manifest_hash = _document_hash(updated)

    receipt = {
        "publication_version": PUBLICATION_VERSION,
        "publication_id": execution_id,
        "execution_id": execution_id,
        "execution_hash": execution_hash,
        "parent_source_file_id": execution.get("source_file_id"),
        "status": "PUBLISHED",
        "published_at": now,
        "manifest_path": str(destination.resolve()),
        "manifest_hash": manifest_hash,
        "entries_added": len(added),
        "entries_already_present": len(already_present),
        "published_workbooks": len(entries),
        "published_rows": sum(int(item["row_count"]) for item in entries),
        "dropped_rows": int(execution["dropped_rows"]),
        "entries": entries,
    }
    _write_json(receipt_path, receipt)
    return receipt_path


def _publication_entries(execution: Mapping[str, object]) -> list[dict[str, object]]:
    if execution.get("status") != "READY_FOR_PUBLICATION":
        raise SilverPublicationError("Execution is not READY_FOR_PUBLICATION.")
    if int(execution.get("held_rows", -1)) != 0:
        raise SilverPublicationError("Execution still contains held rows.")
    partitions = execution.get("partitions")
    if not isinstance(partitions, list) or not partitions:
        raise SilverPublicationError("Execution has no partitions.")
    entries = []
    source_ids = set()
    accepted_rows = 0
    dropped_rows = 0
    for partition in partitions:
        if not isinstance(partition, dict):
            raise SilverPublicationError("Execution partition is malformed.")
        if partition.get("publication_state") not in {
            "PUBLISHABLE",
            "PUBLISHABLE_WITH_WARNINGS",
        }:
            raise SilverPublicationError("A partition is not publishable.")
        profile_path = Path(_required_text(partition, "validation_profile_path"))
        silver_path = Path(_required_text(partition, "silver_result_path"))
        profile = _read_object(profile_path)
        silver = _read_object(silver_path)
        source_id = _required_text(profile, "source_file_id")
        if source_id in source_ids or silver.get("source_file_id") != source_id:
            raise SilverPublicationError(
                "Partition source identity is duplicated or mismatched."
            )
        source_ids.add(source_id)
        canonical_path = Path(_required_text(profile, "canonical_path"))
        if (
            profile.get("publication_state") != partition.get("publication_state")
            or str(canonical_path.resolve())
            != str(Path(_required_text(silver, "accepted_path")).resolve())
            or not canonical_path.is_file()
        ):
            raise SilverPublicationError(
                "Partition evidence does not match its artifacts."
            )
        row_count = _nonnegative_int(profile, "row_count")
        if row_count != _nonnegative_int(silver, "accepted_rows"):
            raise SilverPublicationError("Partition row counts do not match.")
        accepted_rows += row_count
        dropped_rows += _nonnegative_int(silver, "dropped_rows")
        entries.append(
            {
                "source_file_id": source_id,
                "canonical_path": str(canonical_path.resolve()),
                "profile_path": str(profile_path.resolve()),
                "reporting_month": _required_text(profile, "reporting_month"),
                "country": _required_text(profile, "country"),
                "row_count": row_count,
                "canonical_sha256": _file_hash(canonical_path),
                "profile_sha256": _file_hash(profile_path),
                "remediation_execution_id": execution["execution_id"],
                "parent_source_file_id": execution["source_file_id"],
            }
        )
    if (
        accepted_rows != _nonnegative_int(execution, "silver_rows")
        or dropped_rows != _nonnegative_int(execution, "dropped_rows")
        or accepted_rows + dropped_rows != _nonnegative_int(execution, "input_rows")
    ):
        raise SilverPublicationError("Execution row counts do not reconcile.")
    return entries


def _validate_existing_receipt(receipt: Mapping[str, object]) -> None:
    manifest_path = Path(_required_text(receipt, "manifest_path"))
    manifest = _read_object(manifest_path)
    entries = receipt.get("entries")
    if not isinstance(entries, list) or not all(isinstance(item, dict) for item in entries):
        raise SilverPublicationError("Publication receipt entries are malformed.")
    current = {
        _required_text(item, "source_file_id"): item
        for item in _entry_list(manifest)
    }
    for entry in entries:
        source_id = _required_text(entry, "source_file_id")
        if source_id not in current or _entry_identity(
            current[source_id]
        ) != _entry_identity(entry):
            raise SilverPublicationError("Published entry is missing or changed.")
        canonical_path = Path(_required_text(entry, "canonical_path"))
        profile_path = Path(_required_text(entry, "profile_path"))
        if (
            _file_hash(canonical_path) != _required_text(entry, "canonical_sha256")
            or _file_hash(profile_path) != _required_text(entry, "profile_sha256")
        ):
            raise SilverPublicationError("Published artifact changed after publication.")


def _load_manifest(
    manifest_path: Path, bootstrap_manifest: Path | str | None
) -> dict[str, object]:
    if manifest_path.exists():
        manifest = _read_object(manifest_path)
        if manifest.get("manifest_version") != MANIFEST_VERSION:
            raise SilverPublicationError("Unsupported publication manifest version.")
        return manifest
    if bootstrap_manifest is None or not Path(bootstrap_manifest).exists():
        return {"entries": []}
    bootstrap = _read_object(Path(bootstrap_manifest))
    return {"entries": _entry_list(bootstrap)}


def _entry_list(manifest: Mapping[str, object]) -> list[dict[str, object]]:
    entries = manifest.get("entries")
    if not isinstance(entries, list) or not all(
        isinstance(item, dict) for item in entries
    ):
        raise SilverPublicationError("Publication manifest entries are malformed.")
    return list(entries)


def _entry_identity(entry: Mapping[str, object]) -> tuple[object, ...]:
    return tuple(
        entry.get(key)
        for key in (
            "source_file_id",
            "canonical_path",
            "profile_path",
            "reporting_month",
            "country",
            "row_count",
        )
    )


def _read_object(path: Path) -> dict[str, object]:
    try:
        value = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as exc:
        raise SilverPublicationError(f"Cannot read JSON artifact {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise SilverPublicationError(f"JSON artifact {path} must be an object.")
    return value


def _required_text(value: Mapping[str, object], key: str) -> str:
    text = value.get(key)
    if not isinstance(text, str) or not text.strip():
        raise SilverPublicationError(f"{key} must be a non-empty string.")
    return text


def _uuid_text(value: Mapping[str, object], key: str) -> str:
    try:
        return str(UUID(_required_text(value, key)))
    except ValueError as exc:
        raise SilverPublicationError(f"{key} must be a UUID.") from exc


def _nonnegative_int(value: Mapping[str, object], key: str) -> int:
    number = value.get(key)
    if not isinstance(number, int) or isinstance(number, bool) or number < 0:
        raise SilverPublicationError(f"{key} must be a non-negative integer.")
    return number


def _file_hash(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _document_hash(value: Mapping[str, object]) -> str:
    return hashlib.sha256(
        json.dumps(
            value, sort_keys=True, separators=(",", ":"), ensure_ascii=True
        ).encode()
    ).hexdigest()


def _write_json(path: Path, value: Mapping[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")
    temporary.replace(path)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Publish verified Silver partitions.")
    parser.add_argument("execution", type=Path)
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    parser.add_argument(
        "--bootstrap-manifest", type=Path, default=DEFAULT_BOOTSTRAP_MANIFEST
    )
    parser.add_argument("--client-id", type=UUID)
    parser.add_argument("--client-name")
    parser.add_argument("--database-url")
    args = parser.parse_args(argv)
    if bool(args.client_id) != bool(args.client_name):
        parser.error("--client-id and --client-name must be provided together")
    try:
        receipt = publish_silver_execution(
            args.execution,
            manifest_path=args.manifest,
            bootstrap_manifest=args.bootstrap_manifest,
        )
        if args.client_id:
            import psycopg

            from analystops.persistence.postgres import (
                DEFAULT_DATABASE_URL,
                persist_publication_result,
            )

            url = (
                args.database_url
                or os.environ.get("DATABASE_URL")
                or DEFAULT_DATABASE_URL
            )
            with psycopg.connect(url, autocommit=True) as connection:
                persist_publication_result(
                    connection,
                    receipt,
                    client_id=args.client_id,
                    client_name=args.client_name,
                )
    except (OSError, ValueError, SilverPublicationError) as exc:
        parser.exit(1, f"publication failed: {exc}\n")
    print(receipt)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
