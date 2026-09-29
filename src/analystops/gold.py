"""Load published Silver rows into PostgreSQL Gold analytics."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from uuid import UUID, uuid4

import psycopg
from psycopg import Connection

from analystops.validation.silver import _price_policy, _validate_row


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_MANIFEST = PROJECT_ROOT / "data" / "publication" / "silver" / "manifest.json"
DEFAULT_DATABASE_URL = (
    "postgresql://analystops:analystops_dev@127.0.0.1:5432/analystops"
)


class GoldLoadError(ValueError):
    """Raised when published Silver evidence cannot be safely loaded."""


def load_gold(
    client_id: UUID | str,
    *,
    client_name: str,
    manifest_path: Path | str = DEFAULT_MANIFEST,
    database_url: str = DEFAULT_DATABASE_URL,
) -> dict[str, object]:
    """Synchronize one tenant's PostgreSQL Gold facts with the manifest."""

    try:
        tenant_id = UUID(str(client_id))
    except ValueError as exc:
        raise GoldLoadError("client_id must be a UUID.") from exc
    if not client_name.strip():
        raise GoldLoadError("client_name is required.")

    manifest_path = Path(manifest_path)
    run_id = uuid4()
    manifest_hash: str | None = None
    manifest_items = changed_items = removed_items = loaded_rows = 0
    try:
        connection = psycopg.connect(database_url, autocommit=True)
    except psycopg.Error as exc:
        raise GoldLoadError(f"Cannot connect to PostgreSQL: {exc}") from exc

    try:
        with connection.transaction():
            _tenant_scope(connection, tenant_id)
            connection.execute(
                """
                INSERT INTO analystops.clients (id, name)
                VALUES (%s, %s)
                ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name
                """,
                (tenant_id, client_name.strip()),
            )
            connection.execute(
                """
                INSERT INTO analystops.gold_run (id, client_id, started_at, status)
                VALUES (%s, %s, %s, 'RUNNING')
                """,
                (run_id, tenant_id, datetime.now(UTC)),
            )

        manifest_hash = _file_hash(manifest_path)
        manifest = _read_object(manifest_path)
        entries = manifest.get("entries")
        if not isinstance(entries, list):
            raise GoldLoadError("Publication manifest entries must be a list.")
        manifest_items = len(entries)

        with connection.transaction():
            _tenant_scope(connection, tenant_id)
            existing = {
                row[0]: row[1:]
                for row in connection.execute(
                    """
                    SELECT source_file_id, canonical_hash, profile_hash,
                           reporting_month, country, row_count
                    FROM analystops.gold_input_item WHERE client_id = %s
                    """,
                    (tenant_id,),
                ).fetchall()
            }
            connection.execute(
                "CREATE TEMP TABLE gold_stage "
                "(LIKE analystops.fact_sales_line INCLUDING DEFAULTS) ON COMMIT DROP"
            )
            seen: set[str] = set()
            for entry_number, entry in enumerate(entries, start=1):
                if not isinstance(entry, dict):
                    raise GoldLoadError(f"Manifest entry {entry_number} must be an object.")
                source_id = _required_text(entry, "source_file_id", entry_number)
                if source_id in seen:
                    raise GoldLoadError(f"Duplicate source_file_id: {source_id}")
                seen.add(source_id)
                canonical_path = _artifact_path(
                    entry, "canonical_path", manifest_path, entry_number
                )
                profile_path = _artifact_path(
                    entry, "profile_path", manifest_path, entry_number
                )
                canonical_hash = _file_hash(canonical_path)
                profile_hash = _file_hash(profile_path)
                _check_published_hash(entry, "canonical_sha256", canonical_hash)
                _check_published_hash(entry, "profile_sha256", profile_hash)
                row_count = _nonnegative_int(entry, "row_count", entry_number)
                reporting_month = _required_text(entry, "reporting_month", entry_number)
                country = _required_text(entry, "country", entry_number)
                if existing.get(source_id) == (
                    canonical_hash,
                    profile_hash,
                    reporting_month,
                    country,
                    row_count,
                ):
                    continue

                profile = _read_object(profile_path)
                _validate_profile(
                    profile,
                    source_id=source_id,
                    canonical_path=canonical_path,
                    reporting_month=reporting_month,
                    country=country,
                    row_count=row_count,
                )
                connection.execute(
                    "DELETE FROM analystops.gold_input_item "
                    "WHERE client_id = %s AND source_file_id = %s",
                    (tenant_id, source_id),
                )
                connection.execute(
                    """
                    INSERT INTO analystops.gold_input_item
                    (client_id, source_file_id, canonical_path, profile_path,
                     canonical_hash, profile_hash, reporting_month, country, row_count)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        tenant_id,
                        source_id,
                        str(canonical_path.resolve()),
                        str(profile_path.resolve()),
                        canonical_hash,
                        profile_hash,
                        reporting_month,
                        country,
                        row_count,
                    ),
                )
                observed_rows, observed_metrics = _stage_rows(
                    connection, tenant_id, source_id, canonical_path
                )
                if observed_rows != row_count:
                    raise GoldLoadError(
                        f"Canonical row count mismatch for {source_id}: "
                        f"expected {row_count}, found {observed_rows}."
                    )
                _reconcile_metrics(profile, observed_metrics, source_id)
                changed_items += 1
                loaded_rows += observed_rows

            removed = set(existing) - seen
            removed_items = len(removed)
            for source_id in removed:
                connection.execute(
                    "DELETE FROM analystops.gold_input_item "
                    "WHERE client_id = %s AND source_file_id = %s",
                    (tenant_id, source_id),
                )
            connection.execute(
                "INSERT INTO analystops.fact_sales_line SELECT * FROM gold_stage"
            )
            connection.execute(
                """
                UPDATE analystops.gold_run
                SET completed_at = %s, status = 'SUCCESS', manifest_hash = %s,
                    manifest_items = %s, changed_items = %s, removed_items = %s,
                    loaded_rows = %s
                WHERE id = %s
                """,
                (
                    datetime.now(UTC),
                    manifest_hash,
                    manifest_items,
                    changed_items,
                    removed_items,
                    loaded_rows,
                    run_id,
                ),
            )
    except Exception as exc:
        try:
            with connection.transaction():
                _tenant_scope(connection, tenant_id)
                connection.execute(
                    """
                    UPDATE analystops.gold_run
                    SET completed_at = %s, status = 'FAILED', manifest_hash = %s,
                        manifest_items = %s, error_message = %s
                    WHERE id = %s
                    """,
                    (datetime.now(UTC), manifest_hash, manifest_items, str(exc), run_id),
                )
        except psycopg.Error:
            pass
        if isinstance(exc, GoldLoadError):
            raise
        if isinstance(exc, (OSError, json.JSONDecodeError, psycopg.Error)):
            raise GoldLoadError(str(exc)) from exc
        raise
    finally:
        connection.close()

    return {
        "run_id": str(run_id),
        "client_id": str(tenant_id),
        "database_backend": "postgresql",
        "manifest_items": manifest_items,
        "changed_items": changed_items,
        "removed_items": removed_items,
        "loaded_rows": loaded_rows,
    }


def _tenant_scope(connection: Connection, client_id: UUID) -> None:
    connection.execute("SET LOCAL ROLE analystops_app")
    connection.execute(
        "SELECT set_config('app.client_id', %s, true)", (str(client_id),)
    )


def _stage_rows(
    connection: Connection,
    client_id: UUID,
    source_id: str,
    canonical_path: Path,
) -> tuple[int, dict[str, Decimal]]:
    count = 0
    metrics = {"net_revenue": Decimal("0"), "gross_sales": Decimal("0")}
    with connection.cursor().copy(
        """
        COPY gold_stage
        (client_id, source_file_id, source_row_number, invoice_id, product_id,
         product_description, quantity, transaction_timestamp, unit_price,
         customer_id, country) FROM STDIN
        """
    ) as copy:
        with canonical_path.open() as lines:
            for line_number, line in enumerate(lines, start=1):
                try:
                    row = json.loads(line)
                except json.JSONDecodeError as exc:
                    raise GoldLoadError(
                        f"Invalid JSON at {canonical_path}:{line_number}."
                    ) from exc
                try:
                    quantity, price, timestamp = _validate_row(
                        row, source_id, None, canonical_path, line_number
                    )
                except ValueError as exc:
                    raise GoldLoadError(str(exc)) from exc
                revenue = Decimal(quantity) * price
                if _price_policy(row, quantity, price) not in {
                    "BAD_DEBT_ADJUSTMENT",
                    "UNRECOGNIZED_NEGATIVE_PRICE",
                }:
                    metrics["net_revenue"] += revenue
                    if quantity > 0:
                        metrics["gross_sales"] += revenue
                copy.write_row(
                    (
                        client_id,
                        source_id,
                        row["source_row_number"],
                        row["invoice_id"],
                        row["product_id"],
                        row["product_description"],
                        quantity,
                        timestamp,
                        price,
                        row["customer_id"],
                        row["country"],
                    )
                )
                count += 1
    return count, metrics


def _validate_profile(
    profile: dict[str, object],
    *,
    source_id: str,
    canonical_path: Path,
    reporting_month: str,
    country: str,
    row_count: int,
) -> None:
    expected = {
        "source_file_id": source_id,
        "canonical_path": str(canonical_path.resolve()),
        "reporting_month": reporting_month,
        "country": country,
        "row_count": row_count,
    }
    for key, value in expected.items():
        observed = profile.get(key)
        if key == "canonical_path" and isinstance(observed, str):
            observed = str(Path(observed).resolve())
        if observed != value:
            raise GoldLoadError(f"Profile {key} mismatch for {source_id}.")
    if profile.get("publication_state") not in {
        "PUBLISHABLE",
        "PUBLISHABLE_WITH_WARNINGS",
    }:
        raise GoldLoadError(f"Profile is not publishable for {source_id}.")


def _reconcile_metrics(
    profile: dict[str, object], observed: dict[str, Decimal], source_id: str
) -> None:
    expected = profile.get("metrics")
    if not isinstance(expected, dict):
        raise GoldLoadError(f"Profile metrics are required for {source_id}.")
    for key, value in observed.items():
        try:
            matches = Decimal(str(expected.get(key))) == value
        except InvalidOperation:
            matches = False
        if not matches:
            raise GoldLoadError(
                f"Profile metric {key} mismatch for {source_id}: "
                f"expected {expected.get(key)}, found {value}."
            )


def _artifact_path(
    entry: dict[str, object], key: str, manifest_path: Path, entry_number: int
) -> Path:
    path = Path(_required_text(entry, key, entry_number))
    if not path.is_absolute():
        path = manifest_path.parent / path
    if not path.is_file():
        raise GoldLoadError(f"Artifact not found: {path}")
    return path


def _required_text(entry: dict[str, object], key: str, entry_number: int) -> str:
    value = entry.get(key)
    if not isinstance(value, str) or not value:
        raise GoldLoadError(f"Manifest entry {entry_number} requires {key}.")
    return value


def _nonnegative_int(entry: dict[str, object], key: str, entry_number: int) -> int:
    value = entry.get(key)
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise GoldLoadError(f"Manifest entry {entry_number} requires nonnegative {key}.")
    return value


def _check_published_hash(entry: dict[str, object], key: str, observed: str) -> None:
    expected = entry.get(key)
    if expected is not None and expected != observed:
        raise GoldLoadError(f"Published {key} does not match its artifact.")


def _read_object(path: Path) -> dict[str, object]:
    value = json.loads(path.read_text())
    if not isinstance(value, dict):
        raise GoldLoadError(f"JSON document must be an object: {path}")
    return value


def _file_hash(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--client-id", required=True)
    parser.add_argument("--client-name", required=True)
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    parser.add_argument(
        "--database-url", default=os.environ.get("DATABASE_URL", DEFAULT_DATABASE_URL)
    )
    args = parser.parse_args(argv)
    result = load_gold(
        args.client_id,
        client_name=args.client_name,
        manifest_path=args.manifest,
        database_url=args.database_url,
    )
    result["completed_at"] = datetime.now(UTC).isoformat()
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
