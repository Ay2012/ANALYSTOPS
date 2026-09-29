"""Safely expire terminal LangGraph checkpoints after audit retention."""

from __future__ import annotations

import argparse
import json
import os
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import psycopg

from analystops.persistence.postgres import DEFAULT_DATABASE_URL
from analystops.workflows.silver_remediation_graph import (
    DEFAULT_WORKFLOW_DIR,
    build_silver_remediation_graph,
    checkpoint_config,
    checkpoint_lock,
    postgres_checkpointer,
)


PROJECT_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_REPORT_DIR = PROJECT_ROOT / "data" / "operations" / "checkpoint-retention"
DEFAULT_RETENTION_DAYS = 30
POLICY_VERSION = "checkpoint-retention-v1"
TERMINAL_STATUSES = {
    "REMEDIATION_REJECTED",
    "READY_FOR_PUBLICATION",
    "SILVER_BLOCKED",
    "SILVER_PUBLISHABLE",
    "SILVER_PUBLISHABLE_WITH_WARNINGS",
}


def run_checkpoint_retention(
    database_url: str,
    *,
    client_id: UUID | str,
    retention_days: int = DEFAULT_RETENTION_DAYS,
    apply: bool = False,
    workflow_dir: Path | str = DEFAULT_WORKFLOW_DIR,
    report_dir: Path | str = DEFAULT_REPORT_DIR,
    now: datetime | None = None,
) -> Path:
    """Report or delete audited terminal checkpoints for one tenant."""

    tenant_id = UUID(str(client_id))
    if retention_days < 0:
        raise ValueError("retention_days must be non-negative.")
    observed_at = now or datetime.now(UTC)
    if observed_at.tzinfo is None:
        raise ValueError("now must be timezone-aware.")
    cutoff = observed_at - timedelta(days=retention_days)
    audit_root = Path(workflow_dir)
    decisions: list[dict[str, object]] = []

    with postgres_checkpointer(database_url) as checkpointer:
        graph = build_silver_remediation_graph(None, checkpointer=checkpointer)
        with psycopg.connect(database_url) as connection:
            rows = connection.execute(
                """
                SELECT DISTINCT ON (thread_id)
                    thread_id,
                    (checkpoint->>'ts')::timestamptz AS updated_at
                FROM analystops.checkpoints
                WHERE thread_id LIKE %s
                ORDER BY thread_id, (checkpoint->>'ts')::timestamptz DESC
                """,
                (f"{tenant_id}:%",),
            ).fetchall()

        for storage_thread_id, updated_at in rows:
            thread_id = _public_thread_id(storage_thread_id, tenant_id)
            config = checkpoint_config(thread_id, str(tenant_id))
            snapshot = graph.get_state(config)
            status = str(snapshot.values.get("status", ""))
            audit_path = audit_root / str(tenant_id) / thread_id / "workflow.json"
            reason = _retention_reason(
                snapshot,
                status=status,
                updated_at=updated_at,
                cutoff=cutoff,
                audit_path=audit_path,
            )
            decision = {
                "thread_id": thread_id,
                "storage_thread_id": storage_thread_id,
                "status": status or None,
                "updated_at": updated_at.isoformat(),
                "audit_path": str(audit_path),
                "eligible": reason == "ELIGIBLE",
                "reason": reason,
                "deleted": False,
            }
            if apply and reason == "ELIGIBLE":
                decision["deleted_rows"] = _delete_thread(
                    database_url, storage_thread_id, expected_updated_at=updated_at
                )
                if decision["deleted_rows"] is None:
                    decision["eligible"] = False
                    decision["reason"] = "CHANGED_DURING_RUN"
                else:
                    decision["deleted"] = True
            decisions.append(decision)

    report = {
        "policy_version": POLICY_VERSION,
        "run_id": str(uuid4()),
        "observed_at": observed_at.isoformat(),
        "client_id": str(tenant_id),
        "retention_days": retention_days,
        "cutoff": cutoff.isoformat(),
        "mode": "APPLY" if apply else "DRY_RUN",
        "threads_scanned": len(decisions),
        "threads_eligible": sum(bool(item["eligible"]) for item in decisions),
        "threads_deleted": sum(bool(item["deleted"]) for item in decisions),
        "decisions": decisions,
    }
    timestamp = observed_at.strftime("%Y%m%dT%H%M%SZ")
    path = Path(report_dir) / f"{timestamp}_{report['run_id']}.json"
    _write_json(path, report)
    return path


def _retention_reason(
    snapshot: Any,
    *,
    status: str,
    updated_at: datetime,
    cutoff: datetime,
    audit_path: Path,
) -> str:
    if any(
        getattr(task, "interrupts", ())
        for task in getattr(snapshot, "tasks", ())
    ):
        return "PENDING_REVIEW"
    if snapshot.next:
        return "ACTIVE_OR_RETRYABLE"
    if status not in TERMINAL_STATUSES:
        return "NON_TERMINAL_STATUS"
    if updated_at >= cutoff:
        return "WITHIN_RETENTION"
    if not audit_path.is_file():
        return "AUDIT_MISSING"
    return "ELIGIBLE"


def _public_thread_id(storage_thread_id: str, client_id: UUID) -> str:
    prefix = f"{client_id}:"
    if not storage_thread_id.startswith(prefix):
        raise ValueError("Checkpoint thread does not belong to the requested client.")
    return str(UUID(storage_thread_id.removeprefix(prefix)))


def _delete_thread(
    database_url: str,
    storage_thread_id: str,
    *,
    expected_updated_at: datetime,
) -> dict[str, int] | None:
    deleted: dict[str, int] = {}
    with checkpoint_lock(database_url, storage_thread_id):
        with psycopg.connect(database_url) as connection:
            latest = connection.execute(
                """
                SELECT MAX((checkpoint->>'ts')::timestamptz)
                FROM analystops.checkpoints
                WHERE thread_id = %s
                """,
                (storage_thread_id,),
            ).fetchone()[0]
            if latest != expected_updated_at:
                return None
            for table in ("checkpoint_writes", "checkpoint_blobs", "checkpoints"):
                cursor = connection.execute(
                    f"DELETE FROM analystops.{table} WHERE thread_id = %s",
                    (storage_thread_id,),
                )
                deleted[table] = cursor.rowcount
    return deleted


def _write_json(path: Path, value: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")
    temporary.replace(path)


def _positive_days(value: str) -> int:
    days = int(value)
    if days < 1:
        raise argparse.ArgumentTypeError("retention days must be at least 1")
    return days


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Report or remove audited terminal LangGraph checkpoints."
    )
    parser.add_argument("--client-id", type=UUID, required=True)
    parser.add_argument(
        "--retention-days", type=_positive_days, default=DEFAULT_RETENTION_DAYS
    )
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--workflow-dir", type=Path, default=DEFAULT_WORKFLOW_DIR)
    parser.add_argument("--report-dir", type=Path, default=DEFAULT_REPORT_DIR)
    parser.add_argument("--database-url")
    args = parser.parse_args(argv)
    database_url = (
        args.database_url
        or os.environ.get("CHECKPOINT_DATABASE_URL")
        or os.environ.get("DATABASE_URL")
        or DEFAULT_DATABASE_URL
    )
    try:
        print(
            run_checkpoint_retention(
                database_url,
                client_id=args.client_id,
                retention_days=args.retention_days,
                apply=args.apply,
                workflow_dir=args.workflow_dir,
                report_dir=args.report_dir,
            )
        )
    except (OSError, psycopg.Error, ValueError) as exc:
        parser.exit(1, f"checkpoint retention failed: {exc}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
