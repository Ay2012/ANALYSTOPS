"""Run bounded Bronze-to-Silver workflows without losing per-workbook state."""

from __future__ import annotations

import argparse
import json
import os
from collections import Counter
from collections.abc import Callable, Iterable
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime
from pathlib import Path
from threading import local
from typing import Any
from uuid import UUID, uuid4

from psycopg import Connection

from analystops.agents.bronze_remediation import (
    ESCALATION_MODEL,
    MAX_OUTPUT_TOKENS,
    PRIMARY_ATTEMPTS,
    PRIMARY_MODEL,
)
from analystops.ingestion.validate import read_result
from analystops.persistence.postgres import (
    persist_batch_result,
    persist_bronze_result,
    persist_workflow_result,
)

from .bronze_to_silver import (
    DEFAULT_SILVER_DIR,
    DEFAULT_VALIDATION_DIR,
    DEFAULT_WORKFLOW_DIR,
    BronzeToSilverWorkflowError,
    run_bronze_to_silver,
)


PROJECT_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_BATCH_DIR = PROJECT_ROOT / "data" / "workflows" / "batches"
BATCH_VERSION = "bronze-to-silver-batch-v1"
SKIPPED_STATES = {"DUPLICATE", "QUARANTINED"}


def run_bronze_to_silver_batch(
    inputs: Iterable[Path | str],
    *,
    client_factory: Callable[[], Any] | None = None,
    output_dir: Path | str = DEFAULT_BATCH_DIR,
    workflow_output_dir: Path | str = DEFAULT_WORKFLOW_DIR,
    silver_dir: Path | str = DEFAULT_SILVER_DIR,
    validation_dir: Path | str = DEFAULT_VALIDATION_DIR,
    resume_from: Path | str | None = None,
    max_records: int = 10,
    workers: int = 4,
    connection: Connection | None = None,
    client_id: UUID | str | None = None,
    client_name: str | None = None,
    primary_model: str = PRIMARY_MODEL,
    escalation_model: str | None = ESCALATION_MODEL,
    primary_attempts: int = PRIMARY_ATTEMPTS,
    max_output_tokens: int = MAX_OUTPUT_TOKENS,
) -> Path:
    """Process a bounded batch and checkpoint after every workbook."""

    if max_records < 1:
        raise ValueError("max_records must be at least 1.")
    if workers < 1:
        raise ValueError("workers must be at least 1.")
    if connection is not None and (client_id is None or not client_name):
        raise ValueError("Database persistence requires client_id and client_name.")
    if connection is None and (client_id is not None or client_name is not None):
        raise ValueError("client_id and client_name require a database connection.")

    selected = _result_paths(inputs)[:max_records]
    if not selected:
        raise ValueError("No Bronze result files found.")
    selected_text = [str(path.resolve()) for path in selected]
    if resume_from:
        report_path = Path(resume_from)
        report = _read_json(report_path)
        if report.get("batch_version") != BATCH_VERSION:
            raise ValueError("Unsupported batch report version.")
        if report.get("selected_bronze_records") != selected_text:
            raise ValueError("Resume inputs do not match the original batch.")
        recording = report.get("operational_recording")
        if isinstance(recording, dict) and recording.get("requested") is True:
            if connection is None:
                raise ValueError("Database-backed batches require persistence on resume.")
            if recording.get("client_id") != str(client_id):
                raise ValueError("Resume client_id does not match the original batch.")
        report["resumed_at"] = datetime.now(UTC).isoformat()
    else:
        batch_id = str(uuid4())
        report_path = Path(output_dir) / batch_id / "batch.json"
        report = {
            "batch_version": BATCH_VERSION,
            "batch_run_id": batch_id,
            "started_at": datetime.now(UTC).isoformat(),
            "resumed_at": None,
            "completed_at": None,
            "status": "RUNNING",
            "configuration": {
                "max_records": max_records,
                "workers": workers,
                "primary_model": primary_model,
                "escalation_model": escalation_model,
                "primary_attempts": primary_attempts,
                "max_output_tokens": max_output_tokens,
            },
            "selected_bronze_records": selected_text,
            "items": [],
            "summary": {},
            "operational_recording": {
                "requested": connection is not None,
                "client_id": str(client_id) if client_id is not None else None,
                "persisted": None,
                "error": None,
            },
        }
    if connection is not None:
        recording = report.get("operational_recording")
        if not isinstance(recording, dict):
            recording = {"persisted": None, "error": None}
            report["operational_recording"] = recording
        recording["requested"] = True
        recording["client_id"] = str(client_id)

    items = {
        str(item["bronze_result_path"]): item
        for item in report.get("items", [])
        if isinstance(item, dict) and isinstance(item.get("bronze_result_path"), str)
    }
    thread_state = local()

    def client_for_thread() -> Any | None:
        if client_factory is None:
            return None
        if not hasattr(thread_state, "client"):
            thread_state.client = client_factory()
        return thread_state.client

    if connection is not None:
        for item in items.values():
            if item.get("status") == "PERSISTENCE_FAILED":
                _persist_item(
                    item,
                    connection,
                    client_id=client_id,
                    client_name=client_name or "",
                )

    pending = []
    for path in selected:
        key = str(path.resolve())
        existing = items.get(key)
        if existing and not (
            existing.get("status") == "FAILED" and existing.get("retryable") is True
        ):
            continue
        pending.append(path)

    report["status"] = "RUNNING"
    report["completed_at"] = None
    _checkpoint(
        report_path,
        report,
        selected,
        items,
        connection=connection,
        client_id=client_id,
        client_name=client_name,
    )
    if pending:
        with ThreadPoolExecutor(max_workers=min(workers, len(pending))) as executor:
            futures = {
                executor.submit(
                    _run_one,
                    path,
                    client_for_thread,
                    workflow_output_dir,
                    silver_dir,
                    validation_dir,
                    primary_model,
                    escalation_model,
                    primary_attempts,
                    max_output_tokens,
                ): path
                for path in pending
            }
            for future in as_completed(futures):
                path = futures[future]
                try:
                    item = future.result()
                except Exception as exc:
                    item = {
                        "bronze_result_path": str(path.resolve()),
                        "bronze_record_hash": None,
                        "file_hash": None,
                        "bronze_state": None,
                        "status": "FAILED",
                        "workflow_status": None,
                        "workflow_path": None,
                        "workflow_run_id": None,
                        "publication_state": None,
                        "retryable": False,
                        "failure": {
                            "code": "BATCH_WORKER_ERROR",
                            "error_type": type(exc).__name__,
                            "message": str(exc)[:2000],
                            "retryable": False,
                        },
                        "persisted": None,
                        "token_usage": _empty_tokens(),
                        "attempts": 0,
                        "failed_attempts": 0,
                        "escalated": False,
                    }
                if connection is not None:
                    _persist_item(
                        item,
                        connection,
                        client_id=client_id,
                        client_name=client_name or "",
                    )
                items[str(path.resolve())] = item
                _checkpoint(
                    report_path,
                    report,
                    selected,
                    items,
                    connection=connection,
                    client_id=client_id,
                    client_name=client_name,
                )

    report["completed_at"] = datetime.now(UTC).isoformat()
    report["status"] = (
        "COMPLETED_WITH_FAILURES"
        if any(
            item.get("status") in {"FAILED", "PERSISTENCE_FAILED"}
            for item in items.values()
        )
        else "COMPLETED"
    )
    _checkpoint(
        report_path,
        report,
        selected,
        items,
        connection=connection,
        client_id=client_id,
        client_name=client_name,
    )
    return report_path


def _run_one(
    path: Path,
    client_for_thread: Callable[[], Any | None],
    workflow_output_dir: Path | str,
    silver_dir: Path | str,
    validation_dir: Path | str,
    primary_model: str,
    escalation_model: str | None,
    primary_attempts: int,
    max_output_tokens: int,
) -> dict[str, object]:
    bronze = read_result(path)
    state = str(bronze.get("lifecycle_state"))
    if state in SKIPPED_STATES:
        return {
            "bronze_result_path": str(path.resolve()),
            "bronze_record_hash": bronze.get("record_hash"),
            "file_hash": bronze.get("file_hash"),
            "bronze_state": state,
            "status": f"SKIPPED_{state}",
            "workflow_status": None,
            "workflow_path": None,
            "workflow_run_id": None,
            "publication_state": None,
            "retryable": False,
            "failure": None,
            "persisted": None,
            "token_usage": _empty_tokens(),
            "attempts": 0,
            "failed_attempts": 0,
            "escalated": False,
        }

    client = client_for_thread() if state == "AWAITING_REVIEW" else None
    try:
        workflow_path = run_bronze_to_silver(
            path,
            client=client,
            output_dir=workflow_output_dir,
            silver_dir=silver_dir,
            validation_dir=validation_dir,
            primary_model=primary_model,
            escalation_model=escalation_model,
            primary_attempts=primary_attempts,
            max_output_tokens=max_output_tokens,
        )
    except BronzeToSilverWorkflowError as exc:
        if exc.report_path is None:
            raise
        workflow_path = exc.report_path
    return _workflow_item(path, bronze, workflow_path, escalation_model)


def _workflow_item(
    bronze_path: Path,
    bronze: dict[str, object],
    workflow_path: Path,
    escalation_model: str | None,
) -> dict[str, object]:
    workflow = _read_json(workflow_path)
    agent_path = workflow.get("agent_run_path") or workflow.get("agent_proposal_path")
    agent = _read_json(Path(agent_path)) if isinstance(agent_path, str) else {}
    attempts = agent.get("attempts", [])
    if not isinstance(attempts, list):
        attempts = []
    token_usage = agent.get("token_usage", _empty_tokens())
    if not isinstance(token_usage, dict):
        token_usage = _empty_tokens()
    failure = workflow.get("failure")
    retryable = bool(
        isinstance(failure, dict) and failure.get("retryable") is True
    )
    status = str(workflow.get("status"))
    return {
        "bronze_result_path": str(bronze_path.resolve()),
        "bronze_record_hash": bronze.get("record_hash"),
        "file_hash": bronze.get("file_hash"),
        "bronze_state": bronze.get("lifecycle_state"),
        "status": status,
        "workflow_status": status,
        "workflow_path": str(workflow_path.resolve()),
        "workflow_run_id": workflow.get("workflow_run_id"),
        "publication_state": workflow.get("publication_state"),
        "retryable": retryable,
        "failure": failure,
        "persisted": None,
        "token_usage": token_usage,
        "attempts": len(attempts),
        "failed_attempts": sum(
            isinstance(attempt, dict) and attempt.get("status") == "FAILED"
            for attempt in attempts
        ),
        "escalated": bool(
            escalation_model
            and any(
                isinstance(attempt, dict)
                and attempt.get("model") == escalation_model
                for attempt in attempts
            )
        ),
    }


def _persist_item(
    item: dict[str, object],
    connection: Connection,
    *,
    client_id: UUID | str | None,
    client_name: str,
) -> None:
    try:
        if item.get("workflow_path"):
            persist_workflow_result(
                connection,
                str(item["workflow_path"]),
                client_id=str(client_id),
                client_name=client_name,
            )
        else:
            persist_bronze_result(
                connection,
                str(item["bronze_result_path"]),
                client_id=str(client_id),
                client_name=client_name,
            )
        item["persisted"] = True
        item["status"] = (
            item.get("workflow_status")
            or item.pop("pre_persistence_status", None)
            or item["status"]
        )
        item.pop("persistence_error", None)
    except Exception as exc:
        if item.get("status") != "PERSISTENCE_FAILED":
            item["pre_persistence_status"] = item.get("status")
        item["status"] = "PERSISTENCE_FAILED"
        item["persisted"] = False
        item["persistence_error"] = {
            "error_type": type(exc).__name__,
            "message": str(exc)[:2000],
        }


def _checkpoint(
    path: Path,
    report: dict[str, object],
    selected: list[Path],
    items: dict[str, dict[str, object]],
    *,
    connection: Connection | None,
    client_id: UUID | str | None,
    client_name: str | None,
) -> None:
    report["items"] = [
        items[str(path.resolve())]
        for path in selected
        if str(path.resolve()) in items
    ]
    report["summary"] = _summarize(report["items"], len(selected))
    _write_json(path, report)
    if connection is None:
        return
    recording = report["operational_recording"]
    if not isinstance(recording, dict):
        raise ValueError("operational_recording must be an object.")
    try:
        persist_batch_result(
            connection,
            path,
            client_id=str(client_id),
            client_name=client_name or "",
        )
    except Exception as exc:
        recording["persisted"] = False
        recording["error"] = {
            "error_type": type(exc).__name__,
            "message": str(exc)[:2000],
        }
        if report.get("status") == "COMPLETED":
            report["status"] = "COMPLETED_WITH_FAILURES"
    else:
        recording["persisted"] = True
        recording["error"] = None
    _write_json(path, report)


def _summarize(items: object, selected: int) -> dict[str, object]:
    rows = (
        [item for item in items if isinstance(item, dict)]
        if isinstance(items, list)
        else []
    )
    statuses = Counter(str(item.get("status")) for item in rows)
    token_keys = (
        "input_tokens",
        "cached_input_tokens",
        "uncached_input_tokens",
        "output_tokens",
        "reasoning_tokens",
        "total_tokens",
    )
    return {
        "selected_records": selected,
        "completed_records": len(rows),
        "status_counts": dict(sorted(statuses.items())),
        "attempts": sum(int(item.get("attempts", 0)) for item in rows),
        "failed_attempts": sum(
            int(item.get("failed_attempts", 0)) for item in rows
        ),
        "escalated_records": sum(bool(item.get("escalated")) for item in rows),
        "token_usage": {
            key: sum(
                int(item.get("token_usage", {}).get(key, 0))
                for item in rows
                if isinstance(item.get("token_usage"), dict)
            )
            for key in token_keys
        },
    }


def _result_paths(inputs: Iterable[Path | str]) -> list[Path]:
    paths: list[Path] = []
    for value in inputs:
        path = Path(value)
        paths.extend(sorted(path.rglob("*.json")) if path.is_dir() else [path])
    return list(dict.fromkeys(path.resolve() for path in paths))


def _empty_tokens() -> dict[str, int]:
    return {
        "input_tokens": 0,
        "cached_input_tokens": 0,
        "uncached_input_tokens": 0,
        "output_tokens": 0,
        "reasoning_tokens": 0,
        "total_tokens": 0,
    }


def _read_json(path: Path) -> dict[str, object]:
    try:
        document = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"Cannot read JSON artifact {path}: {exc}") from exc
    if not isinstance(document, dict):
        raise ValueError(f"JSON artifact {path} must be an object.")
    return document


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    temporary.replace(path)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Run a bounded, resumable Bronze-to-Silver batch."
    )
    parser.add_argument("bronze_results", nargs="+", type=Path)
    parser.add_argument("--max-records", type=int, default=10)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--resume", type=Path)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_BATCH_DIR)
    parser.add_argument(
        "--workflow-output-dir", type=Path, default=DEFAULT_WORKFLOW_DIR
    )
    parser.add_argument("--silver-dir", type=Path, default=DEFAULT_SILVER_DIR)
    parser.add_argument(
        "--validation-dir", type=Path, default=DEFAULT_VALIDATION_DIR
    )
    parser.add_argument("--primary-model", default=PRIMARY_MODEL)
    parser.add_argument("--escalation-model", default=ESCALATION_MODEL)
    parser.add_argument("--primary-attempts", type=int, default=PRIMARY_ATTEMPTS)
    parser.add_argument("--max-output-tokens", type=int, default=MAX_OUTPUT_TOKENS)
    parser.add_argument("--client-id", type=UUID)
    parser.add_argument("--client-name")
    parser.add_argument("--database-url")
    args = parser.parse_args(argv)
    if args.max_records < 1:
        parser.error("--max-records must be at least 1")
    if args.workers < 1:
        parser.error("--workers must be at least 1")
    if bool(args.client_id) != bool(args.client_name):
        parser.error("--client-id and --client-name must be provided together")

    selected = _result_paths(args.bronze_results)[: args.max_records]
    needs_agent = False
    for path in selected:
        try:
            needs_agent = (
                read_result(path).get("lifecycle_state") == "AWAITING_REVIEW"
            )
        except ValueError:
            continue
        if needs_agent:
            break
    client_factory = None
    if needs_agent:
        if not os.environ.get("OPENAI_API_KEY"):
            parser.error(
                "OPENAI_API_KEY is not exported; run "
                "`set -a; source .env; set +a` first"
            )
        from openai import OpenAI

        client_factory = OpenAI

    connection = None
    if args.client_id:
        import psycopg

        from analystops.persistence.postgres import DEFAULT_DATABASE_URL

        connection = psycopg.connect(
            args.database_url
            or os.environ.get("DATABASE_URL")
            or DEFAULT_DATABASE_URL,
            autocommit=True,
        )
    try:
        report_path = run_bronze_to_silver_batch(
            args.bronze_results,
            client_factory=client_factory,
            output_dir=args.output_dir,
            workflow_output_dir=args.workflow_output_dir,
            silver_dir=args.silver_dir,
            validation_dir=args.validation_dir,
            resume_from=args.resume,
            max_records=args.max_records,
            workers=args.workers,
            connection=connection,
            client_id=args.client_id,
            client_name=args.client_name,
            primary_model=args.primary_model,
            escalation_model=args.escalation_model,
            primary_attempts=args.primary_attempts,
            max_output_tokens=args.max_output_tokens,
        )
    finally:
        if connection is not None:
            connection.close()
    print(report_path)
    report = _read_json(report_path)
    return 2 if report.get("status") == "COMPLETED_WITH_FAILURES" else 0


if __name__ == "__main__":
    raise SystemExit(main())
