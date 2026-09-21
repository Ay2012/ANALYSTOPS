"""Persist authoritative Bronze results in the PostgreSQL control plane."""

from __future__ import annotations

import argparse
import json
import os
from dataclasses import dataclass
from pathlib import Path
from uuid import UUID

import psycopg
from psycopg import Connection
from psycopg.types.json import Jsonb

from analystops.ingestion.validate import read_result
from analystops.transformations.operations import (
    TransformationPlan,
    load_transformation_plan,
    transformation_plan_hash,
)


DEFAULT_DATABASE_URL = (
    "postgresql://analystops:analystops_dev@127.0.0.1:5432/analystops"
)


@dataclass(frozen=True)
class PersistedBronzeRun:
    client_id: UUID
    workbook_id: UUID
    bronze_run_id: UUID
    inserted: bool
    findings_written: int
    resolutions_written: int


@dataclass(frozen=True)
class PersistedWorkflowRun:
    client_id: UUID
    workflow_run_id: UUID
    workbook_id: UUID
    initial_bronze_run_id: UUID
    resolved_bronze_run_id: UUID | None
    agent_run_id: UUID | None
    transformation_plan_id: UUID | None
    silver_run_id: UUID | None
    inserted: bool


@dataclass(frozen=True)
class PersistedBatchRun:
    client_id: UUID
    batch_run_id: UUID
    status: str
    items_written: int


def persist_bronze_result(
    connection: Connection,
    result_path: Path | str,
    *,
    client_id: UUID | str,
    client_name: str,
    object_uri: str | None = None,
) -> PersistedBronzeRun:
    """Persist one verified Bronze record and return its database identity."""

    tenant_id = UUID(str(client_id))
    if not client_name.strip():
        raise ValueError("client_name must be non-empty.")
    bronze = read_result(result_path)
    file_path = _required_text(bronze, "file_path")
    file_hash = _required_text(bronze, "file_hash")
    record_hash = _required_text(bronze, "record_hash")
    findings = bronze.get("findings")
    if not isinstance(findings, list):
        raise ValueError("Bronze findings must be a list.")
    artifact_uri = object_uri or Path(file_path).resolve().as_uri()

    with connection.transaction():
        connection.execute("SET LOCAL ROLE analystops_app")
        connection.execute(
            "SELECT set_config('app.client_id', %s, true)",
            (str(tenant_id),),
        )
        connection.execute(
            """
            INSERT INTO analystops.clients (id, name)
            VALUES (%s, %s)
            ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name
            """,
            (tenant_id, client_name.strip()),
        )
        workbook_id = connection.execute(
            """
            INSERT INTO analystops.workbooks (
                client_id, file_hash, original_filename, object_uri
            ) VALUES (%s, %s, %s, %s)
            ON CONFLICT (client_id, file_hash) DO UPDATE SET
                original_filename = EXCLUDED.original_filename,
                object_uri = EXCLUDED.object_uri
            RETURNING id
            """,
            (tenant_id, file_hash, Path(file_path).name, artifact_uri),
        ).fetchone()[0]
        inserted_row = connection.execute(
            """
            INSERT INTO analystops.bronze_runs (
                client_id, workbook_id, policy_version, lifecycle_state,
                quality_disposition, decision, record_hash, evidence
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (client_id, record_hash) DO NOTHING
            RETURNING id
            """,
            (
                tenant_id,
                workbook_id,
                _required_text(bronze, "policy_version"),
                _required_text(bronze, "lifecycle_state"),
                _required_text(bronze, "quality_disposition"),
                _required_text(bronze, "decision"),
                record_hash,
                Jsonb(bronze),
            ),
        ).fetchone()
        inserted = inserted_row is not None
        if inserted:
            bronze_run_id = inserted_row[0]
            finding_count, resolution_count = _write_findings(
                connection, tenant_id, bronze_run_id, findings
            )
        else:
            bronze_run_id = connection.execute(
                """
                SELECT id FROM analystops.bronze_runs
                WHERE client_id = %s AND record_hash = %s
                """,
                (tenant_id, record_hash),
            ).fetchone()[0]
            finding_count = 0
            resolution_count = 0

    return PersistedBronzeRun(
        client_id=tenant_id,
        workbook_id=workbook_id,
        bronze_run_id=bronze_run_id,
        inserted=inserted,
        findings_written=finding_count,
        resolutions_written=resolution_count,
    )


def persist_workflow_result(
    connection: Connection,
    workflow_path: Path | str,
    *,
    client_id: UUID | str,
    client_name: str,
) -> PersistedWorkflowRun:
    """Persist one completed workflow and its agent/Silver audit trail."""

    tenant_id = UUID(str(client_id))
    if not client_name.strip():
        raise ValueError("client_name must be non-empty.")
    report = _read_document(workflow_path)
    workflow_id = UUID(_required_text(report, "workflow_run_id"))
    initial_path = _required_text(report, "initial_bronze_record_path")
    initial = read_result(initial_path)
    resolved_path = _optional_path(report.get("resolved_bronze_record_path"))
    resolved = read_result(resolved_path) if resolved_path else None
    agent_run = _optional_document(
        report.get("agent_run_path") or report.get("agent_proposal_path")
    )
    plan_document = _optional_document(report.get("transformation_plan_path"))
    silver = _optional_document(report.get("silver_result_path"))
    validation = _optional_document(report.get("validation_profile_path"))

    file_hash = _required_text(initial, "file_hash")
    if report.get("file_hash") != file_hash:
        raise ValueError("Workflow file_hash does not match initial Bronze.")
    if resolved is not None and resolved.get("file_hash") != file_hash:
        raise ValueError("Resolved Bronze does not match the workflow workbook.")
    if agent_run is not None and (
        agent_run.get("file_hash") != file_hash
        or agent_run.get("bronze_record_hash") != initial.get("record_hash")
    ):
        raise ValueError("Agent run is not bound to initial Bronze.")
    if silver is not None and silver.get("source_file_id") != file_hash:
        raise ValueError("Silver result does not match the workflow workbook.")
    if validation is not None and validation.get("source_file_id") != file_hash:
        raise ValueError("Validation profile does not match the workflow workbook.")
    if plan_document is not None and resolved is None:
        raise ValueError("A transformation plan requires resolved Bronze.")

    parsed_plan = (
        load_transformation_plan(plan_document, resolved)
        if plan_document is not None and resolved is not None
        else None
    )
    plan_hash = transformation_plan_hash(parsed_plan) if parsed_plan else None
    if silver is not None and silver.get("transformation_plan_hash") != plan_hash:
        raise ValueError("Silver result transformation plan hash does not match.")

    with connection.transaction():
        connection.execute("SET LOCAL ROLE analystops_app")
        connection.execute(
            "SELECT set_config('app.client_id', %s, true)", (str(tenant_id),)
        )
        connection.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
            (str(workflow_id),),
        )
        existing = connection.execute(
            """
            SELECT workbook_id, initial_bronze_run_id, resolved_bronze_run_id,
                   agent_run_id, transformation_plan_id, silver_run_id
            FROM analystops.workflow_runs
            WHERE client_id = %s AND id = %s
            """,
            (tenant_id, workflow_id),
        ).fetchone()
        if existing:
            return PersistedWorkflowRun(
                tenant_id, workflow_id, *existing, inserted=False
            )

        initial_run = persist_bronze_result(
            connection,
            initial_path,
            client_id=tenant_id,
            client_name=client_name,
        )
        resolved_run = None
        if resolved_path:
            resolved_run = (
                initial_run
                if resolved_path == initial_path
                else persist_bronze_result(
                    connection,
                    resolved_path,
                    client_id=tenant_id,
                    client_name=client_name,
                )
            )
        agent_run_id = (
            _persist_agent_run(
                connection, tenant_id, initial_run.bronze_run_id, agent_run
            )
            if agent_run is not None
            else None
        )
        plan_id = (
            _persist_plan(
                connection,
                tenant_id,
                resolved_run.bronze_run_id,
                plan_document,
                parsed_plan,
                report,
                resolved,
            )
            if plan_document is not None
            and parsed_plan is not None
            and resolved_run is not None
            and resolved is not None
            else None
        )
        silver_run_id = (
            _persist_silver_run(
                connection,
                tenant_id,
                resolved_run.bronze_run_id,
                plan_id,
                silver,
                report,
            )
            if silver is not None and resolved_run is not None
            else None
        )
        connection.execute(
            """
            INSERT INTO analystops.workflow_runs (
                id, client_id, workbook_id, initial_bronze_run_id,
                resolved_bronze_run_id, agent_run_id, transformation_plan_id,
                silver_run_id, workflow_version, status, publication_state,
                validation_profile, audit, started_at, completed_at
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            """,
            (
                workflow_id,
                tenant_id,
                initial_run.workbook_id,
                initial_run.bronze_run_id,
                resolved_run.bronze_run_id if resolved_run else None,
                agent_run_id,
                plan_id,
                silver_run_id,
                _required_text(report, "workflow_version"),
                _required_text(report, "status"),
                report.get("publication_state"),
                Jsonb(validation) if validation is not None else None,
                Jsonb(report),
                _required_text(report, "started_at"),
                _required_text(report, "completed_at"),
            ),
        )
        return PersistedWorkflowRun(
            tenant_id,
            workflow_id,
            initial_run.workbook_id,
            initial_run.bronze_run_id,
            resolved_run.bronze_run_id if resolved_run else None,
            agent_run_id,
            plan_id,
            silver_run_id,
            inserted=True,
        )


def persist_batch_result(
    connection: Connection,
    report_path: Path | str,
    *,
    client_id: UUID | str,
    client_name: str,
) -> PersistedBatchRun:
    """Upsert one operational batch checkpoint and its completed items."""

    tenant_id = UUID(str(client_id))
    if not client_name.strip():
        raise ValueError("client_name must be non-empty.")
    report = _read_document(report_path)
    batch_id = UUID(_required_text(report, "batch_run_id"))
    status = _required_text(report, "status")
    if status not in {"RUNNING", "COMPLETED", "COMPLETED_WITH_FAILURES"}:
        raise ValueError("Unsupported batch status.")
    selected = report.get("selected_bronze_records")
    items = report.get("items")
    configuration = report.get("configuration")
    summary = report.get("summary")
    if not isinstance(selected, list) or not all(
        isinstance(path, str) and path for path in selected
    ):
        raise ValueError("selected_bronze_records must be a list of paths.")
    if len(set(selected)) != len(selected):
        raise ValueError("selected_bronze_records must be unique.")
    if not isinstance(items, list) or not isinstance(configuration, dict):
        raise ValueError("Batch items and configuration must be present.")
    if not isinstance(summary, dict):
        raise ValueError("Batch summary must be present.")
    selected_count = _nonnegative_int(summary, "selected_records")
    completed_count = _nonnegative_int(summary, "completed_records")
    if selected_count != len(selected) or completed_count != len(items):
        raise ValueError("Batch summary counts do not match its records.")
    selected_indexes = {path: index for index, path in enumerate(selected)}

    with connection.transaction():
        connection.execute("SET LOCAL ROLE analystops_app")
        connection.execute(
            "SELECT set_config('app.client_id', %s, true)", (str(tenant_id),)
        )
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
            INSERT INTO analystops.batch_runs (
                id, client_id, batch_version, status, selected_records,
                completed_records, configuration, summary, report_uri,
                started_at, resumed_at, completed_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (client_id, id) DO UPDATE SET
                status = EXCLUDED.status,
                selected_records = EXCLUDED.selected_records,
                completed_records = EXCLUDED.completed_records,
                configuration = EXCLUDED.configuration,
                summary = EXCLUDED.summary,
                report_uri = EXCLUDED.report_uri,
                resumed_at = EXCLUDED.resumed_at,
                completed_at = EXCLUDED.completed_at,
                last_checkpoint_at = now()
            """,
            (
                batch_id,
                tenant_id,
                _required_text(report, "batch_version"),
                status,
                selected_count,
                completed_count,
                Jsonb(configuration),
                Jsonb(summary),
                Path(report_path).resolve().as_uri(),
                _required_text(report, "started_at"),
                _optional_text(report.get("resumed_at")),
                _optional_text(report.get("completed_at")),
            ),
        )
        for item in items:
            if not isinstance(item, dict):
                raise ValueError("Each batch item must be an object.")
            result_path = _required_text(item, "bronze_result_path")
            if result_path not in selected_indexes:
                raise ValueError("Batch item does not belong to the selected inputs.")
            tokens = item.get("token_usage")
            if not isinstance(tokens, dict):
                raise ValueError("Batch item token_usage must be an object.")
            failure = item.get("failure") or item.get("persistence_error")
            if failure is not None and not isinstance(failure, dict):
                raise ValueError("Batch item failure must be an object or null.")
            persisted = item.get("persisted")
            if persisted is not None and not isinstance(persisted, bool):
                raise ValueError("Batch item persisted must be boolean or null.")
            workflow_run_id = item.get("workflow_run_id")
            workflow_id = (
                UUID(str(workflow_run_id))
                if persisted is True and workflow_run_id is not None
                else None
            )
            connection.execute(
                """
                INSERT INTO analystops.batch_items (
                    client_id, batch_run_id, item_index, bronze_result_path,
                    bronze_record_hash, file_hash, workflow_run_id,
                    bronze_state, status, publication_state, retryable,
                    persisted, attempts, failed_attempts, escalated,
                    input_tokens, cached_input_tokens, uncached_input_tokens,
                    output_tokens, reasoning_tokens, total_tokens, failure, audit
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                ON CONFLICT (client_id, batch_run_id, item_index) DO UPDATE SET
                    bronze_result_path = EXCLUDED.bronze_result_path,
                    bronze_record_hash = EXCLUDED.bronze_record_hash,
                    file_hash = EXCLUDED.file_hash,
                    workflow_run_id = EXCLUDED.workflow_run_id,
                    bronze_state = EXCLUDED.bronze_state,
                    status = EXCLUDED.status,
                    publication_state = EXCLUDED.publication_state,
                    retryable = EXCLUDED.retryable,
                    persisted = EXCLUDED.persisted,
                    attempts = EXCLUDED.attempts,
                    failed_attempts = EXCLUDED.failed_attempts,
                    escalated = EXCLUDED.escalated,
                    input_tokens = EXCLUDED.input_tokens,
                    cached_input_tokens = EXCLUDED.cached_input_tokens,
                    uncached_input_tokens = EXCLUDED.uncached_input_tokens,
                    output_tokens = EXCLUDED.output_tokens,
                    reasoning_tokens = EXCLUDED.reasoning_tokens,
                    total_tokens = EXCLUDED.total_tokens,
                    failure = EXCLUDED.failure,
                    audit = EXCLUDED.audit,
                    updated_at = now()
                """,
                (
                    tenant_id,
                    batch_id,
                    selected_indexes[result_path],
                    result_path,
                    _optional_text(item.get("bronze_record_hash")),
                    _optional_text(item.get("file_hash")),
                    workflow_id,
                    _optional_text(item.get("bronze_state")),
                    _required_text(item, "status"),
                    _optional_text(item.get("publication_state")),
                    _required_bool(item, "retryable"),
                    persisted,
                    _nonnegative_int(item, "attempts"),
                    _nonnegative_int(item, "failed_attempts"),
                    _required_bool(item, "escalated"),
                    _nonnegative_int(tokens, "input_tokens"),
                    _nonnegative_int(tokens, "cached_input_tokens"),
                    _nonnegative_int(tokens, "uncached_input_tokens"),
                    _nonnegative_int(tokens, "output_tokens"),
                    _nonnegative_int(tokens, "reasoning_tokens"),
                    _nonnegative_int(tokens, "total_tokens"),
                    Jsonb(failure) if failure is not None else None,
                    Jsonb(item),
                ),
            )

    return PersistedBatchRun(tenant_id, batch_id, status, len(items))


def _persist_agent_run(
    connection: Connection,
    client_id: UUID,
    bronze_run_id: UUID,
    agent_run: dict[str, object],
) -> UUID:
    run_id = UUID(_required_text(agent_run, "run_id"))
    attempts = agent_run.get("attempts")
    token_usage = agent_run.get("token_usage")
    human_findings = agent_run.get("human_review_findings")
    if not isinstance(attempts, list) or not isinstance(token_usage, dict):
        raise ValueError("Agent run attempts and token_usage must be present.")
    if not isinstance(human_findings, list):
        raise ValueError("Agent human_review_findings must be a list.")
    status = str(agent_run.get("status", "SUCCEEDED"))
    if status not in {"SUCCEEDED", "FAILED"}:
        raise ValueError("Agent run status must be SUCCEEDED or FAILED.")
    route = None
    if status == "SUCCEEDED":
        route = "HUMAN" if human_findings else "AUTOMATIC"
    inserted = connection.execute(
        """
        INSERT INTO analystops.agent_runs (
            client_id, bronze_run_id, run_id, prompt_version, schema_version,
            policy_version, status, route, latency_ms, input_tokens,
            cached_input_tokens, output_tokens, reasoning_tokens, result,
            started_at, completed_at
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s
        )
        ON CONFLICT (client_id, run_id) DO NOTHING
        RETURNING id
        """,
        (
            client_id,
            bronze_run_id,
            run_id,
            _required_text(agent_run, "prompt_version"),
            _required_text(agent_run, "schema_version"),
            _required_text(agent_run, "policy_version"),
            status,
            route,
            _nonnegative_number(agent_run, "latency_ms"),
            _nonnegative_int(token_usage, "input_tokens"),
            _nonnegative_int(token_usage, "cached_input_tokens"),
            _nonnegative_int(token_usage, "output_tokens"),
            _nonnegative_int(token_usage, "reasoning_tokens"),
            Jsonb(agent_run),
            _required_text(agent_run, "started_at"),
            _required_text(agent_run, "completed_at"),
        ),
    ).fetchone()
    if inserted is None:
        return connection.execute(
            """
            SELECT id FROM analystops.agent_runs
            WHERE client_id = %s AND run_id = %s
            """,
            (client_id, run_id),
        ).fetchone()[0]

    agent_run_id = inserted[0]
    for attempt in attempts:
        if not isinstance(attempt, dict):
            raise ValueError("Each agent attempt must be an object.")
        connection.execute(
            """
            INSERT INTO analystops.agent_attempts (
                client_id, agent_run_id, attempt_number, model, status,
                latency_ms, input_tokens, cached_input_tokens, output_tokens,
                reasoning_tokens, response_id, error
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                client_id,
                agent_run_id,
                _positive_int(attempt, "attempt_number"),
                _required_text(attempt, "model"),
                _required_text(attempt, "status"),
                _nonnegative_number(attempt, "latency_ms"),
                _nonnegative_int(attempt, "input_tokens"),
                _nonnegative_int(attempt, "cached_input_tokens"),
                _nonnegative_int(attempt, "output_tokens"),
                _nonnegative_int(attempt, "reasoning_tokens"),
                _optional_text(attempt.get("response_id")),
                _optional_text(attempt.get("error")),
            ),
        )
    return agent_run_id


def _persist_plan(
    connection: Connection,
    client_id: UUID,
    bronze_run_id: UUID,
    document: dict[str, object],
    plan: TransformationPlan,
    report: dict[str, object],
    bronze: dict[str, object],
) -> UUID:
    plan_hash = transformation_plan_hash(plan)
    inserted = connection.execute(
        """
        INSERT INTO analystops.transformation_plans (
            client_id, bronze_run_id, plan_version, plan_hash, plan,
            approval_status, approved_by, approved_at
        ) VALUES (%s, %s, %s, %s, %s, 'APPROVED', %s, %s)
        ON CONFLICT (client_id, plan_hash) DO NOTHING
        RETURNING id
        """,
        (
            client_id,
            bronze_run_id,
            _required_text(document, "plan_version"),
            plan_hash,
            Jsonb(document),
            _reviewed_by(bronze),
            _required_text(report, "completed_at"),
        ),
    ).fetchone()
    if inserted:
        return inserted[0]
    return connection.execute(
        """
        SELECT id FROM analystops.transformation_plans
        WHERE client_id = %s AND plan_hash = %s
        """,
        (client_id, plan_hash),
    ).fetchone()[0]


def _persist_silver_run(
    connection: Connection,
    client_id: UUID,
    bronze_run_id: UUID,
    plan_id: UUID | None,
    silver: dict[str, object],
    report: dict[str, object],
) -> UUID:
    return connection.execute(
        """
        INSERT INTO analystops.silver_runs (
            client_id, bronze_run_id, transformation_plan_id,
            transformation_version, status, input_rows, accepted_rows,
            rejected_rows, dropped_rows, accepted_artifact_uri,
            rejected_artifact_uri, completed_at
        ) VALUES (%s, %s, %s, %s, 'SUCCEEDED', %s, %s, %s, %s, %s, %s, %s)
        RETURNING id
        """,
        (
            client_id,
            bronze_run_id,
            plan_id,
            _required_text(silver, "transformation_version"),
            _nonnegative_int(silver, "input_rows"),
            _nonnegative_int(silver, "accepted_rows"),
            _nonnegative_int(silver, "rejected_rows"),
            _nonnegative_int(silver, "dropped_rows"),
            Path(_required_text(silver, "accepted_path")).resolve().as_uri(),
            Path(_required_text(silver, "rejected_path")).resolve().as_uri(),
            _required_text(report, "completed_at"),
        ),
    ).fetchone()[0]


def _write_findings(
    connection: Connection,
    client_id: UUID,
    bronze_run_id: UUID,
    findings: list[object],
) -> tuple[int, int]:
    resolution_count = 0
    for finding in findings:
        if not isinstance(finding, dict):
            raise ValueError("Each Bronze finding must be an object.")
        evidence = dict(finding)
        code = _required_text(evidence, "code")
        disposition = _required_text(evidence, "quality_disposition")
        resolution = evidence.pop("review_resolution", None)
        evidence.pop("code")
        evidence.pop("quality_disposition")
        finding_id = connection.execute(
            """
            INSERT INTO analystops.findings (
                client_id, bronze_run_id, code, quality_disposition, evidence
            ) VALUES (%s, %s, %s, %s, %s)
            RETURNING id
            """,
            (client_id, bronze_run_id, code, disposition, Jsonb(evidence)),
        ).fetchone()[0]
        if resolution is None:
            continue
        if not isinstance(resolution, dict):
            raise ValueError("Bronze review resolution must be an object.")
        details = resolution.get("details", {})
        if not isinstance(details, dict):
            raise ValueError("Bronze review resolution details must be an object.")
        connection.execute(
            """
            INSERT INTO analystops.review_resolutions (
                client_id, finding_id, action, details, note,
                reviewed_by, reviewed_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (
                client_id,
                finding_id,
                _required_text(resolution, "action"),
                Jsonb(details),
                str(resolution.get("note", "")),
                _required_text(resolution, "reviewed_by"),
                _required_text(resolution, "reviewed_at"),
            ),
        )
        resolution_count += 1
    return len(findings), resolution_count


def _read_document(path: Path | str) -> dict[str, object]:
    try:
        document = json.loads(Path(path).read_text())
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"Cannot read JSON artifact {path}: {exc}") from exc
    if not isinstance(document, dict):
        raise ValueError(f"JSON artifact {path} must be an object.")
    return document


def _optional_path(value: object) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        raise ValueError("Workflow artifact paths must be non-empty strings or null.")
    return value


def _optional_document(value: object) -> dict[str, object] | None:
    path = _optional_path(value)
    return _read_document(path) if path else None


def _nonnegative_int(payload: dict[str, object], key: str) -> int:
    value = payload.get(key)
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise ValueError(f"{key} must be a non-negative integer.")
    return value


def _positive_int(payload: dict[str, object], key: str) -> int:
    value = _nonnegative_int(payload, key)
    if value == 0:
        raise ValueError(f"{key} must be positive.")
    return value


def _nonnegative_number(payload: dict[str, object], key: str) -> float:
    value = payload.get(key)
    if (
        not isinstance(value, (int, float))
        or isinstance(value, bool)
        or value < 0
    ):
        raise ValueError(f"{key} must be a non-negative number.")
    return float(value)


def _required_bool(payload: dict[str, object], key: str) -> bool:
    value = payload.get(key)
    if not isinstance(value, bool):
        raise ValueError(f"{key} must be a boolean.")
    return value


def _optional_text(value: object) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError("Optional text values must be strings or null.")
    return value or None


def _reviewed_by(bronze: dict[str, object]) -> str:
    findings = bronze.get("findings")
    if isinstance(findings, list):
        for finding in findings:
            if not isinstance(finding, dict):
                continue
            resolution = finding.get("review_resolution")
            if isinstance(resolution, dict):
                reviewer = resolution.get("reviewed_by")
                if isinstance(reviewer, str) and reviewer.strip():
                    return reviewer
    raise ValueError("Transformation plan has no Bronze reviewer identity.")


def _required_text(payload: dict[str, object], key: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{key} must be a non-empty string.")
    return value


def _result_paths(inputs: list[Path]) -> list[Path]:
    paths = []
    for value in inputs:
        paths.extend(sorted(value.rglob("*.json")) if value.is_dir() else [value])
    return paths


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Persist authoritative Bronze results in PostgreSQL."
    )
    parser.add_argument("bronze_results", type=Path, nargs="+")
    parser.add_argument("--client-id", required=True, type=UUID)
    parser.add_argument("--client-name", required=True)
    parser.add_argument(
        "--database-url",
        default=os.environ.get("DATABASE_URL", DEFAULT_DATABASE_URL),
    )
    args = parser.parse_args(argv)

    paths = _result_paths(args.bronze_results)
    if not paths:
        parser.error("No Bronze result files found")
    inserted = 0
    findings = 0
    resolutions = 0
    with psycopg.connect(args.database_url, autocommit=True) as connection:
        for path in paths:
            result = persist_bronze_result(
                connection,
                path,
                client_id=args.client_id,
                client_name=args.client_name,
            )
            inserted += int(result.inserted)
            findings += result.findings_written
            resolutions += result.resolutions_written
    print(
        json.dumps(
            {
                "client_id": str(args.client_id),
                "records": len(paths),
                "inserted_runs": inserted,
                "existing_runs": len(paths) - inserted,
                "findings_written": findings,
                "resolutions_written": resolutions,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
