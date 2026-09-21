"""Coordinate Bronze remediation through validated Silver output."""

from __future__ import annotations

import argparse
import json
import os
from collections.abc import Mapping
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

from analystops.agents.bronze_remediation import (
    ESCALATION_MODEL,
    MAX_OUTPUT_TOKENS,
    PRIMARY_ATTEMPTS,
    PRIMARY_MODEL,
    PROMPT_VERSION,
    PROPOSAL_SCHEMA_VERSION,
    AgentProposal,
    AgentProposalError,
    propose_resolutions,
)
from analystops.ingestion.review import ReviewResolutionError, reassess_workbook
from analystops.ingestion.validate import IntakeResult, read_result, write_result
from analystops.transformations.operations import create_transformation_plan
from analystops.transformations.silver import canonicalize
from analystops.validation.silver import validate_silver_result


PROJECT_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_WORKFLOW_DIR = PROJECT_ROOT / "data" / "workflows" / "bronze-to-silver"
DEFAULT_SILVER_DIR = PROJECT_ROOT / "data" / "silver"
DEFAULT_VALIDATION_DIR = PROJECT_ROOT / "data" / "validation" / "silver"
WORKFLOW_VERSION = "bronze-to-silver-v2"


class BronzeToSilverWorkflowError(RuntimeError):
    """Raised when workflow coordination cannot safely continue."""

    def __init__(
        self,
        message: str,
        *,
        report_path: Path | None = None,
        failed_stage: str | None = None,
        retryable: bool = False,
    ):
        super().__init__(message)
        self.report_path = report_path
        self.failed_stage = failed_stage
        self.retryable = retryable


def run_bronze_to_silver(
    bronze_result_path: Path | str,
    *,
    client: Any | None = None,
    human_resolution: Path | str | Mapping[str, Any] | None = None,
    output_dir: Path | str = DEFAULT_WORKFLOW_DIR,
    silver_dir: Path | str = DEFAULT_SILVER_DIR,
    validation_dir: Path | str = DEFAULT_VALIDATION_DIR,
    primary_model: str = PRIMARY_MODEL,
    escalation_model: str | None = ESCALATION_MODEL,
    primary_attempts: int = PRIMARY_ATTEMPTS,
    max_output_tokens: int = MAX_OUTPUT_TOKENS,
) -> Path:
    """Run one workbook until Silver validation or a human-review boundary."""

    run_id = str(uuid4())
    run_dir = Path(output_dir) / run_id
    report_path = run_dir / "workflow.json"
    report: dict[str, object] = {
        "workflow_version": WORKFLOW_VERSION,
        "workflow_run_id": run_id,
        "started_at": datetime.now(UTC).isoformat(),
        "completed_at": None,
        "status": "RUNNING",
        "current_stage": "INITIALIZATION",
        "failed_stage": None,
        "failure": None,
        "file_hash": None,
        "initial_bronze_record_path": str(Path(bronze_result_path).resolve()),
        "agent_run_path": None,
        "agent_proposal_path": None,
        "review_request_path": None,
        "resolved_bronze_record_path": None,
        "transformation_plan_path": None,
        "silver_result_path": None,
        "validation_profile_path": None,
        "publication_state": None,
    }
    bronze: dict[str, object] | None = None
    try:
        if client is not None and human_resolution is not None:
            raise ValueError(
                "Provide either an agent client or a human resolution, not both."
            )
        report["current_stage"] = "BRONZE_LOAD"
        bronze = read_result(bronze_result_path)
        report["file_hash"] = bronze.get("file_hash")
        return _run_loaded_bronze(
            bronze,
            bronze_result_path,
            client=client,
            human_resolution=human_resolution,
            report=report,
            report_path=report_path,
            run_dir=run_dir,
            silver_dir=silver_dir,
            validation_dir=validation_dir,
            primary_model=primary_model,
            escalation_model=escalation_model,
            primary_attempts=primary_attempts,
            max_output_tokens=max_output_tokens,
        )
    except Exception as exc:
        stage = str(report["current_stage"])
        failure = _failure_document(exc, stage)
        if isinstance(exc, AgentProposalError) and bronze is not None and exc.run_id:
            agent_path = run_dir / "agent-failure.json"
            _write_json(
                agent_path,
                _agent_failure_document(exc, bronze, failure, report),
            )
            report["agent_run_path"] = str(agent_path.resolve())
        report["status"] = "FAILED"
        report["failed_stage"] = stage
        report["failure"] = failure
        failed_path = _finish(report_path, report)
        raise BronzeToSilverWorkflowError(
            str(exc),
            report_path=failed_path,
            failed_stage=stage,
            retryable=bool(failure["retryable"]),
        ) from exc


def _run_loaded_bronze(
    bronze: dict[str, object],
    bronze_result_path: Path | str,
    *,
    client: Any | None,
    human_resolution: Path | str | Mapping[str, Any] | None,
    report: dict[str, object],
    report_path: Path,
    run_dir: Path,
    silver_dir: Path | str,
    validation_dir: Path | str,
    primary_model: str,
    escalation_model: str | None,
    primary_attempts: int,
    max_output_tokens: int,
) -> Path:
    report["current_stage"] = "BRONZE_ROUTING"
    state = bronze.get("lifecycle_state")
    reassessed: IntakeResult | None = None
    if state == "AWAITING_REVIEW":
        if human_resolution is not None:
            report["current_stage"] = "BRONZE_REASSESSMENT"
            reassessed = _reassess(bronze, human_resolution)
        else:
            if client is None:
                raise BronzeToSilverWorkflowError(
                    "AWAITING_REVIEW requires an agent client or human resolution."
                )
            report["current_stage"] = "AGENT_REMEDIATION"
            proposal = propose_resolutions(
                bronze,
                client,
                primary_model=primary_model,
                escalation_model=escalation_model,
                primary_attempts=primary_attempts,
                max_output_tokens=max_output_tokens,
            )
            proposal_path = run_dir / "agent-proposal.json"
            _write_json(proposal_path, proposal.to_dict())
            report["agent_run_path"] = str(proposal_path.resolve())
            report["agent_proposal_path"] = str(proposal_path.resolve())
            if proposal.human_review_findings:
                review_path = run_dir / "human-review.json"
                _write_json(review_path, _human_review_document(proposal))
                report["review_request_path"] = str(review_path.resolve())
                report["status"] = "AWAITING_HUMAN_REVIEW"
                report["current_stage"] = "HUMAN_REVIEW"
                return _finish(report_path, report)
            report["current_stage"] = "BRONZE_REASSESSMENT"
            reassessed = _reassess(
                bronze, _automatic_resolution_document(proposal)
            )
    elif state != "BRONZE_ACCEPTED":
        raise BronzeToSilverWorkflowError(
            f"Workflow cannot process Bronze state {state!r}."
        )

    report["current_stage"] = "BRONZE_REASSESSMENT"
    resolved_path = (
        write_result(reassessed, run_dir / "bronze")
        if reassessed is not None
        else Path(bronze_result_path)
    )
    report["resolved_bronze_record_path"] = str(resolved_path.resolve())
    resolved = read_result(resolved_path)
    if resolved.get("lifecycle_state") != "BRONZE_ACCEPTED":
        report["status"] = "AWAITING_HUMAN_REVIEW"
        report["current_stage"] = "HUMAN_REVIEW"
        return _finish(report_path, report)

    report["current_stage"] = "TRANSFORMATION_PLANNING"
    plan = create_transformation_plan(resolved)
    if plan is not None:
        plan_path = run_dir / "transformation-plan.json"
        _write_json(plan_path, plan)
        report["transformation_plan_path"] = str(plan_path.resolve())

    report["current_stage"] = "SILVER_CANONICALIZATION"
    silver_result = canonicalize(
        resolved_path,
        output_dir=silver_dir,
        plan=plan,
    )
    report["silver_result_path"] = str(silver_result.resolve())
    report["current_stage"] = "SILVER_VALIDATION"
    profile_path = validate_silver_result(
        resolved_path,
        silver_dir=silver_dir,
        output_dir=validation_dir,
    )
    report["validation_profile_path"] = str(profile_path.resolve())
    profile = json.loads(profile_path.read_text())
    publication_state = profile.get("publication_state")
    report["publication_state"] = publication_state
    report["status"] = (
        "SILVER_PUBLISHABLE"
        if publication_state in {"PUBLISHABLE", "PUBLISHABLE_WITH_WARNINGS"}
        else "SILVER_REVIEW_REQUIRED"
    )
    report["current_stage"] = "COMPLETE"
    return _finish(report_path, report)


def _failure_document(exc: Exception, stage: str) -> dict[str, object]:
    root = exc.__cause__ if exc.__cause__ is not None else exc
    codes = {
        "BRONZE_LOAD": "BRONZE_RECORD_ERROR",
        "BRONZE_ROUTING": "WORKFLOW_INPUT_ERROR",
        "BRONZE_REASSESSMENT": "REVIEW_RESOLUTION_ERROR",
        "TRANSFORMATION_PLANNING": "TRANSFORMATION_PLAN_ERROR",
        "SILVER_CANONICALIZATION": "SILVER_CANONICALIZATION_ERROR",
        "SILVER_VALIDATION": "SILVER_VALIDATION_ERROR",
    }
    code = codes.get(stage, "WORKFLOW_ERROR")
    retryable = False
    if isinstance(exc, AgentProposalError):
        code = "AGENT_RETRY_EXHAUSTED" if exc.attempts else "AGENT_INPUT_ERROR"
        errors = [attempt.error or "" for attempt in exc.attempts]
        transient = (
            "APIConnectionError:",
            "APITimeoutError:",
            "RateLimitError:",
            "InternalServerError:",
            "ConnectionError:",
            "TimeoutError:",
        )
        retryable = bool(errors) and all(
            error.startswith(transient) for error in errors
        )
    return {
        "code": code,
        "error_type": type(root).__name__,
        "message": str(exc)[:2000],
        "retryable": retryable,
    }


def _agent_failure_document(
    exc: AgentProposalError,
    bronze: Mapping[str, object],
    failure: Mapping[str, object],
    report: Mapping[str, object],
) -> dict[str, object]:
    attempts = [asdict(attempt) for attempt in exc.attempts]
    input_tokens = sum(attempt.input_tokens for attempt in exc.attempts)
    cached_tokens = sum(attempt.cached_input_tokens for attempt in exc.attempts)
    output_tokens = sum(attempt.output_tokens for attempt in exc.attempts)
    return {
        "run_id": exc.run_id,
        "prompt_version": PROMPT_VERSION,
        "schema_version": PROPOSAL_SCHEMA_VERSION,
        "policy_version": bronze.get("policy_version"),
        "started_at": exc.started_at or report["started_at"],
        "completed_at": datetime.now(UTC).isoformat(),
        "status": "FAILED",
        "route": None,
        "file_hash": bronze.get("file_hash"),
        "bronze_record_hash": bronze.get("record_hash"),
        "resolutions": [],
        "automatic_findings": [],
        "human_review_findings": [],
        "attempts": attempts,
        "latency_ms": round(
            sum(attempt.latency_ms for attempt in exc.attempts), 3
        ),
        "token_usage": {
            "input_tokens": input_tokens,
            "cached_input_tokens": cached_tokens,
            "uncached_input_tokens": input_tokens - cached_tokens,
            "output_tokens": output_tokens,
            "reasoning_tokens": sum(
                attempt.reasoning_tokens for attempt in exc.attempts
            ),
            "total_tokens": input_tokens + output_tokens,
        },
        "failure": dict(failure),
    }


def _reassess(
    bronze: Mapping[str, object],
    resolution: Path | str | Mapping[str, Any],
) -> IntakeResult:
    workbook = bronze.get("file_path")
    if not isinstance(workbook, str) or not workbook:
        raise BronzeToSilverWorkflowError("Bronze file_path must be non-empty.")
    try:
        return reassess_workbook(
            workbook,
            resolution,
            baseline_row_count=_baseline_row_count(bronze),
        )
    except (ReviewResolutionError, ValueError) as exc:
        raise BronzeToSilverWorkflowError(str(exc)) from exc


def _automatic_resolution_document(proposal: AgentProposal) -> dict[str, object]:
    return {
        "file_hash": proposal.file_hash,
        "reviewed_by": f"agent:{proposal.attempts[-1].model}",
        "reviewed_at": datetime.now(UTC).isoformat(),
        "resolutions": _proposed_resolutions(proposal),
    }


def _human_review_document(proposal: AgentProposal) -> dict[str, object]:
    by_code = {
        str(item["finding_code"]): item for item in proposal.resolutions
    }
    return {
        "file_hash": proposal.file_hash,
        "reviewed_by": "",
        "reviewed_at": "",
        "resolutions": _proposed_resolutions(proposal),
        "agent_run_id": proposal.run_id,
        "human_review_findings": [
            {
                "finding_code": code,
                "agent_decision": by_code[code]["decision"],
                "proposed_action": by_code[code]["action"],
                "proposed_details": by_code[code]["details"],
                "defer_reason": by_code[code]["defer_reason"],
            }
            for code in proposal.human_review_findings
        ],
    }


def _proposed_resolutions(proposal: AgentProposal) -> list[dict[str, object]]:
    return [
        {
            "finding_code": item["finding_code"],
            "action": item["action"],
            "details": item["details"],
            "note": f"Agent proposal {proposal.run_id}",
        }
        for item in proposal.resolutions
        if item["decision"] == "PROPOSE"
    ]


def _baseline_row_count(bronze: Mapping[str, object]) -> int | None:
    findings = bronze.get("findings", [])
    if not isinstance(findings, list):
        return None
    for finding in findings:
        if isinstance(finding, dict):
            baseline = finding.get("baseline_row_count")
            if isinstance(baseline, int) and not isinstance(baseline, bool):
                return baseline
    return None


def _finish(path: Path, report: dict[str, object]) -> Path:
    report["completed_at"] = datetime.now(UTC).isoformat()
    _write_json(path, report)
    return path


def _write_json(path: Path, payload: Mapping[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    temporary.replace(path)


def _persist_report(
    path: Path,
    *,
    client_id: UUID | None,
    client_name: str | None,
    database_url: str | None,
) -> None:
    if client_id is None:
        return
    import psycopg

    from analystops.persistence.postgres import (
        DEFAULT_DATABASE_URL,
        persist_workflow_result,
    )

    url = database_url or os.environ.get("DATABASE_URL") or DEFAULT_DATABASE_URL
    with psycopg.connect(url, autocommit=True) as connection:
        persist_workflow_result(
            connection,
            path,
            client_id=client_id,
            client_name=client_name or "",
        )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Run one Bronze workbook through remediation and Silver validation."
    )
    parser.add_argument("bronze_result", type=Path)
    parser.add_argument("--resolution", type=Path)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_WORKFLOW_DIR)
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
    if bool(args.client_id) != bool(args.client_name):
        parser.error("--client-id and --client-name must be provided together")

    try:
        bronze = read_result(args.bronze_result)
    except ValueError as exc:
        parser.exit(1, f"workflow failed: {exc}\n")
    client = None
    if bronze.get("lifecycle_state") == "AWAITING_REVIEW" and not args.resolution:
        if not os.environ.get("OPENAI_API_KEY"):
            parser.error(
                "OPENAI_API_KEY is not exported; run "
                "`set -a; source .env; set +a` first"
            )
        from openai import OpenAI

        client = OpenAI()
    try:
        result = run_bronze_to_silver(
            args.bronze_result,
            client=client,
            human_resolution=args.resolution,
            output_dir=args.output_dir,
            silver_dir=args.silver_dir,
            validation_dir=args.validation_dir,
            primary_model=args.primary_model,
            escalation_model=args.escalation_model,
            primary_attempts=args.primary_attempts,
            max_output_tokens=args.max_output_tokens,
        )
    except BronzeToSilverWorkflowError as exc:
        if exc.report_path is not None:
            try:
                _persist_report(
                    exc.report_path,
                    client_id=args.client_id,
                    client_name=args.client_name,
                    database_url=args.database_url,
                )
            except Exception as persistence_error:
                parser.exit(
                    1,
                    f"workflow failed: {exc}; audit: {exc.report_path}; "
                    f"database persistence failed: {persistence_error}\n",
                )
        parser.exit(1, f"workflow failed: {exc}; audit: {exc.report_path}\n")
    _persist_report(
        result,
        client_id=args.client_id,
        client_name=args.client_name,
        database_url=args.database_url,
    )
    print(result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
