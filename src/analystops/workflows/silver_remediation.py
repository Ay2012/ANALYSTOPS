"""Execute approved Silver remediation through the trusted pipeline."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import pandas as pd

from analystops.agents.silver_remediation import PROPOSAL_SCHEMA_VERSION
from analystops.ingestion.review import reassess_workbook
from analystops.ingestion.validate import (
    _file_hash,
    read_result,
    validate_workbook,
    write_result,
)
from analystops.transformations.operations import create_transformation_plan
from analystops.transformations.silver import canonicalize
from analystops.validation.silver import validate_silver_result


PROJECT_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_REMEDIATION_DIR = PROJECT_ROOT / "data" / "silver" / "remediation"
EXECUTION_VERSION = "silver-remediation-execution-v1"
EXECUTABLE_ACTIONS = {"repartition_by_country_month"}


class SilverRemediationError(RuntimeError):
    """Raised when an approved Silver remediation cannot execute safely."""

    def __init__(self, message: str, *, audit_path: Path | None = None):
        super().__init__(message)
        self.audit_path = audit_path


def execute_silver_remediation(
    profile: Mapping[str, object],
    proposal: Mapping[str, object],
    approval: Mapping[str, object],
    *,
    output_dir: Path | str = DEFAULT_REMEDIATION_DIR,
) -> Path:
    """Execute one hash-bound approval and revalidate every child partition."""

    execution_id = str(uuid4())
    run_dir = Path(output_dir) / execution_id
    audit_path = run_dir / "execution.json"
    audit: dict[str, object] = {
        "execution_version": EXECUTION_VERSION,
        "execution_id": execution_id,
        "started_at": datetime.now(UTC).isoformat(),
        "completed_at": None,
        "status": "RUNNING",
        "current_stage": "APPROVAL_VALIDATION",
        "failure": None,
        "source_file_id": profile.get("source_file_id"),
        "validation_profile_hash": document_hash(profile),
        "proposal_run_id": proposal.get("run_id"),
        "proposal_hash": document_hash(proposal),
        "approval": dict(approval),
        "input_rows": profile.get("row_count"),
        "partition_rows": 0,
        "silver_rows": 0,
        "held_rows": 0,
        "dropped_rows": 0,
        "partitions": [],
    }
    try:
        _validate_approval(profile, proposal, approval)
        actions = {
            str(item["action"])
            for item in _resolutions(proposal)
            if item["decision"] == "PROPOSE"
        }
        unsupported = actions - EXECUTABLE_ACTIONS
        if unsupported:
            raise SilverRemediationError(
                "No deterministic executor exists for: "
                + ", ".join(sorted(unsupported))
            )
        if actions != {"repartition_by_country_month"}:
            raise SilverRemediationError(
                "Approval does not contain an executable repartition operation."
            )

        audit["current_stage"] = "SOURCE_VALIDATION"
        bronze_path = Path(_required_text(profile, "bronze_record_path"))
        bronze = read_result(bronze_path)
        if bronze.get("file_hash") != profile.get("source_file_id"):
            raise SilverRemediationError("Silver profile does not match its Bronze source.")
        workbook_path = Path(_required_text(bronze, "file_path"))
        if not workbook_path.is_absolute() and not workbook_path.exists():
            workbook_path = PROJECT_ROOT / workbook_path
        if not workbook_path.is_file() or _file_hash(workbook_path) != bronze.get(
            "file_hash"
        ):
            raise SilverRemediationError("Source workbook hash changed after Bronze.")
        selected_sheet = _required_text(bronze, "selected_sheet")
        frame = pd.read_excel(workbook_path, sheet_name=selected_sheet)
        if [str(column) for column in frame.columns] != bronze.get("observed_schema"):
            raise SilverRemediationError("Source workbook schema changed after Bronze.")
        if len(frame) != bronze.get("row_count") or len(frame) != profile.get("row_count"):
            raise SilverRemediationError("Source row count does not match approved evidence.")

        audit["current_stage"] = "REPARTITION"
        partitions = _partition_workbook(
            frame,
            run_dir / "submissions",
            source_file_id=str(profile["source_file_id"]),
            source_workbook=workbook_path,
            selected_sheet=selected_sheet,
            proposal=proposal,
            approval=approval,
        )
        audit["partition_rows"] = sum(item["row_count"] for item in partitions)
        if audit["partition_rows"] != audit["input_rows"]:
            raise SilverRemediationError("Partition rows do not reconcile to the input.")

        audit["current_stage"] = "PIPELINE_REENTRY"
        outputs = []
        silver_rows = 0
        held_rows = 0
        for partition in partitions:
            workbook = Path(str(partition["workbook_path"]))
            lineage = {
                "silver_remediation_execution_id": execution_id,
                "source_file_id": profile["source_file_id"],
                "validation_profile_hash": audit["validation_profile_hash"],
                "proposal_run_id": proposal["run_id"],
                "proposal_hash": audit["proposal_hash"],
                "lineage_manifest_path": partition["lineage_manifest_path"],
                "lineage_manifest_hash": partition["lineage_manifest_hash"],
            }
            bronze_result = validate_workbook(workbook)
            child_bronze_path = write_result(
                bronze_result, run_dir / "bronze", lineage=lineage
            )
            if bronze_result.lifecycle_state == "AWAITING_REVIEW":
                held_rows += int(partition["row_count"])
                outputs.append(
                    {
                        **partition,
                        "bronze_record_path": str(child_bronze_path.resolve()),
                        "bronze_state": bronze_result.lifecycle_state,
                        "review_findings": [
                            finding
                            for finding in bronze_result.findings
                            if finding.get("quality_disposition") == "REVIEW"
                        ],
                        "silver_result_path": None,
                        "validation_profile_path": None,
                        "publication_state": "REVIEW_REQUIRED",
                    }
                )
                continue
            if bronze_result.lifecycle_state != "BRONZE_ACCEPTED":
                raise SilverRemediationError(
                    f"Remediated child did not pass Bronze: {workbook} "
                    f"({bronze_result.lifecycle_state})"
                )
            silver_result_path = canonicalize(
                child_bronze_path, output_dir=run_dir / "silver"
            )
            child_profile_path = validate_silver_result(
                child_bronze_path,
                silver_dir=run_dir / "silver",
                output_dir=run_dir / "validation",
            )
            child_profile = _read_object(child_profile_path)
            publication_state = child_profile.get("publication_state")
            if publication_state not in {"PUBLISHABLE", "PUBLISHABLE_WITH_WARNINGS"}:
                raise SilverRemediationError(
                    f"Remediated child still requires review: {workbook}"
                )
            silver_rows += int(child_profile["row_count"])
            outputs.append(
                {
                    **partition,
                    "bronze_record_path": str(child_bronze_path.resolve()),
                    "silver_result_path": str(silver_result_path.resolve()),
                    "validation_profile_path": str(child_profile_path.resolve()),
                    "publication_state": publication_state,
                }
            )
        audit["silver_rows"] = silver_rows
        audit["held_rows"] = held_rows
        audit["partitions"] = outputs
        if silver_rows + held_rows != audit["input_rows"]:
            raise SilverRemediationError("Revalidated and held rows do not reconcile.")
        audit["status"] = (
            "REMEDIATION_REVIEW_REQUIRED"
            if held_rows
            else "READY_FOR_PUBLICATION"
        )
        audit["current_stage"] = "COMPLETE"
        return _finish(audit_path, audit)
    except Exception as exc:
        audit["status"] = "FAILED"
        audit["failure"] = {"type": type(exc).__name__, "message": str(exc)}
        failed_path = _finish(audit_path, audit)
        raise SilverRemediationError(str(exc), audit_path=failed_path) from exc


def resolve_held_partitions(
    execution: Mapping[str, object],
    decision: Mapping[str, object],
    *,
    output_dir: Path | str = DEFAULT_REMEDIATION_DIR,
) -> Path:
    """Apply one explicit duplicate policy to held child partitions."""

    execution_id = _required_text(execution, "execution_id")
    audit_path = Path(output_dir) / execution_id / "execution.json"
    current = _read_object(audit_path)
    if current.get("status") != "REMEDIATION_REVIEW_REQUIRED":
        raise SilverRemediationError("Execution is not awaiting follow-up review.")
    if document_hash(current) != document_hash(execution):
        raise SilverRemediationError("Checkpoint execution does not match its audit.")
    choice = decision.get("decision")
    if choice not in {"CONFIRM_VALID_DUPLICATES", "DEDUPLICATE", "REJECT"}:
        raise SilverRemediationError("Unsupported follow-up decision.")
    reviewer = _required_text(decision, "reviewed_by")
    reviewed_at = datetime.now(UTC).isoformat()
    if choice == "REJECT":
        current["status"] = "REMEDIATION_REJECTED"
        current["followup_decision"] = {
            "decision": choice,
            "reviewed_by": reviewer,
            "reviewed_at": reviewed_at,
        }
        return _finish(audit_path, current)

    action = (
        "confirm_valid_duplicates"
        if choice == "CONFIRM_VALID_DUPLICATES"
        else "deduplicate_in_silver"
    )
    partitions = current.get("partitions")
    if not isinstance(partitions, list):
        raise SilverRemediationError("Execution partitions are malformed.")
    updated = []
    for partition in partitions:
        if not isinstance(partition, dict):
            raise SilverRemediationError("Execution partition is malformed.")
        if partition.get("publication_state") != "REVIEW_REQUIRED":
            updated.append(partition)
            continue
        findings = partition.get("review_findings")
        codes = {
            str(item.get("code"))
            for item in findings
            if isinstance(item, dict)
        } if isinstance(findings, list) else set()
        if codes != {"exact_duplicate_rows"}:
            raise SilverRemediationError(
                "Follow-up duplicate policy cannot resolve other findings."
            )
        bronze = read_result(_required_text(partition, "bronze_record_path"))
        resolution = {
            "file_hash": bronze["file_hash"],
            "reviewed_by": reviewer,
            "reviewed_at": reviewed_at,
            "resolutions": [
                {
                    "finding_code": "exact_duplicate_rows",
                    "action": action,
                    "details": {},
                }
            ],
        }
        resolved = reassess_workbook(bronze["file_path"], resolution)
        if resolved.lifecycle_state != "BRONZE_ACCEPTED":
            raise SilverRemediationError("Duplicate resolution did not pass Bronze.")
        lineage = bronze.get("lineage")
        resolved_path = write_result(
            resolved,
            audit_path.parent / "bronze-resolved",
            lineage=lineage if isinstance(lineage, Mapping) else None,
        )
        plan = create_transformation_plan(read_result(resolved_path))
        silver_path = canonicalize(
            resolved_path,
            output_dir=audit_path.parent / "silver",
            plan=plan,
        )
        profile_path = validate_silver_result(
            resolved_path,
            silver_dir=audit_path.parent / "silver",
            output_dir=audit_path.parent / "validation",
        )
        child_profile = _read_object(profile_path)
        if child_profile.get("publication_state") not in {
            "PUBLISHABLE",
            "PUBLISHABLE_WITH_WARNINGS",
        }:
            raise SilverRemediationError("Resolved child is still not publishable.")
        updated.append(
            {
                **partition,
                "bronze_record_path": str(resolved_path.resolve()),
                "bronze_state": "BRONZE_ACCEPTED",
                "review_findings": [],
                "silver_result_path": str(silver_path.resolve()),
                "validation_profile_path": str(profile_path.resolve()),
                "publication_state": child_profile["publication_state"],
            }
        )

    silver_rows = 0
    dropped_rows = 0
    for partition in updated:
        silver_path = partition.get("silver_result_path")
        if not isinstance(silver_path, str):
            raise SilverRemediationError("A child remains unresolved.")
        result = _read_object(Path(silver_path))
        silver_rows += int(result["accepted_rows"])
        dropped_rows += int(result["dropped_rows"])
    if silver_rows + dropped_rows != current.get("input_rows"):
        raise SilverRemediationError("Final accepted and dropped rows do not reconcile.")
    current.update(
        {
            "partitions": updated,
            "silver_rows": silver_rows,
            "held_rows": 0,
            "dropped_rows": dropped_rows,
            "status": "READY_FOR_PUBLICATION",
            "current_stage": "COMPLETE",
            "followup_decision": {
                "decision": choice,
                "reviewed_by": reviewer,
                "reviewed_at": reviewed_at,
            },
        }
    )
    return _finish(audit_path, current)


def _validate_approval(
    profile: Mapping[str, object],
    proposal: Mapping[str, object],
    approval: Mapping[str, object],
) -> None:
    if profile.get("publication_state") != "REVIEW_REQUIRED":
        raise SilverRemediationError("Only REVIEW_REQUIRED profiles can be remediated.")
    if proposal.get("schema_version") != PROPOSAL_SCHEMA_VERSION:
        raise SilverRemediationError("Unsupported Silver proposal schema version.")
    if proposal.get("source_file_id") != profile.get("source_file_id"):
        raise SilverRemediationError("Proposal source does not match the profile.")
    profile_hash = document_hash(profile)
    if proposal.get("validation_profile_hash") != profile_hash:
        raise SilverRemediationError("Proposal is not bound to this validation profile.")
    expected_approval = {
        "decision",
        "reviewed_by",
        "reviewed_at",
        "proposal_run_id",
        "proposal_hash",
        "validation_profile_hash",
    }
    if set(approval) != expected_approval:
        raise SilverRemediationError("Approval document has an invalid shape.")
    if approval.get("decision") != "APPROVE":
        raise SilverRemediationError("Remediation requires explicit APPROVE.")
    if approval.get("proposal_run_id") != proposal.get("run_id") or approval.get(
        "proposal_hash"
    ) != document_hash(proposal):
        raise SilverRemediationError("Approval is not bound to this proposal.")
    if approval.get("validation_profile_hash") != profile_hash:
        raise SilverRemediationError("Approval is not bound to this profile.")
    _required_text(approval, "reviewed_by")
    reviewed_at = _required_text(approval, "reviewed_at")
    try:
        parsed = datetime.fromisoformat(reviewed_at.replace("Z", "+00:00"))
    except ValueError as exc:
        raise SilverRemediationError("Approval reviewed_at is invalid.") from exc
    if parsed.tzinfo is None:
        raise SilverRemediationError("Approval reviewed_at requires a timezone.")
    if any(item["decision"] != "PROPOSE" for item in _resolutions(proposal)):
        raise SilverRemediationError("Deferred findings cannot be executed.")


def _partition_workbook(
    frame: pd.DataFrame,
    output_dir: Path,
    *,
    source_file_id: str,
    source_workbook: Path,
    selected_sheet: str,
    proposal: Mapping[str, object],
    approval: Mapping[str, object],
) -> list[dict[str, object]]:
    for column in ("InvoiceDate", "Country"):
        if column not in frame:
            raise SilverRemediationError(f"Source workbook is missing {column}.")
    dates = pd.to_datetime(frame["InvoiceDate"], errors="coerce")
    countries = frame["Country"].astype("string").str.strip()
    if dates.isna().any() or countries.isna().any() or (countries == "").any():
        raise SilverRemediationError(
            "Cannot repartition rows with invalid dates or countries."
        )
    partitioned = frame.assign(
        _reporting_month=dates.dt.strftime("%Y-%m"), _country=countries
    )
    outputs = []
    for (month, country), group in partitioned.groupby(
        ["_reporting_month", "_country"], sort=True
    ):
        period_dir = output_dir / str(month)
        workbook_path = period_dir / f"{_slug(country)}_{month}.xlsx"
        lineage_path = workbook_path.with_suffix(".lineage.json")
        if workbook_path.exists() or lineage_path.exists():
            raise SilverRemediationError(f"Refusing to overwrite {workbook_path}.")
        period_dir.mkdir(parents=True, exist_ok=True)
        source_rows = [int(index) + 2 for index in group.index]
        lineage = {
            "source_file_id": source_file_id,
            "source_workbook_path": str(source_workbook.resolve()),
            "source_sheet": selected_sheet,
            "source_row_numbers": source_rows,
            "country": str(country),
            "reporting_month": str(month),
            "row_count": len(group),
            "proposal_run_id": proposal["run_id"],
            "proposal_hash": document_hash(proposal),
            "approval_hash": document_hash(approval),
        }
        lineage_hash = document_hash(lineage)
        _write_json(lineage_path, {**lineage, "lineage_hash": lineage_hash})
        temporary = workbook_path.with_suffix(".xlsx.tmp")
        with pd.ExcelWriter(temporary, engine="openpyxl") as writer:
            group.loc[:, frame.columns].to_excel(
                writer, sheet_name="Transactions", index=False
            )
        temporary.replace(workbook_path)
        outputs.append(
            {
                "country": str(country),
                "reporting_month": str(month),
                "row_count": len(group),
                "workbook_path": str(workbook_path.resolve()),
                "lineage_manifest_path": str(lineage_path.resolve()),
                "lineage_manifest_hash": lineage_hash,
            }
        )
    if not outputs:
        raise SilverRemediationError("Repartitioning produced no child submissions.")
    return outputs


def _resolutions(proposal: Mapping[str, object]) -> list[dict[str, object]]:
    values = proposal.get("resolutions")
    if not isinstance(values, (list, tuple)) or not values:
        raise SilverRemediationError("Proposal has no resolutions.")
    if not all(isinstance(item, dict) for item in values):
        raise SilverRemediationError("Proposal resolutions must be objects.")
    return list(values)


def _required_text(value: Mapping[str, object], key: str) -> str:
    text = value.get(key)
    if not isinstance(text, str) or not text.strip():
        raise SilverRemediationError(f"{key} must be a non-empty string.")
    return text


def _slug(value: object) -> str:
    text = re.sub(r"[^a-z0-9]+", "_", str(value).strip().lower()).strip("_")
    if not text:
        raise SilverRemediationError("Country cannot produce an empty file name.")
    return text


def document_hash(value: Mapping[str, object]) -> str:
    encoded = json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=True
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


def _read_object(path: Path) -> dict[str, object]:
    document = json.loads(path.read_text())
    if not isinstance(document, dict):
        raise SilverRemediationError(f"Expected a JSON object at {path}.")
    return document


def _write_json(path: Path, value: Mapping[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")
    temporary.replace(path)


def _finish(path: Path, audit: dict[str, object]) -> Path:
    audit["completed_at"] = datetime.now(UTC).isoformat()
    _write_json(path, audit)
    return path
