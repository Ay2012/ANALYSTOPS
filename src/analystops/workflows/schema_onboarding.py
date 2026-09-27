"""Approve schema proposals, adapt staging workbooks, and re-enter Bronze."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

import pandas as pd

from analystops.agents.schema_onboarding import PROPOSAL_SCHEMA_VERSION
from analystops.ingestion.validate import (
    EXPECTED_COLUMNS,
    REQUIRED_COLUMNS,
    read_result,
    validate_workbook,
    write_result,
)


PROJECT_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_APPROVAL_DIR = PROJECT_ROOT / "data" / "onboarding" / "approvals"
DEFAULT_CONTRACT_DIR = PROJECT_ROOT / "data" / "onboarding" / "contracts"
DEFAULT_ADAPTED_DIR = PROJECT_ROOT / "data" / "onboarding" / "adapted"
DEFAULT_BRONZE_DIR = PROJECT_ROOT / "data" / "ingestion" / "onboarded"
DEFAULT_AUDIT_DIR = PROJECT_ROOT / "data" / "workflows" / "schema-onboarding"
APPROVAL_VERSION = "schema-onboarding-approval-v1"
CONTRACT_VERSION = "schema-onboarding-contract-v1"
WORKFLOW_VERSION = "schema-onboarding-workflow-v2"
APPROVED_DERIVATIONS = {"divide_columns"}


class SchemaContractError(RuntimeError):
    """Raised when approval or deterministic adaptation is unsafe."""

    def __init__(self, message: str, *, audit_path: Path | None = None):
        super().__init__(message)
        self.audit_path = audit_path


def create_approval_template(
    bronze: Mapping[str, object],
    proposal: Mapping[str, object],
    *,
    client_id: str,
) -> dict[str, object]:
    """Create a pending, proposal-bound approval document."""

    client_id = _bounded_text(client_id, "client_id", 100)
    mappings, derived = _validate_proposal(bronze, proposal)
    observed = _string_list(bronze.get("observed_schema"), "observed_schema")
    mapping_sources = {str(item["source"]) for item in mappings}
    drop_columns = [
        column
        for column in observed
        if column not in EXPECTED_COLUMNS and column not in mapping_sources
    ]
    ignored_sheets = _extra_sheets(bronze)
    return {
        "approval_version": APPROVAL_VERSION,
        "client_id": client_id,
        "proposal_run_id": proposal["run_id"],
        "proposal_hash": _document_hash(proposal),
        "reviewer": "",
        "reviewed_at": "",
        "mapping_decisions": [
            {
                "source": item["source"],
                "target": item["target"],
                "decision": "PENDING",
            }
            for item in mappings
        ],
        "derived_field_decisions": [
            {
                "target": item["target"],
                "decision": "PENDING",
                "operation": (
                    "divide_columns" if len(item["input_columns"]) == 2 else None
                ),
                "parameters": (
                    {
                        "numerator": item["input_columns"][0],
                        "denominator": item["input_columns"][1],
                        "on_zero": "BLOCK",
                    }
                    if len(item["input_columns"]) == 2
                    else {}
                ),
            }
            for item in derived
        ],
        "drop_columns": {"decision": "PENDING", "columns": drop_columns},
        "ignore_unselected_sheets": {
            "decision": "PENDING",
            "sheets": ignored_sheets,
        },
    }


def compile_contract(
    bronze: Mapping[str, object],
    proposal: Mapping[str, object],
    approval: Mapping[str, object],
) -> dict[str, object]:
    """Compile explicit human decisions into a hash-bound adapter contract."""

    mappings, derived = _validate_proposal(bronze, proposal)
    expected_approval_keys = {
        "approval_version",
        "client_id",
        "proposal_run_id",
        "proposal_hash",
        "reviewer",
        "reviewed_at",
        "mapping_decisions",
        "derived_field_decisions",
        "drop_columns",
        "ignore_unselected_sheets",
    }
    if set(approval) != expected_approval_keys:
        raise SchemaContractError("Approval document has an invalid shape.")
    if approval.get("approval_version") != APPROVAL_VERSION:
        raise SchemaContractError("Approval document uses an unsupported version.")
    if approval.get("proposal_run_id") != proposal.get("run_id") or approval.get(
        "proposal_hash"
    ) != _document_hash(proposal):
        raise SchemaContractError("Approval is not bound to this proposal.")
    client_id = _bounded_text(approval.get("client_id"), "client_id", 100)
    reviewer = _bounded_text(approval.get("reviewer"), "reviewer", 200)
    reviewed_at = _timestamp(approval.get("reviewed_at"), "reviewed_at")

    mapping_decisions = _object_list(
        approval.get("mapping_decisions"), "mapping_decisions"
    )
    expected_mappings = {
        (str(item["source"]), str(item["target"])) for item in mappings
    }
    supplied_mappings = set()
    for item in mapping_decisions:
        if set(item) != {"source", "target", "decision"}:
            raise SchemaContractError("A mapping decision is malformed.")
        pair = (str(item["source"]), str(item["target"]))
        if pair in supplied_mappings or pair not in expected_mappings:
            raise SchemaContractError("A mapping decision is duplicated or unproposed.")
        if item["decision"] != "APPROVE":
            raise SchemaContractError(f"Mapping {pair!r} is not approved.")
        supplied_mappings.add(pair)
    if supplied_mappings != expected_mappings:
        raise SchemaContractError("Every proposed mapping needs an explicit decision.")

    derivation_decisions = _object_list(
        approval.get("derived_field_decisions"), "derived_field_decisions"
    )
    expected_derived = {str(item["target"]): item for item in derived}
    derivations = []
    for item in derivation_decisions:
        if set(item) != {"target", "decision", "operation", "parameters"}:
            raise SchemaContractError("A derived-field decision is malformed.")
        target = str(item["target"])
        proposed = expected_derived.get(target)
        if proposed is None or any(entry["target"] == target for entry in derivations):
            raise SchemaContractError(
                "A derived-field decision is duplicated or unproposed."
            )
        if item["decision"] != "APPROVE":
            raise SchemaContractError(f"Derived field {target!r} is not approved.")
        operation = item["operation"]
        parameters = item["parameters"]
        if operation not in APPROVED_DERIVATIONS or not isinstance(parameters, dict):
            raise SchemaContractError("Derived-field operation is not approved.")
        _validate_derivation(operation, parameters, proposed)
        derivations.append(
            {"target": target, "operation": operation, "parameters": parameters}
        )
    if {entry["target"] for entry in derivations} != set(expected_derived):
        raise SchemaContractError(
            "Every proposed derived field needs an explicit decision."
        )

    observed = _string_list(bronze.get("observed_schema"), "observed_schema")
    mapping_sources = {source for source, _ in expected_mappings}
    expected_drop = [
        column
        for column in observed
        if column not in EXPECTED_COLUMNS and column not in mapping_sources
    ]
    drop_columns = _approval_set(
        approval.get("drop_columns"), "drop_columns", expected_drop
    )
    ignored_sheets = _approval_set(
        approval.get("ignore_unselected_sheets"),
        "ignore_unselected_sheets",
        _extra_sheets(bronze),
        value_key="sheets",
    )

    body: dict[str, object] = {
        "contract_version": CONTRACT_VERSION,
        "contract_id": str(uuid4()),
        "client_id": client_id,
        "status": "APPROVED",
        "approved_by": reviewer,
        "approved_at": reviewed_at,
        "source_file_hash": _required_text(bronze, "file_hash"),
        "source_content_fingerprint": bronze.get("content_fingerprint"),
        "bronze_record_hash": _required_text(bronze, "record_hash"),
        "proposal_run_id": proposal["run_id"],
        "proposal_hash": _document_hash(proposal),
        "context_hash": proposal.get("context_hash"),
        "selected_sheet": _required_text(bronze, "selected_sheet"),
        "source_schema": observed,
        "schema_fingerprint": _schema_fingerprint(
            _required_text(bronze, "selected_sheet"), observed
        ),
        "mappings": [
            {"source": source, "target": target}
            for source, target in sorted(expected_mappings)
        ],
        "derived_fields": derivations,
        "drop_columns": drop_columns,
        "ignored_sheets": ignored_sheets,
    }
    body["contract_hash"] = _document_hash(body)
    return body


def execute_onboarding(
    bronze_result_path: Path | str,
    proposal_path: Path | str,
    approval_path: Path | str,
    *,
    contract_dir: Path | str = DEFAULT_CONTRACT_DIR,
    adapted_dir: Path | str = DEFAULT_ADAPTED_DIR,
    bronze_dir: Path | str = DEFAULT_BRONZE_DIR,
    audit_dir: Path | str = DEFAULT_AUDIT_DIR,
) -> Path:
    """Approve, adapt, rerun Bronze, and durably record one onboarding run."""

    run_id = str(uuid4())
    run_dir = Path(audit_dir) / run_id
    audit_path = run_dir / "workflow.json"
    audit: dict[str, object] = {
        "workflow_version": WORKFLOW_VERSION,
        "workflow_run_id": run_id,
        "started_at": datetime.now(UTC).isoformat(),
        "completed_at": None,
        "status": "RUNNING",
        "current_stage": "LOAD_INPUTS",
        "failure": None,
        "source_bronze_path": str(Path(bronze_result_path).resolve()),
        "proposal_path": str(Path(proposal_path).resolve()),
        "approval_path": str(Path(approval_path).resolve()),
        "contract_path": None,
        "adapted_workbooks": [],
        "adapted_bronze_records": [],
    }
    try:
        bronze = read_result(bronze_result_path)
        proposal = _read_document(proposal_path, "proposal")
        approval = _read_document(approval_path, "approval")
        audit["current_stage"] = "CONTRACT_COMPILATION"
        contract = compile_contract(bronze, proposal, approval)
        contract_path = Path(contract_dir) / (
            f"{_safe_name(str(contract['client_id']))}_"
            f"{contract['contract_id']}.contract.json"
        )
        _write_json(contract_path, contract)
        audit["contract_path"] = str(contract_path.resolve())

        audit["current_stage"] = "ADAPTER_EXECUTION"
        adapted_paths = _adapt_workbook(bronze, contract, Path(adapted_dir))
        audit["adapted_workbooks"] = [str(path.resolve()) for path in adapted_paths]

        audit["current_stage"] = "BRONZE_REENTRY"
        lineage = {
            "schema_contract_id": contract["contract_id"],
            "schema_contract_hash": contract["contract_hash"],
            "client_id": contract["client_id"],
            "source_file_hash": contract["source_file_hash"],
        }
        records = []
        states = []
        for adapted_path in adapted_paths:
            result = validate_workbook(adapted_path)
            adapted_bronze_path = write_result(
                result, bronze_dir, lineage=lineage
            )
            states.append(result.lifecycle_state)
            records.append(
                {
                    "workbook_path": str(adapted_path.resolve()),
                    "bronze_path": str(adapted_bronze_path.resolve()),
                    "lifecycle_state": result.lifecycle_state,
                    "row_count": result.row_count,
                }
            )
        audit["adapted_bronze_records"] = records
        audit["status"] = (
            "BRONZE_ACCEPTED"
            if all(state == "BRONZE_ACCEPTED" for state in states)
            else "BRONZE_REVIEW_REQUIRED"
            if all(
                state in {"BRONZE_ACCEPTED", "AWAITING_REVIEW"}
                for state in states
            )
            else "BRONZE_BLOCKED"
        )
        audit["current_stage"] = "COMPLETE"
        return _finish_audit(audit_path, audit)
    except Exception as exc:
        audit["status"] = "FAILED"
        audit["failure"] = {
            "type": type(exc).__name__,
            "message": str(exc),
        }
        failed_path = _finish_audit(audit_path, audit)
        raise SchemaContractError(str(exc), audit_path=failed_path) from exc


def _adapt_workbook(
    bronze: Mapping[str, object],
    contract: Mapping[str, object],
    output_dir: Path,
) -> list[Path]:
    source_path = Path(_required_text(bronze, "file_path"))
    if not source_path.is_absolute():
        source_path = PROJECT_ROOT / source_path
    if not source_path.is_file() or _file_hash(source_path) != contract.get(
        "source_file_hash"
    ):
        raise SchemaContractError("Source workbook hash does not match the contract.")
    if contract.get("contract_hash") != _contract_hash(contract):
        raise SchemaContractError("Schema contract hash mismatch.")

    selected_sheet = str(contract["selected_sheet"])
    frame = pd.read_excel(source_path, sheet_name=selected_sheet)
    columns = [str(column) for column in frame.columns]
    if columns != contract.get("source_schema") or _schema_fingerprint(
        selected_sheet, columns
    ) != contract.get("schema_fingerprint"):
        raise SchemaContractError("Source workbook schema does not match the contract.")

    mappings = {
        str(item["source"]): str(item["target"])
        for item in _object_list(contract.get("mappings"), "contract mappings")
    }
    adapted = frame.rename(columns=mappings).copy()
    if len(adapted.columns) != len(set(str(column) for column in adapted.columns)):
        raise SchemaContractError("Adapter produced duplicate canonical columns.")
    for derivation in _object_list(
        contract.get("derived_fields"), "contract derived_fields"
    ):
        target = str(derivation["target"])
        operation = str(derivation["operation"])
        parameters = derivation["parameters"]
        if operation != "divide_columns" or not isinstance(parameters, dict):
            raise SchemaContractError("Contract contains an unsupported derivation.")
        numerator = str(parameters["numerator"])
        denominator = str(parameters["denominator"])
        numerator_values = pd.to_numeric(frame[numerator], errors="coerce")
        denominator_values = pd.to_numeric(frame[denominator], errors="coerce")
        invalid = numerator_values.isna() | denominator_values.isna()
        if invalid.any():
            raise SchemaContractError(
                f"Cannot derive {target}: inputs contain non-numeric or missing values."
            )
        if (denominator_values == 0).any():
            raise SchemaContractError(
                f"Cannot derive {target}: denominator contains zero values."
            )
        adapted[target] = numerator_values / denominator_values

    canonical_columns = [
        column for column in EXPECTED_COLUMNS if column in adapted.columns
    ]
    missing = [column for column in REQUIRED_COLUMNS if column not in canonical_columns]
    if missing:
        raise SchemaContractError(
            "Adapter did not produce required columns: " + ", ".join(missing)
        )
    adapted = adapted.loc[:, canonical_columns]
    dates = pd.to_datetime(adapted["InvoiceDate"], errors="coerce")
    countries = adapted["Country"].astype("string").str.strip()
    if dates.isna().any() or countries.isna().any() or (countries == "").any():
        raise SchemaContractError(
            "Cannot partition adapted rows with invalid dates or countries."
        )

    adapted["Country"] = countries
    adapted = adapted.assign(_reporting_month=dates.dt.strftime("%Y-%m"))
    output_root = output_dir / _safe_name(str(contract["client_id"])) / str(
        contract["contract_id"]
    )
    output_paths = []
    for (month, country), group in adapted.groupby(
        ["_reporting_month", "Country"], sort=True
    ):
        period_dir = output_root / str(month)
        output_path = period_dir / f"{_submission_slug(country)}_{month}.xlsx"
        if output_path.exists():
            raise SchemaContractError(f"Refusing to overwrite {output_path}.")
        period_dir.mkdir(parents=True, exist_ok=True)
        temporary = output_path.with_suffix(".xlsx.tmp")
        with pd.ExcelWriter(temporary, engine="openpyxl") as writer:
            group.loc[:, canonical_columns].to_excel(
                writer, sheet_name="Transactions", index=False
            )
        temporary.replace(output_path)
        output_paths.append(output_path)
    if not output_paths:
        raise SchemaContractError("Adapter produced no country-month submissions.")
    return output_paths


def _validate_proposal(
    bronze: Mapping[str, object], proposal: Mapping[str, object]
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    if proposal.get("schema_version") != PROPOSAL_SCHEMA_VERSION:
        raise SchemaContractError("Proposal uses an unsupported schema version.")
    if proposal.get("status") != "READY_FOR_HUMAN_REVIEW" or proposal.get(
        "questions"
    ) not in ([], ()): 
        raise SchemaContractError("Proposal is not ready for human review.")
    if proposal.get("bronze_record_hash") != bronze.get("record_hash") or proposal.get(
        "file_hash"
    ) != bronze.get("file_hash"):
        raise SchemaContractError("Proposal is not bound to this Bronze record.")
    mappings = _object_list(proposal.get("suggested_mappings"), "suggested_mappings")
    derived = _object_list(proposal.get("derived_fields"), "derived_fields")
    if not mappings:
        raise SchemaContractError("Proposal has no schema mappings.")
    for item in [*mappings, *derived]:
        if item.get("approval") != "HUMAN_REQUIRED" or item.get("executable") is not False:
            raise SchemaContractError("Proposal contains an executable suggestion.")
    return mappings, derived


def _validate_derivation(
    operation: object,
    parameters: Mapping[str, object],
    proposed: Mapping[str, object],
) -> None:
    if operation != "divide_columns" or set(parameters) != {
        "numerator",
        "denominator",
        "on_zero",
    }:
        raise SchemaContractError("divide_columns parameters are malformed.")
    numerator = parameters["numerator"]
    denominator = parameters["denominator"]
    if parameters["on_zero"] != "BLOCK" or {
        numerator,
        denominator,
    } != set(proposed["input_columns"]):
        raise SchemaContractError(
            "Derived-field parameters do not match the approved proposal."
        )


def _approval_set(
    value: object,
    name: str,
    expected: list[str],
    *,
    value_key: str = "columns",
) -> list[str]:
    if not isinstance(value, dict) or set(value) != {"decision", value_key}:
        raise SchemaContractError(f"{name} approval is malformed.")
    values = _string_list(value[value_key], name)
    if value["decision"] != "APPROVE" or values != expected:
        raise SchemaContractError(f"{name} must be explicitly approved unchanged.")
    return values


def _extra_sheets(bronze: Mapping[str, object]) -> list[str]:
    for finding in bronze.get("findings", []):
        if isinstance(finding, dict) and finding.get("code") == "extra_sheets_present":
            sheets = finding.get("extra_sheets", [])
            return _string_list(sheets, "extra_sheets") if sheets else []
    return []


def _schema_fingerprint(sheet: str, columns: list[str]) -> str:
    return _document_hash({"sheet": sheet, "columns": columns})


def _contract_hash(contract: Mapping[str, object]) -> str:
    unsigned = dict(contract)
    unsigned.pop("contract_hash", None)
    return _document_hash(unsigned)


def _document_hash(document: Mapping[str, object]) -> str:
    encoded = json.dumps(
        document, sort_keys=True, separators=(",", ":"), ensure_ascii=True
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


def _file_hash(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _safe_name(value: str) -> str:
    safe = re.sub(r"[^a-zA-Z0-9_-]+", "-", value).strip("-")
    if not safe:
        raise SchemaContractError("client_id cannot produce an empty file name.")
    return safe


def _submission_slug(value: object) -> str:
    safe = re.sub(r"[^a-z0-9]+", "_", str(value).strip().lower()).strip("_")
    if not safe:
        raise SchemaContractError("Country cannot produce an empty file name.")
    return safe


def _timestamp(value: object, name: str) -> str:
    text = _bounded_text(value, name, 100)
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise SchemaContractError(f"{name} must be an ISO-8601 timestamp.") from exc
    if parsed.tzinfo is None:
        raise SchemaContractError(f"{name} must include a timezone.")
    return parsed.isoformat()


def _bounded_text(value: object, name: str, limit: int) -> str:
    if not isinstance(value, str) or not value.strip() or len(value) > limit:
        raise SchemaContractError(
            f"{name} must be a non-empty string of at most {limit} characters."
        )
    return value.strip()


def _required_text(value: Mapping[str, object], key: str) -> str:
    item = value.get(key)
    if not isinstance(item, str) or not item:
        raise SchemaContractError(f"{key} must be a non-empty string.")
    return item


def _string_list(value: object, name: str) -> list[str]:
    if not isinstance(value, (list, tuple)) or not all(
        isinstance(item, str) and item for item in value
    ):
        raise SchemaContractError(f"{name} must be a list of non-empty strings.")
    return list(value)


def _object_list(value: object, name: str) -> list[dict[str, Any]]:
    if not isinstance(value, (list, tuple)) or not all(
        isinstance(item, dict) for item in value
    ):
        raise SchemaContractError(f"{name} must be a list of objects.")
    return list(value)


def _read_document(path: Path | str, name: str) -> dict[str, object]:
    try:
        value = json.loads(Path(path).read_text())
    except (OSError, json.JSONDecodeError) as exc:
        raise SchemaContractError(f"Cannot read {name}: {exc}") from exc
    if not isinstance(value, dict):
        raise SchemaContractError(f"{name} must be a JSON object.")
    return value


def _write_json(path: Path, document: Mapping[str, object]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(document, indent=2, sort_keys=True) + "\n")
    temporary.replace(path)
    return path


def _finish_audit(path: Path, audit: dict[str, object]) -> Path:
    audit["completed_at"] = datetime.now(UTC).isoformat()
    return _write_json(path, audit)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Approve schema onboarding and re-enter deterministic Bronze."
    )
    commands = parser.add_subparsers(dest="command", required=True)
    template = commands.add_parser("approval-template")
    template.add_argument("bronze_result", type=Path)
    template.add_argument("proposal", type=Path)
    template.add_argument("--client-id", required=True)
    template.add_argument("--output-dir", type=Path, default=DEFAULT_APPROVAL_DIR)

    execute = commands.add_parser("execute")
    execute.add_argument("bronze_result", type=Path)
    execute.add_argument("proposal", type=Path)
    execute.add_argument("approval", type=Path)
    execute.add_argument("--contract-dir", type=Path, default=DEFAULT_CONTRACT_DIR)
    execute.add_argument("--adapted-dir", type=Path, default=DEFAULT_ADAPTED_DIR)
    execute.add_argument("--bronze-dir", type=Path, default=DEFAULT_BRONZE_DIR)
    execute.add_argument("--audit-dir", type=Path, default=DEFAULT_AUDIT_DIR)
    args = parser.parse_args(argv)

    try:
        if args.command == "approval-template":
            bronze = read_result(args.bronze_result)
            proposal = _read_document(args.proposal, "proposal")
            document = create_approval_template(
                bronze, proposal, client_id=args.client_id
            )
            path = args.output_dir / (
                f"{_safe_name(args.client_id)}_"
                f"{proposal['run_id']}.approval.json"
            )
            print(_write_json(path, document))
            return 0
        audit_path = execute_onboarding(
            args.bronze_result,
            args.proposal,
            args.approval,
            contract_dir=args.contract_dir,
            adapted_dir=args.adapted_dir,
            bronze_dir=args.bronze_dir,
            audit_dir=args.audit_dir,
        )
        print(audit_path)
        return 0
    except (SchemaContractError, ValueError) as exc:
        suffix = f"; audit: {exc.audit_path}" if getattr(exc, "audit_path", None) else ""
        parser.exit(1, f"schema onboarding failed: {exc}{suffix}\n")


if __name__ == "__main__":
    raise SystemExit(main())
