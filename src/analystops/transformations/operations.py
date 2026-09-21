"""Approved operations that may resolve Bronze review findings."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path


@dataclass(frozen=True)
class OperationSpec:
    finding_codes: tuple[str, ...]
    required_parameters: tuple[str, ...] = ()
    risk: str = "LOW"
    approval: str = "AUTOMATIC"
    executes_in_silver: bool = False


@dataclass(frozen=True)
class PlanOperation:
    finding_code: str
    operation: str
    parameters: dict[str, object]


@dataclass(frozen=True)
class TransformationPlan:
    plan_version: str
    source_file_id: str
    bronze_record_hash: str
    operations: tuple[PlanOperation, ...]


class TransformationPlanError(ValueError):
    """Raised when a transformation plan is malformed or unauthorized."""


APPROVED_OPERATIONS = {
    "select_sheet": OperationSpec(
        ("ambiguous_transaction_sheets",),
        ("sheet_name",),
        risk="HIGH",
        approval="HUMAN",
    ),
    "map_columns": OperationSpec(
        ("renamed_required_columns",),
        ("mapping",),
        executes_in_silver=True,
    ),
    "confirm_numeric_format": OperationSpec(
        ("quantity_parse_failures", "price_parse_failures"),
        ("format",),
        executes_in_silver=True,
    ),
    "confirm_date_format": OperationSpec(
        ("date_parse_failures", "non_iso_date_strings"),
        ("format",),
        executes_in_silver=True,
    ),
    "confirm_valid_duplicates": OperationSpec(
        ("exact_duplicate_rows",),
        risk="HIGH",
        approval="HUMAN",
    ),
    "deduplicate_in_silver": OperationSpec(
        ("exact_duplicate_rows",),
        risk="HIGH",
        approval="HUMAN",
        executes_in_silver=True,
    ),
    "confirm_zero_activity": OperationSpec(
        ("empty_transaction_sheet",),
        risk="MEDIUM",
        approval="HUMAN",
    ),
    "confirm_expected_volume": OperationSpec(
        ("unexpectedly_low_row_count",),
        risk="MEDIUM",
        approval="HUMAN",
    ),
    "materialize_values_in_silver": OperationSpec(
        ("formula_cells",),
        risk="HIGH",
        approval="HUMAN",
        executes_in_silver=True,
    ),
}

PLAN_VERSION = "transformation-plan-v1"


def allowed_operations(finding_code: str) -> tuple[str, ...]:
    """Return approved operation names for one Bronze finding."""

    return tuple(
        name
        for name, operation in APPROVED_OPERATIONS.items()
        if finding_code in operation.finding_codes
    )


def load_transformation_plan(
    value: Path | str | Mapping[str, object],
    bronze: Mapping[str, object],
) -> TransformationPlan:
    """Parse a plan and prove that it matches signed Bronze review evidence."""

    document = _load_plan_document(value)
    required_keys = {
        "plan_version",
        "source_file_id",
        "bronze_record_hash",
        "operations",
    }
    if set(document) != required_keys:
        raise TransformationPlanError(
            f"Plan fields must be exactly: {', '.join(sorted(required_keys))}."
        )
    if document["plan_version"] != PLAN_VERSION:
        raise TransformationPlanError("Unsupported transformation plan version.")
    if document["source_file_id"] != bronze.get("file_hash"):
        raise TransformationPlanError("Plan source_file_id does not match Bronze.")
    if document["bronze_record_hash"] != bronze.get("record_hash"):
        raise TransformationPlanError("Plan is not bound to this Bronze record.")

    items = document["operations"]
    if not isinstance(items, list):
        raise TransformationPlanError("Plan operations must be a list.")
    operations = tuple(_parse_plan_operation(item) for item in items)
    _validate_against_bronze(operations, bronze)
    return TransformationPlan(
        plan_version=PLAN_VERSION,
        source_file_id=str(document["source_file_id"]),
        bronze_record_hash=str(document["bronze_record_hash"]),
        operations=operations,
    )


def plan_required(bronze: Mapping[str, object]) -> bool:
    """Return whether Bronze approved an operation that Silver must execute."""

    return bool(_approved_silver_operations(bronze))


def create_transformation_plan(
    bronze: Mapping[str, object],
) -> dict[str, object] | None:
    """Build and validate the executable plan authorized by Bronze."""

    approved = _approved_silver_operations(bronze)
    if not approved:
        return None
    document: dict[str, object] = {
        "plan_version": PLAN_VERSION,
        "source_file_id": bronze.get("file_hash"),
        "bronze_record_hash": bronze.get("record_hash"),
        "operations": [
            {
                "finding_code": code,
                "operation": action,
                "parameters": details,
            }
            for code, (action, details) in approved.items()
        ],
    }
    load_transformation_plan(document, bronze)
    return document


def transformation_plan_hash(plan: TransformationPlan) -> str:
    encoded = json.dumps(
        asdict(plan), sort_keys=True, separators=(",", ":"), ensure_ascii=True
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


def _load_plan_document(
    value: Path | str | Mapping[str, object],
) -> dict[str, object]:
    if isinstance(value, Mapping):
        return dict(value)
    try:
        document = json.loads(Path(value).read_text())
    except (OSError, json.JSONDecodeError) as exc:
        raise TransformationPlanError(f"Cannot read transformation plan: {exc}") from exc
    if not isinstance(document, dict):
        raise TransformationPlanError("Transformation plan must be a JSON object.")
    return document


def _parse_plan_operation(value: object) -> PlanOperation:
    if not isinstance(value, dict):
        raise TransformationPlanError("Each plan operation must be an object.")
    required_keys = {"finding_code", "operation", "parameters"}
    if set(value) != required_keys:
        raise TransformationPlanError(
            f"Operation fields must be exactly: {', '.join(sorted(required_keys))}."
        )
    finding_code = value["finding_code"]
    operation_name = value["operation"]
    parameters = value["parameters"]
    if not isinstance(finding_code, str) or not finding_code:
        raise TransformationPlanError("finding_code must be a non-empty string.")
    if not isinstance(operation_name, str) or operation_name not in APPROVED_OPERATIONS:
        raise TransformationPlanError(f"Operation {operation_name!r} is not approved.")
    spec = APPROVED_OPERATIONS[operation_name]
    if not spec.executes_in_silver:
        raise TransformationPlanError(
            f"Operation {operation_name!r} is not executable in Silver."
        )
    if finding_code not in spec.finding_codes:
        raise TransformationPlanError(
            f"Operation {operation_name!r} cannot resolve {finding_code!r}."
        )
    if not isinstance(parameters, dict):
        raise TransformationPlanError("Operation parameters must be an object.")
    _validate_parameters(operation_name, parameters, spec.required_parameters)
    return PlanOperation(finding_code, operation_name, dict(parameters))


def _validate_parameters(
    operation: str,
    parameters: dict[str, object],
    required: tuple[str, ...],
) -> None:
    if set(parameters) != set(required):
        raise TransformationPlanError(
            f"{operation} parameters must be exactly: {', '.join(required) or 'none'}."
        )
    if operation == "map_columns":
        mapping = parameters["mapping"]
        if (
            not isinstance(mapping, dict)
            or not mapping
            or not all(
                isinstance(source, str)
                and source
                and isinstance(target, str)
                and target
                for source, target in mapping.items()
            )
        ):
            raise TransformationPlanError(
                "map_columns mapping must contain non-empty string pairs."
            )
    elif operation == "confirm_numeric_format":
        if parameters["format"] not in {"currency", "number"}:
            raise TransformationPlanError(
                "confirm_numeric_format supports 'currency' or 'number'."
            )
    elif operation == "confirm_date_format":
        value = parameters["format"]
        if not isinstance(value, str) or not value:
            raise TransformationPlanError(
                "confirm_date_format requires a Python datetime format."
            )
        try:
            sample = datetime(2001, 2, 3, 4, 5).strftime(value)
            datetime.strptime(sample, value)
        except ValueError as exc:
            raise TransformationPlanError(
                "confirm_date_format requires a valid Python datetime format."
            ) from exc


def _validate_against_bronze(
    operations: tuple[PlanOperation, ...],
    bronze: Mapping[str, object],
) -> None:
    approved = _approved_silver_operations(bronze)
    supplied: dict[str, PlanOperation] = {}
    for operation in operations:
        if operation.finding_code in supplied:
            raise TransformationPlanError(
                f"Duplicate plan operation for {operation.finding_code!r}."
            )
        supplied[operation.finding_code] = operation
    if set(supplied) != set(approved):
        raise TransformationPlanError(
            "Plan operations do not match executable Bronze resolutions."
        )
    for code, (action, details) in approved.items():
        operation = supplied[code]
        if operation.operation != action or operation.parameters != details:
            raise TransformationPlanError(
                f"Plan operation for {code!r} does not match Bronze approval."
            )


def _approved_silver_operations(
    bronze: Mapping[str, object],
) -> dict[str, tuple[str, dict[str, object]]]:
    findings = bronze.get("findings")
    if not isinstance(findings, list):
        raise TransformationPlanError("Bronze findings must be a list.")
    approved: dict[str, tuple[str, dict[str, object]]] = {}
    for finding in findings:
        if not isinstance(finding, dict):
            raise TransformationPlanError("Bronze findings must be objects.")
        resolution = finding.get("review_resolution")
        if resolution is None:
            continue
        if not isinstance(resolution, dict):
            raise TransformationPlanError("Bronze review resolution must be an object.")
        action = resolution.get("action")
        spec = APPROVED_OPERATIONS.get(str(action))
        if spec is None:
            raise TransformationPlanError(f"Bronze operation {action!r} is not approved.")
        if not spec.executes_in_silver:
            continue
        code = finding.get("code")
        details = resolution.get("details", {})
        if not isinstance(code, str) or not isinstance(details, dict):
            raise TransformationPlanError("Bronze review resolution is malformed.")
        approved[code] = (str(action), dict(details))
    return approved
