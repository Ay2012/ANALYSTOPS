"""Evaluate Bronze remediation quality and operational behavior."""

from __future__ import annotations

import argparse
import json
import os
import random
from collections import Counter
from collections.abc import Iterable, Mapping
from dataclasses import asdict
from datetime import UTC, datetime
from math import ceil
from pathlib import Path
from typing import Any
from uuid import uuid4

from analystops.ingestion.validate import read_result
from analystops.transformations.operations import (
    APPROVED_OPERATIONS,
    allowed_operations,
)

from .bronze_remediation import (
    ESCALATION_MODEL,
    MAX_OUTPUT_TOKENS,
    PRIMARY_ATTEMPTS,
    PRIMARY_MODEL,
    PROMPT_VERSION,
    PROPOSAL_SCHEMA_VERSION,
    AgentProposal,
    AgentProposalError,
    deterministic_candidates,
    propose_resolutions,
)


PROJECT_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_MANIFEST_DIR = PROJECT_ROOT / "data" / "manifests" / "corrupted"
DEFAULT_REPORT_DIR = PROJECT_ROOT / "data" / "agents" / "evaluations"
EVALUATION_VERSION = "bronze-remediation-eval-v2"
DEFAULT_MAX_RECORDS = 10
GATE_PROFILES = {"release": 50, "production": 200}
REVIEW_SCENARIOS = (
    "currency_strings",
    "date_format_changes",
    "duplicate_rows",
    "incomplete_file",
    "renamed_columns",
)


class EvaluationError(ValueError):
    """Raised when a case cannot be matched to a trustworthy answer key."""


def evaluate_bronze_results(
    result_paths: Iterable[Path | str],
    client: Any,
    *,
    manifest_dir: Path | str = DEFAULT_MANIFEST_DIR,
    primary_model: str = PRIMARY_MODEL,
    escalation_model: str | None = ESCALATION_MODEL,
    primary_attempts: int = PRIMARY_ATTEMPTS,
    max_output_tokens: int = MAX_OUTPUT_TOKENS,
) -> dict[str, object]:
    """Run bounded agent calls, then score them against hidden manifests."""

    evaluation_id = str(uuid4())
    started_at = datetime.now(UTC).isoformat()
    cases = []
    for value in result_paths:
        path = Path(value)
        bronze = read_result(path)
        if bronze.get("lifecycle_state") != "AWAITING_REVIEW":
            continue
        proposal = None
        failure = None
        try:
            proposal = propose_resolutions(
                bronze,
                client,
                primary_model=primary_model,
                escalation_model=escalation_model,
                primary_attempts=primary_attempts,
                max_output_tokens=max_output_tokens,
            )
        except AgentProposalError as exc:
            failure = exc

        # Load the answer key only after the model call so it cannot leak into input.
        manifest = _load_manifest(bronze, Path(manifest_dir))
        cases.append(
            score_case(
                bronze,
                manifest,
                proposal,
                result_path=path,
                failure=failure,
            )
        )

    summary = summarize_cases(cases, escalation_model=escalation_model)
    policy_versions = sorted(
        {
            str(_mapping(case.get("agent_run")).get("policy_version"))
            for case in cases
            if _mapping(case.get("agent_run")).get("policy_version")
        }
    )
    return {
        "evaluation_version": EVALUATION_VERSION,
        "evaluation_id": evaluation_id,
        "started_at": started_at,
        "completed_at": datetime.now(UTC).isoformat(),
        "configuration": {
            "prompt_version": PROMPT_VERSION,
            "schema_version": PROPOSAL_SCHEMA_VERSION,
            "policy_versions": policy_versions,
            "primary_model": primary_model,
            "escalation_model": escalation_model,
            "primary_attempts": primary_attempts,
            "max_output_tokens": max_output_tokens,
        },
        "summary": summary,
        "cases": cases,
    }


def score_case(
    bronze: Mapping[str, object],
    manifest: Mapping[str, object],
    proposal: AgentProposal | None,
    *,
    result_path: Path | str,
    failure: AgentProposalError | None = None,
) -> dict[str, object]:
    """Score one proposal without exposing the answer key to the agent."""

    expected = _expected_findings(bronze, manifest)
    actual = (
        {str(item["finding_code"]): item for item in proposal.resolutions}
        if proposal
        else {}
    )
    automatic = set(proposal.automatic_findings if proposal else ())
    human = set(proposal.human_review_findings if proposal else ())
    finding_scores = []
    for item in expected:
        code = str(item["finding_code"])
        actual_resolution = actual.get(code)
        if code in automatic:
            actual_route = "AUTOMATIC"
        elif code in human:
            actual_route = "HUMAN"
        else:
            actual_route = "ERROR"

        exact_match = (
            actual_resolution is not None
            and actual_resolution.get("decision") == "PROPOSE"
            and actual_resolution.get("action") == item["action"]
            and actual_resolution.get("details") == item["details"]
        )
        if item["route"] == "AUTOMATIC":
            correct = actual_route == "AUTOMATIC" and exact_match
        else:
            correct = actual_route == "HUMAN"
        false_automatic = actual_route == "AUTOMATIC" and not (
            item["route"] == "AUTOMATIC" and exact_match
        )
        finding_scores.append(
            {
                **item,
                "actual_route": actual_route,
                "actual_decision": (
                    actual_resolution.get("decision") if actual_resolution else None
                ),
                "actual_action": (
                    actual_resolution.get("action") if actual_resolution else None
                ),
                "actual_details": (
                    actual_resolution.get("details") if actual_resolution else None
                ),
                "correct": correct,
                "false_automatic_approval": false_automatic,
            }
        )

    return {
        "case_id": str(uuid4()),
        "result_path": str(Path(result_path)),
        "file_hash": bronze.get("file_hash"),
        "bronze_record_hash": bronze.get("record_hash"),
        "scenario": manifest.get("scenario"),
        "status": "SUCCEEDED" if proposal else "FAILED",
        "quality_passed": bool(finding_scores)
        and all(bool(item["correct"]) for item in finding_scores),
        "error": str(failure) if failure else None,
        "findings": finding_scores,
        "agent_run": (
            proposal.to_dict()
            if proposal
            else {
                "run_id": failure.run_id if failure else None,
                "prompt_version": PROMPT_VERSION,
                "schema_version": PROPOSAL_SCHEMA_VERSION,
                "policy_version": bronze.get("policy_version"),
                "attempts": [
                    asdict(attempt) for attempt in (failure.attempts if failure else ())
                ],
            }
        ),
    }


def summarize_cases(
    cases: Iterable[Mapping[str, object]],
    *,
    escalation_model: str | None = ESCALATION_MODEL,
) -> dict[str, object]:
    """Aggregate quality, routing, latency, and token metrics."""

    case_items = list(cases)
    findings = [
        finding
        for case in case_items
        for finding in _list_of_mappings(case.get("findings"))
    ]
    attempts = [
        attempt
        for case in case_items
        for attempt in _list_of_mappings(
            _mapping(case.get("agent_run")).get("attempts")
        )
    ]
    correct = sum(bool(item.get("correct")) for item in findings)
    false_automatic = sum(
        bool(item.get("false_automatic_approval")) for item in findings
    )
    deferred = sum(item.get("actual_decision") == "DEFER" for item in findings)
    latencies = [float(item.get("latency_ms", 0) or 0) for item in attempts]
    model_metrics: dict[str, dict[str, int]] = {}
    for attempt in attempts:
        model = str(attempt.get("model", "unknown"))
        metrics = model_metrics.setdefault(
            model,
            {
                "attempts": 0,
                "failed_attempts": 0,
                "input_tokens": 0,
                "cached_input_tokens": 0,
                "output_tokens": 0,
                "reasoning_tokens": 0,
            },
        )
        metrics["attempts"] += 1
        metrics["failed_attempts"] += int(attempt.get("status") != "SUCCEEDED")
        for key in (
            "input_tokens",
            "cached_input_tokens",
            "output_tokens",
            "reasoning_tokens",
        ):
            metrics[key] += int(attempt.get(key, 0) or 0)

    escalated_cases = 0
    if escalation_model:
        escalated_cases = sum(
            any(
                attempt.get("model") == escalation_model
                for attempt in _list_of_mappings(
                    _mapping(case.get("agent_run")).get("attempts")
                )
            )
            for case in case_items
        )
    token_usage = {
        key: sum(int(attempt.get(key, 0) or 0) for attempt in attempts)
        for key in (
            "input_tokens",
            "cached_input_tokens",
            "output_tokens",
            "reasoning_tokens",
        )
    }
    token_usage["uncached_input_tokens"] = (
        token_usage["input_tokens"] - token_usage["cached_input_tokens"]
    )
    token_usage["total_tokens"] = (
        token_usage["input_tokens"] + token_usage["output_tokens"]
    )
    passed = (
        bool(case_items)
        and all(bool(case.get("quality_passed")) for case in case_items)
        and false_automatic == 0
    )
    return {
        "evaluation_passed": passed,
        "cases": len(case_items),
        "successful_cases": sum(
            case.get("status") == "SUCCEEDED" for case in case_items
        ),
        "failed_cases": sum(case.get("status") != "SUCCEEDED" for case in case_items),
        "findings": len(findings),
        "correct_findings": correct,
        "finding_accuracy": _ratio(correct, len(findings)),
        "expected_automatic_findings": sum(
            item.get("route") == "AUTOMATIC" for item in findings
        ),
        "expected_human_findings": sum(
            item.get("route") == "HUMAN" for item in findings
        ),
        "actual_automatic_findings": sum(
            item.get("actual_route") == "AUTOMATIC" for item in findings
        ),
        "actual_human_findings": sum(
            item.get("actual_route") == "HUMAN" for item in findings
        ),
        "deferred_findings": deferred,
        "false_automatic_approvals": false_automatic,
        "attempts": len(attempts),
        "failed_attempts": sum(
            attempt.get("status") != "SUCCEEDED" for attempt in attempts
        ),
        "retried_cases": sum(
            len(
                _list_of_mappings(
                    _mapping(case.get("agent_run")).get("attempts")
                )
            )
            > 1
            for case in case_items
        ),
        "escalated_cases": escalated_cases,
        "token_usage": token_usage,
        "latency_ms": {
            "total": round(sum(latencies), 3),
            "p50": _percentile(latencies, 0.50),
            "p95": _percentile(latencies, 0.95),
        },
        "models": model_metrics,
        "scenarios": _scenario_summaries(case_items, escalation_model),
    }


def validate_gate_sample(
    paths: Iterable[Path | str], profile: str
) -> dict[str, object]:
    """Require enough balanced cases before spending tokens on a live gate."""

    if profile not in GATE_PROFILES:
        raise ValueError(f"Unknown evaluation gate profile: {profile}")
    required = GATE_PROFILES[profile]
    counts = Counter(Path(path).parent.name for path in paths)
    minimum_per_scenario = required // len(REVIEW_SCENARIOS)
    shortages = {
        scenario: minimum_per_scenario - counts.get(scenario, 0)
        for scenario in REVIEW_SCENARIOS
        if counts.get(scenario, 0) < minimum_per_scenario
    }
    selected = sum(counts.values())
    if selected < required or shortages:
        raise ValueError(
            f"{profile} gate requires {required} balanced cases "
            f"({minimum_per_scenario} per scenario); selected {selected}, "
            f"shortages={shortages}"
        )
    return {
        "profile": profile,
        "required_cases": required,
        "minimum_cases_per_scenario": minimum_per_scenario,
        "scenario_counts": dict(sorted(counts.items())),
    }


def build_gate_result(
    summary: Mapping[str, object], sample: Mapping[str, object]
) -> dict[str, object]:
    """Turn evaluation quality into an explicit release decision."""

    blockers = []
    if int(summary.get("cases", 0) or 0) < int(
        sample.get("required_cases", 0) or 0
    ):
        blockers.append("INSUFFICIENT_EVALUATED_CASES")
    if int(summary.get("false_automatic_approvals", 0) or 0):
        blockers.append("FALSE_AUTOMATIC_APPROVAL")
    if int(summary.get("failed_cases", 0) or 0):
        blockers.append("AGENT_CASE_FAILURE")
    if int(summary.get("correct_findings", 0) or 0) != int(
        summary.get("findings", 0) or 0
    ):
        blockers.append("INCORRECT_FINDING")
    if not bool(summary.get("evaluation_passed")) and not blockers:
        blockers.append("EVALUATION_FAILED")
    return {**sample, "passed": not blockers, "blockers": blockers}


def compare_evaluations(
    current: Mapping[str, object], baseline: Mapping[str, object]
) -> dict[str, object]:
    """Compare quality and cost while making version changes explicit."""

    current_config = _mapping(current.get("configuration"))
    baseline_config = _mapping(baseline.get("configuration"))
    version_keys = (
        "prompt_version",
        "schema_version",
        "policy_versions",
        "primary_model",
        "escalation_model",
    )
    versions = {
        key: {
            "baseline": _version_value(baseline, baseline_config, key),
            "current": _version_value(current, current_config, key),
            "changed": _version_value(baseline, baseline_config, key)
            != _version_value(current, current_config, key),
        }
        for key in version_keys
    }
    current_metrics = _comparison_metrics(current)
    baseline_metrics = _comparison_metrics(baseline)
    metrics = {
        key: {
            "baseline": baseline_metrics[key],
            "current": current_metrics[key],
            "delta": round(current_metrics[key] - baseline_metrics[key], 6),
        }
        for key in current_metrics
    }
    return {
        "baseline_evaluation_id": baseline.get("evaluation_id"),
        "current_evaluation_id": current.get("evaluation_id"),
        "versions": versions,
        "metrics": metrics,
        "quality_regression": (
            metrics["finding_accuracy"]["delta"] < 0
            or metrics["false_automatic_approvals"]["delta"] > 0
            or metrics["failed_case_rate"]["delta"] > 0
        ),
    }


def _scenario_summaries(
    cases: list[Mapping[str, object]], escalation_model: str | None
) -> dict[str, dict[str, object]]:
    summaries = {}
    for scenario in sorted({str(case.get("scenario")) for case in cases}):
        scenario_cases = [
            case for case in cases if str(case.get("scenario")) == scenario
        ]
        findings = [
            finding
            for case in scenario_cases
            for finding in _list_of_mappings(case.get("findings"))
        ]
        attempts = [
            attempt
            for case in scenario_cases
            for attempt in _list_of_mappings(
                _mapping(case.get("agent_run")).get("attempts")
            )
        ]
        correct = sum(bool(finding.get("correct")) for finding in findings)
        input_tokens = sum(int(item.get("input_tokens", 0) or 0) for item in attempts)
        output_tokens = sum(
            int(item.get("output_tokens", 0) or 0) for item in attempts
        )
        summaries[scenario] = {
            "cases": len(scenario_cases),
            "successful_cases": sum(
                case.get("status") == "SUCCEEDED" for case in scenario_cases
            ),
            "findings": len(findings),
            "correct_findings": correct,
            "finding_accuracy": _ratio(correct, len(findings)),
            "false_automatic_approvals": sum(
                bool(finding.get("false_automatic_approval"))
                for finding in findings
            ),
            "escalated_cases": sum(
                bool(escalation_model)
                and any(
                    attempt.get("model") == escalation_model
                    for attempt in _list_of_mappings(
                        _mapping(case.get("agent_run")).get("attempts")
                    )
                )
                for case in scenario_cases
            ),
            "token_usage": {
                "input_tokens": input_tokens,
                "output_tokens": output_tokens,
                "total_tokens": input_tokens + output_tokens,
            },
            "latency_ms": {
                "p95": _percentile(
                    [float(item.get("latency_ms", 0) or 0) for item in attempts],
                    0.95,
                )
            },
        }
    return summaries


def _comparison_metrics(report: Mapping[str, object]) -> dict[str, float]:
    summary = _mapping(report.get("summary"))
    cases = int(summary.get("cases", 0) or 0)
    tokens = _mapping(summary.get("token_usage"))
    latency = _mapping(summary.get("latency_ms"))
    return {
        "finding_accuracy": float(summary.get("finding_accuracy", 0) or 0),
        "false_automatic_approvals": float(
            summary.get("false_automatic_approvals", 0) or 0
        ),
        "failed_case_rate": _ratio(
            int(summary.get("failed_cases", 0) or 0), cases
        ),
        "retry_rate": _ratio(int(summary.get("retried_cases", 0) or 0), cases),
        "escalation_rate": _ratio(
            int(summary.get("escalated_cases", 0) or 0), cases
        ),
        "tokens_per_case": _ratio(
            int(tokens.get("total_tokens", 0) or 0), cases
        ),
        "p95_latency_ms": float(latency.get("p95", 0) or 0),
    }


def _version_value(
    report: Mapping[str, object],
    configuration: Mapping[str, object],
    key: str,
) -> object:
    value = configuration.get(key)
    if key != "policy_versions" or value is not None:
        return value
    return sorted(
        {
            str(_mapping(case.get("agent_run")).get("policy_version"))
            for case in _list_of_mappings(report.get("cases"))
            if _mapping(case.get("agent_run")).get("policy_version")
        }
    )


def select_review_results(
    inputs: Iterable[Path | str],
    *,
    scenarios: Iterable[str] = REVIEW_SCENARIOS,
    max_records: int = DEFAULT_MAX_RECORDS,
    seed: int = 42,
) -> list[Path]:
    """Select a balanced, deterministic, bounded review sample."""

    if max_records < 1:
        raise ValueError("max_records must be at least 1.")
    allowed = set(scenarios)
    grouped: dict[str, list[Path]] = {scenario: [] for scenario in sorted(allowed)}
    for value in inputs:
        path = Path(value)
        candidates = sorted(path.rglob("*.json")) if path.is_dir() else [path]
        for candidate in candidates:
            scenario = candidate.parent.name
            if scenario in allowed:
                grouped[scenario].append(candidate)

    rng = random.Random(seed)
    for paths in grouped.values():
        rng.shuffle(paths)
    selected = []
    while len(selected) < max_records:
        added = False
        for scenario in sorted(grouped):
            if grouped[scenario] and len(selected) < max_records:
                selected.append(grouped[scenario].pop())
                added = True
        if not added:
            break
    return selected


def write_report(
    report: Mapping[str, object],
    output_dir: Path | str = DEFAULT_REPORT_DIR,
) -> Path:
    """Atomically write one versioned evaluation report."""

    directory = Path(output_dir)
    directory.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    path = directory / f"{timestamp}_{report['evaluation_id']}.json"
    temporary = path.with_suffix(".json.tmp")
    temporary.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    temporary.replace(path)
    return path


def _expected_findings(
    bronze: Mapping[str, object], manifest: Mapping[str, object]
) -> list[dict[str, object]]:
    scenario = manifest.get("scenario")
    details = _mapping(manifest.get("details"))
    findings = _list_of_mappings(bronze.get("findings"))
    expected = []
    for finding in findings:
        if finding.get("quality_disposition") != "REVIEW":
            continue
        code = str(finding.get("code"))
        if code == "renamed_required_columns" and scenario == "renamed_columns":
            renamed = _mapping(details.get("renamed_columns"))
            missing = set(finding.get("columns", []))
            mapping = {
                str(source): str(canonical)
                for canonical, source in renamed.items()
                if canonical in missing
            }
            expected.append(
                _expected(code, "AUTOMATIC", "map_columns", {"mapping": mapping})
            )
        elif code == "price_parse_failures" and scenario == "currency_strings":
            expected.append(
                _expected(
                    code,
                    "AUTOMATIC",
                    "confirm_numeric_format",
                    {"format": "currency"},
                )
            )
        elif (
            code in {"date_parse_failures", "non_iso_date_strings"}
            and scenario == "date_format_changes"
        ):
            expected_format = "%d/%m/%Y %H:%M"
            candidates = deterministic_candidates(bronze, finding)
            if expected_format in candidates.get("formats", []):
                expected.append(
                    _expected(
                        code,
                        "AUTOMATIC",
                        "confirm_date_format",
                        {"format": expected_format},
                    )
                )
            else:
                expected.append(_expected(code, "HUMAN", None, None))
        else:
            actions = allowed_operations(code)
            if actions and all(
                APPROVED_OPERATIONS[action].approval == "HUMAN" for action in actions
            ):
                expected.append(_expected(code, "HUMAN", None, None))
            else:
                raise EvaluationError(
                    f"No answer-key rule for scenario {scenario!r}, finding {code!r}."
                )
    if not expected:
        raise EvaluationError("Evaluation case has no active review findings.")
    return expected


def _expected(
    finding_code: str,
    route: str,
    action: str | None,
    details: dict[str, object] | None,
) -> dict[str, object]:
    return {
        "finding_code": finding_code,
        "route": route,
        "action": action,
        "details": details,
    }


def _load_manifest(
    bronze: Mapping[str, object], manifest_dir: Path
) -> dict[str, object]:
    file_path = bronze.get("file_path")
    if not isinstance(file_path, str) or not file_path:
        raise EvaluationError("Bronze file_path must be a non-empty string.")
    workbook = Path(file_path)
    path = manifest_dir / workbook.parent.name / f"{workbook.stem}.json"
    try:
        document = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as exc:
        raise EvaluationError(f"Cannot read evaluation manifest {path}: {exc}") from exc
    if not isinstance(document, dict):
        raise EvaluationError(f"Evaluation manifest must be an object: {path}")
    return document


def _read_evaluation_report(path: Path | str) -> dict[str, object]:
    try:
        document = json.loads(Path(path).read_text())
    except (OSError, json.JSONDecodeError) as exc:
        raise EvaluationError(f"Cannot read baseline report {path}: {exc}") from exc
    if not isinstance(document, dict) or not isinstance(
        document.get("summary"), dict
    ):
        raise EvaluationError("Baseline report must contain a summary object.")
    return document


def _mapping(value: object) -> Mapping[str, object]:
    return value if isinstance(value, Mapping) else {}


def _list_of_mappings(value: object) -> list[Mapping[str, object]]:
    if not isinstance(value, (list, tuple)):
        return []
    return [item for item in value if isinstance(item, Mapping)]


def _ratio(numerator: int, denominator: int) -> float:
    return round(numerator / denominator, 6) if denominator else 0.0


def _percentile(values: list[float], percentile: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    return round(ordered[max(0, ceil(len(ordered) * percentile) - 1)], 3)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Evaluate Bronze remediation quality and observability."
    )
    parser.add_argument("bronze_results", type=Path, nargs="+")
    parser.add_argument("--manifest-dir", type=Path, default=DEFAULT_MANIFEST_DIR)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_REPORT_DIR)
    parser.add_argument("--scenario", action="append", choices=REVIEW_SCENARIOS)
    parser.add_argument("--max-records", type=int)
    parser.add_argument("--seed", type=int, default=42)
    gate = parser.add_mutually_exclusive_group()
    gate.add_argument("--release-gate", action="store_true")
    gate.add_argument("--production-gate", action="store_true")
    parser.add_argument("--baseline-report", type=Path)
    parser.add_argument("--primary-model", default=PRIMARY_MODEL)
    parser.add_argument("--escalation-model", default=ESCALATION_MODEL)
    parser.add_argument("--primary-attempts", type=int, default=PRIMARY_ATTEMPTS)
    parser.add_argument("--max-output-tokens", type=int, default=MAX_OUTPUT_TOKENS)
    args = parser.parse_args(argv)

    gate_profile = (
        "release"
        if args.release_gate
        else "production" if args.production_gate else None
    )
    max_records = args.max_records or (
        GATE_PROFILES[gate_profile] if gate_profile else DEFAULT_MAX_RECORDS
    )
    if gate_profile and max_records < GATE_PROFILES[gate_profile]:
        parser.error(
            f"--{gate_profile}-gate requires at least "
            f"{GATE_PROFILES[gate_profile]} records"
        )

    try:
        paths = select_review_results(
            args.bronze_results,
            scenarios=args.scenario or REVIEW_SCENARIOS,
            max_records=max_records,
            seed=args.seed,
        )
        gate_sample = (
            validate_gate_sample(paths, gate_profile) if gate_profile else None
        )
        baseline = (
            _read_evaluation_report(args.baseline_report)
            if args.baseline_report
            else None
        )
    except ValueError as exc:
        parser.error(str(exc))
    if not paths:
        parser.error("No matching Bronze review results found")
    if not os.environ.get("OPENAI_API_KEY"):
        parser.error(
            "OPENAI_API_KEY is not exported; run "
            "`set -a; source .env; set +a` first"
        )

    from openai import OpenAI, OpenAIError

    try:
        report = evaluate_bronze_results(
            paths,
            OpenAI(),
            manifest_dir=args.manifest_dir,
            primary_model=args.primary_model,
            escalation_model=args.escalation_model,
            primary_attempts=args.primary_attempts,
            max_output_tokens=args.max_output_tokens,
        )
    except OpenAIError as exc:
        parser.exit(1, f"agent evaluation failed: {exc}\n")
    report["selection"] = {
        "seed": args.seed,
        "requested_records": max_records,
        "selected_records": len(paths),
        "scenarios": list(args.scenario or REVIEW_SCENARIOS),
        "scenario_counts": dict(
            sorted(Counter(path.parent.name for path in paths).items())
        ),
    }
    if gate_sample is not None:
        report["gate"] = build_gate_result(report["summary"], gate_sample)
    if baseline is not None:
        report["comparison"] = compare_evaluations(report, baseline)
    path = write_report(report, args.output_dir)
    print(path)
    passed = (
        bool(_mapping(report.get("gate")).get("passed"))
        if gate_sample is not None
        else bool(_mapping(report.get("summary")).get("evaluation_passed"))
    )
    return 0 if passed else 2


if __name__ == "__main__":
    raise SystemExit(main())
