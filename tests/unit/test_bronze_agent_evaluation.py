from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from analystops.agents.bronze_remediation import AgentAttempt, AgentProposal
from analystops.agents.evaluate_bronze import (
    REVIEW_SCENARIOS,
    build_gate_result,
    compare_evaluations,
    score_case,
    summarize_cases,
    validate_gate_sample,
)


def bronze_record(finding: dict[str, object]) -> dict[str, object]:
    return {
        "file_hash": "a" * 64,
        "record_hash": "b" * 64,
        "policy_version": "bronze-v1",
        "findings": [finding],
    }


def proposal(
    resolution: dict[str, object],
    *,
    automatic: tuple[str, ...] = (),
    human: tuple[str, ...] = (),
    attempts: tuple[AgentAttempt, ...] = (),
) -> AgentProposal:
    return AgentProposal(
        run_id="run-1",
        prompt_version="bronze-remediation-v4",
        schema_version="bronze-remediation-proposal-v2",
        policy_version="bronze-v1",
        started_at="2026-09-21T10:00:00+00:00",
        completed_at="2026-09-21T10:00:01+00:00",
        file_hash="a" * 64,
        bronze_record_hash="b" * 64,
        resolutions=(resolution,),
        automatic_findings=automatic,
        human_review_findings=human,
        attempts=attempts,
    )


class EvaluationTests(unittest.TestCase):
    def test_exact_automatic_resolution_passes(self) -> None:
        code = "renamed_required_columns"
        bronze = bronze_record(
            {"code": code, "quality_disposition": "REVIEW", "columns": ["Quantity"]}
        )
        manifest = {
            "scenario": "renamed_columns",
            "details": {"renamed_columns": {"Quantity": "Units"}},
        }
        agent = proposal(
            {
                "finding_code": code,
                "decision": "PROPOSE",
                "action": "map_columns",
                "details": {"mapping": {"Units": "Quantity"}},
                "defer_reason": None,
            },
            automatic=(code,),
        )

        case = score_case(bronze, manifest, agent, result_path="result.json")

        self.assertTrue(case["quality_passed"])
        self.assertFalse(case["findings"][0]["false_automatic_approval"])

    def test_wrong_automatic_mapping_is_a_false_approval(self) -> None:
        code = "renamed_required_columns"
        bronze = bronze_record(
            {"code": code, "quality_disposition": "REVIEW", "columns": ["Quantity"]}
        )
        manifest = {
            "scenario": "renamed_columns",
            "details": {"renamed_columns": {"Quantity": "Units"}},
        }
        agent = proposal(
            {
                "finding_code": code,
                "decision": "PROPOSE",
                "action": "map_columns",
                "details": {"mapping": {"Price": "Quantity"}},
                "defer_reason": None,
            },
            automatic=(code,),
        )

        case = score_case(bronze, manifest, agent, result_path="result.json")

        self.assertFalse(case["quality_passed"])
        self.assertTrue(case["findings"][0]["false_automatic_approval"])

    def test_human_deferral_and_observability_are_aggregated(self) -> None:
        code = "exact_duplicate_rows"
        bronze = bronze_record(
            {"code": code, "quality_disposition": "REVIEW", "count": 10}
        )
        manifest = {"scenario": "duplicate_rows", "details": {}}
        attempts = (
            AgentAttempt(
                model="gpt-5.6-luna",
                status="FAILED",
                attempt_number=1,
                latency_ms=100,
                input_tokens=50,
                output_tokens=10,
                reasoning_tokens=4,
            ),
            AgentAttempt(
                model="gpt-5.6-terra",
                status="SUCCEEDED",
                attempt_number=2,
                latency_ms=300,
                input_tokens=60,
                cached_input_tokens=20,
                output_tokens=15,
                reasoning_tokens=5,
            ),
        )
        agent = proposal(
            {
                "finding_code": code,
                "decision": "DEFER",
                "action": None,
                "details": {},
                "defer_reason": "BUSINESS_KNOWLEDGE_REQUIRED",
            },
            human=(code,),
            attempts=attempts,
        )
        case = score_case(bronze, manifest, agent, result_path="result.json")

        summary = summarize_cases([case])

        self.assertTrue(summary["evaluation_passed"])
        self.assertEqual(summary["deferred_findings"], 1)
        self.assertEqual(summary["escalated_cases"], 1)
        self.assertEqual(summary["token_usage"]["input_tokens"], 110)
        self.assertEqual(summary["token_usage"]["reasoning_tokens"], 9)
        self.assertEqual(summary["token_usage"]["total_tokens"], 135)
        self.assertEqual(summary["token_usage"]["uncached_input_tokens"], 90)
        self.assertEqual(summary["latency_ms"]["p95"], 300)
        self.assertEqual(
            summary["scenarios"]["duplicate_rows"]["token_usage"]["total_tokens"],
            135,
        )

    def test_ambiguous_date_evidence_is_expected_to_reach_human_review(self) -> None:
        code = "non_iso_date_strings"
        bronze = bronze_record(
            {
                "code": code,
                "quality_disposition": "REVIEW",
                "examples": ["03/03/2011 11:43"],
            }
        )
        manifest = {"scenario": "date_format_changes", "details": {}}
        agent = proposal(
            {
                "finding_code": code,
                "decision": "DEFER",
                "action": None,
                "details": {},
                "defer_reason": "AMBIGUOUS_CANDIDATES",
            },
            human=(code,),
        )

        case = score_case(bronze, manifest, agent, result_path="result.json")

        self.assertTrue(case["quality_passed"])

    def test_release_gate_requires_balance_and_blocks_false_approval(self) -> None:
        paths = [
            Path(scenario) / f"case-{number}.json"
            for scenario in REVIEW_SCENARIOS
            for number in range(10)
        ]
        sample = validate_gate_sample(paths, "release")
        summary = {
            "evaluation_passed": False,
            "cases": 50,
            "findings": 50,
            "correct_findings": 49,
            "failed_cases": 0,
            "false_automatic_approvals": 1,
        }

        gate = build_gate_result(summary, sample)

        self.assertFalse(gate["passed"])
        self.assertIn("FALSE_AUTOMATIC_APPROVAL", gate["blockers"])
        with self.assertRaises(ValueError):
            validate_gate_sample(paths[:-1], "release")

    def test_evaluation_comparison_tracks_versions_and_quality_deltas(self) -> None:
        baseline = evaluation_report(
            "baseline",
            prompt="prompt-v1",
            policy="bronze-v1",
            accuracy=1.0,
            false_automatic=0,
        )
        current = evaluation_report(
            "current",
            prompt="prompt-v2",
            policy="bronze-v2",
            accuracy=0.98,
            false_automatic=1,
        )

        comparison = compare_evaluations(current, baseline)

        self.assertTrue(comparison["versions"]["prompt_version"]["changed"])
        self.assertTrue(comparison["versions"]["policy_versions"]["changed"])
        self.assertEqual(
            comparison["metrics"]["finding_accuracy"]["delta"], -0.02
        )
        self.assertTrue(comparison["quality_regression"])


def evaluation_report(
    evaluation_id: str,
    *,
    prompt: str,
    policy: str,
    accuracy: float,
    false_automatic: int,
) -> dict[str, object]:
    return {
        "evaluation_id": evaluation_id,
        "configuration": {
            "prompt_version": prompt,
            "schema_version": "schema-v1",
            "policy_versions": [policy],
            "primary_model": "luna",
            "escalation_model": "terra",
        },
        "summary": {
            "cases": 50,
            "finding_accuracy": accuracy,
            "false_automatic_approvals": false_automatic,
            "failed_cases": 0,
            "retried_cases": 1,
            "escalated_cases": 1,
            "token_usage": {"total_tokens": 40000},
            "latency_ms": {"p95": 2000},
        },
    }


if __name__ == "__main__":
    unittest.main()
