"""Persisted LangGraph routing for Silver remediation and human approval."""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, TypedDict
from uuid import UUID, uuid4

from langgraph.checkpoint.sqlite import SqliteSaver
from langgraph.graph import END, START, StateGraph
from langgraph.types import Command, interrupt

from analystops.agents.silver_remediation import (
    ESCALATION_MODEL,
    MAX_OUTPUT_TOKENS,
    PRIMARY_ATTEMPTS,
    PRIMARY_MODEL,
    propose_silver_remediation,
)
from analystops.workflows.silver_remediation import (
    DEFAULT_REMEDIATION_DIR,
    SilverRemediationError,
    document_hash,
    execute_silver_remediation,
    resolve_held_partitions,
)


PROJECT_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_WORKFLOW_DIR = PROJECT_ROOT / "data" / "workflows" / "silver-remediation"
DEFAULT_CHECKPOINT_DB = DEFAULT_WORKFLOW_DIR / "checkpoints.sqlite"
WORKFLOW_VERSION = "silver-remediation-graph-v2"


class SilverGraphState(TypedDict, total=False):
    profile_path: str
    profile: dict[str, object]
    publication_state: str
    proposal: dict[str, object]
    human_decision: dict[str, object]
    execution: dict[str, object]
    followup_decision: dict[str, object]
    status: str


def build_silver_remediation_graph(
    client: Any | None,
    *,
    checkpointer: Any,
    primary_model: str = PRIMARY_MODEL,
    escalation_model: str | None = ESCALATION_MODEL,
    primary_attempts: int = PRIMARY_ATTEMPTS,
    max_output_tokens: int = MAX_OUTPUT_TOKENS,
    remediation_dir: Path | str = DEFAULT_REMEDIATION_DIR,
):
    """Compile the Silver review graph around existing policy functions."""

    def load_profile(state: SilverGraphState) -> SilverGraphState:
        path = Path(state["profile_path"])
        profile = json.loads(path.read_text())
        if not isinstance(profile, dict):
            raise ValueError("Silver profile must be a JSON object.")
        publication_state = profile.get("publication_state")
        if not isinstance(publication_state, str):
            raise ValueError("Silver publication_state must be a string.")
        return {"profile": profile, "publication_state": publication_state}

    def route_profile(state: SilverGraphState) -> str:
        return (
            "propose_remediation"
            if state["publication_state"] == "REVIEW_REQUIRED"
            else "complete_without_remediation"
        )

    def propose_remediation(state: SilverGraphState) -> SilverGraphState:
        if client is None:
            raise ValueError("An agent client is required before the human interrupt.")
        proposal = propose_silver_remediation(
            state["profile"],
            client,
            primary_model=primary_model,
            escalation_model=escalation_model,
            primary_attempts=primary_attempts,
            max_output_tokens=max_output_tokens,
        )
        return {
            "proposal": proposal.to_dict(),
            "status": "AWAITING_HUMAN_REVIEW",
        }

    def human_review(state: SilverGraphState) -> SilverGraphState:
        decision = interrupt(
            {
                "type": "SILVER_REMEDIATION_APPROVAL",
                "source_file_id": state["profile"].get("source_file_id"),
                "resolutions": state["proposal"]["resolutions"],
                "instruction": "Approve or reject deterministic Silver remediation.",
            }
        )
        if not isinstance(decision, dict):
            raise ValueError("Human decision must be an object.")
        action = decision.get("decision")
        reviewer = decision.get("reviewed_by")
        if action not in {"APPROVE", "REJECT"}:
            raise ValueError("Human decision must be APPROVE or REJECT.")
        if not isinstance(reviewer, str) or not reviewer.strip():
            raise ValueError("Human decision requires reviewed_by.")
        recorded = {
            "decision": action,
            "reviewed_by": reviewer.strip(),
            "reviewed_at": datetime.now(UTC).isoformat(),
            "proposal_run_id": state["proposal"]["run_id"],
            "proposal_hash": document_hash(state["proposal"]),
            "validation_profile_hash": document_hash(state["profile"]),
        }
        return {
            "human_decision": recorded,
            "status": (
                "REMEDIATION_APPROVED"
                if action == "APPROVE"
                else "REMEDIATION_REJECTED"
            ),
        }

    def route_human_decision(state: SilverGraphState) -> str:
        return (
            "execute_remediation"
            if state["status"] == "REMEDIATION_APPROVED"
            else END
        )

    def execute_remediation(state: SilverGraphState) -> SilverGraphState:
        execution_path = execute_silver_remediation(
            state["profile"],
            state["proposal"],
            state["human_decision"],
            output_dir=remediation_dir,
        )
        execution = json.loads(execution_path.read_text())
        return {"execution": execution, "status": str(execution["status"])}

    def route_execution(state: SilverGraphState) -> str:
        return (
            "review_held_partitions"
            if state["status"] == "REMEDIATION_REVIEW_REQUIRED"
            else END
        )

    def review_held_partitions(state: SilverGraphState) -> SilverGraphState:
        held = [
            {
                "workbook_path": item.get("workbook_path"),
                "row_count": item.get("row_count"),
                "review_findings": item.get("review_findings"),
            }
            for item in state["execution"].get("partitions", [])
            if isinstance(item, dict)
            and item.get("publication_state") == "REVIEW_REQUIRED"
        ]
        decision = interrupt(
            {
                "type": "SILVER_CHILD_REVIEW",
                "held_partitions": held,
                "allowed_decisions": [
                    "CONFIRM_VALID_DUPLICATES",
                    "DEDUPLICATE",
                    "REJECT",
                ],
                "instruction": "Choose an explicit policy for localized duplicates.",
            }
        )
        if not isinstance(decision, dict):
            raise ValueError("Follow-up decision must be an object.")
        execution_path = resolve_held_partitions(
            state["execution"], decision, output_dir=remediation_dir
        )
        execution = json.loads(execution_path.read_text())
        return {
            "execution": execution,
            "followup_decision": dict(decision),
            "status": str(execution["status"]),
        }

    def complete_without_remediation(state: SilverGraphState) -> SilverGraphState:
        publication_state = state["publication_state"]
        statuses = {
            "PUBLISHABLE": "SILVER_PUBLISHABLE",
            "PUBLISHABLE_WITH_WARNINGS": "SILVER_PUBLISHABLE_WITH_WARNINGS",
            "BLOCKED": "SILVER_BLOCKED",
        }
        if publication_state not in statuses:
            raise ValueError(f"Unsupported Silver publication state {publication_state!r}.")
        return {"status": statuses[publication_state]}

    builder = StateGraph(SilverGraphState)
    builder.add_node("load_profile", load_profile)
    builder.add_node("propose_remediation", propose_remediation)
    builder.add_node("human_review", human_review)
    builder.add_node("execute_remediation", execute_remediation)
    builder.add_node("review_held_partitions", review_held_partitions)
    builder.add_node("complete_without_remediation", complete_without_remediation)
    builder.add_edge(START, "load_profile")
    builder.add_conditional_edges("load_profile", route_profile)
    builder.add_edge("propose_remediation", "human_review")
    builder.add_conditional_edges("human_review", route_human_decision)
    builder.add_conditional_edges("execute_remediation", route_execution)
    builder.add_edge("review_held_partitions", END)
    builder.add_edge("complete_without_remediation", END)
    return builder.compile(checkpointer=checkpointer)


def _write_audit(graph: Any, config: dict[str, object], output_dir: Path) -> Path:
    snapshot = graph.get_state(config)
    thread_id = str(config["configurable"]["thread_id"])
    interrupts = [
        item.value
        for task in snapshot.tasks
        for item in getattr(task, "interrupts", ())
    ]
    audit = {
        "workflow_version": WORKFLOW_VERSION,
        "thread_id": thread_id,
        "updated_at": datetime.now(UTC).isoformat(),
        "next": list(snapshot.next),
        "interrupts": interrupts,
        **snapshot.values,
    }
    path = output_dir / thread_id / "workflow.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".json.tmp")
    temporary.write_text(json.dumps(audit, indent=2, sort_keys=True) + "\n")
    temporary.replace(path)
    return path


def _thread_id(value: str) -> str:
    try:
        return str(UUID(value))
    except ValueError as exc:
        raise argparse.ArgumentTypeError("thread id must be a UUID") from exc


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Run or resume persisted Silver remediation approval."
    )
    parser.add_argument("--checkpoint-db", type=Path, default=DEFAULT_CHECKPOINT_DB)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_WORKFLOW_DIR)
    subparsers = parser.add_subparsers(dest="command", required=True)

    start = subparsers.add_parser("start")
    start.add_argument("silver_profile", type=Path)
    start.add_argument("--thread-id", type=_thread_id, default=str(uuid4()))
    start.add_argument("--primary-model", default=PRIMARY_MODEL)
    start.add_argument("--escalation-model", default=ESCALATION_MODEL)
    start.add_argument("--primary-attempts", type=int, default=PRIMARY_ATTEMPTS)
    start.add_argument("--max-output-tokens", type=int, default=MAX_OUTPUT_TOKENS)

    resume = subparsers.add_parser("resume")
    resume.add_argument("thread_id", type=_thread_id)
    resume.add_argument(
        "--decision",
        choices=[
            "APPROVE",
            "CONFIRM_VALID_DUPLICATES",
            "DEDUPLICATE",
            "REJECT",
        ],
        required=True,
    )
    resume.add_argument("--reviewed-by", required=True)
    retry = subparsers.add_parser("retry")
    retry.add_argument("thread_id", type=_thread_id)
    args = parser.parse_args(argv)

    args.checkpoint_db.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(args.checkpoint_db, check_same_thread=False)
    try:
        checkpointer = SqliteSaver(connection)
        if args.command == "start":
            profile = json.loads(args.silver_profile.read_text())
            needs_agent = (
                isinstance(profile, dict)
                and profile.get("publication_state") == "REVIEW_REQUIRED"
            )
            if needs_agent and not os.environ.get("OPENAI_API_KEY"):
                parser.error(
                    "OPENAI_API_KEY is not exported; run "
                    "`set -a; source .env; set +a` first"
                )
            client = None
            if needs_agent:
                from openai import OpenAI

                client = OpenAI()
            graph = build_silver_remediation_graph(
                client,
                checkpointer=checkpointer,
                primary_model=args.primary_model,
                escalation_model=args.escalation_model,
                primary_attempts=args.primary_attempts,
                max_output_tokens=args.max_output_tokens,
            )
            config = {"configurable": {"thread_id": args.thread_id}}
            graph.invoke(
                {"profile_path": str(args.silver_profile.resolve())}, config=config
            )
        elif args.command == "resume":
            graph = build_silver_remediation_graph(None, checkpointer=checkpointer)
            config = {"configurable": {"thread_id": args.thread_id}}
            graph.invoke(
                Command(
                    resume={
                        "decision": args.decision,
                        "reviewed_by": args.reviewed_by,
                    }
                ),
                config=config,
            )
        else:
            graph = build_silver_remediation_graph(None, checkpointer=checkpointer)
            config = {"configurable": {"thread_id": args.thread_id}}
            graph.invoke(None, config=config)
        print(_write_audit(graph, config, args.output_dir))
    except (OSError, json.JSONDecodeError, ValueError, SilverRemediationError) as exc:
        parser.exit(1, f"silver graph failed: {exc}\n")
    finally:
        connection.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
