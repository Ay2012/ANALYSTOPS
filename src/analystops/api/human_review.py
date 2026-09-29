"""Authenticated HTTP boundary for pending Silver remediation reviews."""

from __future__ import annotations

import argparse
import hmac
import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, Mapping
from uuid import UUID

import psycopg
import uvicorn
from fastapi import Depends, FastAPI, HTTPException, status
from fastapi.responses import JSONResponse
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from langgraph.types import Command
from pydantic import BaseModel

from analystops.persistence.postgres import DEFAULT_DATABASE_URL
from analystops.workflows.silver_remediation_graph import (
    DEFAULT_WORKFLOW_DIR,
    build_silver_remediation_graph,
    checkpoint_config,
    checkpoint_lock,
    postgres_checkpointer,
    write_workflow_audit,
)


TOKEN_ENV = "ANALYSTOPS_REVIEW_TOKENS"
ALLOWED_DECISIONS = {
    "SILVER_REMEDIATION_APPROVAL": {"APPROVE", "REJECT"},
    "SILVER_CHILD_REVIEW": {
        "CONFIRM_VALID_DUPLICATES",
        "DEDUPLICATE",
        "REJECT",
    },
}


class ReviewNotFoundError(ValueError):
    pass


class ReviewConflictError(ValueError):
    pass


@dataclass(frozen=True)
class ReviewPrincipal:
    client_id: UUID
    reviewed_by: str


class TokenRegistry:
    def __init__(self, entries: Mapping[str, ReviewPrincipal] | None = None):
        self._entries = tuple(
            (token, principal) for token, principal in (entries or {}).items()
        )

    @classmethod
    def from_json(cls, raw: str | None) -> TokenRegistry:
        if not raw:
            return cls()
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise ValueError(f"{TOKEN_ENV} must be valid JSON.") from exc
        if not isinstance(payload, dict):
            raise ValueError(f"{TOKEN_ENV} must be a JSON object.")

        entries: dict[str, ReviewPrincipal] = {}
        for token, identity in payload.items():
            if not isinstance(token, str) or len(token) < 16:
                raise ValueError("Review bearer tokens must contain at least 16 characters.")
            if not isinstance(identity, dict):
                raise ValueError("Each review token must map to an identity object.")
            try:
                client_id = UUID(str(identity["client_id"]))
                reviewed_by = str(identity["reviewed_by"]).strip()
            except (KeyError, ValueError) as exc:
                raise ValueError(
                    "Each review identity requires client_id and reviewed_by."
                ) from exc
            if not reviewed_by:
                raise ValueError("reviewed_by must be non-empty.")
            entries[token] = ReviewPrincipal(client_id, reviewed_by)
        return cls(entries)

    @classmethod
    def from_environment(cls) -> TokenRegistry:
        return cls.from_json(os.environ.get(TOKEN_ENV))

    @property
    def configured(self) -> bool:
        return bool(self._entries)

    def authenticate(self, token: str) -> ReviewPrincipal | None:
        principal = None
        for expected, candidate in self._entries:
            if hmac.compare_digest(token, expected):
                principal = candidate
        return principal


class ReviewDecision(BaseModel):
    decision: Literal[
        "APPROVE",
        "CONFIRM_VALID_DUPLICATES",
        "DEDUPLICATE",
        "REJECT",
    ]


class ReviewService:
    def __init__(
        self,
        database_url: str = DEFAULT_DATABASE_URL,
        output_dir: Path | str = DEFAULT_WORKFLOW_DIR,
    ):
        self.database_url = database_url
        self.output_dir = Path(output_dir)

    def list_pending(self, client_id: UUID) -> list[dict[str, object]]:
        prefix = f"{client_id}:%"
        reviews = []
        with postgres_checkpointer(self.database_url) as checkpointer:
            with psycopg.connect(self.database_url) as connection:
                rows = connection.execute(
                    """
                    SELECT DISTINCT thread_id
                    FROM analystops.checkpoints
                    WHERE thread_id LIKE %s
                    ORDER BY thread_id
                    """,
                    (prefix,),
                ).fetchall()
            graph = build_silver_remediation_graph(None, checkpointer=checkpointer)
            for (storage_thread_id,) in rows:
                thread_id = storage_thread_id.rsplit(":", 1)[-1]
                snapshot = graph.get_state(checkpoint_config(thread_id, str(client_id)))
                interrupts = _interrupts(snapshot)
                if interrupts:
                    reviews.append(
                        {
                            "thread_id": thread_id,
                            "status": snapshot.values.get("status"),
                            "review_type": interrupts[0].get("type"),
                            "source_file_id": snapshot.values.get("profile", {}).get(
                                "source_file_id"
                            ),
                        }
                    )
        return reviews

    def get_review(self, client_id: UUID, thread_id: UUID) -> dict[str, object]:
        with postgres_checkpointer(self.database_url) as checkpointer:
            graph = build_silver_remediation_graph(None, checkpointer=checkpointer)
            snapshot = graph.get_state(
                checkpoint_config(str(thread_id), str(client_id))
            )
            if not snapshot.values:
                raise ReviewNotFoundError("Review thread was not found.")
            return _review_document(client_id, thread_id, snapshot)

    def decide(
        self,
        principal: ReviewPrincipal,
        thread_id: UUID,
        decision: str,
    ) -> dict[str, object]:
        config = checkpoint_config(str(thread_id), str(principal.client_id))
        storage_thread_id = str(config["configurable"]["thread_id"])
        with checkpoint_lock(self.database_url, storage_thread_id):
            with postgres_checkpointer(self.database_url) as checkpointer:
                graph = build_silver_remediation_graph(
                    None, checkpointer=checkpointer
                )
                snapshot = graph.get_state(config)
                if not snapshot.values:
                    raise ReviewNotFoundError("Review thread was not found.")
                interrupts = _interrupts(snapshot)
                if not interrupts:
                    raise ReviewConflictError(
                        "Review thread is not awaiting a decision."
                    )
                review_type = str(interrupts[0].get("type"))
                if decision not in ALLOWED_DECISIONS.get(review_type, set()):
                    raise ReviewConflictError(
                        f"{decision} is not allowed for {review_type}."
                    )
                graph.invoke(
                    Command(
                        resume={
                            "decision": decision,
                            "reviewed_by": principal.reviewed_by,
                        }
                    ),
                    config=config,
                )
                audit_path = write_workflow_audit(graph, config, self.output_dir)
                result = _review_document(
                    principal.client_id,
                    thread_id,
                    graph.get_state(config),
                )
                result["audit_path"] = str(audit_path)
                return result


def _interrupts(snapshot: object) -> list[dict[str, object]]:
    return [
        dict(item.value)
        for task in getattr(snapshot, "tasks", ())
        for item in getattr(task, "interrupts", ())
        if isinstance(item.value, dict)
    ]


def _review_document(
    client_id: UUID, thread_id: UUID, snapshot: object
) -> dict[str, object]:
    interrupts = _interrupts(snapshot)
    return {
        "client_id": str(client_id),
        "thread_id": str(thread_id),
        "pending": bool(interrupts),
        "next": list(snapshot.next),
        "interrupts": interrupts,
        "state": dict(snapshot.values),
    }


def create_app(
    *,
    service: ReviewService | None = None,
    tokens: TokenRegistry | None = None,
) -> FastAPI:
    review_service = service or ReviewService(
        database_url=(
            os.environ.get("CHECKPOINT_DATABASE_URL")
            or os.environ.get("DATABASE_URL")
            or DEFAULT_DATABASE_URL
        )
    )
    token_registry = tokens or TokenRegistry.from_environment()
    bearer = HTTPBearer(auto_error=False)
    application = FastAPI(
        title="AnalystOps Human Review API",
        version="1.0.0",
    )

    def authenticate(
        credentials: HTTPAuthorizationCredentials | None = Depends(bearer),
    ) -> ReviewPrincipal:
        if credentials is None:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Bearer token required.",
                headers={"WWW-Authenticate": "Bearer"},
            )
        principal = token_registry.authenticate(credentials.credentials)
        if principal is None:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid bearer token.",
                headers={"WWW-Authenticate": "Bearer"},
            )
        return principal

    @application.exception_handler(ReviewNotFoundError)
    def review_not_found(_request: object, exc: ReviewNotFoundError) -> JSONResponse:
        return JSONResponse(status_code=404, content={"detail": str(exc)})

    @application.exception_handler(ReviewConflictError)
    def review_conflict(_request: object, exc: ReviewConflictError) -> JSONResponse:
        return JSONResponse(status_code=409, content={"detail": str(exc)})

    @application.get("/health")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    @application.get("/v1/reviews")
    def list_reviews(
        principal: ReviewPrincipal = Depends(authenticate),
    ) -> dict[str, object]:
        return {
            "client_id": str(principal.client_id),
            "reviews": review_service.list_pending(principal.client_id),
        }

    @application.get("/v1/reviews/{thread_id}")
    def get_review(
        thread_id: UUID,
        principal: ReviewPrincipal = Depends(authenticate),
    ) -> dict[str, object]:
        return review_service.get_review(principal.client_id, thread_id)

    @application.post("/v1/reviews/{thread_id}/decisions")
    def decide_review(
        thread_id: UUID,
        payload: ReviewDecision,
        principal: ReviewPrincipal = Depends(authenticate),
    ) -> dict[str, object]:
        return review_service.decide(principal, thread_id, payload.decision)

    return application


app = create_app()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Serve the human review API.")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
    args = parser.parse_args(argv)
    tokens = TokenRegistry.from_environment()
    if not tokens.configured:
        parser.error(f"{TOKEN_ENV} must configure at least one reviewer token")
    uvicorn.run(create_app(tokens=tokens), host=args.host, port=args.port)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
