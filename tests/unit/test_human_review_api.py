from __future__ import annotations

import unittest
from uuid import UUID

from fastapi.testclient import TestClient

from analystops.api.human_review import (
    ReviewNotFoundError,
    ReviewPrincipal,
    TokenRegistry,
    create_app,
)


CLIENT_ID = UUID("a599f9da-35b3-5ada-89c4-67ff449294d6")
THREAD_ID = UUID("11111111-1111-4111-8111-111111111111")
TOKEN = "review-token-with-enough-entropy"


class FakeReviewService:
    def __init__(self):
        self.calls: list[tuple[object, ...]] = []

    def list_pending(self, client_id: UUID) -> list[dict[str, object]]:
        self.calls.append(("list", client_id))
        return [{"thread_id": str(THREAD_ID), "status": "AWAITING_HUMAN_REVIEW"}]

    def get_review(self, client_id: UUID, thread_id: UUID) -> dict[str, object]:
        self.calls.append(("get", client_id, thread_id))
        if thread_id != THREAD_ID:
            raise ReviewNotFoundError("Review thread was not found.")
        return {"client_id": str(client_id), "thread_id": str(thread_id)}

    def decide(
        self, principal: ReviewPrincipal, thread_id: UUID, decision: str
    ) -> dict[str, object]:
        self.calls.append(("decide", principal, thread_id, decision))
        return {
            "client_id": str(principal.client_id),
            "reviewed_by": principal.reviewed_by,
            "decision": decision,
        }


class HumanReviewApiTests(unittest.TestCase):
    def setUp(self) -> None:
        self.service = FakeReviewService()
        registry = TokenRegistry(
            {TOKEN: ReviewPrincipal(CLIENT_ID, "reviewer@example.com")}
        )
        self.client = TestClient(
            create_app(service=self.service, tokens=registry)
        )
        self.headers = {"Authorization": f"Bearer {TOKEN}"}

    def test_authentication_and_tenant_identity_are_server_derived(self) -> None:
        self.assertEqual(self.client.get("/health").status_code, 200)
        self.assertEqual(self.client.get("/v1/reviews").status_code, 401)
        self.assertEqual(
            self.client.get(
                "/v1/reviews",
                headers={"Authorization": "Bearer invalid-token-value"},
            ).status_code,
            401,
        )

        response = self.client.get("/v1/reviews", headers=self.headers)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["client_id"], str(CLIENT_ID))
        self.assertEqual(self.service.calls[-1], ("list", CLIENT_ID))

    def test_review_decision_uses_authenticated_reviewer(self) -> None:
        response = self.client.post(
            f"/v1/reviews/{THREAD_ID}/decisions",
            headers=self.headers,
            json={"decision": "REJECT"},
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["reviewed_by"], "reviewer@example.com")
        self.assertEqual(self.service.calls[-1][3], "REJECT")
        self.assertEqual(
            self.client.post(
                f"/v1/reviews/{THREAD_ID}/decisions",
                headers=self.headers,
                json={"decision": "ARBITRARY_CODE"},
            ).status_code,
            422,
        )

    def test_not_found_is_a_scoped_404(self) -> None:
        response = self.client.get(
            f"/v1/reviews/{UUID(int=0)}", headers=self.headers
        )

        self.assertEqual(response.status_code, 404)
        self.assertEqual(response.json()["detail"], "Review thread was not found.")

    def test_token_registry_rejects_weak_or_malformed_configuration(self) -> None:
        with self.assertRaisesRegex(ValueError, "at least 16"):
            TokenRegistry.from_json(
                '{"short":{"client_id":"%s","reviewed_by":"reviewer"}}'
                % CLIENT_ID
            )
        with self.assertRaisesRegex(ValueError, "valid JSON"):
            TokenRegistry.from_json("{")


if __name__ == "__main__":
    unittest.main()
