from __future__ import annotations

from typing import Any

from ._client_core import CollectionPage, _path_segment


class IntelligenceEndpoints:
    def list_spaces(self, **query: Any) -> CollectionPage:
        return self.list("/v1/spaces", query=query)

    def get_space(self, space_id: str) -> dict[str, Any]:
        return self.get(f"/v1/spaces/{space_id}")

    def ask_space(
        self,
        space_id: str,
        input_data: dict[str, Any],
        *,
        idempotency_key: str | None = None,
    ) -> dict[str, Any]:
        space_key = _path_segment(space_id)
        return self.post(
            f"/v1/spaces/{space_key}/ask",
            input_data,
            idempotency_key=idempotency_key,
        )

    def ask_connected_chat_space(
        self,
        input_data: dict[str, Any] | None = None,
        *,
        idempotency_key: str | None = None,
    ) -> dict[str, Any]:
        return self.post("/v1/chat/ask", input_data or {}, idempotency_key=idempotency_key)

    def build_space_context_pack(self, space_id: str, input_data: dict[str, Any] | None = None) -> dict[str, Any]:
        space_key = _path_segment(space_id)
        return self.post(f"/v1/spaces/{space_key}/context-pack", input_data or {})

    def build_connected_chat_context_pack(self, input_data: dict[str, Any] | None = None) -> dict[str, Any]:
        return self.post("/v1/chat/context-pack", input_data or {})

    def preview_handoff(self, space_id: str, input_data: dict[str, Any]) -> dict[str, Any]:
        space_key = _path_segment(space_id)
        return self.post(f"/v1/spaces/{space_key}/handoff-preview/view", input_data, effect="read")

    def get_context_plan(self, space_id: str, input_data: dict[str, Any]) -> dict[str, Any]:
        space_key = _path_segment(space_id)
        return self.post(f"/v1/spaces/{space_key}/context-plan/view", input_data, effect="read")

    def create_connected_chat_session_digest(self, input_data: dict[str, Any] | None = None) -> dict[str, Any]:
        return self.post("/v1/chat/session-digest", input_data or {})

    def list_space_answers(self, space_id: str, **query: Any) -> CollectionPage:
        space_key = _path_segment(space_id)
        return self.list(f"/v1/spaces/{space_key}/answers", query=query)

    def get_space_answer(self, space_id: str, answer_id: str) -> dict[str, Any]:
        space_key = _path_segment(space_id)
        answer_key = _path_segment(answer_id)
        return self.get(f"/v1/spaces/{space_key}/answers/{answer_key}")

    def get_space_decision_graph(self, space_id: str) -> dict[str, Any]:
        space_key = _path_segment(space_id)
        return self.get(f"/v1/spaces/{space_key}/decision-graph")

    def get_space_answer_contract(self, space_id: str) -> dict[str, Any]:
        space_key = _path_segment(space_id)
        return self.get(f"/v1/spaces/{space_key}/answer-contract")

    def list_space_learning_candidates(self, space_id: str, **query: Any) -> CollectionPage:
        space_key = _path_segment(space_id)
        return self.list(f"/v1/spaces/{space_key}/learning-candidates", query=query)

    def create_space_learning_candidate(self, space_id: str, input_data: dict[str, Any]) -> dict[str, Any]:
        space_key = _path_segment(space_id)
        return self.post(f"/v1/spaces/{space_key}/learning-candidates", input_data)

    def create_space_learning_candidate_from_artifact(self, space_id: str, input_data: dict[str, Any]) -> dict[str, Any]:
        space_key = _path_segment(space_id)
        return self.post(f"/v1/spaces/{space_key}/learning-candidates/from-artifact", input_data)

    def promote_space_learning_candidate(self, space_id: str, candidate_id: str) -> dict[str, Any]:
        space_key = _path_segment(space_id)
        candidate_key = _path_segment(candidate_id)
        return self.post(f"/v1/spaces/{space_key}/learning-candidates/{candidate_key}/promote")

    def reject_space_learning_candidate(self, space_id: str, candidate_id: str, input_data: dict[str, Any] | None = None) -> dict[str, Any]:
        space_key = _path_segment(space_id)
        candidate_key = _path_segment(candidate_id)
        return self.post(f"/v1/spaces/{space_key}/learning-candidates/{candidate_key}/reject", input_data or {})

    def correct_space_learning_candidate(
        self,
        space_id: str,
        candidate_id: str,
        input_data: dict[str, Any],
        *,
        idempotency_key: str | None = None,
    ) -> dict[str, Any]:
        space_key = _path_segment(space_id)
        candidate_key = _path_segment(candidate_id)
        return self.post(
            f"/v1/spaces/{space_key}/learning-candidates/{candidate_key}/corrections",
            input_data,
            idempotency_key=idempotency_key,
        )

    def supersede_space_learning_candidate(
        self,
        space_id: str,
        candidate_id: str,
        input_data: dict[str, Any],
        *,
        idempotency_key: str | None = None,
    ) -> dict[str, Any]:
        space_key = _path_segment(space_id)
        candidate_key = _path_segment(candidate_id)
        return self.post(
            f"/v1/spaces/{space_key}/learning-candidates/{candidate_key}/supersede",
            input_data,
            idempotency_key=idempotency_key,
        )

    def list_space_promotion_gates(self, space_id: str, **query: Any) -> CollectionPage:
        space_key = _path_segment(space_id)
        return self.list(f"/v1/spaces/{space_key}/promotion-gates", query=query)

    def list_space_intelligence_projections(self, space_id: str, **query: Any) -> CollectionPage:
        space_key = _path_segment(space_id)
        return self.list(f"/v1/spaces/{space_key}/projections", query=query)

    def list_space_learning_queue_items(self, space_id: str, **query: Any) -> CollectionPage:
        space_key = _path_segment(space_id)
        return self.list(f"/v1/spaces/{space_key}/learning-queue", query=query)

    def create_space_learning_queue_item(self, space_id: str, input_data: dict[str, Any]) -> dict[str, Any]:
        space_key = _path_segment(space_id)
        return self.post(f"/v1/spaces/{space_key}/learning-queue", input_data)

    def claim_space_learning_queue_item(self, space_id: str, input_data: dict[str, Any] | None = None, *, idempotency_key: str | None = None) -> dict[str, Any]:
        space_key = _path_segment(space_id)
        return self.post(f"/v1/spaces/{space_key}/learning-queue/claim", input_data or {}, idempotency_key=idempotency_key)

    def complete_space_learning_queue_item(
        self, space_id: str, item_id: str, input_data: dict[str, Any]
    ) -> dict[str, Any]:
        space_key = _path_segment(space_id)
        item_key = _path_segment(item_id)
        return self.post(f"/v1/spaces/{space_key}/learning-queue/{item_key}/complete", input_data)

    def submit_space_learning_queue_result(self, space_id: str, item_id: str, input_data: dict[str, Any]) -> dict[str, Any]:
        space_key = _path_segment(space_id)
        item_key = _path_segment(item_id)
        return self.post(f"/v1/spaces/{space_key}/learning-queue/{item_key}/result", input_data)

    def get_space_learning_frontier(self, space_id: str) -> dict[str, Any]:
        space_key = _path_segment(space_id)
        return self.get(f"/v1/spaces/{space_key}/learning-frontier")
