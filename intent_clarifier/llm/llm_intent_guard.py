from __future__ import annotations

from typing import Any

from ..intent_schemas import Resolution


MAX_RETRIES = 2


def validate_or_retry(
    payload: dict,
    *,
    sticky_clarify: bool = True,
) -> Resolution:

    initial_action = payload.get("action")

    for _ in range(MAX_RETRIES):

        try:
            result = Resolution(**payload)

            if sticky_clarify and initial_action == "clarify":
                if result.action != "clarify":
                    raise ValueError("Sticky clarify violation")

            return result

        except Exception as exc:

            # minimal repair strategy
            payload = _repair_payload(payload, exc)

    raise ValueError("LLM output could not be validated")


def _repair_payload(payload: dict, exc: Exception) -> dict:

    # basic repair heuristics
    if "action" not in payload:
        payload["action"] = "clarify"

    if payload["action"] == "clarify":
        payload.setdefault(
            "clarification_prompt",
            "Could you clarify what exactly you want to analyze?"
        )
        payload.pop("intent", None)

    return payload