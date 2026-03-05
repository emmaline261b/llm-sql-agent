from __future__ import annotations

from typing import Optional

from .schemas import Resolution
from .rules.engine import try_resolve
from .alternatives.engine import generate_alternatives
from .llm.clarifier import clarify_with_llm
from .llm.guard import validate_or_retry


def resolve(
    question: str,
    ui_language: Optional[str] = None,
    *,
    enable_trace: bool = False
) -> Resolution:

    trace = {} if enable_trace else None

    # ---------------------------
    # Stage 1: rules
    # ---------------------------

    rules_result = try_resolve(question)

    if rules_result is not None:

        if rules_result.action == "clarify":

            res = Resolution(
                action="clarify",
                clarification_prompt=rules_result.clarification_prompt,
                alternatives=rules_result.alternatives or [],
                assumptions=[],
                trace=trace
            )

            return res

        if rules_result.action == "execute":

            alternatives = generate_alternatives(
                rules_result.intent,
                rules_result.assumptions or []
            )

            res = Resolution(
                action="execute",
                intent=rules_result.intent,
                alternatives=alternatives,
                assumptions=rules_result.assumptions or [],
                trace=trace
            )

            return res

    # ---------------------------
    # Stage 2: LLM fallback
    # ---------------------------

    raw_llm = clarify_with_llm(
        question=question,
        ui_language=ui_language
    )

    decision = validate_or_retry(
        raw_llm,
        sticky_clarify=True
    )

    # bezpieczeństwo: execute z LLM nie dostaje alternatives
    if decision.action == "execute":
        decision.alternatives = []

    if enable_trace:
        decision.trace = trace

    return decision