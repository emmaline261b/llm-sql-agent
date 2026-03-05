from __future__ import annotations

from typing import Optional

from .schemas import Resolution
from .rules.engine import try_resolve
from .alternatives.engine import generate_alternatives
from .llm.clarifier import clarify_with_llm
from .llm.guard import validate_or_retry

import logging

logger = logging.getLogger(__name__)


def resolve(
    question: str,
    ui_language: Optional[str] = None,
    *,
    enable_trace: bool = False
) -> Resolution:
    logger.info("intent.resolve.start question=%s", question)

    trace = {} if enable_trace else None

    # ---------------------------
    # Stage 1: rules
    # ---------------------------

    rules_result = try_resolve(question)
    logger.debug("intent.resolve.rules_checked")

    if rules_result is not None:

        if rules_result.action == "clarify":

            logger.info("intent.resolve.rules_clarify")
            res = Resolution(
                action="clarify",
                clarification_prompt=rules_result.clarification_prompt,
                alternatives=rules_result.alternatives or [],
                assumptions=[],
                trace=trace
            )

            return res

        if rules_result.action == "execute":

            logger.info("intent.resolve.rules_execute")
            alternatives = generate_alternatives(
                rules_result.intent,
                rules_result.assumptions or []
            )

            logger.debug("intent.resolve.alternatives_generated count=%s", len(alternatives))
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

    logger.info("intent.resolve.llm_fallback")
    raw_llm = clarify_with_llm(
        question=question,
        ui_language=ui_language
    )

    logger.debug("intent.resolve.llm_raw_action=%s", raw_llm.get("action"))

    decision = validate_or_retry(
        raw_llm,
        sticky_clarify=True
    )

    logger.info("intent.resolve.llm_decision action=%s", decision.action)
    # bezpieczeństwo: execute z LLM nie dostaje alternatives
    if decision.action == "execute":
        decision.alternatives = []
        logger.warning("intent.resolve.llm_execute_alternatives_removed")

    if enable_trace:
        decision.trace = trace

    logger.info("intent.resolve.end action=%s", decision.action)
    return decision