from __future__ import annotations

import logging
from typing import Any, Dict, Optional

from db.engine import engine
from db.execution import execute_sql

from intent_2.resolver import resolve
from intent_2.schemas import Resolution as IntentResolution

from sql_builder import build_sql
from sql_builder.sql_validator import validate_sql, SQLValidationError

from .types import QueryServiceResult


logger = logging.getLogger(__name__)


def run_question(
    question: str,
    *,
    limit: int = 500,
    statement_timeout_ms: int = 30_000,
    ui_language: Optional[str] = None,
) -> QueryServiceResult:
    """
    End-to-end runner:
    question -> intent_2 -> sql_builder -> validate -> execute_sql -> result
    """

    logger.info("query_service.run_question.start question=%s", question)

    intent_res: IntentResolution = resolve(question, ui_language=ui_language)

    if intent_res.action == "clarify":
        payload = intent_res.model_dump()
        logger.info("query_service.run_question.clarify")
        return QueryServiceResult(
            action="clarify",
            clarification=payload,
            alternatives=payload.get("alternatives") or [],
            assumptions=payload.get("assumptions") or [],
        )

    # execute
    assert intent_res.intent is not None

    logger.info(
        "query_service.run_question.execute intent entity=%s metric=%s analysis=%s",
        intent_res.intent.entity,
        intent_res.intent.metric,
        intent_res.intent.analysis_type,
    )

    plan = build_sql(intent_res.intent)
    sql = plan.sql
    params: Dict[str, Any] = plan.params or {}

    # Validate SQL (read-only, single statement, analytics-only, limit required)
    try:
        validate_sql(sql, require_limit=True, allowed_schemas=("analytics",))
    except SQLValidationError as e:
        logger.warning("query_service.sql_validation_failed error=%s", str(e))
        raise ValueError(f"SQL validation failed: {e}") from e

    # Optional: if you now allow trailing semicolon, normalize before execution
    sql = sql.rstrip().rstrip(";")

    logger.info("query_service.execute_sql.start limit=%s timeout_ms=%s", limit, statement_timeout_ms)

    res = execute_sql(
        engine=engine,
        sql=sql,
        params=params,
        limit=limit,
        statement_timeout_ms=statement_timeout_ms,
    )

    logger.info("query_service.execute_sql.done rows=%s", res.row_count)

    payload_intent = intent_res.intent.model_dump()
    payload_alts = [a.model_dump() for a in (intent_res.alternatives or [])]
    payload_assumptions = [a.message for a in (intent_res.assumptions or [])]

    return QueryServiceResult(
        action="execute",
        intent=payload_intent,
        sql=sql,
        params=params,
        data={"columns": res.columns, "row_count": res.row_count, "rows": res.rows},
        alternatives=payload_alts,
        assumptions=payload_assumptions,
    )