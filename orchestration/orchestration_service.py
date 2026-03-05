import logging
from typing import Any, Dict

from db.db_engine import engine
from db.db_execution import execute_sql

from intent_clarifier.intent_resolver import resolve
from sql_builder.sql_builder_service import build_sql

from data_analyzer.data_analyzer import analyze as analyze_facts
from data_analyzer.data_schema import QueryResult

from llm_sql.llm.client_ollama import call_ollama_json
from data_analyzer.data_prompts import SYSTEM_PL, build_user_prompt

from .orchestration_models import AskRequest, AskResponse
from .orchestration_mapping import analysis_spec_from_intent

from .orchestration_errors import (
    IntentResolutionError,
    SQLBuildError,
    QueryExecutionError,
    AnalysisError,
    LLMError,
)

logger = logging.getLogger(__name__)


def _safe_intent_payload(intent_res) -> Dict[str, Any]:
    d = intent_res.model_dump() if hasattr(intent_res, "model_dump") else dict(intent_res)
    return {
        "action": d.get("action"),
        "normalized_question": d.get("normalized_question"),
        "questions": d.get("questions") or [],
        "assumptions": d.get("assumptions") or [],
        "notes": d.get("notes") or [],
        "alternatives": d.get("alternatives") or [],
    }



def handle_ask(req: AskRequest, *, llm_model: str = "qwen2.5:7b-instruct") -> AskResponse:
    logger.info("ask.start question=%s", req.question)

    try:
        intent_res = resolve(req.question)
    except Exception as e:
        raise IntentResolutionError(str(e)) from e

    safe = _safe_intent_payload(intent_res)

    action = safe.get("action")
    if action == "clarify":
        logger.info("ask.clarify questions=%s alternatives=%s",
                    len(safe["questions"]), len(safe["alternatives"]))
        return AskResponse(**safe)

    if action != "execute":
        logger.warning("ask.unknown_action action=%s", action)
        return AskResponse(**safe)

    # Deterministyczny SQL plan (nie ujawniamy)
    try:
        plan = build_sql(intent_res.intent)
    except Exception as e:
        raise SQLBuildError(str(e)) from e

    # Execute SQL
    try:
        res = execute_sql(
            engine=engine,
            sql=plan.sql if hasattr(plan, "sql") else plan["sql"],
            params=plan.params if hasattr(plan, "params") else (plan.get("params") or {}),
            limit=req.limit,
            statement_timeout_ms=30_000,
        )
    except ValueError as e:
        raise QueryExecutionError(f"SQL rejected: {e}") from e
    except RuntimeError as e:
        raise QueryExecutionError(str(e)) from e

    data = {"columns": res.columns, "row_count": res.row_count, "rows": res.rows}
    logger.info("ask.run_sql.done rows=%s", res.row_count)

    facts = None
    ai_insights = None

    if req.analyze:
        analysis_spec = analysis_spec_from_intent(intent_res)
        try:
            facts = analyze_facts(QueryResult(**data), analysis_spec)
        except Exception as e:
            raise AnalysisError(str(e)) from e

        try:
            ai_insights = call_ollama_json(
                model=llm_model,
                system=SYSTEM_PL,
                user=build_user_prompt(req.question, facts, language=analysis_spec.language),
            )
        except Exception as e:
            raise LLMError(str(e)) from e

    logger.info("ask.done")
    return AskResponse(
        **safe,
        data=data,
        facts=facts,
        ai_insights=ai_insights,
    )