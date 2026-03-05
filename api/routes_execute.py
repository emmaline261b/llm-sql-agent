# api/routes_execute.py
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Any

from db.engine import engine
from db.execution import execute_sql

from intent_clarifier.resolver import resolve
from llm_sql.llm.models import ChatRequest
from llm_sql.llm.planner import generate_plan

router = APIRouter(tags=["execution"])


class ExecuteRequest(BaseModel):
    sql: str
    params: dict[str, Any] = {}
    limit: int = 500


@router.post("/execute")
def execute(req: ExecuteRequest):
    try:
        res = execute_sql(
            engine=engine,
            sql=req.sql,
            params=req.params,
            limit=req.limit,
            statement_timeout_ms=30_000,
        )
        return {"columns": res.columns, "row_count": res.row_count, "rows": res.rows}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except RuntimeError as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/ask")
def ask(req: ChatRequest):
    # Intent v2 (rules-first, LLM fallback)
    intent_res = resolve(req.question)

    if intent_res.action == "clarify":
        # front should display clarification_prompt + alternatives buttons
        raise HTTPException(status_code=422, detail=intent_res.model_dump())

    # For now planner still works off natural language,
    # but we also pass structured intent to reduce ambiguity.
    plan = generate_plan(
        req.question,
        intent=intent_res.intent.model_dump(),
    )

    try:
        res = execute_sql(
            engine=engine,
            sql=plan["sql"],
            params=plan.get("params") or {},
            limit=500,
            statement_timeout_ms=30_000,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"SQL rejected: {e}")
    except RuntimeError as e:
        raise HTTPException(status_code=500, detail=str(e))

    return {
        "intent": intent_res.model_dump(),
        "plan": plan,
        "data": {"columns": res.columns, "row_count": res.row_count, "rows": res.rows},
    }