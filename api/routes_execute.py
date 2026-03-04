from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Any

from db.engine import engine
from db.execution import execute_sql
from llm.models import ChatRequest
from llm.planner import generate_plan


router = APIRouter(tags=["execution"])


class ExecuteRequest(BaseModel):
    sql: str
    params: dict[str, Any] = {}
    limit: int = 1000


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
        return {
            "columns": res.columns,
            "row_count": res.row_count,
            "rows": res.rows,
        }
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except RuntimeError as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/ask")
def ask(req: ChatRequest):
    """
    Full flow: question -> LLM plan (JSON) -> execute SQL -> return plan + data
    """
    plan = generate_plan(req.question)

    try:
        res = execute_sql(
            engine=engine,
            sql=plan["sql"],
            params=plan.get("params") or {},
            limit=1000,
            statement_timeout_ms=30_000,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"SQL rejected: {e}")
    except RuntimeError as e:
        raise HTTPException(status_code=500, detail=str(e))

    return {
        "plan": plan,
        "data": {
            "columns": res.columns,
            "row_count": res.row_count,
            "rows": res.rows,
        },
    }