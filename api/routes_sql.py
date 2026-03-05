from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Any, Literal, Optional

from intent_clarifier.intent_schemas import Intent
from sql_builder import build_sql
import logging
logger = logging.getLogger(__name__)

router = APIRouter(tags=["2. sql-builder"])

IntentAction = Literal["clarify", "execute"]


class BuildSQLRequest(BaseModel):
    # This matches the /intent response shape you currently return
    action: IntentAction
    intent: Optional[dict[str, Any]] = None
    assumptions: list[str] = []
    questions: list[str] = []
    alternatives: list[dict[str, Any]] = []


class BuildSQLResponse(BaseModel):
    sql: str
    params: dict[str, Any] = {}


@router.post("/build-sql", response_model=BuildSQLResponse)
def build_sql_endpoint(req: BuildSQLRequest):
    if req.action == "clarify":
        raise HTTPException(
            status_code=422,
            detail={
                "error": "Intent requires clarification; SQL cannot be built.",
                "questions": req.questions,
                "alternatives": req.alternatives,
            },
        )

    if not req.intent:
        raise HTTPException(status_code=400, detail="Missing intent payload for action=execute.")

    try:
        intent = Intent(**req.intent)
        plan = build_sql(intent)
        return BuildSQLResponse(sql=plan.sql, params=plan.params)
    except NotImplementedError as e:
        raise HTTPException(status_code=400, detail=f"Intent not supported by sql_builder yet: {e}")
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))