# api/routes_intent.py

from fastapi import APIRouter
from pydantic import BaseModel
from typing import Literal, Optional

from intent_2.resolver import resolve
from llm_sql.llm.models import ChatRequest


router = APIRouter(prefix="", tags=["intent"])

IntentAction = Literal["clarify", "execute"]


class IntentResponse(BaseModel):
    action: IntentAction
    questions: list[str] = []
    normalized_question: str | None = None
    assumptions: list[str] = []
    notes: list[str] = []
    intent: Optional[dict] = None
    alternatives: list[dict] = []


@router.post("/intent", response_model=IntentResponse)
def intent(req: ChatRequest):

    result = resolve(req.question)

    if result.action == "clarify":
        return IntentResponse(
            action="clarify",
            questions=[result.clarification_prompt],
            alternatives=[a.model_dump() for a in result.alternatives],
        )

    return IntentResponse(
        action="execute",
        intent=result.intent.model_dump(),
        assumptions=[a.message for a in result.assumptions],
        alternatives=[a.model_dump() for a in result.alternatives],
    )