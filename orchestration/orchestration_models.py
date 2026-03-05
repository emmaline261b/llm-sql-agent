from pydantic import BaseModel, Field
from typing import Any, Dict, List, Optional, Literal

class Assumption(BaseModel):
    code: str
    message: str
    details: Dict[str, Any] = {}

class AskRequest(BaseModel):
    question: str
    limit: int = Field(default=500, ge=1, le=5000)
    analyze: bool = True


class AskResponse(BaseModel):
    action: str  # "clarify" | "execute"

    normalized_question: Optional[str] = None
    questions: List[str] = []
    assumptions: List[Assumption] = []
    notes: List[str] = []
    alternatives: List[Dict[str, Any]] = []

    data: Optional[Dict[str, Any]] = None
    facts: Optional[Dict[str, Any]] = None
    ai_insights: Optional[Dict[str, Any]] = None