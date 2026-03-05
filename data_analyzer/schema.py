from pydantic import BaseModel, Field
from typing import List, Any, Dict, Optional


class QueryResult(BaseModel):
    columns: List[str]
    row_count: int
    rows: List[Dict[str, Any]]


class AnalysisSpec(BaseModel):
    primary_metric: str
    entity_label_col: str
    entity_id_col: str

    sort_direction: str = Field(default="desc", pattern="^(asc|desc)$")

    top_k: int = Field(default=3, ge=0)
    bottom_k: int = Field(default=3, ge=0)

    tie_epsilon: float = Field(default=1e-6, ge=0)

    language: str = "pl"


class AnalyzeRequest(BaseModel):
    question: Optional[str] = None
    result: QueryResult
    analysis_spec: AnalysisSpec


class Narrative(BaseModel):
    summary: str
    insights: List[str]
    caveats: List[str] = []


class AnalyzeResponse(BaseModel):
    facts: Dict[str, Any]
    narrative: Narrative