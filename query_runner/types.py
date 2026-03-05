from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Literal, Optional


Action = Literal["clarify", "execute"]


@dataclass(frozen=True)
class QueryServiceResult:
    action: Action

    # clarify
    clarification: Optional[dict] = None

    # execute
    intent: Optional[dict] = None
    sql: Optional[str] = None
    params: Optional[Dict[str, Any]] = None
    data: Optional[dict] = None

    # for UI
    alternatives: Optional[list[dict]] = None
    assumptions: Optional[list[str]] = None