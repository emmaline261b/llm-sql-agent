from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional


@dataclass(frozen=True)
class SQLPlan:
    sql: str
    params: Dict[str, Any]
    result_shape: str = "table"
    chart: Optional[dict] = None