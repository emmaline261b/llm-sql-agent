from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Any, Tuple, Optional

from intent_clarifier.schemas import TimeAxis, TimeWindow, TimeWindowMode


@dataclass(frozen=True)
class TimeFilter:
    """
    A small representation of time restriction.
    - where_sql: SQL predicate for the fact date column (e.g., "fh.report_date = (select max...)")
    - params: parameters used in the predicate
    - cte_sql: optional CTEs needed by the predicate
    """
    cte_sql: str
    where_sql: str
    params: Dict[str, Any]


def build_time_filter(
    *,
    axis: TimeAxis,
    window: TimeWindow,
    fact_alias: str,
) -> TimeFilter:
    """
    Converts intent.time_window to SQL for a given fact alias.
    Supports:
    - most_recent
    - last_n
    - between_dates (incl. CURRENT_YEAR_START/CURRENT_DATE placeholders)
    """
    col = f"{fact_alias}.{axis.value}"

    if window.mode == TimeWindowMode.most_recent:
        cte = f"""
        latest_date AS (
          SELECT MAX({col}) AS dt
          FROM {fact_alias}_src
        )
        """.strip()
        # note: {fact_alias}_src is a convention; build.py will define it as a CTE
        where_sql = f"{col} = (SELECT dt FROM latest_date)"
        return TimeFilter(cte_sql=cte, where_sql=where_sql, params={})

    if window.mode == TimeWindowMode.last_n:
        n = window.n or 12
        cte = f"""
        last_dates AS (
          SELECT DISTINCT {col} AS dt
          FROM {fact_alias}_src
          ORDER BY dt DESC
          LIMIT %(last_n)s
        )
        """.strip()
        where_sql = f"{col} IN (SELECT dt FROM last_dates)"
        return TimeFilter(cte_sql=cte, where_sql=where_sql, params={"last_n": n})

    if window.mode == TimeWindowMode.between_dates:
        start = _resolve_date_placeholder(window.start_date)
        end = _resolve_date_placeholder(window.end_date)
        where_sql = f"{col} BETWEEN %(start_date)s AND %(end_date)s"
        return TimeFilter(cte_sql="", where_sql=where_sql, params={"start_date": start, "end_date": end})

    raise NotImplementedError(f"Unsupported time window mode: {window.mode}")


def _resolve_date_placeholder(value: Optional[str]) -> str:
    if not value:
        raise ValueError("between_dates requires start_date and end_date")

    if value == "CURRENT_DATE":
        return "CURRENT_DATE"

    if value == "CURRENT_YEAR_START":
        # Postgres expression for Jan 1 of current year:
        return "date_trunc('year', CURRENT_DATE)::date"

    # assume caller passes ISO date literal; keep as-is (param value)
    return value