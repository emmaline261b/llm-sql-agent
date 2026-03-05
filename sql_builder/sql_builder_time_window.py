from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Dict, Any, Optional

from intent_clarifier.intent_schemas import TimeAxis, TimeWindow, TimeWindowMode


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class TimeFilter:
    """
    Time restriction for a fact table.

    - cte_sql: optional CTEs needed by the predicate
    - where_sql: SQL predicate for the fact date column
    - params: bind params for SQLAlchemy (:name style)
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

    Conventions:
    - sql_builder_service.py defines a CTE named "{fact_alias}_src" containing the fact rows.
    - This function uses SQLAlchemy bind params (":param") everywhere.

    Supports:
    - most_recent
    - last_n
    - between_dates (with placeholders)
      - CURRENT_YEAR_START / CURRENT_DATE
      - LAST_COMPLETED_QUARTER_START / LAST_COMPLETED_QUARTER_END
    """
    col = f"{fact_alias}.{axis.value}"

    logger.debug(
        "time_window.build.start mode=%s axis=%s fact_alias=%s",
        window.mode,
        axis,
        fact_alias,
    )

    if window.mode == TimeWindowMode.most_recent:
        cte = f"""
        latest_date AS (
          SELECT MAX({col}) AS dt
          FROM {fact_alias}_src {fact_alias}
        )
        """.strip()

        where_sql = f"{col} = (SELECT dt FROM latest_date)"
        tf = TimeFilter(cte_sql=cte, where_sql=where_sql, params={})

        logger.debug("time_window.build.done most_recent where=%s", tf.where_sql)
        return tf

    if window.mode == TimeWindowMode.last_n:
        n = window.n or 12
        cte = f"""
        last_dates AS (
          SELECT DISTINCT {col} AS dt
          FROM {fact_alias}_src {fact_alias}
          ORDER BY dt DESC
          LIMIT :last_n
        )
        """.strip()

        where_sql = f"{col} IN (SELECT dt FROM last_dates)"
        tf = TimeFilter(cte_sql=cte, where_sql=where_sql, params={"last_n": n})

        logger.debug("time_window.build.done last_n=%s where=%s", n, tf.where_sql)
        return tf

    if window.mode == TimeWindowMode.between_dates:
        start_sql, start_param = _resolve_date_placeholder(window.start_date, param_name="start_date")
        end_sql, end_param = _resolve_date_placeholder(window.end_date, param_name="end_date")

        where_sql = f"{col} BETWEEN {start_sql} AND {end_sql}"

        params: Dict[str, Any] = {}
        if start_param is not None:
            params["start_date"] = start_param
        if end_param is not None:
            params["end_date"] = end_param

        tf = TimeFilter(cte_sql="", where_sql=where_sql, params=params)

        logger.debug(
            "time_window.build.done between_dates where=%s params=%s",
            tf.where_sql,
            list(tf.params.keys()),
        )
        return tf

    logger.warning("time_window.build.unsupported mode=%s", window.mode)
    raise NotImplementedError(f"Unsupported time window mode: {window.mode}")


def _resolve_date_placeholder(value: Optional[str], *, param_name: str) -> tuple[str, Optional[str]]:
    """
    Returns (sql_fragment, param_value_or_none).

    - If returns a SQL expression, param_value_or_none is None.
    - If returns a bind parameter (e.g. ":start_date"), param_value_or_none is the value to bind.
    """
    if not value:
        raise ValueError("between_dates requires start_date and end_date")

    # Allow placeholders as SQL expressions (not params)
    if value == "CURRENT_DATE":
        return "CURRENT_DATE", None

    if value == "CURRENT_YEAR_START":
        return "date_trunc('year', CURRENT_DATE)::date", None

    # Closed quarter window placeholders (last completed quarter)
    if value == "LAST_COMPLETED_QUARTER_START":
        # Start of last completed quarter:
        # e.g. if today is in Q2, this becomes Q1 start (Jan 1)
        return "(date_trunc('quarter', CURRENT_DATE) - interval '3 months')::date", None

    if value == "LAST_COMPLETED_QUARTER_END":
        # End of last completed quarter:
        # e.g. if today is in Q2, this becomes Mar 31
        return "(date_trunc('quarter', CURRENT_DATE) - interval '1 day')::date", None

    # Otherwise treat as a literal date string bound as a parameter
    return f":{param_name}", value