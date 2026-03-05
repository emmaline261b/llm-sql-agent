from __future__ import annotations

from typing import Dict, Any
import logging

from intent_clarifier.intent_schemas import Intent, AnalysisType, Metric, Entity, Scope, TimeAxis
from .sql_builder_validator import validate_sql
from .sql_builder_types import SQLPlan
from .sql_builder_postgres import q_table
from .sql_builder_time_window import build_time_filter

logger = logging.getLogger(__name__)

def _with_clause(ctes: list[str]) -> str:
    ctes = [c.strip() for c in (ctes or []) if c and c.strip()]
    if not ctes:
        return ""
    return "WITH\n" + ",\n".join(ctes) + "\n"

def build_sql(intent: Intent) -> SQLPlan:
    """
    Deterministic SQL builder for a subset of intents.
    Expand supported cases gradually.
    """

    logger.info(
        "sql_builder.start analysis=%s entity=%s metric=%s scope=%s",
        intent.analysis_type,
        intent.entity,
        intent.metric,
        intent.scope,
    )

    if intent.analysis_type == AnalysisType.rank:
        return _build_rank(intent)

    logger.warning("sql_builder.unsupported_analysis analysis=%s", intent.analysis_type)
    raise NotImplementedError(f"Unsupported analysis_type: {intent.analysis_type}")


def _build_rank(intent: Intent) -> SQLPlan:
    logger.debug(
        "sql_builder.rank.start entity=%s metric=%s scope=%s",
        intent.entity,
        intent.metric,
        intent.scope,
    )

    if intent.scope != Scope.universe:
        logger.warning("sql_builder.rank.unsupported_scope scope=%s", intent.scope)
        raise NotImplementedError("rank currently supports only scope=universe")

    if intent.entity != Entity.fund:
        logger.warning("sql_builder.rank.unsupported_entity entity=%s", intent.entity)
        raise NotImplementedError("rank currently supports only entity=fund")

    if intent.metric in (Metric.market_value, Metric.weight_pct, Metric.shares):
        logger.debug("sql_builder.rank.path holdings_metric")
        return _rank_funds_by_holdings_metric(intent)

    if intent.metric == Metric.total_return:
        logger.debug("sql_builder.rank.path total_return")
        return _rank_funds_by_total_return(intent)

    logger.warning("sql_builder.rank.unsupported_metric metric=%s", intent.metric)
    raise NotImplementedError(f"Unsupported metric for rank: {intent.metric}")


def _rank_funds_by_holdings_metric(intent: Intent) -> SQLPlan:
    logger.debug(
        "sql_builder.rank_holdings.start axis=%s window=%s",
        intent.time_axis,
        intent.time_window.mode,
    )

    if intent.time_axis != TimeAxis.report_date:
        logger.error("sql_builder.rank_holdings.invalid_axis axis=%s", intent.time_axis)
        raise ValueError("holdings metrics require time_axis=report_date")

    top_n = intent.ranking.top_n if intent.ranking else 10
    logger.debug("sql_builder.rank_holdings.top_n=%s", top_n)

    fh = q_table("analytics", "fact_holding")
    df = q_table("analytics", "dim_fund")

    time_filter = build_time_filter(axis=intent.time_axis, window=intent.time_window, fact_alias="fh")

    metric_col = {
        Metric.market_value: "market_value",
        Metric.weight_pct: "weight_pct",
        Metric.shares: "shares",
    }[intent.metric]

    logger.debug("sql_builder.rank_holdings.metric_column=%s", metric_col)

    fh = q_table("analytics", "fact_holding")
    df = q_table("analytics", "dim_fund")

    with_sql = _with_clause([
        f"""
        fh_src AS (
          SELECT *
          FROM {fh} fh
        )
        """,
        time_filter.cte_sql,
    ])

    sql = f"""
    {with_sql}SELECT
      fh.fund_key,
      df.fund_name AS fund_name,
      SUM(fh.{metric_col}) AS value
    FROM fh_src fh
    JOIN {df} df ON df.fund_key = fh.fund_key
    WHERE {time_filter.where_sql}
    GROUP BY fh.fund_key, df.fund_name
    ORDER BY value DESC
    LIMIT :limit
    """.strip()

    params: Dict[str, Any] = {"limit": top_n, **time_filter.params}

    logger.info("sql_builder.rank_holdings.done params=%s", list(params.keys()))
    logger.debug("sql_builder.sql\n%s", sql)

    validate_sql(sql, require_limit=True, allowed_schemas=("analytics",))
    return SQLPlan(sql=sql, params=params)


def _rank_funds_by_total_return(intent: Intent) -> SQLPlan:
    logger.debug(
        "sql_builder.rank_returns.start axis=%s window=%s",
        intent.time_axis,
        intent.time_window.mode,
    )

    if intent.time_axis != TimeAxis.month_end:
        logger.error("sql_builder.rank_returns.invalid_axis axis=%s", intent.time_axis)
        raise ValueError("total_return requires time_axis=month_end")

    top_n = intent.ranking.top_n if intent.ranking else 10
    logger.debug("sql_builder.rank_returns.top_n=%s", top_n)

    fr = q_table("analytics", "fact_fund_return")
    df = q_table("analytics", "dim_fund")

    time_filter = build_time_filter(axis=intent.time_axis, window=intent.time_window, fact_alias="fr")

    with_sql = _with_clause([
        f"""
        fr_src AS (
          SELECT *
          FROM {fr} fr
        )
        """,
        time_filter.cte_sql,
    ])

    sql = f"""
    {with_sql}SELECT
      fr.fund_key,
      df.fund_name AS fund_name,
      fr.total_return AS value
    FROM fr_src fr
    JOIN {df} df ON df.fund_key = fr.fund_key
    WHERE {time_filter.where_sql}
    ORDER BY value DESC
    LIMIT :limit
    """.strip()

    params: Dict[str, Any] = {"limit": top_n, **time_filter.params}

    logger.info("sql_builder.rank_returns.done params=%s", list(params.keys()))
    logger.debug("sql_builder.sql\n%s", sql)

    validate_sql(sql, require_limit=True, allowed_schemas=("analytics",))
    return SQLPlan(sql=sql, params=params)

