from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, List

from ..schemas import (
    Intent,
    Entity,
    Metric,
    AnalysisType,
    Scope,
    TimeAxis,
    TimeWindow,
    TimeWindowMode,
    Ranking,
    Identifiers,
)

from .patterns import (
    normalize,
    contains_any,
    extract_top_n,
    extract_identifiers,
    FUND_KW,
    SECURITY_KW,
    HOLDINGS_KW,
    RETURN_KW,
    RANK_KW,
    TREND_KW,
    MARKET_VALUE_KW,
    WEIGHT_KW,
    SHARES_KW,
    MOST_RECENT_KW,
    YTD_KW,
    LAST_QUARTER_KW,
    THIS_QUARTER_KW,
)

from .assumptions import (
    make_assumption,
    DEFAULT_TOP_N,
    DEFAULT_RANKING_BY_MARKET_VALUE,
    DEFAULT_METRIC_MARKET_VALUE,
    DEFAULT_TIME_WINDOW_MOST_RECENT,  # kept for backward compatibility; message updated below

    INFER_ENTITY_FUND,
    INFER_ENTITY_SECURITY,
    INFER_METRIC_TOTAL_RETURN,
    INFER_METRIC_WEIGHT_PCT,
    INFER_METRIC_SHARES,
    INFER_ANALYSIS_RANK,
    INFER_ANALYSIS_SNAPSHOT,
    INFER_ANALYSIS_TREND,
    INFER_SCOPE_UNIVERSE,
    INFER_SCOPE_SINGLE,
    INFER_TIME_AXIS_REPORT_DATE,
    INFER_TIME_AXIS_MONTH_END,
)


@dataclass
class RulesOutcome:
    action: str
    intent: Optional[Intent] = None
    clarification_prompt: Optional[str] = None
    assumptions: Optional[List] = None
    alternatives: Optional[List] = None


def try_resolve(question: str) -> Optional[RulesOutcome]:
    text = normalize(question)
    assumptions: List = []

    # ---------------------------------------
    # identifiers
    # ---------------------------------------

    ids = extract_identifiers(text)

    identifiers = Identifiers(
        fund_key=ids.fund_key,
        security_key=ids.security_key,
        registrant_cik=ids.registrant_cik,
        isin=ids.isin,
        cusip=ids.cusip,
        ticker=ids.ticker,
    )

    # ---------------------------------------
    # entity inference
    # ---------------------------------------

    entity: Optional[Entity] = None

    if contains_any(text, FUND_KW):
        entity = Entity.fund
        assumptions.append(make_assumption(INFER_ENTITY_FUND, "Entity inferred as fund"))

    if contains_any(text, SECURITY_KW) or contains_any(text, HOLDINGS_KW):
        entity = Entity.security
        assumptions.append(make_assumption(INFER_ENTITY_SECURITY, "Entity inferred as security"))

    if entity is None:
        entity = Entity.fund
        assumptions.append(make_assumption(INFER_ENTITY_FUND, "Default entity assumed as fund"))

    # ---------------------------------------
    # metric inference
    # ---------------------------------------

    metric: Optional[Metric] = None

    if contains_any(text, RETURN_KW):
        metric = Metric.total_return
        assumptions.append(make_assumption(INFER_METRIC_TOTAL_RETURN, "Metric inferred as total return"))

    elif contains_any(text, WEIGHT_KW):
        metric = Metric.weight_pct
        assumptions.append(make_assumption(INFER_METRIC_WEIGHT_PCT, "Metric inferred as weight percentage"))

    elif contains_any(text, SHARES_KW):
        metric = Metric.shares
        assumptions.append(make_assumption(INFER_METRIC_SHARES, "Metric inferred as shares"))

    else:
        metric = Metric.market_value
        assumptions.append(make_assumption(DEFAULT_METRIC_MARKET_VALUE, "Metric defaulted to market value"))

    # ---------------------------------------
    # analysis type
    # ---------------------------------------

    if contains_any(text, RANK_KW):
        analysis_type = AnalysisType.rank
        assumptions.append(make_assumption(INFER_ANALYSIS_RANK, "Analysis type inferred as ranking"))

    elif contains_any(text, TREND_KW):
        analysis_type = AnalysisType.trend
        assumptions.append(make_assumption(INFER_ANALYSIS_TREND, "Analysis type inferred as trend"))

    else:
        analysis_type = AnalysisType.snapshot
        assumptions.append(make_assumption(INFER_ANALYSIS_SNAPSHOT, "Analysis type inferred as snapshot"))

    # ---------------------------------------
    # scope
    # ---------------------------------------

    if identifiers.fund_key or identifiers.security_key:
        scope = Scope.single
        assumptions.append(make_assumption(INFER_SCOPE_SINGLE, "Scope inferred as single entity"))
    else:
        scope = Scope.universe
        assumptions.append(make_assumption(INFER_SCOPE_UNIVERSE, "Scope inferred as full universe"))

    # ---------------------------------------
    # time axis
    # ---------------------------------------

    if metric == Metric.total_return:
        time_axis = TimeAxis.month_end
        assumptions.append(make_assumption(INFER_TIME_AXIS_MONTH_END, "Time axis inferred as month_end"))
    else:
        time_axis = TimeAxis.report_date
        assumptions.append(make_assumption(INFER_TIME_AXIS_REPORT_DATE, "Time axis inferred as report_date"))

    # ---------------------------------------
    # time window
    #
    # Goal: compare closed quarters (quarter-to-quarter), avoid mixing stale and fresh dates.
    #
    # Semantics:
    # - "ytd" => current year to date
    # - "last quarter" / "recent" / "most recent" / default => last completed quarter
    # - "this quarter" => map to last completed quarter (still closed/comparable)
    #
    # NOTE: this uses between_dates placeholders that sql_builder/time_window.py must resolve:
    #   LAST_COMPLETED_QUARTER_START / LAST_COMPLETED_QUARTER_END
    # ---------------------------------------

    if contains_any(text, YTD_KW):
        time_window = TimeWindow(
            mode=TimeWindowMode.between_dates,
            start_date="CURRENT_YEAR_START",
            end_date="CURRENT_DATE",
        )

    else:
        # Treat "this quarter", "last quarter", "most recent"/"recent", and default as last completed quarter.
        # (Keeps comparisons on a closed reporting period.)
        if contains_any(text, THIS_QUARTER_KW) or contains_any(text, LAST_QUARTER_KW) or contains_any(text, MOST_RECENT_KW):
            time_window = TimeWindow(
                mode=TimeWindowMode.between_dates,
                start_date="LAST_COMPLETED_QUARTER_START",
                end_date="LAST_COMPLETED_QUARTER_END",
            )
        else:
            time_window = TimeWindow(
                mode=TimeWindowMode.between_dates,
                start_date="LAST_COMPLETED_QUARTER_START",
                end_date="LAST_COMPLETED_QUARTER_END",
            )
            assumptions.append(
                make_assumption(
                    DEFAULT_TIME_WINDOW_MOST_RECENT,
                    "Time window defaulted to last completed quarter",
                )
            )

    # ---------------------------------------
    # ranking
    # ---------------------------------------

    ranking = None

    if analysis_type == AnalysisType.rank:
        top_n = extract_top_n(text)

        if top_n is None:
            top_n = 10
            assumptions.append(make_assumption(DEFAULT_TOP_N, "Ranking defaulted to top 10"))

        ranking = Ranking(top_n=top_n)

        assumptions.append(
            make_assumption(
                DEFAULT_RANKING_BY_MARKET_VALUE,
                "Ranking metric defaulted to market value",
            )
        )

    # ---------------------------------------
    # intent
    # ---------------------------------------

    intent = Intent(
        entity=entity,
        metric=metric,
        analysis_type=analysis_type,
        scope=scope,
        time_axis=time_axis,
        time_window=time_window,
        identifiers=identifiers,
        ranking=ranking,
    )

    return RulesOutcome(
        action="execute",
        intent=intent,
        assumptions=assumptions,
    )