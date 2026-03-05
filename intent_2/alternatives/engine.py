from __future__ import annotations

import copy
from typing import List

from ..schemas import (
    Alternative,
    Assumption,
    Intent,
    Metric,
    AnalysisType,
    TimeAxis,
    TimeWindow,
    TimeWindowMode,
    Ranking,
)
from ..rules.assumptions import (
    DEFAULT_TOP_N,
    DEFAULT_TIME_WINDOW_MOST_RECENT,
    DEFAULT_METRIC_MARKET_VALUE,
)


def generate_alternatives(intent: Intent, assumptions: List[Assumption]) -> List[Alternative]:
    """
    Deterministic alternatives based ONLY on assumptions made by rules.
    Each alternative must be fully executable => full Intent payload.
    """
    alt_list: List[Alternative] = []
    codes = {a.code for a in assumptions}

    # Helpers
    def add_alt(alt_id: str, label: str, new_intent: Intent) -> None:
        alt_list.append(Alternative(id=alt_id, label=label, intent=new_intent))

    # --------------------------
    # 1) Top-N variations
    # --------------------------
    if intent.analysis_type == AnalysisType.rank and DEFAULT_TOP_N in codes:
        # Top 50
        i2 = intent.model_copy(deep=True)
        if i2.ranking is None:
            i2.ranking = Ranking(top_n=50)
        else:
            i2.ranking.top_n = 50
        add_alt(
            alt_id="rank_top_50",
            label="Top 50 (instead of top 10)",
            new_intent=i2,
        )

    # --------------------------
    # 2) Metric variations (only if metric defaulted to market_value)
    # --------------------------
    if DEFAULT_METRIC_MARKET_VALUE in codes:
        # If current intent is fund + holdings metric, we can propose total_return only if entity is fund.
        # Note: total_return requires time_axis=month_end and entity=fund (enforced by schema).
        if intent.entity.value == "fund":
            i_ret = intent.model_copy(deep=True)
            i_ret.metric = Metric.total_return
            i_ret.time_axis = TimeAxis.month_end

            # Keep analysis type: ranking stays ranking, snapshot stays snapshot.
            # Ensure ranking exists if analysis is rank (schema will enforce).
            if i_ret.analysis_type == AnalysisType.rank and i_ret.ranking is None:
                i_ret.ranking = Ranking(top_n=10)

            add_alt(
                alt_id="metric_total_return",
                label="Rank by total return",
                new_intent=i_ret,
            )

        # For holdings-style analyses, propose weight_pct if relevant
        if intent.time_axis == TimeAxis.report_date:
            i_w = intent.model_copy(deep=True)
            i_w.metric = Metric.weight_pct
            i_w.time_axis = TimeAxis.report_date
            if i_w.analysis_type == AnalysisType.rank and i_w.ranking is None:
                i_w.ranking = Ranking(top_n=10)
            add_alt(
                alt_id="metric_weight_pct",
                label="Rank by portfolio weight (%)",
                new_intent=i_w,
            )

    # --------------------------
    # 3) Time window variations (only if defaulted)
    # --------------------------
    if DEFAULT_TIME_WINDOW_MOST_RECENT in codes:
        # YTD placeholder (matches your current rules TODO)
        i_ytd = intent.model_copy(deep=True)
        i_ytd.time_window = TimeWindow(
            mode=TimeWindowMode.between_dates,
            start_date="CURRENT_YEAR_START",
            end_date="CURRENT_DATE",
        )
        add_alt(
            alt_id="time_ytd",
            label="Year-to-date (YTD)",
            new_intent=i_ytd,
        )

        # Last 12 periods: for returns use month_end, for holdings use report_date
        i_last12 = intent.model_copy(deep=True)
        i_last12.time_window = TimeWindow(mode=TimeWindowMode.last_n, n=12)
        add_alt(
            alt_id="time_last_12",
            label="Last 12 periods",
            new_intent=i_last12,
        )

    # --------------------------
    # Safety: deduplicate by alt_id (if future rules add overlaps)
    # --------------------------
    unique = {}
    for a in alt_list:
        unique[a.id] = a
    return list(unique.values())