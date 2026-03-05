from __future__ import annotations

from enum import Enum
from typing import Any, Literal, Optional, List, Dict
from pydantic import BaseModel, Field, model_validator


# -----------------------
# Database-aligned enums
# -----------------------

class Entity(str, Enum):
    fund = "fund"          # analytics.dim_fund + facts keyed by fund_key
    security = "security"  # analytics.dim_security + fact_holding keyed by security_key
    holding = "holding"    # fact_holding row-level (fund_key, report_date, security_key)


class TimeAxis(str, Enum):
    """
    Which date column is the primary time axis for the query.
    - holdings use report_date (fact_holding.report_date)
    - returns use month_end (fact_fund_return.month_end)
    """
    report_date = "report_date"
    month_end = "month_end"


class Metric(str, Enum):
    """
    Only metrics that are directly supported by your schema.
    """
    market_value = "market_value"     # fact_holding.market_value
    weight_pct = "weight_pct"         # fact_holding.weight_pct
    shares = "shares"                 # fact_holding.shares
    total_return = "total_return"     # fact_fund_return.total_return


class AnalysisType(str, Enum):
    snapshot = "snapshot"  # one time point (most recent / specified)
    rank = "rank"          # top-N based on metric
    trend = "trend"        # time series
    delta = "delta"        # difference between two points/windows
    compare = "compare"    # multiple entities/metrics side-by-side


class Scope(str, Enum):
    single = "single"      # a specific fund/security
    universe = "universe"  # across all funds / securities
    peer_group = "peer_group"  # subset by attributes (e.g., fund_type, domicile)


class TimeWindowMode(str, Enum):
    most_recent = "most_recent"
    last_n = "last_n"              # last N points on the chosen axis
    between_dates = "between_dates"


class SortDirection(str, Enum):
    asc = "asc"
    desc = "desc"


# -----------------------
# Time window
# -----------------------

class TimeWindow(BaseModel):
    mode: TimeWindowMode
    n: Optional[int] = None
    start_date: Optional[str] = None  # YYYY-MM-DD or placeholders (see sql_builder/time_window.py)
    end_date: Optional[str] = None    # YYYY-MM-DD or placeholders

    @model_validator(mode="after")
    def _validate(self) -> "TimeWindow":
        if self.mode == TimeWindowMode.last_n:
            if self.n is None or self.n <= 0:
                raise ValueError("time_window.n must be > 0 when mode=last_n")
            self.start_date = None
            self.end_date = None

        elif self.mode == TimeWindowMode.between_dates:
            if not self.start_date or not self.end_date:
                raise ValueError("start_date and end_date are required when mode=between_dates")
            self.n = None

        else:
            # most_recent
            self.n = None
            self.start_date = None
            self.end_date = None

        return self


# -----------------------
# Ranking / sorting
# -----------------------

class Ranking(BaseModel):
    top_n: int = 10
    direction: SortDirection = SortDirection.desc

    @model_validator(mode="after")
    def _validate(self) -> "Ranking":
        if self.top_n <= 0:
            raise ValueError("ranking.top_n must be > 0")
        return self


class Sort(BaseModel):
    """
    Sorting is intentionally permissive at this layer.
    SQL builder can later restrict allowable columns per entity/metric.
    """
    by: str
    direction: SortDirection = SortDirection.desc


# -----------------------
# Identifiers & filters aligned with dim tables
# -----------------------

class Identifiers(BaseModel):
    """
    Optional identifiers. Rules/LLM can fill these when user specifies them.
    Aligned with keys and common identifiers from dim tables.
    """
    fund_key: Optional[str] = None          # dim_fund.fund_key
    security_key: Optional[str] = None      # dim_security.security_key

    # dim_security
    cusip: Optional[str] = None
    isin: Optional[str] = None
    ticker: Optional[str] = None

    # dim_fund
    registrant_cik: Optional[str] = None
    series_id: Optional[str] = None
    class_id: Optional[str] = None


class DimensionFilters(BaseModel):
    """
    Filters over dim_fund / dim_security attributes.
    Keep as free-form key/value for now, but names should map to columns.
    """
    fund: Dict[str, Any] = Field(default_factory=dict)      # e.g., {"fund_type": "ETF", "domicile": "US"}
    security: Dict[str, Any] = Field(default_factory=dict)  # e.g., {"asset_category": "Equity", "currency": "USD"}


# -----------------------
# Intent (canonical)
# -----------------------

class Intent(BaseModel):
    entity: Entity
    metric: Metric
    analysis_type: AnalysisType
    scope: Scope

    # derived choice: holdings use report_date; returns use month_end
    time_axis: TimeAxis

    # Note: rules can override with between_dates placeholders like:
    # - CURRENT_YEAR_START / CURRENT_DATE
    # - LAST_COMPLETED_QUARTER_START / LAST_COMPLETED_QUARTER_END
    time_window: TimeWindow = Field(default_factory=lambda: TimeWindow(mode=TimeWindowMode.most_recent))

    identifiers: Identifiers = Field(default_factory=Identifiers)
    filters: DimensionFilters = Field(default_factory=DimensionFilters)

    ranking: Optional[Ranking] = None
    sort: Optional[Sort] = None

    @model_validator(mode="after")
    def _validate_cross_fields(self) -> "Intent":
        # 1) Metric -> which fact table/time axis is valid
        holdings_metrics = {Metric.market_value, Metric.weight_pct, Metric.shares}
        returns_metrics = {Metric.total_return}

        if self.metric in holdings_metrics and self.time_axis != TimeAxis.report_date:
            raise ValueError("holdings metrics require time_axis=report_date")
        if self.metric in returns_metrics and self.time_axis != TimeAxis.month_end:
            raise ValueError("total_return requires time_axis=month_end")

        # 2) Entity constraints by metric
        if self.metric == Metric.total_return and self.entity != Entity.fund:
            raise ValueError("metric=total_return requires entity=fund")

        # 3) Ranking constraints
        if self.analysis_type == AnalysisType.rank and self.ranking is None:
            raise ValueError("ranking is required when analysis_type=rank")
        if self.analysis_type != AnalysisType.rank and self.ranking is not None:
            raise ValueError("ranking must be null unless analysis_type=rank")

        # 4) Scope implies some identifier presence (minimal policy; can be extended later)
        if self.scope == Scope.single:
            if self.entity == Entity.fund and not self.identifiers.fund_key:
                raise ValueError("scope=single, entity=fund requires identifiers.fund_key")
            if self.entity == Entity.security and not (
                self.identifiers.security_key
                or self.identifiers.ticker
                or self.identifiers.cusip
                or self.identifiers.isin
            ):
                raise ValueError("scope=single, entity=security requires a security identifier (security_key/ticker/cusip/isin)")

        return self


# -----------------------
# Alternatives & assumptions
# -----------------------

class Alternative(BaseModel):
    id: str
    label: str
    intent: Intent


class Assumption(BaseModel):
    code: str
    message: str
    details: Dict[str, Any] = Field(default_factory=dict)


# -----------------------
# Resolution contract
# -----------------------

Action = Literal["execute", "clarify"]


class Resolution(BaseModel):
    action: Action

    intent: Optional[Intent] = None
    clarification_prompt: Optional[str] = None

    alternatives: List[Alternative] = Field(default_factory=list)
    assumptions: List[Assumption] = Field(default_factory=list)

    trace: Optional[Dict[str, Any]] = None

    @model_validator(mode="after")
    def _validate(self) -> "Resolution":
        if self.action == "execute":
            if self.intent is None:
                raise ValueError("intent is required when action=execute")
            if self.clarification_prompt is not None:
                raise ValueError("clarification_prompt must be null when action=execute")

        if self.action == "clarify":
            if not self.clarification_prompt:
                raise ValueError("clarification_prompt is required when action=clarify")
            if self.intent is not None:
                raise ValueError("intent must be null when action=clarify")

        return self