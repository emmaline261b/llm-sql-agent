from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Optional


_WS_RE = re.compile(r"\s+")
_PUNCT_RE = re.compile(r"[^\w\s\-:/\.]")


def normalize(text: str) -> str:
    """
    Minimal normalization for rule matching:
    - lowercase
    - remove most punctuation
    - compress whitespace
    """
    t = (text or "").strip().lower()
    t = _PUNCT_RE.sub(" ", t)
    t = _WS_RE.sub(" ", t)
    return t


TOP_N_RE = re.compile(r"\btop\s+(\d{1,3})\b", re.IGNORECASE)

CIK_RE = re.compile(r"\bcik\s*[:=]?\s*(\d{4,12})\b", re.IGNORECASE)
FUND_KEY_RE = re.compile(r"\bfund[_\-\s]?key\s*[:=]?\s*([a-z0-9_\-]{3,64})\b", re.IGNORECASE)
SECURITY_KEY_RE = re.compile(r"\bsecurity[_\-\s]?key\s*[:=]?\s*([a-z0-9_\-]{3,64})\b", re.IGNORECASE)

ISIN_RE = re.compile(r"\bisin\s*[:=]?\s*([A-Z]{2}[A-Z0-9]{10})\b", re.IGNORECASE)
CUSIP_RE = re.compile(r"\bcusip\s*[:=]?\s*([A-Z0-9]{8,9})\b", re.IGNORECASE)
TICKER_RE = re.compile(r"\bticker\s*[:=]?\s*([A-Z0-9\.\-]{1,10})\b", re.IGNORECASE)


# Keyword sets

FUND_KW = {
    "fund",
    "funds",
    "mutual fund",
    "etf",
    "etfs",
}

SECURITY_KW = {
    "security",
    "securities",
    "stock",
    "stocks",
    "equity",
    "equities",
}

HOLDINGS_KW = {
    "holding",
    "holdings",
    "portfolio",
    "positions",
}

RETURN_KW = {
    "return",
    "returns",
    "performance",
}

RANK_KW = {
    "top",
    "rank",
    "ranking",
}

TREND_KW = {
    "trend",
    "over time",
    "time series",
    "history",
    "historical",
}

MOST_RECENT_KW = {
    "most recent",
    "latest",
    "current",
}

ALL_YEAR_KW = {
    "ytd",
    "year to date",
    "this year",
    "all year",
    "annual",
}

MARKET_VALUE_KW = {
    "market value",
    "aum",
    "assets",
}

WEIGHT_KW = {
    "weight",
    "weights",
    "weight %",
}

SHARES_KW = {
    "shares",
    "units",
}


def contains_any(text: str, keywords: set[str]) -> bool:
    return any(kw in text for kw in keywords)


def extract_top_n(text: str) -> Optional[int]:
    m = TOP_N_RE.search(text)
    if m:
        try:
            return int(m.group(1))
        except ValueError:
            return None
    return None


@dataclass(frozen=True)
class ExtractedIds:
    fund_key: Optional[str] = None
    security_key: Optional[str] = None
    registrant_cik: Optional[str] = None
    isin: Optional[str] = None
    cusip: Optional[str] = None
    ticker: Optional[str] = None


def extract_identifiers(text: str) -> ExtractedIds:
    fund_key = None
    security_key = None
    registrant_cik = None
    isin = None
    cusip = None
    ticker = None

    m = FUND_KEY_RE.search(text)
    if m:
        fund_key = m.group(1)

    m = SECURITY_KEY_RE.search(text)
    if m:
        security_key = m.group(1)

    m = CIK_RE.search(text)
    if m:
        registrant_cik = m.group(1)

    m = ISIN_RE.search(text)
    if m:
        isin = m.group(1).upper()

    m = CUSIP_RE.search(text)
    if m:
        cusip = m.group(1).upper()

    m = TICKER_RE.search(text)
    if m:
        ticker = m.group(1).upper()

    return ExtractedIds(
        fund_key=fund_key,
        security_key=security_key,
        registrant_cik=registrant_cik,
        isin=isin,
        cusip=cusip,
        ticker=ticker,
    )