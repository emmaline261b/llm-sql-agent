import logging
import math
import statistics
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


def _is_number(x: Any) -> bool:
    if x is None:
        return False
    try:
        v = float(x)
        return math.isfinite(v)
    except Exception:
        return False


def _safe_float(x: Any) -> Optional[float]:
    if not _is_number(x):
        return None
    return float(x)


def _relative_close(a: float, b: float, eps: float) -> bool:
    # "prawie równe" w ujęciu względnym + absolutnym fallback
    denom = max(1.0, abs(a), abs(b))
    return abs(a - b) / denom <= eps


def _build_rows(result, spec) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    cols = list(result.columns or [])

    # Walidacja: wymagane kolumny powinny być w columns (jeśli columns jest podane)
    missing_in_columns = []
    for name in (spec.primary_metric, spec.entity_label_col, spec.entity_id_col):
        if cols and name not in cols:
            missing_in_columns.append(name)

    if missing_in_columns:
        raise ValueError(
            "Missing required column(s) in result.columns: "
            f"{missing_in_columns}. Have={cols}"
        )

    rows: List[Dict[str, Any]] = []
    dropped = 0
    nulls = 0

    # result.rows to List[Dict[str, Any]]
    for r in result.rows:
        raw_val = r.get(spec.primary_metric)
        val = _safe_float(raw_val)
        if val is None:
            nulls += 1
            dropped += 1
            continue

        rows.append(
            {
                "id": r.get(spec.entity_id_col),
                "label": r.get(spec.entity_label_col),
                "value": val,
            }
        )

    reverse = spec.sort_direction.lower() == "desc"
    rows.sort(key=lambda x: x["value"], reverse=reverse)

    meta = {
        "dropped_rows": dropped,
        "null_or_non_numeric_metric_rows": nulls,
        "sorted": True,
        "sort_direction": "desc" if reverse else "asc",
        "input_row_count": getattr(result, "row_count", None),
    }
    return rows, meta


def _summary_stats(values: List[float]) -> Dict[str, Any]:
    if not values:
        return {}

    mean_v = statistics.mean(values)
    median_v = statistics.median(values)
    std_v = statistics.pstdev(values) if len(values) > 1 else 0.0

    return {
        "min": min(values),
        "max": max(values),
        "mean": mean_v,
        "median": median_v,
        "std": std_v,
    }


def _top_bottom(rows: List[Dict[str, Any]], spec) -> Dict[str, Any]:
    if not rows:
        return {"top": [], "bottom": []}

    top_k = max(0, int(spec.top_k))
    bottom_k = max(0, int(spec.bottom_k))

    top = rows[:top_k] if top_k else []
    bottom = rows[-bottom_k:] if bottom_k else []

    return {"top": top, "bottom": bottom}


def _gaps(rows: List[Dict[str, Any]], sort_direction: str) -> Dict[str, Any]:
    if len(rows) < 2:
        return {}

    diffs_abs: List[float] = []
    diffs_pct: List[Optional[float]] = []

    for i in range(len(rows) - 1):
        v1 = rows[i]["value"]
        v2 = rows[i + 1]["value"]

        d = abs(v1 - v2)
        diffs_abs.append(d)

        denom = abs(v2)
        diffs_pct.append((d / denom) if denom > 0 else None)

    max_gap = max(diffs_abs)
    idx = diffs_abs.index(max_gap)

    first = rows[0]["value"]
    second = rows[1]["value"]
    top1_vs_top2_abs = abs(first - second)
    top1_vs_top2_pct = (top1_vs_top2_abs / abs(second)) if abs(second) > 0 else None

    pct_values = [x for x in diffs_pct if x is not None]

    return {
        "top1_vs_top2_abs": top1_vs_top2_abs,
        "top1_vs_top2_pct": top1_vs_top2_pct,
        "largest_adjacent_gap_abs": max_gap,
        "largest_adjacent_gap_between_ranks": [idx + 1, idx + 2],
        "avg_adjacent_gap_abs": statistics.mean(diffs_abs) if diffs_abs else None,
        "avg_adjacent_gap_pct": statistics.mean(pct_values) if pct_values else None,
        "direction": sort_direction,
    }


def _concentration(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not rows:
        return {}

    values = [r["value"] for r in rows]
    total = sum(values)

    if total == 0:
        return {}

    top1 = rows[0]["value"] / total
    top3 = (sum(values[:3]) / total) if len(values) >= 3 else None
    top5 = (sum(values[:5]) / total) if len(values) >= 5 else None

    return {
        "total": total,
        "top1_share_of_total": top1,
        "top3_share_of_total": top3,
        "top5_share_of_total": top5,
    }


def _tie_groups(rows: List[Dict[str, Any]], eps: float) -> Dict[str, Any]:
    if len(rows) < 2:
        return {"groups": []}

    groups = []
    start = 0

    while start < len(rows):
        end = start
        v0 = rows[start]["value"]

        while end + 1 < len(rows) and _relative_close(v0, rows[end + 1]["value"], eps):
            end += 1

        if end > start:
            groups.append(
                {
                    "ranks": list(range(start + 1, end + 2)),
                    "value": v0,
                    "count": end - start + 1,
                }
            )

        start = end + 1

    return {"groups": groups}


def _notable_patterns(rows: List[Dict[str, Any]]) -> List[str]:
    n = len(rows)
    if n < 6:
        return []

    values = [r["value"] for r in rows]
    patterns = []

    candidates = [
        (3, 7),
        (5, 5),
        (1, n - 1),
    ]

    for k, m in candidates:
        if k + m > n or m <= 0:
            continue

        a = sum(values[:k])
        b = sum(values[k : k + m])

        if b == 0:
            continue

        ratio = a / b
        if 0.98 <= ratio <= 1.02:
            patterns.append(f"Sum(top{k}) ≈ sum(next{m}) (ratio={ratio:.2f})")

    return patterns


def analyze(result, spec) -> Dict[str, Any]:
    logger.info(
        "data_analyzer.start metric=%s rows=%s sort=%s",
        spec.primary_metric,
        getattr(result, "row_count", None),
        getattr(spec, "sort_direction", None),
    )

    rows, meta = _build_rows(result, spec)

    if not rows:
        logger.info("data_analyzer.done rows=0 (no valid numeric values) dropped=%s", meta.get("dropped_rows"))
        return {
            "metric": spec.primary_metric,
            "rows": 0,
            "ranked": True,
            "top": [],
            "bottom": [],
            "summary_stats": {},
            "gaps": {},
            "concentration": {},
            "ties": {"groups": []},
            "notable_patterns": [],
            "data_quality": meta,
        }

    values = [r["value"] for r in rows]

    stats = _summary_stats(values)
    tb = _top_bottom(rows, spec)
    gaps = _gaps(rows, meta["sort_direction"])
    conc = _concentration(rows)
    ties = _tie_groups(rows, float(spec.tie_epsilon))
    patterns = _notable_patterns(rows)

    facts = {
        "metric": spec.primary_metric,
        "rows": len(rows),
        "ranked": True,
        "top": tb["top"],
        "bottom": tb["bottom"],
        "summary_stats": stats,
        "gaps": gaps,
        "concentration": conc,
        "ties": ties,
        "notable_patterns": patterns,
        "data_quality": meta,
        "best": rows[0],
        "worst": rows[-1],
    }

    logger.info(
        "data_analyzer.done rows=%s min=%s max=%s dropped=%s",
        len(rows),
        stats.get("min"),
        stats.get("max"),
        meta.get("dropped_rows"),
    )

    return facts