import re
from dataclasses import dataclass
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError


FORBIDDEN_PATTERNS = [
    r"\bINSERT\b",
    r"\bUPDATE\b",
    r"\bDELETE\b",
    r"\bDROP\b",
    r"\bALTER\b",
    r"\bTRUNCATE\b",
    r"\bCREATE\b",
    r"\bGRANT\b",
    r"\bREVOKE\b",
    r"\bVACUUM\b",
    r"\bANALYZE\b",
    r"\bCOPY\b",
]


@dataclass
class ExecutionResult:
    columns: list[str]
    rows: list[dict[str, Any]]
    row_count: int


def _strip_sql_comments(sql: str) -> str:
    # remove -- line comments
    sql = re.sub(r"--.*?$", "", sql, flags=re.MULTILINE)
    # remove /* */ block comments
    sql = re.sub(r"/\*.*?\*/", "", sql, flags=re.DOTALL)
    return sql


def validate_sql_readonly(sql: str) -> None:
    if not sql or not sql.strip():
        raise ValueError("SQL is empty")

    cleaned = _strip_sql_comments(sql).strip()
    upper = cleaned.upper()

    if not (upper.startswith("SELECT") or upper.startswith("WITH")):
        raise ValueError("Only SELECT/WITH queries are allowed")

    for pat in FORBIDDEN_PATTERNS:
        if re.search(pat, upper):
            raise ValueError("Forbidden SQL detected")

    # basic scope restriction (POC): force analytics schema usage
    # allow information_schema / pg_catalog only if you decide later; for now block
    if re.search(r"\bRAW_NPORT\b", upper):
        raise ValueError("raw_nport is not allowed (analytics only)")


def enforce_limit(sql: str, limit: int) -> str:
    # If user/LLM already set a LIMIT, keep it.
    if re.search(r"\bLIMIT\b", sql, flags=re.IGNORECASE):
        return sql if sql.strip().endswith(";") else sql.strip() + ";"
    s = sql.rstrip().rstrip(";")
    return f"{s}\nLIMIT {int(limit)};"


def execute_sql(
    engine: Engine,
    sql: str,
    params: dict[str, Any] | None = None,
    limit: int = 1000,
    statement_timeout_ms: int = 30_000,
) -> ExecutionResult:
    validate_sql_readonly(sql)
    sql = enforce_limit(sql, limit=limit)

    try:
        with engine.connect() as conn:
            # per-transaction safety
            conn.execute(text(f"SET LOCAL statement_timeout = {int(statement_timeout_ms)}"))

            result = conn.execute(text(sql), params or {})
            mappings = result.mappings().all()

            cols = list(mappings[0].keys()) if mappings else list(result.keys())
            rows = [dict(r) for r in mappings]

            return ExecutionResult(columns=cols, rows=rows, row_count=len(rows))
    except SQLAlchemyError as e:
        raise RuntimeError(f"Database error: {e}") from e