from __future__ import annotations

import logging
import re
from typing import Iterable


logger = logging.getLogger(__name__)


# Basic, conservative checks (not a full SQL parser).
# Goal: prevent multi-statement and non-read-only queries.


FORBIDDEN_KEYWORDS: tuple[str, ...] = (
    "insert",
    "update",
    "delete",
    "drop",
    "alter",
    "create",
    "truncate",
    "grant",
    "revoke",
    "copy",
    "vacuum",
    "analyze",
    "call",
    "do",
    "execute",
    "prepare",
    "deallocate",
    "listen",
    "notify",
    "lock",
)

# Semicolons are typically used for multiple statements.
# We'll disallow them entirely (except trailing whitespace-only after one statement is still a ';' -> reject).
SEMICOLON_RE = re.compile(r";")
TRAILING_SEMICOLON_RE = re.compile(r";\s*$")

# Must start with SELECT or WITH (ignoring whitespace and comments).
START_RE = re.compile(r"^\s*(with\b|select\b)", re.IGNORECASE)

# A cheap way to detect schema-qualified references.
# We'll require that any schema-qualified table reference is analytics.*
# This is conservative: it may reject some valid queries if they contain other schema mentions.
SCHEMA_REF_RE = re.compile(r'(?:"?([a-zA-Z_][a-zA-Z0-9_]*)"?)[\s]*\.', re.IGNORECASE)

# Require LIMIT somewhere (for safety).
LIMIT_RE = re.compile(r"\blimit\b", re.IGNORECASE)

# Strip SQL single-line and block comments for keyword scanning.
LINE_COMMENT_RE = re.compile(r"--[^\n]*")
BLOCK_COMMENT_RE = re.compile(r"/\*.*?\*/", re.DOTALL)


class SQLValidationError(ValueError):
    pass


def validate_sql(
    sql: str,
    *,
    require_limit: bool = True,
    allowed_schemas: Iterable[str] = ("analytics",),
) -> None:
    """
    Validate SQL produced by sql_builder.

    Rules (default):
    - Must start with WITH or SELECT
    - No semicolons (single-statement)
    - Reject forbidden keywords (write/DDL/unsafe)
    - Only allowed schemas when schema-qualified names appear (default: analytics)
    - LIMIT required (optional toggle)

    Raises SQLValidationError on failure.
    """

    if not sql or not isinstance(sql, str):
        logger.warning("sql_validator.reject empty_or_non_string")
        raise SQLValidationError("SQL is empty or not a string")

    sql_stripped = sql.strip()
    logger.debug("sql_validator.start sql_len=%s", len(sql_stripped))

    # 1) Start token
    if not START_RE.match(sql_stripped):
        logger.warning("sql_validator.reject bad_start")
        raise SQLValidationError("SQL must start with WITH or SELECT")

    # 2) Allow a single trailing semicolon, forbid any other semicolons
    semicolon_count = sql_stripped.count(";")

    if semicolon_count > 1:
        logger.warning("sql_validator.reject multiple_semicolons")
        raise SQLValidationError("Multiple SQL statements are not allowed")

    if semicolon_count == 1 and not TRAILING_SEMICOLON_RE.search(sql_stripped):
        logger.warning("sql_validator.reject semicolon_not_trailing")
        raise SQLValidationError("Semicolon allowed only at end of query")

    # 3) Keyword blacklist scan (remove comments to reduce false positives)
    normalized = _strip_comments(sql_stripped).lower()
    for kw in FORBIDDEN_KEYWORDS:
        if re.search(rf"\b{re.escape(kw)}\b", normalized):
            logger.warning("sql_validator.reject forbidden_keyword=%s", kw)
            raise SQLValidationError(f"Forbidden SQL keyword detected: {kw}")

    # 4) Schema restriction
    allowed = {s.lower() for s in allowed_schemas}
    schemas_seen = {m.group(1).lower() for m in SCHEMA_REF_RE.finditer(normalized)}

    # Ignore common non-schema tokens that can match "<token>." patterns
    # (e.g., CTE aliases like "fh." or "df.")
    # We only enforce when the token is actually a schema name we can detect.
    # This is conservative: if someone writes public.table, we'll catch "public".
    disallowed = {s for s in schemas_seen if s not in allowed and s not in _likely_aliases(normalized)}

    if disallowed:
        logger.warning("sql_validator.reject disallowed_schemas=%s", sorted(disallowed))
        raise SQLValidationError(f"Disallowed schema(s) referenced: {sorted(disallowed)}")

    # 5) Limit requirement
    if require_limit and not LIMIT_RE.search(normalized):
        logger.warning("sql_validator.reject missing_limit")
        raise SQLValidationError("SQL must include LIMIT")

    logger.info("sql_validator.ok sql_len=%s", len(sql_stripped))

    # 6) Remove trailing semicolon
    sql = sql.rstrip().rstrip(";")


def _strip_comments(sql: str) -> str:
    sql = BLOCK_COMMENT_RE.sub(" ", sql)
    sql = LINE_COMMENT_RE.sub(" ", sql)
    return sql


def _likely_aliases(normalized_sql: str) -> set[str]:
    """
    Best-effort: collect CTE names and FROM/JOIN aliases to avoid treating them as schemas.
    This is not a parser; it's a heuristic to reduce false positives.
    """
    aliases: set[str] = set()

    # CTE names: WITH name AS (
    for m in re.finditer(r"\bwith\s+([a-zA-Z_][a-zA-Z0-9_]*)\s+as\s*\(", normalized_sql, re.IGNORECASE):
        aliases.add(m.group(1).lower())

    # FROM table alias / JOIN table alias: ... FROM xyz alias
    for m in re.finditer(r"\bfrom\s+[^\s]+\s+([a-zA-Z_][a-zA-Z0-9_]*)\b", normalized_sql, re.IGNORECASE):
        aliases.add(m.group(1).lower())
    for m in re.finditer(r"\bjoin\s+[^\s]+\s+([a-zA-Z_][a-zA-Z0-9_]*)\b", normalized_sql, re.IGNORECASE):
        aliases.add(m.group(1).lower())

    # common short aliases you use a lot
    aliases.update({"fh", "fr", "df"})

    return aliases