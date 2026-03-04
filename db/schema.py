# db/schema.py
from functools import lru_cache
from sqlalchemy import text
from db.engine import engine

def get_analytics_schema_rows() -> list[dict]:
    q = """
    SELECT table_name, column_name, data_type
    FROM information_schema.columns
    WHERE table_schema = 'analytics'
    ORDER BY table_name, ordinal_position;
    """
    with engine.connect() as conn:
        return conn.execute(text(q)).mappings().all()

def _format_analytics_schema_text(rows: list[dict]) -> str:
    lines = []
    current = None
    for r in rows:
        t = r["table_name"]
        if t != current:
            current = t
            lines.append(f"\nTABLE analytics.{t}")
        lines.append(f"  - {r['column_name']} ({r['data_type']})")
    return "\n".join(lines).strip()

@lru_cache(maxsize=1)
def get_analytics_schema_text() -> str:
    return _format_analytics_schema_text(get_analytics_schema_rows())

def clear_schema_cache() -> None:
    get_analytics_schema_text.cache_clear()