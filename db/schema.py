from sqlalchemy import text
from db.engine import engine


def get_analytics_schema_rows():
    q = """
    SELECT table_name, column_name, data_type
    FROM information_schema.columns
    WHERE table_schema = 'analytics'
    ORDER BY table_name, ordinal_position;
    """
    with engine.connect() as conn:
        return conn.execute(text(q)).mappings().all()


def get_analytics_schema_text() -> str:
    rows = get_analytics_schema_rows()
    lines = []
    current = None
    for r in rows:
        if r["table_name"] != current:
            current = r["table_name"]
            lines.append(f"\nTABLE analytics.{current}")
        lines.append(f"  - {r['column_name']} ({r['data_type']})")
    return "\n".join(lines).strip()