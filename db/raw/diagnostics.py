from fastapi import HTTPException
from sqlalchemy import text
from db.db_engine import engine


def raw_nport_tables():
    q = """
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = 'raw_nport'
    ORDER BY table_name;
    """
    with engine.connect() as conn:
        rows = conn.execute(text(q)).fetchall()
    return {"count": len(rows), "tables": [r[0] for r in rows]}


def raw_nport_topcounts(limit: int = 20):
    q = """
    SELECT relname AS table_name, n_live_tup::bigint AS approx_rows
    FROM pg_stat_user_tables
    WHERE schemaname = 'raw_nport'
    ORDER BY n_live_tup DESC
    LIMIT :limit;
    """
    with engine.connect() as conn:
        rows = conn.execute(text(q), {"limit": limit}).mappings().all()
    return {"top": rows}


def raw_nport_columns(table: str):
    q = """
    SELECT column_name, data_type
    FROM information_schema.columns
    WHERE table_schema = 'raw_nport'
      AND table_name = :t
    ORDER BY ordinal_position;
    """
    with engine.connect() as conn:
        rows = conn.execute(text(q), {"t": table}).mappings().all()
    if not rows:
        raise HTTPException(status_code=404, detail="table not found in raw_nport")
    return {"table": table, "columns": rows}