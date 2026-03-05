from fastapi import APIRouter
from sqlalchemy import text

from db.engine import engine
from db.raw.diagnostics import raw_nport_tables, raw_nport_topcounts, raw_nport_columns
from db.schema import get_analytics_schema_rows

router = APIRouter(prefix="", tags=["db"])


@router.get("/health")
def health():
    return {"status": "ok"}


@router.get("/db-check")
def db_check():
    with engine.connect() as conn:
        result = conn.execute(text("SELECT 1")).scalar()
    return {"db": result}


@router.get("/raw-nport-tables")
def _raw_nport_tables():
    return raw_nport_tables()


@router.get("/raw-nport-topcounts")
def _raw_nport_topcounts(limit: int = 20):
    return raw_nport_topcounts(limit=limit)


@router.get("/raw-nport-columns")
def _raw_nport_columns(table: str):
    return raw_nport_columns(table=table)


@router.get("/schema/analytics")
def analytics_schema():
    return get_analytics_schema_rows()