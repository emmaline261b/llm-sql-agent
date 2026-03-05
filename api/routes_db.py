from fastapi import APIRouter, HTTPException
from sqlalchemy import text

from db.db_engine import engine
from db.raw.diagnostics import raw_nport_tables, raw_nport_topcounts, raw_nport_columns
from db.db_schema import get_analytics_schema_rows
from pydantic import BaseModel
from typing import Any
import logging

from db.db_execution import execute_sql

logger = logging.getLogger(__name__)

router = APIRouter(prefix="", tags=["3. db"])

class RunSQLRequest(BaseModel):
    sql: str
    params: dict[str, Any] = {}
    limit: int = 500



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


# ============================================
@router.post("/run-sql")
def run_sql(req: RunSQLRequest):

    logger.info("db.run_sql.start limit=%s", req.limit)
    logger.info("db.run_sql.sql\n%s", req.sql)

    try:
        sql = req.sql.rstrip().rstrip(";")

        res = execute_sql(
            engine=engine,
            sql=sql,
            params=req.params,
            limit=req.limit,
            statement_timeout_ms=30_000,
        )

        logger.info("db.run_sql.done rows=%s", res.row_count)

        return {
            "columns": res.columns,
            "row_count": res.row_count,
            "rows": res.rows,
        }

    except ValueError as e:
        logger.warning("db.run_sql.bad_request error=%s", str(e))
        raise HTTPException(status_code=400, detail=str(e))

    except RuntimeError as e:
        logger.error("db.run_sql.server_error error=%s", str(e))
        raise HTTPException(status_code=500, detail=str(e))