from fastapi import FastAPI, HTTPException
from sqlalchemy import text

from api.routes_execute import router as execute_router
from db.engine import engine
from db.schema import get_analytics_schema_rows
from llm.models import ChatRequest
from llm.planner import generate_plan
from raw.diagnostics import raw_nport_tables, raw_nport_topcounts, raw_nport_columns

app = FastAPI(title="llm-sql-agent POC")
app.include_router(execute_router)


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/db-check")
def db_check():
    with engine.connect() as conn:
        result = conn.execute(text("SELECT 1")).scalar()
    return {"db": result}


@app.get("/raw-nport-tables")
def _raw_nport_tables():
    return raw_nport_tables()


@app.get("/raw-nport-topcounts")
def _raw_nport_topcounts(limit: int = 20):
    return raw_nport_topcounts(limit=limit)


@app.get("/raw-nport-columns")
def _raw_nport_columns(table: str):
    return raw_nport_columns(table=table)


@app.get("/schema/analytics")
def analytics_schema():
    return get_analytics_schema_rows()


@app.post("/chat")
def chat(req: ChatRequest):
    try:
        return generate_plan(req.question)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
