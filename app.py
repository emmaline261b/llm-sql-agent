from fastapi import FastAPI

from api.routes_execute import router as execute_router
from api.routes_intent import router as intent_router
from api.routes_db import router as db_router
import logging


app = FastAPI(title="llm-sql-agent POC")

app.include_router(db_router)
app.include_router(intent_router)
app.include_router(execute_router)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)