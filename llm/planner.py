from pathlib import Path

from db.schema import get_analytics_schema_text
from llm.client_ollama import call_ollama_json
from llm.validation import validate_plan
from llm.guard import basic_sql_guard

SYSTEM_PROMPT = Path("semantic/sql_planner.system.txt").read_text(encoding="utf-8")

def apply_defaults(plan: dict) -> dict:
    plan = dict(plan or {})

    plan.setdefault("params", {})
    plan.setdefault("result_shape", "table")
    plan.setdefault("chart", {})

    chart = dict(plan["chart"] or {})
    chart.setdefault("type", "table")
    chart.setdefault("x", None)
    chart.setdefault("y", None)
    chart.setdefault("series", None)
    chart.setdefault("title", "Result")

    plan["chart"] = chart

    plan.setdefault("answer_brief", "")
    plan.setdefault("followups", [])
    plan.setdefault("assumptions", [])

    return plan

def generate_plan(question: str, model: str = "qwen2.5:7b-instruct") -> dict:
    schema_text = get_analytics_schema_text()

    user_prompt = f"""User question: {question}

Database schema (only analytics.* allowed):
{schema_text}

Return JSON only, matching the JSON Schema.
SQL must be Postgres, SELECT/WITH only, query only analytics.* tables.
"""

    plan = call_ollama_json(model=model, system=SYSTEM_PROMPT, user=user_prompt)
    plan = apply_defaults(plan)
    validate_plan(plan)
    plan["sql"] = basic_sql_guard(plan.get("sql", ""))
    return plan
