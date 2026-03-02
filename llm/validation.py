import json
from pathlib import Path
from jsonschema import validate

SCHEMA = json.loads(Path("semantic/llm_output.schema.json").read_text(encoding="utf-8"))

def validate_plan(plan: dict) -> None:
    validate(instance=plan, schema=SCHEMA)