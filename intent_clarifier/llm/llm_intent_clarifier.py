from __future__ import annotations

from typing import Optional, Dict, Any

from llm_sql.llm.client_ollama import call_ollama_json
from .llm_intent_prompts import SYSTEM_PROMPT, USER_PROMPT_TEMPLATE

DEFAULT_MODEL = "qwen2.5:7b-instruct"


def clarify_with_llm(
    question: str,
    ui_language: Optional[str] = None,  # kept for interface compatibility
    *,
    model: str = DEFAULT_MODEL,
) -> Dict[str, Any]:
    system = SYSTEM_PROMPT
    user = USER_PROMPT_TEMPLATE.format(question=question)

    return call_ollama_json(
        model=model,
        system=system,
        user=user,
    )