import json
from typing import Any, Dict
import httpx


OLLAMA_URL = "http://localhost:11434"


def call_ollama_json(model: str, system: str, user: str) -> Dict[str, Any]:
    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
        "stream": False,
        "options": {"temperature": 0.1},
    }

    with httpx.Client(timeout=120.0) as client:
        r = client.post(f"{OLLAMA_URL}/api/chat", json=payload)
        r.raise_for_status()
        data = r.json()

    content = data["message"]["content"].strip()

    # Strip markdown fences
    if content.startswith("```"):
        parts = content.split("```")
        if len(parts) >= 2:
            content = parts[1].strip()
            if content.startswith("json"):
                content = content[4:].strip()

    try:
        return json.loads(content)
    except json.JSONDecodeError as e:
        raise RuntimeError(
            f"LLM did not return valid JSON.\nRaw output:\n{content}"
        ) from e