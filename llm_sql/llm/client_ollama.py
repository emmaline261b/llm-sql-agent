import json
from typing import Any, Dict
import httpx

OLLAMA_URL = "http://127.0.0.1:11434"


def _extract_first_json_object(s: str) -> str:
    """
    Extract first top-level JSON object from a string.
    Works even if the model adds extra text before/after.
    """
    s = s.strip()

    # strip code fences if present
    if s.startswith("```"):
        parts = s.split("```", 2)
        s = parts[1].strip() if len(parts) > 1 else s
        if s.startswith("json"):
            s = s[4:].strip()

    # fast path: already looks like json object
    if s.startswith("{") and s.endswith("}"):
        return s

    # bracket scanning to find a balanced top-level object
    start = s.find("{")
    if start == -1:
        raise ValueError("No JSON object start '{' found.")

    depth = 0
    in_str = False
    esc = False
    for i in range(start, len(s)):
        ch = s[i]
        if in_str:
            if esc:
                esc = False
            elif ch == "\\":
                esc = True
            elif ch == '"':
                in_str = False
        else:
            if ch == '"':
                in_str = True
            elif ch == "{":
                depth += 1
            elif ch == "}":
                depth -= 1
                if depth == 0:
                    return s[start : i + 1]

    raise ValueError("No balanced JSON object found.")


def _parse_json_from_content(content: str) -> Dict[str, Any]:
    content = (content or "").strip()

    try:
        return json.loads(content)
    except json.JSONDecodeError:
        extracted = _extract_first_json_object(content)
        try:
            return json.loads(extracted)
        except Exception as e:
            raise RuntimeError(
                "LLM did not return valid JSON.\n"
                f"Raw output:\n{content}"
            ) from e


def call_ollama_json(model: str, system: str, user: str) -> Dict[str, Any]:
    """
    Calls Ollama and forces JSON output.
    Tries /api/chat first. If 404, falls back to /api/generate.
    """

    chat_payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
        "stream": False,
        "options": {"temperature": 0.1},
    }

    with httpx.Client(timeout=180.0) as client:
        r = client.post(f"{OLLAMA_URL}/api/chat", json=chat_payload)

        if r.status_code == 404:
            # fallback: /api/generate (prompt-based)
            gen_payload = {
                "model": model,
                "prompt": f"{system}\n\n{user}",
                "stream": False,
                "options": {"temperature": 0.1},
            }
            r = client.post(f"{OLLAMA_URL}/api/generate", json=gen_payload)

        r.raise_for_status()
        data = r.json()

    # Response shape differs between chat and generate
    if isinstance(data, dict) and "message" in data and isinstance(data["message"], dict):
        content = (data["message"].get("content") or "").strip()
        return _parse_json_from_content(content)

    if isinstance(data, dict) and "response" in data:
        content = (data.get("response") or "").strip()
        return _parse_json_from_content(content)

    raise RuntimeError(f"Unexpected Ollama response shape: keys={list(data.keys()) if isinstance(data, dict) else type(data)}")