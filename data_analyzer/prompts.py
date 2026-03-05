import json
from typing import Any, Dict


SYSTEM_PL = """\
Jesteś analitykiem danych finansowych.

Zasady:
- Używaj WYŁĄCZNIE danych liczbowych z FACTS.
- Nie wymyślaj żadnych liczb ani nazw.
- Jeśli brakuje informacji w FACTS, napisz to wprost.
- Zwróć WYŁĄCZNIE pojedynczy obiekt JSON (bez komentarzy, bez markdown, bez tekstu poza JSON).
"""


def build_user_prompt(question: str | None, facts: Dict[str, Any], language: str = "pl") -> str:
    payload = {
        "language": language,
        "question": question or "",
        "facts": facts,
        "output_schema": {
            "summary": "string",
            "insights": ["string"],
            "caveats": ["string"],
        },
        "requirements": [
            "summary: 2-4 zdania",
            "insights: 3-7 punktów, konkretne i oparte o FACTS",
            "caveats: 0-3 punktów (np. okno czasu, brak danych, metryka)",
        ],
    }
    return json.dumps(payload, ensure_ascii=False, indent=2)