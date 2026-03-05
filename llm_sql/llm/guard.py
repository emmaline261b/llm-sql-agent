from fastapi import HTTPException

def basic_sql_guard(sql: str) -> str:
    s = (sql or "").strip().rstrip(";")
    low = s.lower()

    if not (low.startswith("select") or low.startswith("with")):
        raise HTTPException(status_code=400, detail="Only SELECT/WITH allowed")

    banned = ["insert", "update", "delete", "drop", "alter", "create", "truncate", "grant", "revoke"]
    if any(b in low for b in banned):
        raise HTTPException(status_code=400, detail="Forbidden SQL keyword")

    if " limit " not in low:
        s += " LIMIT 500"
    return s