from __future__ import annotations

import re

_IDENTIFIER_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")


def q_ident(name: str) -> str:
    """
    Extremely conservative identifier quoting.
    Only allow safe identifiers; raise otherwise.
    (You should not pass user-provided strings here.)
    """
    if not _IDENTIFIER_RE.match(name):
        raise ValueError(f"Unsafe identifier: {name}")
    return f'"{name}"'


def q_table(schema: str, table: str) -> str:
    return f'{q_ident(schema)}.{q_ident(table)}'