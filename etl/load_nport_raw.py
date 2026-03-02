from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Iterable

from dotenv import load_dotenv
import psycopg

load_dotenv()

PG_DSN = os.environ["PG_DSN"]

EXTRACTED_DIR = Path("data/nport/extracted")

# -------- helpers

_ident_re = re.compile(r"[^a-zA-Z0-9_]+")

def to_sql_ident(name: str) -> str:
    """
    Convert header/table name to safe snake_case-ish SQL identifier.
    """
    n = name.strip()
    n = n.replace(" ", "_")
    n = n.replace("-", "_")
    n = _ident_re.sub("_", n)
    n = n.strip("_").lower()
    if not n:
        n = "col"
    if n[0].isdigit():
        n = f"c_{n}"
    return n

def first_line(path: Path) -> str:
    with path.open("r", encoding="utf-8", errors="replace") as f:
        return f.readline().rstrip("\n")

def iter_quarters() -> Iterable[Path]:
    for q in sorted(EXTRACTED_DIR.iterdir()):
        if q.is_dir() and re.fullmatch(r"\d{4}q[1-4]", q.name):
            yield q

def is_tsv(path: Path) -> bool:
    return path.suffix.lower() in (".tsv", ".txt", ".tab")

# -------- main load

def ensure_schema(conn) -> None:
    conn.execute("CREATE SCHEMA IF NOT EXISTS raw_nport;")

def create_table(conn, table: str, cols: list[str]) -> None:
    # keep all as TEXT in RAW; add quarter + rownum for traceability
    col_defs = ",\n  ".join([f'"{c}" TEXT' for c in cols])
    sql = f"""
    CREATE TABLE IF NOT EXISTS raw_nport."{table}" (
      _quarter TEXT NOT NULL,
      _rownum  BIGINT NOT NULL,
      {col_defs}
    );
    """
    conn.execute(sql)

def truncate_quarter(conn, table: str, quarter: str) -> None:
    conn.execute(
        f'DELETE FROM raw_nport."{table}" WHERE _quarter = %s;',
        (quarter,),
    )

def copy_tsv(conn, table: str, quarter: str, path: Path, cols: list[str]) -> None:
    """
    Load TSV with header row into raw_nport.table
    We COPY into a temp table to compute rownum and inject _quarter.
    """
    tmp = f"tmp_{table}"

    conn.execute(f'DROP TABLE IF EXISTS raw_nport."{tmp}";')
    # temp staging without _quarter/_rownum
    col_defs = ",\n  ".join([f'"{c}" TEXT' for c in cols])
    conn.execute(
        f'CREATE UNLOGGED TABLE raw_nport."{tmp}" (\n  {col_defs}\n);'
    )

    with path.open("r", encoding="utf-8", errors="replace") as f:
        columns_sql = ",".join([f'"{c}"' for c in cols])

        copy_sql = f"""
        COPY raw_nport."{tmp}" ({columns_sql})
        FROM STDIN
        WITH (
            FORMAT csv,
            DELIMITER E'\\t',
            HEADER true,
            QUOTE E'\\b',
            ESCAPE E'\\b'
        );
        """

        with conn.cursor() as cur:
            with cur.copy(copy_sql) as cp:
                for line in f:
                    cp.write(line)

    # insert with quarter + rownum
    insert_sql = f"""
    INSERT INTO raw_nport."{table}" (_quarter, _rownum, {",".join([f'"{c}"' for c in cols])})
    SELECT
      %s AS _quarter,
      row_number() OVER ()::bigint AS _rownum,
      {",".join([f'"{c}"' for c in cols])}
    FROM raw_nport."{tmp}";
    """
    conn.execute(insert_sql, (quarter,))
    conn.execute(f'DROP TABLE raw_nport."{tmp}";')

def main() -> None:
    if not EXTRACTED_DIR.exists():
        raise SystemExit(f"Missing {EXTRACTED_DIR}. Run extract first.")

    with psycopg.connect(PG_DSN) as conn:
        conn.execute("SET statement_timeout = '0';")  # POC: no timeout during bulk load
        ensure_schema(conn)

        for qdir in iter_quarters():
            quarter = qdir.name
            print(f"\n=== Loading quarter {quarter} ===")

            # find TSV-like files in quarter folder (recursively)
            files = sorted([p for p in qdir.rglob("*") if p.is_file() and is_tsv(p)])
            if not files:
                print(f"No TSV files found in {qdir}")
                continue

            for fpath in files:
                # table name based on filename without extension
                table = to_sql_ident(fpath.stem)

                header = first_line(fpath)
                raw_cols = header.split("\t")
                cols = [to_sql_ident(c) for c in raw_cols]

                # avoid duplicate col names after sanitization
                seen = {}
                deduped = []
                for c in cols:
                    n = seen.get(c, 0)
                    if n == 0:
                        deduped.append(c)
                    else:
                        deduped.append(f"{c}_{n+1}")
                    seen[c] = n + 1
                cols = deduped

                print(f"  - {fpath.name} -> raw_nport.{table} ({len(cols)} cols)")

                with conn.transaction():
                    create_table(conn, table, cols)
                    truncate_quarter(conn, table, quarter)
                    copy_tsv(conn, table, quarter, fpath, cols)

        print("\nDONE: RAW load complete")

if __name__ == "__main__":
    main()