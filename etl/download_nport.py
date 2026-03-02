from __future__ import annotations

import os
from pathlib import Path
import httpx

# SEC N-PORT quarterly ZIPs (latest 4 quarters listed by SEC page)
QUARTERS = ["2025q1", "2025q2", "2025q3", "2025q4"]
BASE_URL = "https://www.sec.gov/files/dera/data/form-n-port-data-sets"

DEST_DIR = Path("data/nport/zips")
DEST_DIR.mkdir(parents=True, exist_ok=True)

# SEC expects an identifying User-Agent. Put your email / repo here.
HEADERS = {
    "User-Agent": "llm-sql-agent-poc (contact: your-email@example.com)",
    "Accept-Encoding": "gzip, deflate, br",
}


def download_file(url: str, dest: Path) -> None:
    if dest.exists() and dest.stat().st_size > 0:
        print(f"SKIP (exists): {dest.name}")
        return

    tmp = dest.with_suffix(dest.suffix + ".part")
    print(f"DOWNLOADING: {url}")
    with httpx.Client(headers=HEADERS, timeout=120.0, follow_redirects=True) as client:
        with client.stream("GET", url) as r:
            r.raise_for_status()
            total = int(r.headers.get("Content-Length", "0")) or None
            downloaded = 0

            with tmp.open("wb") as f:
                for chunk in r.iter_bytes(chunk_size=1024 * 1024):
                    f.write(chunk)
                    downloaded += len(chunk)
                    if total:
                        pct = downloaded * 100 // total
                        print(f"\r  {pct:3d}%  {downloaded/1e6:8.1f}MB/{total/1e6:8.1f}MB", end="")
                    else:
                        print(f"\r  {downloaded/1e6:8.1f}MB", end="")

    print("\nDONE:", dest.name)
    tmp.rename(dest)


def main():
    for q in QUARTERS:
        url = f"{BASE_URL}/{q}_nport.zip"
        dest = DEST_DIR / f"{q}_nport.zip"
        download_file(url, dest)


if __name__ == "__main__":
    main()