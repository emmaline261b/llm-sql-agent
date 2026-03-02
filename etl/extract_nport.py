from __future__ import annotations

from pathlib import Path
import zipfile

ZIPS_DIR = Path("data/nport/zips")
OUT_DIR = Path("data/nport/extracted")

def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    zips = sorted(ZIPS_DIR.glob("*_nport.zip"))
    if not zips:
        raise SystemExit(f"No ZIPs found in {ZIPS_DIR}")

    for z in zips:
        quarter = z.stem.replace("_nport", "")  # e.g. 2025q1
        q_out = OUT_DIR / quarter
        q_out.mkdir(parents=True, exist_ok=True)

        print(f"Extracting {z.name} -> {q_out}")
        with zipfile.ZipFile(z, "r") as zf:
            zf.extractall(q_out)

    print("DONE")

if __name__ == "__main__":
    main()