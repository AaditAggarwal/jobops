"""USCIS H-1B Employer Data Hub loader (DESIGN.md §5.2).

The yearly CSVs are versioned files behind a browser-only site, so they are
downloaded manually once (uscis.gov -> H-1B Employer Data Hub Files) into
data/uscis/. This loader is idempotent: delete-and-reload by src.

Header names have varied across export years ("Fiscal Year" vs "fiscal_year",
"Employer (Petitioner) Name" vs "Employer"); _pick() tolerates the variants.
"""

from __future__ import annotations

import csv
import glob
import sys
from pathlib import Path
from typing import Any

from jobops.db import get_conn, heartbeat
from jobops.ingest.common import REPO_ROOT, normalize_company

DATA_DIR = REPO_ROOT / "data" / "uscis"

FY_KEYS = ("fiscal year", "fiscal_year", "fy")
EMPLOYER_KEYS = ("employer (petitioner) name", "employer", "employer name",
                 "petitioner name")
INITIAL_APPROVAL_KEYS = ("initial approval", "initial approvals", "initial_approval")
INITIAL_DENIAL_KEYS = ("initial denial", "initial denials", "initial_denial")
CONTINUING_APPROVAL_KEYS = ("continuing approval", "continuing approvals",
                            "continuing_approval")


def _pick(row: dict[str, Any], keys: tuple[str, ...]) -> str | None:
    """Return the first matching column value, comparing case-insensitively."""
    lowered = {(k or "").strip().lower(): v for k, v in row.items()}
    for k in keys:
        if k in lowered and lowered[k] not in (None, ""):
            return lowered[k]
    return None


def _int(val: str | None) -> int:
    """Parse hub-export integers, tolerating commas and blanks."""
    if not val:
        return 0
    try:
        return int(str(val).replace(",", "").strip())
    except ValueError:
        return 0


def parse_row(row: dict[str, Any]) -> tuple[int | None, str, int, int, int] | None:
    """Extract (fiscal_year, employer, initial_appr, initial_den, cont_appr).

    Returns None for rows without an employer name (summary/blank lines).
    Pure — unit-tested against header variants.
    """
    employer = _pick(row, EMPLOYER_KEYS)
    if not employer or not employer.strip():
        return None
    fy_raw = _pick(row, FY_KEYS)
    fy = _int(fy_raw) or None
    return (
        fy,
        employer.strip(),
        _int(_pick(row, INITIAL_APPROVAL_KEYS)),
        _int(_pick(row, INITIAL_DENIAL_KEYS)),
        _int(_pick(row, CONTINUING_APPROVAL_KEYS)),
    )


def run() -> None:
    """Delete-and-reload all data/uscis/*.csv into sponsor_records."""
    paths = sorted(glob.glob(str(DATA_DIR / "*.csv")))
    if not paths:
        print(f"[uscis_hub] no CSVs in {DATA_DIR} — download from uscis.gov "
              "(H-1B Employer Data Hub Files) first")
        heartbeat("uscis_hub", ok=False, detail="no input files")
        return
    inserted = 0
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("DELETE FROM sponsor_records WHERE src = 'uscis_hub'")
        for path in paths:
            n = 0
            with open(path, newline="", encoding="utf-8-sig") as f:
                rows = []
                for raw in csv.DictReader(f):
                    parsed = parse_row(raw)
                    if not parsed:
                        continue
                    fy, employer, ia, idn, ca = parsed
                    rows.append(("uscis_hub", fy, employer,
                                 normalize_company(employer), ia, idn, ca))
                cur.executemany(
                    """INSERT INTO sponsor_records
                       (src, fiscal_year, employer_raw, employer_norm,
                        initial_approvals, initial_denials, continuing_approvals)
                       VALUES (%s,%s,%s,%s,%s,%s,%s)""",
                    rows,
                )
                n = len(rows)
            inserted += n
            print(f"[uscis_hub] {Path(path).name}: {n} rows")
    heartbeat("uscis_hub", ok=True, detail=f"{inserted} rows from {len(paths)} files")
    print(f"[uscis_hub] done: {inserted} rows")


if __name__ == "__main__":
    sys.exit(run())
