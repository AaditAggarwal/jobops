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
import io
import sys
from pathlib import Path
from typing import Any

from jobops.db import get_conn, heartbeat, require_db
from jobops.ingest.common import REPO_ROOT, normalize_company

DATA_DIR = REPO_ROOT / "data" / "uscis"
BATCH = 5000  # insert chunk size

FY_KEYS = ("fiscal year", "fiscal_year", "fy")
EMPLOYER_KEYS = ("employer (petitioner) name", "employer", "employer name",
                 "petitioner name")
# The 2026 export renamed the approval columns: "Initial Approval" became
# "New Employment Approval" and "Continuing Approval" became "Continuation
# Approval". Both spellings are accepted so older downloads keep working.
INITIAL_APPROVAL_KEYS = ("initial approval", "initial approvals", "initial_approval",
                         "new employment approval", "new employment approvals")
INITIAL_DENIAL_KEYS = ("initial denial", "initial denials", "initial_denial",
                       "new employment denial", "new employment denials")
CONTINUING_APPROVAL_KEYS = ("continuing approval", "continuing approvals",
                            "continuing_approval", "continuation approval",
                            "continuation approvals")


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


def sniff_encoding(raw: bytes) -> str:
    """Pick a text encoding from a file's leading bytes (pure).

    The Data Hub exports UTF-16 with a BOM (they are tab-separated despite the
    .csv extension); older ones were UTF-8. Guessing wrong fails on byte 0.
    """
    if raw[:2] in (b"\xff\xfe", b"\xfe\xff"):
        return "utf-16"
    return "utf-8-sig"


def sniff_delimiter(header: str) -> str:
    """Pick the delimiter from a header line: tab if present, else comma (pure)."""
    return "\t" if header.count("\t") > header.count(",") else ","


def read_rows(path: str) -> list[dict[str, str]]:
    """Read one Data Hub export into dict rows, whatever its encoding/delimiter."""
    raw = Path(path).read_bytes()
    text = raw.decode(sniff_encoding(raw), errors="replace")
    first = text.splitlines()[0] if text else ""
    return list(csv.DictReader(io.StringIO(text), delimiter=sniff_delimiter(first)))


def run() -> None:
    """Delete-and-reload all data/uscis/*.csv into sponsor_records."""
    require_db("uscis_hub")
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
            rows = []
            skipped = 0
            for raw in read_rows(path):
                parsed = parse_row(raw)
                if not parsed:
                    skipped += 1
                    continue
                fy, employer, ia, idn, ca = parsed
                rows.append(("uscis_hub", fy, employer,
                             normalize_company(employer), ia, idn, ca))
            # Batched: a year file is ~200k rows and one executemany of that
            # size over the session pooler is a long single statement.
            for i in range(0, len(rows), BATCH):
                cur.executemany(
                    """INSERT INTO sponsor_records
                       (src, fiscal_year, employer_raw, employer_norm,
                        initial_approvals, initial_denials, continuing_approvals)
                       VALUES (%s,%s,%s,%s,%s,%s,%s)""",
                    rows[i:i + BATCH],
                )
            n = len(rows)
            inserted += n
            print(f"[uscis_hub] {Path(path).name}: {n} rows"
                  f" ({skipped} skipped: no employer name)")
    heartbeat("uscis_hub", ok=True, detail=f"{inserted} rows from {len(paths)} files")
    print(f"[uscis_hub] done: {inserted} rows")


if __name__ == "__main__":
    sys.exit(run())
