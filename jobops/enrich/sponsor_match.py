"""Match companies to USCIS sponsor records and score sponsorship likelihood.

DESIGN.md §5.3. The scoring/matching core is pure (thoroughly unit-tested —
this badge decides where application time goes); run() applies it to every
company still marked 'unknown'.

Caveats encoded in docs, not just code: unknown != no (young startups may
sponsor); H-1B history != new-grad OPT willingness; the badge triages, a
human check settles edge cases before applying.
"""

from __future__ import annotations

import datetime
import sys
from typing import Any

from rapidfuzz import fuzz

from jobops.db import get_conn, heartbeat

MATCH_THRESHOLD = 0.90  # token_sort_ratio/100 below this -> no match at all


def current_fiscal_year() -> int:
    """US federal fiscal year (FY starts Oct 1)."""
    today = datetime.date.today()
    return today.year + (1 if today.month >= 10 else 0)


def pick_best_match(
    norm_name: str, candidates: list[dict[str, Any]]
) -> dict[str, Any] | None:
    """Choose the sponsor_records aggregate best matching a normalized name.

    Candidates come from a pg_trgm similarity prefilter; rapidfuzz
    token_sort_ratio confirms. Below MATCH_THRESHOLD returns None —
    a wrong-company match is worse than no match.
    """
    best, best_sim = None, 0.0
    for row in candidates:
        sim = fuzz.token_sort_ratio(norm_name, row["employer_norm"]) / 100
        if sim > best_sim:
            best, best_sim = row, sim
    if not best or best_sim < MATCH_THRESHOLD:
        return None
    return best


def compute_score(
    approvals: int, denials: int, latest_fy: int | None, now_fy: int | None = None
) -> tuple[float, str]:
    """Score 0..1 + status from aggregated H-1B petition history.

    50+ recent initial approvals ≈ routine sponsor (volume saturates there);
    approval rate penalizes deny-heavy shops; recency decays data older than
    two fiscal years. Thresholds per DESIGN.md §5.3, with the recency cutoff
    made relative to the current fiscal year instead of the doc's hardcoded
    2024 (deviation noted in PROGRESS.md).
    """
    if approvals <= 0:
        return 0.05, "unlikely"
    now_fy = now_fy or current_fiscal_year()
    recency = 1.0 if (latest_fy or 0) >= now_fy - 2 else 0.6
    volume = min(approvals / 50, 1.0)
    # Laplace-smoothed (+5): a 3-approval shop must not get a perfect rate
    # signal from 3 data points — without this, any recent clean sponsor
    # scored 'verified' and the badge degenerated to binary. (Deviation from
    # the DESIGN.md sketch, noted in PROGRESS.md.)
    approval_rate = approvals / (approvals + denials + 5)
    score = round(0.5 * volume + 0.3 * approval_rate + 0.2 * recency, 3)
    status = "verified" if score >= 0.5 else ("likely" if score >= 0.2 else "unlikely")
    return score, status


def score_company(cur, norm_name: str) -> tuple[float, str]:
    """Look up one normalized company name; returns (score, status)."""
    cur.execute(
        """
        SELECT employer_norm,
               sum(initial_approvals) AS a, sum(initial_denials) AS d,
               max(fiscal_year) AS yr
        FROM sponsor_records
        WHERE employer_norm %% %s          -- pg_trgm similarity prefilter
        GROUP BY employer_norm
        """,
        (norm_name,),
    )
    best = pick_best_match(norm_name, cur.fetchall())
    if not best:
        return 0.0, "unknown"
    return compute_score(best["a"] or 0, best["d"] or 0, best["yr"])


def run() -> None:
    """Score every company still marked sponsor_status='unknown'."""
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("SELECT count(*) AS n FROM sponsor_records")
        if cur.fetchone()["n"] == 0:
            print("[sponsor_match] sponsor_records is empty — run the ETL first")
            heartbeat("sponsor_match", ok=False, detail="no sponsor data")
            return
        cur.execute(
            "SELECT id, name_normalized FROM companies WHERE sponsor_status = 'unknown'"
        )
        companies = cur.fetchall()
        counts: dict[str, int] = {}
        for co in companies:
            score, status = score_company(cur, co["name_normalized"])
            counts[status] = counts.get(status, 0) + 1
            if status != "unknown":
                cur.execute(
                    "UPDATE companies SET sponsor_score = %s, sponsor_status = %s "
                    "WHERE id = %s",
                    (score, status, co["id"]),
                )
    detail = ", ".join(f"{k}={v}" for k, v in sorted(counts.items())) or "0 companies"
    heartbeat("sponsor_match", ok=True, detail=detail)
    print(f"[sponsor_match] {len(companies)} scored: {detail}")


if __name__ == "__main__":
    sys.exit(run())
