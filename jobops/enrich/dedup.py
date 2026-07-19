"""Cross-source job dedup.

The same posting often arrives via multiple sources (ATS poller + SimplifyJobs
repo + later an email alert). Rule: same normalized company + very similar
title (rapidfuzz token_sort_ratio >= 92) + same location, first seen within
14 days -> the LATER row is marked status='skipped' with a note pointing at
the kept row. The earliest row wins because speed-to-apply is measured from
first sighting.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any

from rapidfuzz import fuzz

from jobops.db import get_conn, heartbeat, query

TITLE_THRESHOLD = 92
WINDOW_DAYS = 14


def _same_location(a: str | None, b: str | None) -> bool:
    if a is None or b is None:
        return a is None and b is None
    return a.strip().casefold() == b.strip().casefold()


def find_duplicates(rows: list[dict[str, Any]]) -> list[tuple[str, str, str]]:
    """Pure dedup decision: returns (dup_id, kept_id, note) triples.

    `rows` need keys: id, company_norm, title, location, source, first_seen_at.
    Rows are compared within the same normalized company only; the earliest
    first_seen_at in a duplicate cluster is kept. A row already marked
    duplicate is not reused as a keeper.
    """
    by_company: dict[str, list[dict[str, Any]]] = {}
    for r in rows:
        by_company.setdefault(r["company_norm"], []).append(r)

    decisions: list[tuple[str, str, str]] = []
    for group in by_company.values():
        group.sort(key=lambda r: r["first_seen_at"])
        dropped: set[str] = set()
        for i, later in enumerate(group):
            for earlier in group[:i]:
                if earlier["id"] in dropped:
                    continue
                if later["first_seen_at"] - earlier["first_seen_at"] > timedelta(days=WINDOW_DAYS):
                    continue
                if not _same_location(later["location"], earlier["location"]):
                    continue
                if fuzz.token_sort_ratio(later["title"], earlier["title"]) < TITLE_THRESHOLD:
                    continue
                dropped.add(later["id"])
                decisions.append((
                    later["id"],
                    earlier["id"],
                    f"duplicate of {earlier['id']} via {earlier['source']}",
                ))
                break
    return decisions


def run() -> None:
    """Mark cross-source duplicates among recent status='new' jobs as skipped."""
    rows = query(
        """
        SELECT j.id, j.title, j.location, j.source, j.first_seen_at,
               co.name_normalized AS company_norm
        FROM jobs j JOIN companies co ON co.id = j.company_id
        WHERE j.status = 'new'
          AND j.first_seen_at > now() - interval '30 days'
        """
    )
    decisions = find_duplicates(rows)
    with get_conn() as conn, conn.cursor() as cur:
        for dup_id, _kept_id, note in decisions:
            cur.execute(
                "UPDATE jobs SET status = 'skipped', note = %s WHERE id = %s",
                (note, dup_id),
            )
    heartbeat("dedup", ok=True, detail=f"{len(decisions)} duplicates skipped of {len(rows)} candidates")
    print(f"[dedup] {len(decisions)} duplicates skipped of {len(rows)} candidates")


if __name__ == "__main__":
    run()
