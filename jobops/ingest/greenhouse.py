"""Greenhouse board poller.

GET https://boards-api.greenhouse.io/v1/boards/{token}/jobs        (light list)
GET https://boards-api.greenhouse.io/v1/boards/{token}/jobs/{id}   (detail)

List-then-detail: the ?content=true list is multi-MB per board (full JD HTML
for every posting), which made runs crawl on throttled CI runner IPs. The
light list is a few KB; the detail endpoint is hit only for NEWLY inserted
jobs (capped per run), so request volume tracks new postings, not board size.
Same pattern as smartrecruiters.py. See DESIGN.md §4.3.
"""

from __future__ import annotations

import time
from datetime import datetime
from typing import Any

import httpx

from jobops.db import execute, get_conn, heartbeat
from jobops.ingest.common import (
    get_with_backoff,
    insert_job,
    load_watchlist,
    looks_new_grad,
    polite_client,
    upsert_company,
)
from jobops.notify.discord import notify_new_jobs

API = "https://boards-api.greenhouse.io/v1/boards/{token}/jobs"
DETAIL_CAP = 25  # max detail fetches per board per run; protects backfill runs


def map_posting(j: dict[str, Any]) -> dict[str, Any]:
    """Map one Greenhouse job payload to jobs-table fields (pure, fixture-tested).

    posted_at prefers first_published over updated_at — updated_at moves on
    every edit and would misstate posting age.
    """
    posted = j.get("first_published") or j.get("updated_at")
    return {
        "external_id": str(j["id"]),
        "title": j["title"],
        "location": (j.get("location") or {}).get("name"),
        "url": j["absolute_url"],
        "description": j.get("content"),  # HTML-entity-escaped; stripped in enrichment
        "posted_at": datetime.fromisoformat(posted) if posted else None,
    }


def poll_board(token: str, client: httpx.Client) -> list[str]:
    """Poll one board token (light list); returns ids of newly inserted jobs.

    New jobs get a follow-up detail fetch (capped) to fill description and
    recompute is_new_grad from the JD before notifications read the row.
    """
    r = get_with_backoff(client, API.format(token=token))
    if r.status_code == 404:  # board renamed/removed — check_watchlist will flag it
        print(f"[greenhouse:{token}] 404 board not found")
        return []
    r.raise_for_status()
    data = r.json()
    jobs = data.get("jobs", [])
    company_name = (jobs[0].get("company_name") if jobs else None) or token
    with get_conn() as conn, conn.cursor() as cur:
        company_id = upsert_company(cur, company_name, "greenhouse", token)
    new: list[tuple[str, dict[str, Any]]] = []  # (job_id, mapped fields)
    for j in jobs:
        m = map_posting(j)
        jid = insert_job(source="greenhouse", company_id=company_id, raw=j, **m)
        if jid:
            new.append((jid, m))
    for jid, m in new[:DETAIL_CAP]:
        try:
            r = get_with_backoff(client, API.format(token=token) + f"/{m['external_id']}")
            r.raise_for_status()
            content = r.json().get("content")
            if content:
                execute(
                    "UPDATE jobs SET description = %s, is_new_grad = %s WHERE id = %s",
                    (content, looks_new_grad(m["title"], content), jid),
                )
        except Exception as e:  # detail is best-effort; the row already exists
            print(f"[greenhouse:{token}] detail {m['external_id']}: {e}")
    return [jid for jid, _ in new]


def run() -> None:
    """Poll every watched Greenhouse board sequentially; one failure never kills the run."""
    tokens = load_watchlist().get("greenhouse", [])
    all_new: list[str] = []
    failures = 0
    t0 = time.monotonic()
    with polite_client() as client:
        for i, token in enumerate(tokens, 1):
            try:
                all_new += poll_board(token, client)
            except Exception as e:
                failures += 1
                print(f"[greenhouse:{token}] {e}")
            if i % 25 == 0:
                print(f"[greenhouse] {i}/{len(tokens)} boards, {len(all_new)} new so far")
    notify_new_jobs(all_new)
    heartbeat("greenhouse", ok=failures == 0,
              detail=f"{len(tokens) - failures}/{len(tokens)} boards, "
                     f"{len(all_new)} new, {time.monotonic() - t0:.0f}s")
    print(f"[greenhouse] done: {len(all_new)} new, {failures} failed boards")


if __name__ == "__main__":
    run()
