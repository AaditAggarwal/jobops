"""Ashby job-board poller.

GET https://api.ashbyhq.com/posting-api/job-board/{token}?includeCompensation=true
See DESIGN.md §4.3.
"""

from __future__ import annotations

import time
from datetime import datetime
from typing import Any

import httpx

from jobops.db import get_conn, heartbeat
from jobops.ingest.common import (
    get_with_backoff,
    insert_job,
    load_watchlist,
    polite_client,
    upsert_company,
)
from jobops.notify.discord import notify_new_jobs

API = "https://api.ashbyhq.com/posting-api/job-board/{token}"


def map_posting(j: dict[str, Any]) -> dict[str, Any]:
    """Map one Ashby job payload to jobs-table fields (pure, fixture-tested).

    descriptionPlain preferred over descriptionHtml — the jobs.description
    column wants text, and the full payload stays in raw anyway.
    """
    published = j.get("publishedAt")
    return {
        "external_id": j["id"],
        "title": j["title"],
        "location": j.get("location"),
        "url": j["jobUrl"],
        "description": j.get("descriptionPlain") or j.get("descriptionHtml"),
        "posted_at": datetime.fromisoformat(published) if published else None,
    }


def poll_board(token: str, client: httpx.Client) -> list[str]:
    """Poll one Ashby token; returns ids of newly inserted jobs."""
    r = get_with_backoff(
        client, API.format(token=token), params={"includeCompensation": "true"}
    )
    if r.status_code == 404:
        print(f"[ashby:{token}] 404 board not found")
        return []
    r.raise_for_status()
    jobs = r.json().get("jobs", [])
    # Ashby's posting API has no company display name; token doubles as name.
    with get_conn() as conn, conn.cursor() as cur:
        company_id = upsert_company(cur, token, "ashby", token)
    new_ids = []
    for j in jobs:
        if not j.get("isListed", True):
            continue
        m = map_posting(j)
        jid = insert_job(source="ashby", company_id=company_id, raw=j, **m)
        if jid:
            new_ids.append(jid)
    return new_ids


def run() -> None:
    """Poll every watched Ashby board sequentially; one failure never kills the run."""
    tokens = load_watchlist().get("ashby", [])
    all_new: list[str] = []
    failures = 0
    t0 = time.monotonic()
    with polite_client() as client:
        for i, token in enumerate(tokens, 1):
            try:
                all_new += poll_board(token, client)
            except Exception as e:
                failures += 1
                print(f"[ashby:{token}] {e}")
            if i % 25 == 0:
                print(f"[ashby] {i}/{len(tokens)} boards, {len(all_new)} new so far")
    notify_new_jobs(all_new)
    heartbeat("ashby", ok=failures == 0,
              detail=f"{len(tokens) - failures}/{len(tokens)} boards, "
                     f"{len(all_new)} new, {time.monotonic() - t0:.0f}s")
    print(f"[ashby] done: {len(all_new)} new, {failures} failed boards")


if __name__ == "__main__":
    run()
