"""Greenhouse board poller.

GET https://boards-api.greenhouse.io/v1/boards/{token}/jobs?content=true
One request per watched board per run. See DESIGN.md §4.3.
"""

from __future__ import annotations

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

API = "https://boards-api.greenhouse.io/v1/boards/{token}/jobs"


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
    """Poll one board token; returns ids of newly inserted jobs."""
    r = get_with_backoff(client, API.format(token=token), params={"content": "true"})
    if r.status_code == 404:  # board renamed/removed — check_watchlist will flag it
        print(f"[greenhouse:{token}] 404 board not found")
        return []
    r.raise_for_status()
    data = r.json()
    jobs = data.get("jobs", [])
    company_name = jobs[0]["company_name"] if jobs else token
    with get_conn() as conn, conn.cursor() as cur:
        company_id = upsert_company(cur, company_name, "greenhouse", token)
    new_ids = []
    for j in jobs:
        m = map_posting(j)
        jid = insert_job(source="greenhouse", company_id=company_id, raw=j, **m)
        if jid:
            new_ids.append(jid)
    return new_ids


def run() -> None:
    """Poll every watched Greenhouse board sequentially; one failure never kills the run."""
    tokens = load_watchlist().get("greenhouse", [])
    all_new: list[str] = []
    failures = 0
    with polite_client() as client:
        for token in tokens:
            try:
                all_new += poll_board(token, client)
            except Exception as e:
                failures += 1
                print(f"[greenhouse:{token}] {e}")
    notify_new_jobs(all_new)
    heartbeat("greenhouse", ok=failures == 0,
              detail=f"{len(tokens) - failures}/{len(tokens)} boards, {len(all_new)} new")
    print(f"[greenhouse] done: {len(all_new)} new, {failures} failed boards")


if __name__ == "__main__":
    run()
