"""Lever board poller.

GET https://api.lever.co/v0/postings/{token}?mode=json
See DESIGN.md §4.3.
"""

from __future__ import annotations

from datetime import datetime, timezone
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

API = "https://api.lever.co/v0/postings/{token}"


def map_posting(p: dict[str, Any]) -> dict[str, Any]:
    """Map one Lever posting payload to jobs-table fields (pure, fixture-tested)."""
    created = p.get("createdAt")
    return {
        "external_id": p["id"],
        "title": p["text"],
        "location": (p.get("categories") or {}).get("location"),
        "url": p["hostedUrl"],
        "description": p.get("descriptionPlain") or p.get("description"),
        "posted_at": (
            datetime.fromtimestamp(created / 1000, tz=timezone.utc) if created else None
        ),
    }


def poll_board(token: str, client: httpx.Client) -> list[str]:
    """Poll one Lever token; returns ids of newly inserted jobs."""
    r = get_with_backoff(client, API.format(token=token), params={"mode": "json"})
    if r.status_code == 404:
        print(f"[lever:{token}] 404 board not found")
        return []
    r.raise_for_status()
    postings = r.json()
    # Lever's API has no company display name; the token doubles as the name
    # until sponsor-matching/enrichment improves it.
    with get_conn() as conn, conn.cursor() as cur:
        company_id = upsert_company(cur, token, "lever", token)
    new_ids = []
    for p in postings:
        m = map_posting(p)
        jid = insert_job(source="lever", company_id=company_id, raw=p, **m)
        if jid:
            new_ids.append(jid)
    return new_ids


def run() -> None:
    """Poll every watched Lever board sequentially; one failure never kills the run."""
    tokens = load_watchlist().get("lever", [])
    all_new: list[str] = []
    failures = 0
    with polite_client() as client:
        for i, token in enumerate(tokens, 1):
            try:
                all_new += poll_board(token, client)
            except Exception as e:
                failures += 1
                print(f"[lever:{token}] {e}")
            if i % 25 == 0:
                print(f"[lever] {i}/{len(tokens)} boards, {len(all_new)} new so far")
    notify_new_jobs(all_new)
    heartbeat("lever", ok=failures == 0,
              detail=f"{len(tokens) - failures}/{len(tokens)} boards, {len(all_new)} new")
    print(f"[lever] done: {len(all_new)} new, {failures} failed boards")


if __name__ == "__main__":
    run()
