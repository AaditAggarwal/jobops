"""Poller for curated GitHub new-grad listing repos (SimplifyJobs et al).

Reads listings.json via raw.githubusercontent.com — one request per repo per
run, no API quota needed. These listings cover Workday/Taleo/iCIMS shops the
ATS pollers can't reach. See DESIGN.md §4.3.
"""

from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any

import httpx

from jobops.db import get_conn, heartbeat
from jobops.ingest.common import insert_job, polite_client, upsert_company
from jobops.notify.discord import notify_new_jobs

REPOS = [
    ("SimplifyJobs", "New-Grad-Positions", "dev", ".github/scripts/listings.json"),
]

RAW_URL = "https://raw.githubusercontent.com/{owner}/{repo}/{branch}/{path}"


def map_listing(item: dict[str, Any]) -> dict[str, Any]:
    """Map one listings.json entry to jobs-table fields (pure, fixture-tested)."""
    posted = item.get("date_posted")
    return {
        "external_id": str(item["id"]),
        "title": item["title"],
        "location": ", ".join(item.get("locations", [])) or None,
        "url": item["url"],
        "description": None,
        "posted_at": (
            datetime.fromtimestamp(posted, tz=timezone.utc) if posted else None
        ),
    }


def poll_repo(owner: str, repo: str, branch: str, path: str,
              client: httpx.Client) -> list[str]:
    """Ingest active listings from one repo; returns ids of newly inserted jobs."""
    r = client.get(RAW_URL.format(owner=owner, repo=repo, branch=branch, path=path))
    r.raise_for_status()
    new_ids = []
    company_ids: dict[str, str] = {}  # per-run cache; listings repeat companies a lot
    for item in r.json():
        if not item.get("active", True) or not item.get("is_visible", True):
            continue
        name = item["company_name"]
        if name not in company_ids:
            with get_conn() as conn, conn.cursor() as cur:
                company_ids[name] = upsert_company(cur, name, None, None)
        m = map_listing(item)
        jid = insert_job(source="github_repo", company_id=company_ids[name],
                         raw=item, **m)
        if jid:
            new_ids.append(jid)
    return new_ids


def run() -> None:
    """Poll every listing repo sequentially; one failure never kills the run."""
    headers = {}
    # GITHUB_TOKEN in Actions (mapped from the GH_PAT secret); GH_PAT locally via .env
    if tok := os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_PAT"):
        headers["Authorization"] = f"Bearer {tok}"
    all_new: list[str] = []
    failures = 0
    with polite_client(headers=headers) as client:
        for owner, repo, branch, path in REPOS:
            try:
                all_new += poll_repo(owner, repo, branch, path, client)
            except Exception as e:
                failures += 1
                print(f"[github_repo:{owner}/{repo}] {e}")
    notify_new_jobs(all_new)
    heartbeat("github_repos", ok=failures == 0,
              detail=f"{len(REPOS) - failures}/{len(REPOS)} repos, {len(all_new)} new")
    print(f"[github_repo] done: {len(all_new)} new, {failures} failed repos")


if __name__ == "__main__":
    run()
