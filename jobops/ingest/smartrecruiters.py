"""SmartRecruiters postings poller.

GET https://api.smartrecruiters.com/v1/companies/{token}/postings   (paginated)
GET .../postings/{id}                                               (detail)

The list endpoint has no job description, so the detail endpoint is fetched
for NEWLY inserted jobs only — request volume stays proportional to new
postings, not board size. Note: this API returns 200 with totalFound=0 for
unknown tokens (never 404), so check_watchlist treats 0 postings as suspect.
See DESIGN.md §4.3.
"""

from __future__ import annotations

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
from jobops.notify.discord import notify_new_job

API = "https://api.smartrecruiters.com/v1/companies/{token}/postings"
PAGE_SIZE = 100


def map_posting(item: dict[str, Any], token: str) -> dict[str, Any]:
    """Map one SmartRecruiters list item to jobs-table fields (pure, fixture-tested).

    The list item carries no posting URL; the canonical pattern
    jobs.smartrecruiters.com/{company}/{id} is constructed here and replaced
    with the exact postingUrl once the detail is fetched.
    """
    released = item.get("releasedDate")
    loc = item.get("location") or {}
    return {
        "external_id": str(item["id"]),
        "title": item["name"],
        "location": loc.get("fullLocation") or loc.get("city"),
        "url": f"https://jobs.smartrecruiters.com/{token}/{item['id']}",
        "description": None,  # filled from the detail endpoint for new jobs
        "posted_at": datetime.fromisoformat(released) if released else None,
    }


def description_from_detail(detail: dict[str, Any]) -> str:
    """Concatenate the jobAd sections of a posting detail into one text blob."""
    sections = (detail.get("jobAd") or {}).get("sections") or {}
    parts = []
    for sec in sections.values():
        title, text = sec.get("title", ""), sec.get("text", "")
        if text:
            parts.append(f"{title}\n{text}" if title else text)
    return "\n\n".join(parts)


def fetch_pages(token: str, client: httpx.Client) -> list[dict[str, Any]]:
    """Fetch all posting list pages for a company via limit/offset pagination."""
    items: list[dict[str, Any]] = []
    offset = 0
    while True:
        r = get_with_backoff(client, API.format(token=token),
                             params={"limit": PAGE_SIZE, "offset": offset})
        r.raise_for_status()
        page = r.json()
        items.extend(page.get("content", []))
        offset += PAGE_SIZE
        if offset >= page.get("totalFound", 0):
            return items


def poll_board(token: str, client: httpx.Client) -> list[str]:
    """Poll one SmartRecruiters company; returns ids of newly inserted jobs."""
    items = fetch_pages(token, client)
    if not items:
        print(f"[smartrec:{token}] 0 postings — token may be wrong (API never 404s)")
        return []
    company_name = (items[0].get("company") or {}).get("name") or token
    with get_conn() as conn, conn.cursor() as cur:
        company_id = upsert_company(cur, company_name, "smartrecruiters", token)
    new_ids = []
    for item in items:
        m = map_posting(item, token)
        jid = insert_job(source="smartrec", company_id=company_id, raw=item, **m)
        if not jid:
            continue
        new_ids.append(jid)
        try:
            r = get_with_backoff(client, API.format(token=token) + f"/{item['id']}")
            r.raise_for_status()
            detail = r.json()
            desc = description_from_detail(detail)
            execute(
                """UPDATE jobs SET description = %s, url = COALESCE(%s, url),
                                   is_new_grad = %s WHERE id = %s""",
                (desc, detail.get("postingUrl"),
                 looks_new_grad(m["title"], desc), jid),
            )
        except Exception as e:  # detail is best-effort; the row already exists
            print(f"[smartrec:{token}] detail {item['id']}: {e}")
    return new_ids


def run() -> None:
    """Poll every watched SmartRecruiters company; one failure never kills the run."""
    tokens = load_watchlist().get("smartrecruiters", [])
    new_total, failures = 0, 0
    with polite_client() as client:
        for token in tokens:
            try:
                new_ids = poll_board(token, client)
                new_total += len(new_ids)
                for jid in new_ids:
                    notify_new_job(jid)
            except Exception as e:
                failures += 1
                print(f"[smartrec:{token}] {e}")
    heartbeat("smartrecruiters", ok=failures == 0,
              detail=f"{len(tokens) - failures}/{len(tokens)} boards, {new_total} new")
    print(f"[smartrec] done: {new_total} new, {failures} failed boards")


if __name__ == "__main__":
    run()
