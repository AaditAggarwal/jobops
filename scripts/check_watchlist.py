"""Watchlist health check: flag board tokens that no longer resolve.

Usage: uv run python scripts/check_watchlist.py
Exits non-zero if any token is dead so it can gate CI or a cron alert.
No DB required — this only talks to the ATS endpoints, one request per token.
"""

from __future__ import annotations

import sys

import httpx

from jobops.ingest.common import load_watchlist, polite_client

CHECKS = {
    "greenhouse": "https://boards-api.greenhouse.io/v1/boards/{token}/jobs",
    "lever": "https://api.lever.co/v0/postings/{token}?mode=json",
    "ashby": "https://api.ashbyhq.com/posting-api/job-board/{token}",
    "smartrecruiters": "https://api.smartrecruiters.com/v1/companies/{token}/postings?limit=1",
}


def check_token(ats: str, token: str, client: httpx.Client) -> tuple[bool, str]:
    """Return (alive, detail) for one board token."""
    try:
        r = client.get(CHECKS[ats].format(token=token))
    except httpx.HTTPError as e:
        return False, f"error: {e}"
    if r.status_code == 404:
        return False, "404"
    if r.status_code != 200:
        return False, f"HTTP {r.status_code}"
    data = r.json()
    if ats == "greenhouse":
        n = len(data.get("jobs", []))
    elif ats == "lever":
        n = len(data)
    elif ats == "ashby":
        n = len(data.get("jobs", []))
    else:  # smartrecruiters 200s for unknown tokens; 0 postings = dead/wrong token
        n = data.get("totalFound", 0)
        if n == 0:
            return False, "0 postings (token likely wrong — SR never 404s)"
    return True, f"{n} postings"


def main() -> int:
    watch = load_watchlist()
    dead = 0
    with polite_client() as client:
        for ats, url_tpl in CHECKS.items():
            for token in watch.get(ats, []):
                alive, detail = check_token(ats, token, client)
                mark = "ok " if alive else "DEAD"
                print(f"{mark} [{ats}:{token}] {detail}")
                dead += 0 if alive else 1
    print(f"\n{dead} dead token(s)" if dead else "\nall tokens healthy")
    return 1 if dead else 0


if __name__ == "__main__":
    sys.exit(main())
