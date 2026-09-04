"""Live probes for ATS board tokens — the shared truth for "does this board exist?".

Used by scripts/check_watchlist.py (health of boards we already poll) and
scripts/discover_boards.py (verifying candidate tokens before they enter the
watchlist). One request per token, honest User-Agent, no retries: a probe is a
cheap yes/no, and a throttled answer is treated as "unknown", never as "alive".
"""

from __future__ import annotations

from typing import NamedTuple

import httpx

from jobops.ingest.common import SWE_TITLE_PAT, US_LOCATION_PAT

CHECKS = {
    "greenhouse": "https://boards-api.greenhouse.io/v1/boards/{token}/jobs",
    "lever": "https://api.lever.co/v0/postings/{token}?mode=json",
    "ashby": "https://api.ashbyhq.com/posting-api/job-board/{token}",
    "smartrecruiters": "https://api.smartrecruiters.com/v1/companies/{token}/postings?limit=1",
}

ATS_ORDER = ["ashby", "greenhouse", "lever", "smartrecruiters"]


class Probe(NamedTuple):
    """Outcome of one board probe. `alive` is None when the answer was inconclusive."""

    alive: bool | None
    count: int
    detail: str
    swe: int = 0  # postings whose titles read as software/engineering roles
    us: int = 0   # postings located in the United States


def _rows(ats: str, data: object) -> list[dict]:
    """The posting rows of an ATS list payload (pure)."""
    if ats == "lever":
        rows = data if isinstance(data, list) else []
    elif isinstance(data, dict):
        rows = data.get("jobs") or data.get("content") or []
    else:
        rows = []
    return [r for r in rows if isinstance(r, dict)]


def titles(ats: str, data: object) -> list[str]:
    """Extract posting titles from an ATS list payload (pure).

    Lever calls the title `text` (see jobops/ingest/lever.py); everyone else
    uses `title`.
    """
    return [str(r.get("title") or r.get("text") or "") for r in _rows(ats, data)]


def locations(ats: str, data: object) -> list[str]:
    """Extract posting locations from an ATS list payload (pure).

    Each ATS nests location differently: Greenhouse and SmartRecruiters use an
    object, Lever buries it under `categories`, Ashby uses a plain string.
    """
    out = []
    for r in _rows(ats, data):
        loc = r.get("location")
        if isinstance(loc, dict):  # greenhouse {"name": ...}; SR {"city","country"}
            loc = loc.get("name") or " ".join(
                str(loc.get(k, "")) for k in ("city", "region", "country")
            )
        elif not loc:  # lever
            loc = (r.get("categories") or {}).get("location") if isinstance(
                r.get("categories"), dict) else None
        out.append(str(loc or ""))
    return out


def count_swe_titles(ats: str, data: object) -> int:
    """How many of a board's postings read as software/engineering roles (pure)."""
    return sum(1 for t in titles(ats, data) if SWE_TITLE_PAT.search(t))


def count_us_postings(ats: str, data: object) -> int:
    """How many of a board's postings are located in the US (pure)."""
    return sum(1 for loc in locations(ats, data) if US_LOCATION_PAT.search(loc))


def count_postings(ats: str, data: object) -> int:
    """Extract the posting count from an ATS list payload (pure)."""
    if ats == "lever":
        return len(data) if isinstance(data, list) else 0
    if not isinstance(data, dict):
        return 0
    if ats in ("greenhouse", "ashby"):
        return len(data.get("jobs", []))
    return int(data.get("totalFound", 0))  # smartrecruiters


def classify(ats: str, status: int, data: object) -> Probe:
    """Turn an HTTP status + payload into a Probe verdict (pure).

    SmartRecruiters never 404s — it returns 200 with totalFound 0 for unknown
    tokens — so zero postings is its dead-token signal. For the other three a
    live board with zero open roles is still a real board and stays alive.
    429/5xx are inconclusive (None): a throttled probe must not evict a board.
    """
    if status == 404:
        return Probe(False, 0, "404")
    if status == 429 or status >= 500:
        return Probe(None, 0, f"HTTP {status} (inconclusive)")
    if status != 200:
        return Probe(False, 0, f"HTTP {status}")
    n = count_postings(ats, data)
    if ats == "smartrecruiters" and n == 0:
        return Probe(False, 0, "0 postings (SR never 404s - token likely wrong)")
    swe = count_swe_titles(ats, data)
    us = count_us_postings(ats, data)
    return Probe(True, n, f"{n} postings, {swe} engineering, {us} US", swe, us)


def probe(ats: str, token: str, client: httpx.Client) -> Probe:
    """Probe one board token live. Never raises — network errors are inconclusive."""
    try:
        r = client.get(CHECKS[ats].format(token=token))
    except httpx.HTTPError as e:
        return Probe(None, 0, f"error: {type(e).__name__}")
    try:
        data = r.json() if r.status_code == 200 else None
    except ValueError:
        return Probe(False, 0, "non-JSON response")
    return classify(ats, r.status_code, data)
