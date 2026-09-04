"""Shared ingestion core: company normalization, new-grad classifier, upserts.

Every poller funnels through these functions so dedup keys (company
name_normalized, jobs (source, external_id)) stay consistent across sources.
See DESIGN.md §4.2.
"""

from __future__ import annotations

import os
import re
import time
import zlib
from datetime import datetime
from pathlib import Path
from typing import Any

import httpx
import psycopg
import yaml
from psycopg.types.json import Jsonb

from jobops.db import get_conn

USER_AGENT = "jobops/1.0 (personal job tracker)"
REPO_ROOT = Path(__file__).resolve().parents[2]
WATCHLIST_PATH = REPO_ROOT / "data" / "watchlist.yaml"
TAIL_WATCHLIST_PATH = REPO_ROOT / "data" / "watchlist_tail.yaml"

TIER_PATHS = {"core": WATCHLIST_PATH, "tail": TAIL_WATCHLIST_PATH}


def _read_watchlist(path: Path) -> dict[str, list[str]]:
    """Load one watchlist YAML file as {ats: [token, ...]}; missing file -> {}."""
    if not path.exists():
        print(f"[watchlist] {path} not found")
        return {}
    data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    return {ats: list(tokens or []) for ats, tokens in data.items()}


def merge_watchlists(*lists: dict[str, list[str]]) -> dict[str, list[str]]:
    """Merge tier watchlists per ATS, preserving order and dropping duplicates (pure)."""
    merged: dict[str, list[str]] = {}
    for wl in lists:
        for ats, tokens in wl.items():
            seen = merged.setdefault(ats, [])
            known = {t.lower() for t in seen}
            seen.extend(t for t in tokens if t.lower() not in known and not known.add(t.lower()))
    return merged


def load_watchlist(tier: str | None = None) -> dict[str, list[str]]:
    """Load the board watchlist as {ats: [token, ...]} for one cadence tier.

    Boards are split across two files so CI can poll them at different rates
    without ever running more than two sequential streams against one provider:
    `core` (data/watchlist.yaml, the majors — every cycle) and `tail`
    (data/watchlist_tail.yaml, the long tail — hourly). `tier` defaults to
    JOBOPS_TIER, and to "all" locally so a manual run covers everything.
    """
    tier = (tier or os.environ.get("JOBOPS_TIER") or "all").lower()
    if tier == "all":
        return merge_watchlists(*(_read_watchlist(p) for p in TIER_PATHS.values()))
    if tier not in TIER_PATHS:
        raise ValueError(f"unknown watchlist tier {tier!r} (expected core/tail/all)")
    return _read_watchlist(TIER_PATHS[tier])


def shard_tokens(tokens: list[str]) -> list[str]:
    """Filter tokens to this process's shard per JOBOPS_SHARD ("i/n"), if set.

    Big providers are split across parallel CI jobs as disjoint SEQUENTIAL
    streams (user-approved 2026-07-20 amendment to the polite-client rule:
    at most 2 streams per provider, never the same board twice). The hash is
    crc32, not hash() — Python randomizes hash() per process, which would
    break the disjoint/complete guarantee across jobs.
    """
    spec = os.environ.get("JOBOPS_SHARD")
    if not spec:
        return rotate_tokens(tokens)
    idx, count = (int(x) for x in spec.split("/"))
    mine = [t for t in tokens if zlib.crc32(t.lower().encode()) % count == idx]
    print(f"[shard {spec}] {len(mine)}/{len(tokens)} boards")
    return rotate_tokens(mine)


def rotate_tokens(tokens: list[str], now: float | None = None) -> list[str]:
    """Rotate the polling order by a per-half-hour offset.

    A job killed by the CI timeout always dies partway through the list; with
    a fixed order the SAME tail boards would never be polled. Rotating the
    starting point each 30-min cycle guarantees every board gets covered
    within a few cycles even when runs are cut short.
    """
    if not tokens:
        return tokens
    offset = int((now if now is not None else time.time()) // 1800) % len(tokens)
    return tokens[offset:] + tokens[:offset]

# A board's own job titles are the only honest evidence of what it hires for,
# and the probe response already contains them - so relevance costs no extra
# request. A "live board" that has never posted an engineering role is a live
# board we should not spend a poll-cycle slot on.
SWE_TITLE_PAT = re.compile(
    r"\b(software|swe|engineer(ing)?|developer|programmer|data scien(ce|tist)|"
    r"machine learning|deep learning|\bml\b|\bai\b|research scientist|"
    r"applied scientist|backend|back.end|frontend|front.end|full.?stack|"
    r"infrastructure|platform|devops|\bsre\b|site reliability|security engineer|"
    r"mobile|ios|android|firmware|embedded|robotics|compiler|quantitative)\b",
    re.I,
)


# US locations, as they appear in ATS payloads. Targeting is US roles with
# H-1B sponsorship, so a board that posts only outside the US is noise — this
# was the single loudest complaint after the first backfill.
US_LOCATION_PAT = re.compile(
    r"(united states|\bu\.?s\.?a?\b|remote.{0,12}\b(us|usa|united states)\b|"
    r"\b(al|az|ar|ca|co|ct|de|fl|ga|hi|id|il|in|ia|ks|ky|la|me|md|ma|mi|mn|ms|"
    r"mo|mt|ne|nv|nh|nj|nm|ny|nc|nd|oh|ok|or|pa|ri|sc|sd|tn|tx|ut|vt|va|wa|wv|"
    r"wi|wy|dc)\b\s*(,|$)|"
    r"\b(california|texas|new york|washington|massachusetts|illinois|colorado|"
    r"georgia|florida|virginia|arizona|oregon|utah|north carolina|pennsylvania|"
    r"san francisco|new york city|seattle|austin|boston|chicago|denver|atlanta|"
    r"los angeles|palo alto|mountain view|sunnyvale|san jose|santa clara|"
    r"bellevue|redmond|cambridge|brooklyn|miami|dallas|houston|philadelphia|"
    r"san diego|portland|nashville|pittsburgh|minneapolis|detroit|phoenix)\b)",
    re.I,
)

NEW_GRAD_PAT = re.compile(
    r"\b(new ?grad(uate)?|university grad(uate)?|entry.?level|early career|campus|"
    r"(software|swe).{0,30}(intern(ship)?\b|20(2[6-9])))\b",
    re.I,
)
SENIOR_PAT = re.compile(
    r"\b(senior|staff|principal|lead|manager|director|sr\.?)\b", re.I
)

CORP_SUFFIX_PAT = re.compile(
    r"\b(inc|llc|ltd|corp|corporation|co|company|technologies|technology|labs|"
    r"holdings|group|usa|us)\b"
)


def normalize_company(name: str) -> str:
    """Canonicalize a company name for cross-source matching.

    Lowercases, drops punctuation and common corporate suffixes, collapses
    whitespace: "Stripe, Inc." -> "stripe". Used as the companies unique key
    and by the sponsor matcher later, so changes here reshape identity —
    keep conservative.
    """
    n = name.lower().strip()
    n = re.sub(r"[,\.'’]", "", n)
    n = CORP_SUFFIX_PAT.sub("", n)
    return re.sub(r"\s+", " ", n).strip()


def looks_us_location(location: str | None) -> bool:
    """Whether a posting's location is US-based (or unstated) — pure.

    Unstated locations pass: plenty of real US postings leave the field empty,
    and this gates notifications, where a false negative is a missed job.
    """
    loc = (location or "").strip()
    return not loc or bool(US_LOCATION_PAT.search(loc))


def looks_new_grad(title: str, jd: str = "") -> bool:
    """Classify whether a posting is plausibly a new-grad/entry-level SWE role.

    Seniority markers in the title veto immediately; otherwise a new-grad
    signal in the title qualifies. The JD fallback only applies when the title
    is technical: "entry level" and "early career" appear in the body text of
    all sorts of postings, and without this guard the classifier flagged
    non-technical roles like "Faculty Administrative Assistant".

    This gates Discord notifications, so favor precision on the veto side.
    """
    if SENIOR_PAT.search(title):
        return False
    if NEW_GRAD_PAT.search(title):
        return True
    return bool(SWE_TITLE_PAT.search(title) and NEW_GRAD_PAT.search(jd[:2000]))


def upsert_company(
    cur: psycopg.Cursor,
    name: str,
    ats_type: str | None,
    ats_token: str | None,
) -> str:
    """Insert or fetch a company by normalized name; returns its id.

    On conflict, ats_type/ats_token only overwrite when the new value is
    non-empty — sources without board tokens (github_repos, email) must not
    clobber a real token set by an ATS poller.
    """
    cur.execute(
        """
        INSERT INTO companies (name, name_normalized, ats_type, ats_token)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (name_normalized) DO UPDATE SET
            ats_type  = COALESCE(NULLIF(EXCLUDED.ats_type, ''), companies.ats_type),
            ats_token = COALESCE(NULLIF(EXCLUDED.ats_token, ''), companies.ats_token)
        RETURNING id
        """,
        (name, normalize_company(name), ats_type, ats_token),
    )
    return cur.fetchone()["id"]


RAW_JD_KEYS = frozenset(
    {"content", "description", "descriptionHtml", "descriptionPlain",
     "descriptionBody", "plaintext", "jobAd"}
)


def trim_raw(raw: dict[str, Any], description: str | None) -> dict[str, Any]:
    """Drop description blobs from a raw payload when the JD is stored separately.

    The "keep raw payloads" rule exists so enrichment can be re-run over history
    without re-fetching — it is not a mandate to store the same 40 KB of job
    description twice per row. When `description` is non-empty the JD is already
    in its own column, so the duplicate keys are removed and listed under
    `_trimmed_keys` so the payload stays self-describing. Rows with no stored
    description (e.g. beyond a poller's detail cap) keep their raw payload
    intact. This is the difference between ~3 KB and ~25 KB per job, and a
    free-tier database that survives the year.
    """
    if not description or not isinstance(raw, dict):
        return raw
    dropped = sorted(k for k in raw if k in RAW_JD_KEYS)
    if not dropped:
        return raw
    trimmed = {k: v for k, v in raw.items() if k not in RAW_JD_KEYS}
    trimmed["_trimmed_keys"] = dropped
    return trimmed


def insert_job(
    source: str,
    external_id: str,
    company_id: str,
    title: str,
    location: str | None,
    url: str,
    description: str | None,
    posted_at: datetime | str | None,
    raw: dict[str, Any],
) -> str | None:
    """Insert a job if unseen; returns the new job id, or None if it existed.

    Idempotent via ON CONFLICT (source, external_id) DO NOTHING, so pollers
    can re-run over full board payloads safely. is_new_grad is computed here
    at insert time from title + description, and the raw payload is trimmed of
    JD duplicates (see trim_raw) before it is stored.
    """
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO jobs (source, external_id, company_id, title, location,
                              url, description, posted_at, raw, is_new_grad)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (source, external_id) DO NOTHING
            RETURNING id
            """,
            (
                source,
                external_id,
                company_id,
                title,
                location,
                url,
                description,
                posted_at,
                Jsonb(trim_raw(raw, description)),
                looks_new_grad(title, description or ""),
            ),
        )
        row = cur.fetchone()
        return row["id"] if row else None


def polite_client(**kwargs: Any) -> httpx.Client:
    """An httpx client with our honest User-Agent and a sane timeout."""
    headers = {"User-Agent": USER_AGENT} | kwargs.pop("headers", {})
    return httpx.Client(headers=headers, timeout=20, **kwargs)


def get_with_backoff(
    client: httpx.Client, url: str, retries: int | None = None, **kwargs: Any
) -> httpx.Response:
    """GET with simple backoff on 429/5xx; returns the last response.

    Polite-client rule: we never hammer — on throttle/server error, sleep
    (respecting Retry-After when present) and retry. 404 and other 4xx
    return immediately for the caller to interpret.

    Patience is env-tunable because shared CI runner IPs get 429'd far more
    than a home IP, and long sleeps × many boards blow the workflow timeout:
    JOBOPS_BACKOFF_RETRIES (default 2) and JOBOPS_BACKOFF_CAP seconds
    (default 30). A board that stays throttled is skipped this cycle and
    self-heals on the next run.
    """
    if retries is None:
        retries = int(os.environ.get("JOBOPS_BACKOFF_RETRIES", "2"))
    cap = float(os.environ.get("JOBOPS_BACKOFF_CAP", "30"))
    resp = client.get(url, **kwargs)
    for attempt in range(retries):
        if resp.status_code != 429 and resp.status_code < 500:
            break
        wait = float(resp.headers.get("Retry-After") or 2 ** (attempt + 1))
        time.sleep(min(wait, cap))
        resp = client.get(url, **kwargs)
    return resp
