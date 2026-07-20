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


def load_watchlist() -> dict[str, list[str]]:
    """Load data/watchlist.yaml as {ats: [token, ...]}; missing file -> {}."""
    if not WATCHLIST_PATH.exists():
        print(f"[watchlist] {WATCHLIST_PATH} not found")
        return {}
    return yaml.safe_load(WATCHLIST_PATH.read_text(encoding="utf-8")) or {}


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


def looks_new_grad(title: str, jd: str = "") -> bool:
    """Classify whether a posting is plausibly a new-grad/entry-level SWE role.

    Seniority markers in the title veto immediately; otherwise a new-grad
    signal in the title or the first 2000 chars of the JD qualifies. This
    gates Discord notifications, so favor precision on the veto side.
    """
    if SENIOR_PAT.search(title):
        return False
    return bool(NEW_GRAD_PAT.search(title) or NEW_GRAD_PAT.search(jd[:2000]))


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
    at insert time from title + description.
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
                Jsonb(raw),
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
