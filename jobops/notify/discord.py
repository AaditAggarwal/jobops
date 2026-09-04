"""Discord webhook notifications for newly ingested postings (DESIGN.md §4.6).

Only is_new_grad postings ping — the whole point is signal, not noise. The
badge encodes the company's sponsor_status so a glance answers "worth it?".
"""

from __future__ import annotations

import os
import time

import httpx

from jobops.db import query_one
from jobops.ingest.common import looks_us_location

_warned_no_webhook = False

BADGES = {"verified": "\U0001f7e2", "likely": "\U0001f7e1"}  # green / yellow circle
DEFAULT_BADGE = "⚪"  # white circle


def notify_new_job(job_id: str) -> bool:
    """Send a Discord push for a new job if it looks new-grad; returns True if sent.

    No-ops (with a single warning) when DISCORD_WEBHOOK is unset so pollers
    work in environments without notifications configured. Never raises — a
    failed ping must not fail an ingest run.
    """
    global _warned_no_webhook
    webhook = os.environ.get("DISCORD_WEBHOOK")
    if not webhook:
        if not _warned_no_webhook:
            print("[discord] DISCORD_WEBHOOK not set; notifications disabled")
            _warned_no_webhook = True
        return False

    j = query_one(
        """
        SELECT j.title, j.url, j.location, j.is_new_grad,
               co.name, co.sponsor_status, co.sponsor_score
        FROM jobs j JOIN companies co ON co.id = j.company_id
        WHERE j.id = %s
        """,
        (job_id,),
    )
    if not j or not j["is_new_grad"]:
        return False
    if not looks_us_location(j["location"]):
        # Several watched boards are global. A Bengaluru or Dublin posting is
        # not actionable on an F-1 → H-1B path, and it crowds out the ones that
        # are — the loudest complaint after the first backfill.
        return False

    badge = BADGES.get(j["sponsor_status"], DEFAULT_BADGE)
    content = f"{badge} **{j['name']}** — {j['title']} ({j['location'] or 'location n/a'})\n{j['url']}"
    try:
        httpx.post(webhook, json={"content": content}, timeout=10).raise_for_status()
    except Exception as e:
        print(f"[discord] notify failed: {e}")
        return False
    return True


ALERT_BADGE = "🚨"  # rotating light

NOTIFY_CAP = 15  # max pings per poller run (backfills; Discord throttles ~30/min)


def notify_new_jobs(job_ids: list[str], cap: int = NOTIFY_CAP) -> int:
    """Notify for a batch of new job ids, sending at most `cap` pings.

    The cap exists for backfill runs (a first poll inserts a board's whole
    history) — Discord webhooks throttle around 30 req/min, and a thousand
    phone pings helps nobody. Steady-state cycles see far fewer than `cap`
    new-grad postings. Returns the number actually sent.
    """
    if cap <= 0:
        return 0
    sent = 0
    for i, jid in enumerate(job_ids):
        if sent >= cap:
            print(f"[discord] cap reached ({cap}); "
                  f"{len(job_ids) - i} remaining new jobs not checked")
            break
        if notify_new_job(jid):
            sent += 1
            time.sleep(0.5)  # stay far under the webhook rate limit
    return sent


def notify_infra_failure(what: str, detail: str) -> bool:
    """Alert Discord that the pipeline itself is broken; returns True if sent.

    Job pings are the only signal this system normally emits, so an
    infrastructure failure looks exactly like a quiet job market: the database
    was deleted on 2026-07-25 and the silence read as "no new roles" for six
    weeks. Anything that stops the pipeline must say so out loud.
    """
    webhook = os.environ.get("DISCORD_WEBHOOK")
    if not webhook:
        print(f"[discord] DISCORD_WEBHOOK not set; infra alert suppressed: {what}")
        return False
    content = "\n".join([
        f"{ALERT_BADGE} **jobops pipeline down** — {what}",
        f"```{detail[:1500]}```",
        "No job notifications will arrive until this is fixed.",
    ])
    try:
        httpx.post(webhook, json={"content": content}, timeout=10).raise_for_status()
    except Exception as e:  # noqa: BLE001 - alerting must never raise
        print(f"[discord] infra alert failed: {e}")
        return False
    return True
