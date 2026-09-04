"""Retention: prune stale postings so the DB stays inside hosted size limits.

Policy:
- jobs older than RETENTION_DAYS (first_seen_at) are deleted, except:
  - new-grad flagged rows get NEW_GRAD_RETENTION_DAYS (they're the targets
    and feed funnel metrics),
  - any row referenced by an application or a resume_version is never deleted.
- heartbeats older than RETENTION_DAYS are deleted.

Old postings are dead postings — boards fill or close them long before 30
days. Deleting whole rows (rather than nulling raw/description) keeps the
"always keep raw payloads" convention true for every row we retain.
"""

from __future__ import annotations

import os

from jobops.db import execute, heartbeat, query_one, require_db
from jobops.ingest.common import RAW_JD_KEYS
from jobops.notify.discord import notify_infra_failure

RETENTION_DAYS = 30
NEW_GRAD_RETENTION_DAYS = 90

# Hosted free tiers cap the database around 500 MB and stop accepting writes —
# or pause the project outright — when it fills. That is how the first Supabase
# project died (silently, on 2026-07-25), so size is now a monitored signal.
SIZE_LIMIT_MB = int(os.environ.get("JOBOPS_DB_LIMIT_MB", "500"))
SIZE_WARN_FRACTION = 0.8


def db_size_mb() -> float:
    """Total size of the current database in megabytes."""
    row = query_one("SELECT pg_database_size(current_database()) AS b")
    return round((row["b"] if row else 0) / 1_048_576, 1)


COMPACT_BATCH = 5000


def compact_raw() -> int:
    """Apply trim_raw's rule in SQL to rows an older poller wrote; returns rows fixed.

    `trim_raw` only helps rows inserted after it shipped, and the table already
    held 32k rows carrying a full duplicate of the job description inside
    `raw` (74 MB of the 114 MB total). This is the same rule expressed in SQL —
    strip the JD keys, record which ones were stripped — so old rows heal on
    the next cycle instead of sitting there until retention ages them out.

    Idempotent: rows already carrying `_trimmed_keys` are skipped, and rows with
    no stored description are left alone (their raw payload is the only copy).
    """
    keys = sorted(RAW_JD_KEYS)
    fixed = execute(
        """
        WITH target AS (
            SELECT id FROM jobs
            WHERE description IS NOT NULL
              AND NOT (raw ? '_trimmed_keys')
              AND raw ?| %(keys)s
            LIMIT %(batch)s
        )
        UPDATE jobs j
        SET raw = (
                SELECT (j.raw - %(keys)s)
                       || jsonb_build_object('_trimmed_keys', to_jsonb(present.k))
                FROM (
                    SELECT array_agg(key ORDER BY key) AS k
                    FROM jsonb_object_keys(j.raw) AS key
                    WHERE key = ANY(%(keys)s)
                ) AS present
            )
        FROM target
        WHERE j.id = target.id
        """,
        {"keys": keys, "batch": COMPACT_BATCH},
    )
    if fixed:
        print(f"[retention] compacted raw payloads on {fixed} legacy rows")
    return fixed


def check_size() -> float:
    """Report database size, alerting Discord past 80% of the free-tier cap."""
    mb = db_size_mb()
    pct = 100 * mb / SIZE_LIMIT_MB if SIZE_LIMIT_MB else 0
    print(f"[retention] database at {mb} MB ({pct:.0f}% of the {SIZE_LIMIT_MB} MB cap)")
    if mb >= SIZE_LIMIT_MB * SIZE_WARN_FRACTION:
        notify_infra_failure(
            f"database is {mb} MB, {pct:.0f}% of the {SIZE_LIMIT_MB} MB free-tier cap",
            "Tighten RETENTION_DAYS or upgrade the plan. A full free-tier "
            "database stops accepting writes and is eventually deleted.",
        )
    return mb


def run() -> None:
    """Delete stale, unreferenced postings and old heartbeat rows; watch DB size."""
    require_db("retention")
    deleted = execute(
        """
        DELETE FROM jobs j
        WHERE j.first_seen_at < now() - make_interval(days => %s)
          AND (j.is_new_grad IS NOT TRUE
               OR j.first_seen_at < now() - make_interval(days => %s))
          AND NOT EXISTS (SELECT 1 FROM applications a WHERE a.job_id = j.id)
          AND NOT EXISTS (SELECT 1 FROM resume_versions r WHERE r.job_id = j.id)
        """,
        (RETENTION_DAYS, NEW_GRAD_RETENTION_DAYS),
    )
    hb = execute(
        "DELETE FROM heartbeats WHERE ran_at < now() - make_interval(days => %s)",
        (RETENTION_DAYS,),
    )
    compacted = compact_raw()
    mb = check_size()
    heartbeat("retention", ok=True,
              detail=f"{deleted} jobs, {hb} heartbeats pruned, "
                     f"{compacted} raw compacted; db {mb} MB")
    print(f"[retention] pruned {deleted} stale jobs, {hb} old heartbeats")


if __name__ == "__main__":
    run()
