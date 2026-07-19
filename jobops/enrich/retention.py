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

from jobops.db import execute, heartbeat

RETENTION_DAYS = 30
NEW_GRAD_RETENTION_DAYS = 90


def run() -> None:
    """Delete stale, unreferenced postings and old heartbeat rows."""
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
    heartbeat("retention", ok=True, detail=f"{deleted} jobs, {hb} heartbeats pruned")
    print(f"[retention] pruned {deleted} stale jobs, {hb} old heartbeats")


if __name__ == "__main__":
    run()
