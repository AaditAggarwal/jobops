"""Run every poller once, then dedup — the local-cron equivalent of poll.yml.

Usage: uv run python scripts/poll_all.py
Schedule with Windows Task Scheduler / cron every 10 minutes if not using
GitHub Actions. One failing poller never blocks the rest.
"""

from __future__ import annotations

import sys

from jobops.db import require_db
from jobops.enrich import dedup, retention, sponsor_match
from jobops.ingest import ashby, github_repos, greenhouse, lever, smartrecruiters

POLLERS = [
    ("greenhouse", greenhouse.run),
    ("lever", lever.run),
    ("ashby", ashby.run),
    ("smartrecruiters", smartrecruiters.run),
    ("github_repos", github_repos.run),
    ("dedup", dedup.run),
    ("sponsor_match", sponsor_match.run),
    ("retention", retention.run),
]


def main() -> int:
    """Run every poller and enrichment step once; returns an exit code."""
    require_db("poll_all")  # fail in one second, not once per poller
    failed = []
    for name, run in POLLERS:
        try:
            run()
        except Exception as e:  # each run() already catches per-board errors
            failed.append(name)
            print(f"[poll_all:{name}] {e}")
    if failed:
        print(f"[poll_all] failed: {', '.join(failed)}")
        return 1
    print("[poll_all] all pollers completed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
