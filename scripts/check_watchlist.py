"""Watchlist health check: flag board tokens that no longer resolve.

Usage: uv run python scripts/check_watchlist.py [--tier core|tail|all]
Exits non-zero if any token is dead so it can gate CI or a cron alert.
No DB required - this only talks to the ATS endpoints, one request per token.
"""

from __future__ import annotations

import argparse
import sys

from jobops.ingest.board_probe import ATS_ORDER, probe
from jobops.ingest.common import load_watchlist, polite_client


def main() -> int:
    """Probe every watchlist token and report dead ones; returns an exit code."""
    ap = argparse.ArgumentParser()
    ap.add_argument("--tier", default="all", choices=["core", "tail", "all"])
    args = ap.parse_args()

    watch = load_watchlist(tier=args.tier)
    dead = 0
    with polite_client() as client:
        for ats in ATS_ORDER:
            for token in watch.get(ats, []):
                p = probe(ats, token, client)
                mark = "ok  " if p.alive else ("????" if p.alive is None else "DEAD")
                print(f"{mark} [{ats}:{token}] {p.detail}")
                dead += 1 if p.alive is False else 0
    print(f"\n{dead} dead token(s)" if dead else "\nall tokens healthy")
    return 1 if dead else 0


if __name__ == "__main__":
    sys.exit(main())
