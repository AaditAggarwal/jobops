"""Pipeline preflight: verify the database is usable before a polling cycle runs.

Run as the first job of the poll workflow. On failure it alerts Discord ONCE
(the poller matrix would otherwise fire one identical alert per job) and exits
non-zero so every downstream job is skipped instead of spending a full 28-minute
timeout failing every insert.

Usage: uv run python scripts/preflight.py
"""

from __future__ import annotations

import sys

from jobops.db import check_connection, query_one, redacted_dsn
from jobops.notify.discord import notify_infra_failure


def main() -> int:
    """Check DB reachability and schema presence; alert + fail fast if broken."""
    target = redacted_dsn()
    err = check_connection()
    if err:
        print(f"[preflight] DATABASE UNREACHABLE at {target} -> {err}", file=sys.stderr)
        notify_infra_failure(f"database unreachable at `{target}`", err)
        return 1

    try:
        row = query_one("SELECT count(*) AS n FROM jobs")
    except Exception as e:  # noqa: BLE001 - reachable but unusable is still down
        detail = f"{type(e).__name__}: {' '.join(str(e).split())[:300]}"
        print(f"[preflight] schema check failed: {detail}", file=sys.stderr)
        notify_infra_failure(
            f"database at `{target}` is reachable but has no usable schema "
            "(run scripts/migrate.py)",
            detail,
        )
        return 1

    print(f"[preflight] ok - {target}, {row['n']} jobs")
    return 0


if __name__ == "__main__":
    sys.exit(main())
