"""Database access for jobops: connection pool + small query helpers.

No ORM by design (see CLAUDE.md) — SQL lives in code as plain strings, rows
come back as dicts via psycopg's dict_row.
"""

from __future__ import annotations

import os
import sys
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any
from urllib.parse import urlsplit

import psycopg
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

DEFAULT_DATABASE_URL = "postgresql://jobops:jobops@localhost:5432/jobops"
CONNECT_TIMEOUT = int(os.environ.get("JOBOPS_DB_CONNECT_TIMEOUT", "10"))

_pool: ConnectionPool | None = None


def database_url() -> str:
    """Return the Postgres connection string.

    Reads DATABASE_URL from the environment, falling back to the local
    docker-compose default so `docker compose up` + migrate works with zero
    configuration.
    """
    return os.environ.get("DATABASE_URL") or DEFAULT_DATABASE_URL


def get_pool() -> ConnectionPool:
    """Return the process-wide connection pool, creating it on first use.

    Max size is env-tunable (JOBOPS_DB_MAX_CONN, default 4): CI runs several
    single-threaded pollers in parallel against Supabase's session pooler
    (hard cap 15 clients), so each CI process must hold exactly 1 connection.
    """
    global _pool
    if _pool is None:
        _pool = ConnectionPool(
            database_url(),
            min_size=1,
            max_size=int(os.environ.get("JOBOPS_DB_MAX_CONN", "4")),
            open=True,
            timeout=float(os.environ.get("JOBOPS_DB_POOL_TIMEOUT", "30")),
            reconnect_timeout=float(os.environ.get("JOBOPS_DB_RECONNECT_TIMEOUT", "60")),
            kwargs={"row_factory": dict_row, "connect_timeout": CONNECT_TIMEOUT},
        )
    return _pool


def check_connection() -> str | None:
    """Try one direct connection; return None if healthy, else a one-line reason.

    Deliberately bypasses the pool: the pool retries a dead host for minutes,
    which is how a deleted database once burned 28-minute CI jobs on every
    board in the watchlist before failing.
    """
    try:
        with psycopg.connect(database_url(), connect_timeout=CONNECT_TIMEOUT) as conn:
            conn.execute("SELECT 1")
        return None
    except Exception as e:  # noqa: BLE001 - any failure here means "unusable"
        return f"{type(e).__name__}: {' '.join(str(e).split())[:300]}"


def require_db(source: str) -> None:
    """Abort the process immediately (exit 2) when the database is unreachable.

    Every poller calls this before its first request so an infrastructure
    outage costs one second and one clear log line, not a full polling cycle
    of failed inserts. Notification of the outage is the workflow preflight
    job's business (jobops.notify.discord.notify_infra_failure), not each
    poller's - seven matrix jobs must not fire seven identical alerts.
    """
    err = check_connection()
    if err is None:
        return
    print(f"[{source}] DATABASE UNREACHABLE at {redacted_dsn()} -> {err}", file=sys.stderr)
    print(f"[{source}] aborting before polling; nothing can be stored", file=sys.stderr)
    sys.exit(2)


def redacted_dsn() -> str:
    """The connection target as host:port/dbname - never the password."""
    u = urlsplit(database_url())
    return f"{u.hostname}:{u.port or 5432}{u.path}"


@contextmanager
def get_conn() -> Iterator[psycopg.Connection]:
    """Check a connection out of the pool; commits on clean exit, rolls back on error."""
    with get_pool().connection() as conn:
        yield conn


def query(sql: str, params: Any = None) -> list[dict[str, Any]]:
    """Run a SELECT and return all rows as dicts."""
    with get_conn() as conn:
        return conn.execute(sql, params).fetchall()


def query_one(sql: str, params: Any = None) -> dict[str, Any] | None:
    """Run a SELECT and return the first row as a dict, or None."""
    with get_conn() as conn:
        return conn.execute(sql, params).fetchone()


def execute(sql: str, params: Any = None) -> int:
    """Run an INSERT/UPDATE/DELETE and return the affected row count."""
    with get_conn() as conn:
        return conn.execute(sql, params).rowcount


def heartbeat(source: str, ok: bool, detail: str | None = None) -> None:
    """Record a run outcome for a poller/ETL job (see CLAUDE.md error conventions)."""
    execute(
        "INSERT INTO heartbeats (source, ok, detail) VALUES (%s, %s, %s)",
        (source, ok, detail),
    )
