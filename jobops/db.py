"""Database access for jobops: connection pool + small query helpers.

No ORM by design (see CLAUDE.md) — SQL lives in code as plain strings, rows
come back as dicts via psycopg's dict_row.
"""

from __future__ import annotations

import os
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

import psycopg
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

DEFAULT_DATABASE_URL = "postgresql://jobops:jobops@localhost:5432/jobops"

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
            kwargs={"row_factory": dict_row},
        )
    return _pool


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
