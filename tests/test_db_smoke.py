"""Integration smoke test: all expected tables exist after migration.

Skipped unless DATABASE_URL is set — never runs against an implicit default.
"""

import os

import pytest

EXPECTED_TABLES = {
    "schema_migrations",
    "companies",
    "jobs",
    "resume_versions",
    "applications",
    "application_events",
    "contacts",
    "interactions",
    "follow_ups",
    "interviews",
    "prep_log",
    "heartbeats",
}

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not os.environ.get("DATABASE_URL"),
        reason="DATABASE_URL not set",
    ),
]


def test_all_core_tables_exist():
    from jobops.db import query

    rows = query(
        "SELECT tablename FROM pg_tables WHERE schemaname = 'public'"
    )
    present = {r["tablename"] for r in rows}
    missing = EXPECTED_TABLES - present
    assert not missing, f"missing tables: {sorted(missing)}"
