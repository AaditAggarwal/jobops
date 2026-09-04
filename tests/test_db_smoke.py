"""Integration smoke test: all expected tables exist after migration.

Skipped unless JOBOPS_INTEGRATION is set. DATABASE_URL used to be the gate, but
the package now auto-loads .env, so it is set on every developer machine and no
longer signals intent to run tests against a live database.
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
        not os.environ.get("JOBOPS_INTEGRATION"),
        reason="set JOBOPS_INTEGRATION=1 to run against the live database",
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
