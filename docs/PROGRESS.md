# PROGRESS

## Status: Phase 1 complete (scaffold + core schema + migration runner)

## Sessions completed

### Session 1 — 2026-07-19 — Phase 1: scaffold, docker, core schema, migrate

**Built:**
- `pyproject.toml` (uv, Python 3.12 pinned via `.python-version`; deps: psycopg[binary,pool], httpx, pyyaml; dev: pytest with an `integration` marker)
- `.gitignore`, `.env.example` (documents DATABASE_URL, ANTHROPIC_API_KEY, DISCORD_WEBHOOK, GH_PAT, Google OAuth file paths)
- `docker-compose.yml`: postgres:16 (named volume `jobops_pgdata`, pg_isready healthcheck, port 5432) + adminer on :8080
- `migrations/001_core.sql`: verbatim DESIGN.md §3 schema (companies, jobs, resume_versions, applications, application_events, contacts, interactions, follow_ups, interviews, prep_log + indexes + pg_trgm/pgcrypto), plus `heartbeats(source, ok, ran_at, detail)` with an index on (source, ran_at DESC)
- `scripts/migrate.py`: applies `migrations/*.sql` ordered by 3-digit prefix, records filenames in `schema_migrations`, idempotent, one transaction per migration (SQL + tracking insert commit together). Filename parsing/ordering/pending-selection are pure functions for unit testing; rejects misnamed files and duplicate numbers.
- `jobops/db.py`: lazy singleton psycopg_pool ConnectionPool (dict_row, min 1 / max 4), `get_conn()` context manager, `query`/`query_one`/`execute` helpers, `heartbeat()` writer. `database_url()` falls back to the docker-compose default when DATABASE_URL is unset.
- `jobops/models.py`: plain dataclasses Company, Job, Application mirroring schema columns (type clarity only, no ORM).
- `scripts/backup.sh`: pg_dump | gzip to `backups/jobops_<stamp>.sql.gz`, prunes to newest 14.
- Tests: `tests/test_migrate.py` (unit, 12 tests over the pure migration logic), `tests/test_db_smoke.py` (`@pytest.mark.integration`, skipped unless DATABASE_URL is set; asserts all 12 expected tables exist).

**Verified acceptance criteria:** `docker compose up -d --wait` then `uv run python scripts/migrate.py` applies 001; re-run prints "up to date" (no-op); `uv run pytest` → 12 passed 1 skipped without DATABASE_URL, 13 passed with it; Adminer at localhost:8080 returns 200 and `\dt` shows all 12 tables.

**Decisions / deviations:**
- `schema_migrations` keys on filename (TEXT PRIMARY KEY) rather than a numeric version column — simplest thing that supports "skip if applied".
- Added `.python-version` (3.12) because uv otherwise picked the system's 3.13; CLAUDE.md fixes the stack at 3.12.
- Gave `heartbeats` a `DEFAULT now()` on ran_at and an index on (source, ran_at DESC) beyond the bare column spec — needed for "latest heartbeat per source" dashboard queries.
- `database_url()` defaults to the local docker URL so zero-config local dev works; the integration test still gates on the env var explicitly so CI never hits an implicit DB.

**Next steps (Phase 2 candidates, per DESIGN.md §4):**
- `jobops/ingest/common.py` (shared upsert path: company get-or-create by name_normalized, jobs ON CONFLICT (source, external_id) DO NOTHING, polite httpx client with jobops User-Agent + backoff)
- Greenhouse/Lever/Ashby/SmartRecruiters pollers with fixture-based tests
- `data/watchlist.yaml` + `scripts/seed_watchlist.py` (watchlist curation needs user input)
- `.github/workflows/poll.yml` once pollers exist

## Notes for future sessions
- psycopg_pool emits a DeprecationWarning unless `open=True` is passed to ConnectionPool — already handled in db.py; keep it if the pool setup is ever touched.
- `scripts/backup.sh` is bash — on Windows run it via Git Bash or WSL; consider a scheduled task later.
