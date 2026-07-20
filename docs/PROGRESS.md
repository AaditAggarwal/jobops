# PROGRESS

## Status: Phase 3 complete (ingestion engine: 5 pollers, watchlist, notify, dedup, scheduling)

## Sessions completed

### Session 2 — 2026-07-19 — Phase 3: ingestion engine

**Built:**
- `jobops/ingest/common.py`: `normalize_company`, `looks_new_grad`, `upsert_company`, `insert_job` (ON CONFLICT (source, external_id) DO NOTHING, is_new_grad computed at insert, raw wrapped in Jsonb), `polite_client` (honest UA, 20s timeout), `get_with_backoff` (429/5xx retry honoring Retry-After), `load_watchlist`.
- Pollers `greenhouse.py`, `lever.py`, `ashby.py`, `smartrecruiters.py`, `github_repos.py`: each with pure `map_posting()` (fixture-tested), `poll_board()`, `run()` + `__main__`, per-board try/except-continue with `[source:token]` logs, one heartbeat row per run.
- `jobops/notify/discord.py`: `notify_new_job` (new-grad only, sponsor badge) + `notify_new_jobs` batch wrapper with a per-run cap of 15 pings (webhook throttles ~30/min; first backfill would otherwise send thousands). No-ops with one warning if DISCORD_WEBHOOK unset.
- `jobops/enrich/dedup.py`: pure `find_duplicates` (same normalized company + token_sort_ratio ≥ 92 + same location + ≤14 days → later row skipped, earliest kept) + `run()`; `migrations/002_jobs_note.sql` adds `jobs.note` for the skip reason.
- `data/watchlist.yaml` (18 greenhouse / 12 lever / 16 ashby / 9 smartrecruiters tokens, every one verified live this session), `scripts/check_watchlist.py` (flags dead tokens, exit 1), `scripts/poll_all.py` (all pollers + dedup, local cron equivalent), `.github/workflows/poll.yml` (*/10 cron).
- Tests: 80 passing — `test_ingest_common.py` (normalize/new-grad edge cases), `test_pollers.py` (field mapping against real trimmed fixtures in `tests/fixtures/`), `test_dedup.py` (11 cases over the pure decision logic).

**Verified acceptance criteria:** first `uv run python scripts/poll_all.py` inserted 15,512 real jobs across 901 companies (5,083 greenhouse / 2,149 lever / 2,154 ashby / 4,208 smartrec / 1,918 github_repo; 268 flagged new-grad; 782 cross-source dups skipped); second run inserted 0 everywhere (idempotent); Discord pings fired (15/poller cap hit); heartbeat rows ok=t for all six sources; pytest 80 passed 1 skipped.

**Decisions / deviations from DESIGN.md sketches:**
- `upsert_company` uses `COALESCE(NULLIF(EXCLUDED.ats_token,''), companies.ats_token)` so token-less sources (github_repos) can't clobber a real board token — the §4.2 sketch would have.
- Greenhouse `posted_at` prefers `first_published` over `updated_at` (updated_at moves on every edit); Ashby uses `descriptionPlain` (exists in the real payload) over `descriptionHtml`.
- SmartRecruiters: the API 200s with `totalFound: 0` for wrong tokens — never 404s — so `check_watchlist` treats 0 postings as dead. List endpoint has no JD, so the detail endpoint is fetched for newly inserted jobs only, newest first, capped at 40/board/run (`DETAIL_CAP`) — backfill rows beyond the cap keep description NULL.
- Notifications are capped at 15 per poller run and fire before dedup, so a cross-source duplicate can ping twice within one cycle. Accepted for now.
- `poll.yml` runs smartrecruiters + dedup beyond the §4.4 sketch; DESIGN.md §4.3's SimplifyJobs path needed branch `dev` (fetched via raw.githubusercontent.com, no contents-API quota).
- `github_repos.py` accepts GITHUB_TOKEN (Actions) or GH_PAT (local .env) for its auth header.

**Next steps (Phase 4 candidates, per DESIGN.md §5):**
- `migrations/003_sponsors.sql` + `jobops/etl/uscis_hub.py` / `dol_lca.py` (user must download USCIS/DOL CSVs into data/)
- `jobops/enrich/sponsor_match.py` + thorough tests (highest-stakes pure function #2)
- Backfill: SmartRecruiters rows beyond the detail cap have description NULL — enrichment should re-fetch or tolerate.


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

### Session 2b — 2026-07-19 — watchlist expansion (user-delegated homework)

Expanded watchlist 55 → 335 verified boards (155 greenhouse / 30 lever / 132 ashby / 18 smartrecruiters). Sources: ATS tokens mined from the 1,918 SimplifyJobs job URLs already in the DB (433 unique, 480 verified incl. curated) + ~120 hand-curated sponsor-friendly companies probed against the ATS APIs. Curation drops applied: ITAR/defense/clearance companies (can't sponsor F-1→H-1B), staffing/consulting mills (SR extraction was ~80% these), non-US-only boards, non-tech, demo/duplicate artifacts; SR boards >600 postings excluded for pagination budget. Notable adds: Jane Street, DRW, Virtu, Five Rings, Marshall Wace, Optiver, Akuna, Point72, HRT + full quant cluster; Waymo, Reddit, Figma, Discord, Snowflake (ashby), PlayStation, NYT, Wiz, Perplexity, xAI, Scale AI. Ranked 3-tier report delivered to user. `shieldai` (seed) left in but flagged as defense. Remaining homework for user: Supabase + GitHub Actions setup (repo has no remote yet) — guide already provided in chat.

### Session 2c — 2026-07-19 — Supabase + Actions cutover, retention

Repo pushed to github.com/AaditAggarwal/jobops (public, branch main). Supabase project provisioned (ca-central-1); schema migrated; 1,030 companies + 33,874 jobs copied up via resumable batched copier (pg_dump stream died on the session pooler — batched psycopg with ON CONFLICT is the reliable path). `.env` DATABASE_URL now points at the Supabase session pooler (IPv4; transaction pooler needs a paid IPv4 add-on on this project — deviation from the §4.4 assumption). Added `jobops/enrich/retention.py`: jobs pruned 30 days after first_seen_at (90 for is_new_grad), never if referenced by applications/resume_versions; heartbeats pruned at 30 days; wired into poll_all.py + poll.yml. Note: a long-lived posting still on a board re-inserts (and may re-notify) after its row ages out — acceptable churn. Actions secrets + first workflow run done by user.

### Session 2d — 2026-07-20 — Actions performance overhaul

Cloud runs were unusable (~10s/board on greenhouse: GitHub runner IPs are heavily 429-throttled by ATS APIs; a 25-min run covered greenhouse+lever only). Fixes, all four at once:
1. **Greenhouse list-then-detail** (biggest win): dropped `?content=true` (multi-MB/board) for the light list; detail endpoint fetched only for newly inserted jobs, capped 25/board/run (mirrors smartrecruiters pattern). is_new_grad computed title-only at insert, refined from JD by the detail pass before notify reads the row. Beyond-cap rows keep description NULL. DESIGN.md §4.3 sketch deviates here.
2. **Parallel workflow jobs**: poll.yml now runs the five pollers as a matrix (one job per ATS) + an `enrich` job (dedup, retention) after. Interpretation of the "sequential polling" ground rule: politeness is per-provider — each provider still sees one sequential client; cross-provider parallelism doesn't hammer anyone.
3. **Backoff patience env-tuned**: JOBOPS_BACKOFF_RETRIES=0 / CAP=5 in Actions (throttled board = skipped this cycle, self-heals next); local defaults stay 2/30.
4. **Watchlist trimmed 335 → ~240** (user-approved "cut the un-renowned"): obscure tier-3 ashby startups, marginal greenhouse simplify-mined boards, consulting-ish lever tokens, and the international-heavy SR boards (Devoteam/Continental/Ubisoft2/Equinox) that caused the non-US notification noise. Also dropped shieldai (defense).
Also: `push:` trigger on poll.yml (paths-ignore docs) — GitHub's cron proved unreliable (silent for 90+ min); every push now smoke-tests immediately. Ashby: dropped includeCompensation param.

## Notes for future sessions
- Notification semantics gap: pings fire at the end of each poller's run(), so a killed/cancelled run inserts jobs that never notify (observed 2026-07-19: 99 new-grad roles silent after Actions timeout kills). Consider a `notified_at` column on jobs so notification becomes a resumable step instead of an in-memory afterthought.
- Classifier noise: looks_new_grad's JD fallback flags non-tech roles (Harvard "Faculty Administrative Assistant" etc. via "entry level"/"early career" appearing in JD text). Tighten: only apply the JD fallback when the title looks technical, and add the US-location notification filter noted above.
- User feedback after first backfill (2026-07-19): notifications included many non-US postings (several watchlist boards are global — Ubisoft2, Devoteam, Continental, octoenergy, brillio-2) and were dominated by single companies. Add a US/remote-US location filter to notify_new_job (or to is_new_grad gating), and consider per-company caps / triage-score gating when §6 jd_score lands. Backfill artifact only for the same-company clustering; the location gap is real in steady state too.
- Local runs don't auto-load `.env` — pollers read os.environ (DATABASE_URL falls back to the docker default; DISCORD_WEBHOOK/GH_PAT must be exported or injected). Consider a tiny env loader or `uv run --env-file` later.
- Discord webhook is set in the user's `.env` and verified working (2026-07-19).
- Lever/Ashby boards `plaid`, `kraken`, `voleon`, `deel` resolve but had 0 postings on 2026-07-19 — watchlist keeps them; `check_watchlist.py` only treats 0-postings as dead for SmartRecruiters.
- psycopg_pool emits a DeprecationWarning unless `open=True` is passed to ConnectionPool — already handled in db.py; keep it if the pool setup is ever touched.
- `scripts/backup.sh` is bash — on Windows run it via Git Bash or WSL; consider a scheduled task later.
