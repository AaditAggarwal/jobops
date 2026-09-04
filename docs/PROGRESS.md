# PROGRESS

## Status: Phase 3 complete + Phase 5 partial (sponsor classifier). Pipeline was DOWN
2026-07-25 -> 2026-09-01 (deleted Supabase project); code fixed, awaiting a new DATABASE_URL.

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

### Session 2e — 2026-07-20 — Phase 5 (partial): sponsorship classifier

Built `migrations/003_sponsors.sql` (§5.2 schema; 003 because 002 was taken by jobs.note), `jobops/etl/uscis_hub.py` (delete-and-reload by src, header-variant-tolerant `parse_row`, heartbeat), `jobops/enrich/sponsor_match.py` (pure `compute_score`/`pick_best_match` + pg_trgm-prefiltered `score_company`), 23 unit tests. Wired into poll_all + workflow enrich job (cheap: only scores sponsor_status='unknown' companies, no-ops while sponsor_records is empty). **Scoring deviations from §5.3 sketch:** approval_rate Laplace-smoothed (+5) — the raw formula gave 'verified' to any recent 1-approval shop (test-caught); recency cutoff is fiscal-year-relative, not hardcoded 2024. DESIGN.md updated in place. **Blocked on data:** uscis.gov WAF 403s non-browser clients (honest-UA rule: no spoofing) — user must download the yearly Data Hub CSVs into data/uscis/ (~3 min, instructions given in chat), then `uv run python -m jobops.etl.uscis_hub && uv run python -m jobops.enrich.sponsor_match` lights up the badges. **Deferred:** DOL LCA ETL (§5.2's second source) — needs openpyxl (not in fixed stack; requires user sign-off) and ~700MB quarterly files; USCIS alone drives the current score formula. Cron moved to */30 (user-approved).

### Session 2g — 2026-07-20 — timeout resilience (final polling architecture)

Runner-IP speed variance is extreme (3.7s/board to 32s/board between jobs of the SAME run), so shard jobs on bad draws still timed out. Final fixes: (1) notifications moved inside the per-board loop (shared NOTIFY_CAP counter) — a killed job can't swallow pings for boards already polled; (2) `rotate_tokens` shifts poll order by a per-30-min offset so timeout tails rotate and every board is covered within a few cycles regardless; (3) poll job timeout 15→28 min. Polling architecture is now correct under worst-case IP draws — remaining timeouts only delay tail-board coverage by a cycle, never lose data or pings.

### Session 2f — 2026-07-20 — 2-way board sharding (politeness amendment)

Cloud diagnosis final: ATS APIs tarpit GitHub runner IPs (~7-30s/request regardless of payload; lever = 30s/board with zero failures), so greenhouse/ashby couldn't finish a sequential pass inside the 15-min job timeout. User chose 2-way sharding over alternate-cycles/trimming (AskUserQuestion). `shard_tokens()` in ingest/common.py filters by `JOBOPS_SHARD="i/n"` env using crc32 (NOT hash() — randomized per process, would break disjointness across CI jobs); workflow matrix runs greenhouse+ashby as 2 jobs each over disjoint halves. CLAUDE.md polite-client rule amended in place (user-approved): max 2 parallel sequential streams per provider, disjoint boards, never the same board concurrently. Tests assert the disjoint/complete/stable partition guarantee.

### Session 3 — 2026-09-01 — outage recovery + corpus expansion

**The outage (root cause).** Every Actions run since ~2026-07-25 failed with
`FATAL: (ENOTFOUND) tenant/user postgres.zzhrhamumvosrlxqpisc not found`. The
Supabase project is gone — `zzhrhamumvosrlxqpisc.supabase.co` no longer resolves
in DNS at all. Most likely the free tier's 500 MB cap: the first backfill put
33,874 jobs with full raw JSONB into it, the project went read-only/paused, and
the grace period expired. Nothing irreplaceable was lost (jobs and companies
rebuild from the live boards; no applications/contacts existed yet).

**Why it was silent for six weeks:** job pings were the pipeline's only output,
so "pipeline down" and "quiet job market" looked identical from the outside.
Each cycle then burned seven 28-minute jobs failing every insert.

**Built:**
- `jobops/db.py`: `check_connection()` (direct connect, bypasses the pool's
  minutes-long reconnect), `require_db(source)` (exit 2 with one clear line),
  `redacted_dsn()`, env-tunable connect/pool/reconnect timeouts. Every poller,
  every enrich module, and poll_all now call `require_db()` first.
- `scripts/preflight.py` + a `preflight` job gating both workflows: one DB check
  per cycle, one Discord alert on failure (`notify_infra_failure`), all poll
  jobs skipped. A dead database now costs ~40s and pings the phone.
- Storage discipline: `trim_raw()` drops JD blobs from `jobs.raw` when the JD is
  already in the `description` column (~25 KB -> ~3 KB per row; rows with no
  stored description keep their payload intact, so re-runnability holds).
  `retention.check_size()` alerts Discord past 80% of the free-tier cap.
- `jobops/ingest/board_probe.py`: shared live board probe. Verdicts are pure and
  tested; 429/5xx are inconclusive (never "dead"), and the same response yields
  posting count, engineering-title density, and US-location density at no extra
  request. `check_watchlist.py` now uses it.
- `scripts/discover_boards.py`: candidate discovery from three public sources —
  new-grad/internship listing repos (real tokens extracted from job URLs), the
  YC directory's "currently hiring" list (yc-oss public JSON, guessed + verified),
  and a curated majors list. Verifies every token live, ranks by source quality
  x engineering density x US density, and applies curation (defense/ITAR,
  staffing mills, non-US). Resumable via a cached probe file.
- Cadence tiers: `data/watchlist.yaml` (core, every 30 min) and
  `data/watchlist_tail.yaml` (generated, hourly) via `load_watchlist(tier=...)`
  / `JOBOPS_TIER`; new `.github/workflows/poll-tail.yml` shares poll.yml's
  concurrency group so the two tiers never poll one provider at the same time.
  This doubles the corpus without opening a third stream at any provider —
  the polite-client rule is unchanged.

**Corpus:** 232 core + 280 tail = **512 verified boards** (was 238). Probed
~4,900 candidates, 1,834 resolved live, top 280 kept by rank. Composition
quotas: 50% new-grad-listing/curated majors, 35% YC companies currently hiring
(team >= 10, US-relevant), 15% other. Pruned 6 dead core tokens (plaid, marqeta,
dbtlabsinc, aurorainnovation, fundamentalresearchlabs, SR Visa).

**Bugs found while building:**
- Lever's posting title field is `text`, not `title` — the board ranker scored
  every Lever board as having zero engineering roles and dropped the provider
  entirely from the first selection. (The lever *poller* always had this right.)
- Curation only checked the company label, but discovery mines tokens out of
  URLs where the name lives in the token — `words_in_token()` splits camel case
  back apart so "NorthStarStaffingSolutions1" is caught.
- SmartRecruiters discovery is opt-in (`--include-sr`): its extraction is ~80%
  staffing firms (session 2b) and its limit=1 probe cannot measure density.

**Notification quality (the two gaps flagged in earlier sessions, now closed):**
`looks_new_grad`'s JD fallback now requires a technical title, and
`notify_new_job` skips non-US locations. Both were necessary before doubling
the corpus, which would otherwise have doubled the noise.

**Tests:** 181 passing (was 155). New: `test_board_probe.py` (probe verdicts,
per-ATS title/location shapes), `test_discovery.py` (token extraction, slug
guessing, curation), `test_watchlist_tiers.py` (tier loading, merge, trim_raw).

**Deviations from DESIGN.md:** §4.4's single poll workflow is now two tiered
workflows sharing one concurrency group (rationale above). DESIGN.md §4 updated
in place.


### Session 3b — 2026-09-04 — new database live, sponsor data loaded

- **New Supabase project** (us-east-1, closer to the runners): migrated, secret
  rotated, first push-triggered run had all 7 poll jobs green in 13s-14m
  (the old ca-central-1 project timed out at 28 min).
- **`.env` is now auto-loaded** by `jobops/__init__.py` (fills only what is
  missing, so CI secrets always win). A local `uv run python -m jobops.etl...`
  had silently fallen back to the docker-compose default and failed against
  whatever Postgres was listening on localhost. This footgun was on the notes
  list since session 2. Consequence: `DATABASE_URL` is now always set locally,
  so `test_db_smoke` gates on `JOBOPS_INTEGRATION` instead.
- **`require_db()` added to the ETL** — it was the one entrypoint I missed, and
  it hung 30s dumping pool noise instead of failing in one line.
- **USCIS Data Hub format changed** and the loader had to be taught it:
  the exports are now **UTF-16, tab-delimited** (despite the .csv extension),
  and the columns were renamed — "Initial Approval" -> "New Employment
  Approval", "Continuing Approval" -> "Continuation Approval". Added
  `sniff_encoding`/`sniff_delimiter`/`read_rows` and the new header variants;
  old-format files still load. Inserts are batched at 5,000.
- **Sponsor data is live:** 231,315 rows from FY2023-FY2026 -> 1,763 companies
  scored: 315 verified, 343 likely, 194 unlikely, 911 unknown. Badges now
  render on notifications.
- **`retention.compact_raw()`**: applies trim_raw's rule in SQL to rows written
  by older code, so the 32k pre-existing rows healed instead of carrying a
  duplicate JD until they aged out. Avg raw payload 2,445 B -> 1,227 B.
- **`heartbeat()` no longer raises.** A pool timeout writing telemetry (caused
  by the bulk USCIS load saturating the pooler's 15-client cap) turned an
  otherwise fully successful enrich job red.

**Size watch:** 231 MB of the 500 MB cap — 163 MB jobs (32.6k rows; description
averages 3,378 B and is the bulk), 56 MB sponsor_records. Sponsor data is a
fixed cost; jobs grow with the corpus.

**Both size levers pulled (user-approved 2026-09-04), not just held in reserve:**
- `RETENTION_DAYS` 30 -> 14. New-grad rows still keep 90 days.
- `prune_descriptions()` drops the JD from non-new-grad postings older than
  `DESCRIPTION_GRACE_DAYS` (3), skipping new-grad rows and anything referenced
  by an application or resume version, and marking `raw._description_pruned` so
  a NULL description stays distinguishable from one a board never sent. The
  grace period exists because is_new_grad is refined from the JD by a follow-up
  detail fetch *after* insert — pruning sooner would corrupt the very
  classification it keys on. Verified against the live table inside a
  rolled-back transaction: 5,000 rows matched, all 296 new-grad descriptions
  untouched. Nothing has actually pruned yet because every row in the new
  database is younger than the grace period.
- This is a deliberate deviation from "always keep raw payloads", recorded in
  the retention module docstring. The cost: a future, better new-grad
  classifier cannot be re-run over pruned history without re-fetching. Accepted
  because those rows are roles this system will never apply to, and a full
  free-tier database is what killed the first project.

## Exact next steps (for the next session)

1. Watch `retention`'s size line in the enrich job (see the size watch above).
2. Confirm the sponsor badges look right on the next few Discord pings, now
   that 315 companies are 'verified'.
3. Re-run `uv run python scripts/discover_boards.py` monthly to refresh the
   tail (probe cache makes it cheap), and `scripts/check_watchlist.py --tier core`
   to prune boards that have moved ATS.
4. Phase 5 leftover: DOL LCA ETL (`jobops/etl/dol_lca.py`) still unbuilt —
   needs `openpyxl` (not in the fixed stack; ask first). USCIS alone drives the
   current score. Re-download the Data Hub CSVs each October for the new FY.
5. Then per DESIGN.md roadmap: §6 JD enrichment (LLM fit scoring), §13 dashboard,
   or §7 resume automation — user will scope via session prompt.

## Notes for future sessions
- Notification semantics gap: pings fire at the end of each poller's run(), so a killed/cancelled run inserts jobs that never notify (observed 2026-07-19: 99 new-grad roles silent after Actions timeout kills). Consider a `notified_at` column on jobs so notification becomes a resumable step instead of an in-memory afterthought.
- Classifier noise: looks_new_grad's JD fallback flags non-tech roles (Harvard "Faculty Administrative Assistant" etc. via "entry level"/"early career" appearing in JD text). Tighten: only apply the JD fallback when the title looks technical, and add the US-location notification filter noted above.
- User feedback after first backfill (2026-07-19): notifications included many non-US postings (several watchlist boards are global — Ubisoft2, Devoteam, Continental, octoenergy, brillio-2) and were dominated by single companies. Add a US/remote-US location filter to notify_new_job (or to is_new_grad gating), and consider per-company caps / triage-score gating when §6 jd_score lands. Backfill artifact only for the same-company clustering; the location gap is real in steady state too.
- Local runs don't auto-load `.env` — pollers read os.environ (DATABASE_URL falls back to the docker default; DISCORD_WEBHOOK/GH_PAT must be exported or injected). Consider a tiny env loader or `uv run --env-file` later.
- Discord webhook is set in the user's `.env` and verified working (2026-07-19).
- Lever/Ashby boards `plaid`, `kraken`, `voleon`, `deel` resolve but had 0 postings on 2026-07-19 — watchlist keeps them; `check_watchlist.py` only treats 0-postings as dead for SmartRecruiters.
- psycopg_pool emits a DeprecationWarning unless `open=True` is passed to ConnectionPool — already handled in db.py; keep it if the pool setup is ever touched.
- `scripts/backup.sh` is bash — on Windows run it via Git Bash or WSL; consider a scheduled task later.
