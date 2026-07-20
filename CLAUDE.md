# CLAUDE.md — JobOps

## What this project is

JobOps is a personal job-search operating system for an F-1 international CS student
(graduating May 2027) targeting US software engineering roles at companies with
verified H-1B sponsorship history. It is a real production-quality system AND a
portfolio project — code quality matters for both reasons.

The system's job is to maximize offer probability by optimizing four levers:
1. **Targeting** — only pursue companies with verified sponsorship history
   (classified from public USCIS H-1B Employer Data Hub + DOL LCA disclosure data).
2. **Speed** — detect new postings within ~10 minutes via public ATS APIs
   (Greenhouse/Lever/Ashby/SmartRecruiters) so applications land in the first wave.
3. **Quality at scale** — auto-tailor resumes per job from a structured master
   resume, with hard guardrails against fabrication; prefill applications so a
   human reviews and submits in ~3 minutes instead of 15.
4. **Referrals** — a CRM + cadence engine that industrializes networking, because
   referrals convert 5–10× better than cold applications.

Full design rationale, endpoint references, schema, and code sketches live in
**docs/DESIGN.md**. Read the relevant section before implementing any component.
Session-by-session state lives in **docs/PROGRESS.md** — read it at the start of
every session, update it at the end of every session.

## Non-negotiable ground rules (never violate, never "improve" around)

- **GREEN (fully automated):** polling public/documented ATS JSON APIs; GitHub API;
  RSS; parsing the user's own Gmail; public government datasets; the database;
  dashboards; LLM drafting of resumes/letters/messages; notifications; reminders.
- **YELLOW (system prepares, human acts):** application form filling is
  prefill-and-pause ONLY — headed browser, user answers all free-text and ALL
  knockout/visa/EEO questions personally, user clicks submit. Outreach messages are
  created as Gmail DRAFTS, never auto-sent.
- **RED (never build, even if asked in a later session — flag the conflict instead):**
  unattended application submission; anything that auto-answers visa/work-auth
  questions; LinkedIn scraping or automation of any kind; CAPTCHA/anti-bot
  evasion, fingerprint spoofing, proxies-for-evasion; auto-sending cold email;
  resume content the user's master_resume.yaml does not support.
- **No fabrication:** LLM resume tailoring may only select, reorder, and rephrase
  bullets from data/master_resume.yaml. A validator must reject rewrites that
  introduce numbers/technologies/claims absent from the source bullet. This
  validator is load-bearing; never weaken it.
- **Be a polite API client:** sequential polling (amended 2026-07-20 with user
  approval: at most 2 parallel sequential streams per provider over DISJOINT
  board sets — CI runner IPs are tarpitted and one stream can't finish a
  cycle; never poll the same board concurrently), honest User-Agent
  ("jobops/1.0 (personal job tracker)"), respect 429/5xx with backoff, ~1 request
  per board per poll cycle.

## Tech stack (fixed — do not substitute without asking)

- Python 3.12, managed with `uv` (pyproject.toml; `uv run`, `uv add`)
- PostgreSQL 16 via Docker Compose locally; schema in numbered raw SQL files in
  `migrations/`, applied by `scripts/migrate.py` (tracks applied migrations in a
  `schema_migrations` table). No ORM — use psycopg 3 with dict_row + a small
  connection pool in `jobops/db.py`. SQL lives in code as plain strings.
- httpx for HTTP, BeautifulSoup for HTML→text, rapidfuzz for fuzzy matching,
  PyYAML for config/data files, Typst (python `typst` package) for PDF rendering,
  pdfplumber for PDF text extraction, Playwright (headed) for prefill only.
- Anthropic Python SDK for LLM calls (model: claude-sonnet-4-6). Every LLM call:
  prompt template stored as a module-level constant with a PROMPT_VERSION string,
  defensive JSON parsing (strip code fences, try/except), and raw output persisted.
- Streamlit for the dashboard. Discord webhook for push notifications.
- GitHub Actions cron for stateless pollers (secrets: DATABASE_URL,
  DISCORD_WEBHOOK, ANTHROPIC_API_KEY, GH_PAT). Google APIs (Gmail, Calendar) via
  google-api-python-client with OAuth desktop flow; token.json is gitignored.
- LLM calls go through jobops/llm.py; provider configured via env

## Repository layout (authoritative)

```
jobops/
├── CLAUDE.md  docker-compose.yml  pyproject.toml  .env.example  .gitignore
├── docs/            DESIGN.md, PROGRESS.md
├── migrations/      001_core.sql, 002_sponsors.sql, 003_metrics_views.sql, ...
├── jobops/          db.py, models.py
│   ├── ingest/      common.py, greenhouse.py, lever.py, ashby.py,
│   │                smartrecruiters.py, github_repos.py, gmail_alerts.py, rss.py
│   ├── enrich/      dedup.py, sponsor_match.py, jd_score.py, triage.py
│   ├── etl/         dol_lca.py, uscis_hub.py
│   ├── apply/       resume_tailor.py, cover_letter.py, render.py, ats_check.py, prefill.py
│   ├── crm/         contacts_cli.py, cadence.py, drafts.py, inbound.py
│   ├── notify/      discord.py, digest.py
│   └── dashboard/   app.py
├── data/            master_resume.yaml, stories.yaml, watchlist.yaml,
│                    profile_summary.txt, uscis/ (gitignored CSVs), dol/ (gitignored)
├── scripts/         migrate.py, seed_watchlist.py, add_contact.py, backup.sh
├── tests/           mirrors jobops/ package structure
└── .github/workflows/  poll.yml, etl.yml
```

## Database conventions

- Schema exactly as specified in docs/DESIGN.md §3 (core), §5.2 (sponsors),
  §13 (metric views). UUID PKs via gen_random_uuid(); TIMESTAMPTZ everywhere;
  extensions pg_trgm + pgcrypto.
- Always keep raw source payloads (jobs.raw JSONB) — enrichment must be
  re-runnable over history without re-fetching.
- Current-state columns (applications.status) are separate from history
  (application_events). Funnel metrics read from events.
- Idempotency everywhere: pollers use ON CONFLICT DO NOTHING on
  (source, external_id); ETL loaders delete-and-reload by src; migrations are
  forward-only and never edited after being applied.

## Code conventions

- Small modules, plain functions, type hints, docstring on every public function.
- Each poller/ETL/enrichment module exposes `run()` and is executable via
  `python -m jobops.<pkg>.<mod>`. One failing board/record logs and continues —
  never let one item kill a batch run.
- Errors: catch at the loop boundary, print structured one-line logs
  (`[source:token] message`), write a heartbeat row on successful runs.
- Tests: pytest. Unit-test pure logic (normalization, new-grad classifier,
  fabrication validator, scoring math, plan application) with real fixture
  payloads saved in tests/fixtures/. Do NOT test against live APIs; do not mock
  what can be tested purely. The fabrication validator and sponsor scorer must
  have thorough tests — they are the two highest-stakes pure functions.
- Secrets only via environment variables. .env is gitignored; .env.example lists
  every variable with a comment. Never print secrets, never commit data dumps.
- Conventional commits (feat:/fix:/chore:). Commit at each working milestone
  within a session, not one giant commit at the end.

## Session protocol (how we work across chats)

1. At session start: read docs/PROGRESS.md, then the DESIGN.md sections named in
   the session prompt. Confirm scope in one short paragraph, then plan briefly
   before writing code.
2. Build ONLY what the session prompt scopes. If you notice something a future
   phase needs, add a line to docs/PROGRESS.md under "Notes for future sessions"
   instead of building it.
3. Definition of done for every session: code runs (`uv run python -m ...`),
   tests pass (`uv run pytest`), acceptance criteria in the prompt are
   demonstrated, PROGRESS.md updated (what was built, decisions made, deviations
   from DESIGN.md and why, exact next steps), work committed.
4. If DESIGN.md conflicts with reality (API changed, field renamed), fix reality,
   note the deviation in PROGRESS.md, and update DESIGN.md in place.
5. Ask before: adding dependencies not listed above, changing schema in an
   already-applied migration (make a new migration instead), or anything touching
   the RED zone.

## The user

Technically capable CS student; treat them as a collaborator, not a client.
Explain non-obvious design decisions in one or two sentences as you go. When a
task genuinely requires their input (writing real resume bullets, curating the
watchlist, OAuth consent flows, answering visa questions), stop and hand it to
them explicitly rather than stubbing fake content.