# The F-1 → OPT → H-1B Job Pipeline
## An implementation-level guide to building your job search as a software system

**Target user:** F-1 CS undergrad, graduating May 2027, needs OPT-friendly employers with H-1B sponsorship history.
**Goal:** Maximize probability of multiple SWE offers from sponsor-friendly companies.
**Approach:** Build it like a product. Human-in-the-loop where it matters, fully automated where it's legal and effective.

---

# 0. Strategy Reality Check (read this before writing code)

Before designing the system, you need the right objective function — otherwise you'll build a very efficient machine for doing the wrong thing.

**The "1000+ applications" framing is a trap.** For international candidates, the funnel math looks roughly like this (aggregated from new-grad outcome surveys and recruiter behavior — treat as directional, not gospel):

| Channel | Response rate (intl. new grad) | Effort per unit |
|---|---|---|
| Cold ATS application, generic resume | ~0.5–2% | Low |
| Cold ATS application, tailored resume, sponsor-verified company, < 24h after posting | ~4–8% | Medium |
| Application + employee referral | ~15–30% | High |
| Recruiter/alumni warm outreach → referral → application | ~20–40% | High |

The dominant variables are: **(1) does the company actually sponsor, (2) how early you apply, (3) referral or not, (4) resume/JD match.** Volume matters only *after* those are optimized. So the system you should build is not an auto-applier; it's a machine that:

1. **Filters the universe** down to companies with verified sponsorship history (this alone eliminates ~70% of postings and 95% of your wasted effort).
2. **Detects postings within minutes** so you're in the first 50 applicants, not applicant #2,400.
3. **Compresses your per-application time** from 45 minutes to ~8 (tailored resume auto-drafted, form pre-filled, you review and click submit).
4. **Industrializes referrals and outreach** with a CRM, because that's where the actual offers come from.
5. **Measures everything** so you reallocate effort weekly based on data.

Realistic target: **300–500 high-quality, sponsor-verified, fast, tailored applications + 150–300 outreach touches over the season**, not 1,000 sprayed ones. The system below supports that at ~2–3 hours/day of your time.

---

# 1. Ground Rules: What Gets Automated and What Doesn't

This section is the legal/ethical spine of the whole project. Get this wrong and you risk banned accounts, blacklisted email domains, rescinded candidacies, and — as an F-1 student — you have less margin for reputational risk than domestic peers.

## Fully automatable (green zone) — the system does it end-to-end
- **Polling public ATS APIs** (Greenhouse, Lever, Ashby, SmartRecruiters all expose *public, documented, unauthenticated* JSON job-board endpoints — this is not scraping, it's using published APIs).
- **GitHub new-grad repos** via the GitHub API (SimplifyJobs/New-Grad-Positions etc.).
- **RSS feeds, Google Alerts, email job alerts** parsed by your pipeline.
- **Public government datasets**: DOL LCA disclosure files and the USCIS H-1B Employer Data Hub are public records. Building a sponsorship classifier on them is exactly what they're published for.
- **Your own database, dashboards, reminders, follow-up scheduling, metrics.**
- **LLM-assisted resume tailoring and cover letter drafting** (with a hard "no fabrication" guardrail — see §7).
- **Drafting** outreach emails/messages (auto-draft, human sends).

## Human-in-the-loop (yellow zone) — the system prepares, you act
- **Application form filling**: an autofill browser extension or a Playwright "prefill-and-pause" script that fills *your own truthful data* into a form *while you watch*, and **you** review and click submit. This is you using a tool, like a password manager — fine. Fully unattended submission is not (most ATS ToS prohibit bots, several ATSes fingerprint automation, and error rates will silently torch applications).
- **Sending outreach**: system drafts and queues; you send. Automated cold email at volume crosses into CAN-SPAM territory and gets your domain blacklisted.
- **LinkedIn actions**: LinkedIn's User Agreement §8.2 explicitly prohibits bots, scrapers, and automated messaging. Accounts get restricted fast, and a restricted LinkedIn mid-job-search is catastrophic. The system may *remind* you who to message and *draft* the message; your fingers do the clicking.

## Not built at all (red zone) — and why it's also strategically dumb
- **LinkedIn scraping/automation tools** (Phantombuster-style): ToS violation, high ban risk, and recruiters increasingly recognize templated bot outreach.
- **Anti-bot evasion** (CAPTCHA solvers, fingerprint spoofing, residential proxies): if a site is actively telling you "no bots," routing around that is where "automation" becomes "circumvention." It also signals to any employer who detects it that you'll cut corners.
- **Fully unattended mass submission** ("auto-apply to 100 jobs while I sleep"): quality collapses, wrong answers get submitted to knockout questions (visa questions especially — a mis-answered "will you require sponsorship?" is unrecoverable), and duplicate/garbage applications get you silently blacklisted in shared ATS databases.
- **Fabricated resume content**: LLMs will happily invent metrics. Your guardrails must make this impossible (§7).

Everything below is designed inside these lines.

---

# 2. System Architecture

## 2.1 Component diagram

```
                        ┌─────────────────────────────────────────────┐
                        │              SCHEDULERS                      │
                        │  GitHub Actions cron (free) / systemd timers │
                        └──────┬──────────────┬───────────────┬───────┘
                               │              │               │
                    every 10 min        nightly          weekly
                               │              │               │
┌──────────────────────────────▼──┐  ┌────────▼─────────┐  ┌──▼──────────────┐
│  INGESTION WORKERS              │  │  ENRICHMENT       │  │  ETL             │
│  greenhouse_poller.py           │  │  sponsor_matcher  │  │  dol_lca_loader  │
│  lever_poller.py                │  │  jd_scorer (LLM)  │  │  uscis_hub_loader│
│  ashby_poller.py                │  │  dedup            │  └──┬──────────────┘
│  smartrecruiters_poller.py      │  └────────┬─────────┘     │
│  github_repo_watcher.py         │           │               │
│  email_alert_parser.py (Gmail)  │           │               │
│  rss_poller.py                  │           │               │
└──────────────┬──────────────────┘           │               │
               │        raw_jobs              │               │
               ▼                              ▼               ▼
        ┌─────────────────────────────────────────────────────────┐
        │                 POSTGRES (single source of truth)        │
        │  companies · jobs · applications · contacts ·            │
        │  interactions · referrals · interviews · resume_versions │
        │  follow_ups · sponsor_records · prep_log · metrics views │
        └───────┬──────────────────────┬──────────────────┬───────┘
                │                      │                  │
        ┌───────▼────────┐   ┌─────────▼────────┐  ┌──────▼─────────┐
        │ NOTIFIER        │   │ APP-ASSIST       │  │ DASHBOARD      │
        │ Discord webhook │   │ resume_tailor.py │  │ Streamlit /    │
        │ + daily email   │   │ cover_letter.py  │  │ FastAPI + SQL  │
        │ digest          │   │ prefill.py       │  │ metric views   │
        └────────────────┘   │ (Playwright,     │  └────────────────┘
                              │  human submits)  │
                              └──────────────────┘
```

## 2.2 Design decisions (and why)

| Decision | Choice | Why |
|---|---|---|
| Language | Python 3.12 | Ecosystem for scraping-adjacent work, LLM SDKs, data |
| DB | Postgres (local Docker; Supabase free tier if you want hosted + auth + REST for free) | Relational fits this domain perfectly; JSONB for raw payloads |
| Scheduler | GitHub Actions cron for pollers (free, serverless, logs built in); local cron/systemd for anything needing your machine | Zero infra cost; Actions gives you retries + logs + secrets |
| Queue | None initially — Postgres `status` columns + `SELECT ... FOR UPDATE SKIP LOCKED` | At your scale (thousands of rows/day) a real queue (Redis/RQ) is premature. Add later if needed |
| LLM | Claude API (Sonnet) for tailoring/scoring; keep prompts + outputs versioned in DB | Deterministic-ish, auditable, cheap at this volume (~$5–15/mo) |
| Notifications | Discord webhook (instant) + Gmail API digest (daily) | Discord webhook is 5 lines of code, free, mobile push |
| Dashboard | Streamlit reading SQL views | 1 file, zero frontend work; migrate to FastAPI+React only if you feel like it |
| Deployment | Docker Compose on your laptop or a $5 VPS; Actions for stateless pollers | Keep state in one Postgres; everything else stateless |

## 2.3 Repository layout

```
jobops/
├── docker-compose.yml            # postgres + adminer + streamlit
├── .env.example                  # DATABASE_URL, ANTHROPIC_API_KEY, DISCORD_WEBHOOK, GMAIL creds
├── pyproject.toml
├── migrations/                   # raw SQL, numbered; applied by scripts/migrate.py
│   ├── 001_core.sql
│   ├── 002_sponsors.sql
│   └── 003_metrics_views.sql
├── jobops/
│   ├── db.py                     # connection pool, helpers
│   ├── models.py                 # dataclasses / pydantic
│   ├── ingest/
│   │   ├── greenhouse.py
│   │   ├── lever.py
│   │   ├── ashby.py
│   │   ├── smartrecruiters.py
│   │   ├── github_repos.py
│   │   ├── gmail_alerts.py
│   │   └── rss.py
│   ├── enrich/
│   │   ├── dedup.py
│   │   ├── sponsor_match.py
│   │   └── jd_score.py           # LLM fit-scoring + keyword extraction
│   ├── etl/
│   │   ├── dol_lca.py            # quarterly DOL LCA disclosure files
│   │   └── uscis_hub.py          # USCIS H-1B Employer Data Hub CSVs
│   ├── apply/
│   │   ├── resume_tailor.py
│   │   ├── cover_letter.py
│   │   ├── render.py             # JSON resume -> PDF (Typst)
│   │   └── prefill.py            # Playwright prefill-and-pause
│   ├── crm/
│   │   ├── cadence.py            # follow-up scheduling
│   │   └── drafts.py             # Gmail draft creation
│   ├── notify/
│   │   ├── discord.py
│   │   └── digest.py
│   └── dashboard/
│       └── app.py                # Streamlit
├── data/
│   ├── master_resume.yaml        # single source of truth for your experience
│   ├── stories.yaml              # behavioral story bank (STAR)
│   └── watchlist.yaml            # ATS board tokens to poll
├── scripts/
│   ├── migrate.py
│   ├── seed_watchlist.py
│   └── backup.sh                 # pg_dump to restic/rclone target, nightly
└── .github/workflows/
    ├── poll.yml                  # */10 * * * * ingestion
    └── etl.yml                   # weekly sponsor data refresh
```

---

# 3. Database Schema (migrations/001_core.sql)

This is the heart of the system. Everything else reads/writes here.

```sql
-- 001_core.sql
CREATE EXTENSION IF NOT EXISTS pg_trgm;   -- fuzzy company matching
CREATE EXTENSION IF NOT EXISTS pgcrypto;  -- gen_random_uuid

CREATE TABLE companies (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name            TEXT NOT NULL,
    name_normalized TEXT NOT NULL,             -- lowercased, suffixes stripped (see sponsor_match.py)
    website         TEXT,
    ats_type        TEXT,                      -- greenhouse | lever | ashby | smartrecruiters | workday | other
    ats_token       TEXT,                      -- board token / slug for polling
    hq_location     TEXT,
    size_bucket     TEXT,                      -- startup | midsize | enterprise
    sponsor_score   NUMERIC(4,3),              -- 0..1, computed by sponsor_match (see 002)
    sponsor_status  TEXT DEFAULT 'unknown',    -- verified | likely | unlikely | no | unknown
    notes           TEXT,
    created_at      TIMESTAMPTZ DEFAULT now(),
    UNIQUE (name_normalized)
);
CREATE INDEX companies_name_trgm ON companies USING gin (name_normalized gin_trgm_ops);

CREATE TABLE jobs (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    company_id      UUID REFERENCES companies(id),
    source          TEXT NOT NULL,             -- greenhouse | lever | ashby | smartrec | github_repo | email | rss | manual
    external_id     TEXT,                      -- ATS job id for dedup
    title           TEXT NOT NULL,
    location        TEXT,
    remote          BOOLEAN,
    url             TEXT NOT NULL,
    description     TEXT,                      -- full JD text
    posted_at       TIMESTAMPTZ,
    first_seen_at   TIMESTAMPTZ DEFAULT now(),
    raw             JSONB,                     -- full API payload, always keep it
    -- enrichment
    is_new_grad     BOOLEAN,                   -- title/JD classifier
    fit_score       NUMERIC(4,3),              -- LLM 0..1 vs your profile
    fit_rationale   TEXT,
    keywords        TEXT[],                    -- extracted for resume tailoring
    visa_flag       TEXT,                      -- jd_says_no_sponsor | jd_silent | jd_says_yes  (regex+LLM on JD)
    status          TEXT DEFAULT 'new',        -- new | triaged | queued | applied | skipped | expired
    UNIQUE (source, external_id)
);
CREATE INDEX jobs_status_idx ON jobs(status);
CREATE INDEX jobs_first_seen_idx ON jobs(first_seen_at DESC);

CREATE TABLE resume_versions (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    job_id          UUID REFERENCES jobs(id),
    base_hash       TEXT NOT NULL,             -- hash of master_resume.yaml at generation time
    diff_summary    TEXT,                      -- human-readable: what changed vs master
    content_json    JSONB NOT NULL,            -- the tailored resume structure
    pdf_path        TEXT,
    ats_keyword_hits INT,
    created_at      TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE applications (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    job_id          UUID REFERENCES jobs(id) UNIQUE,
    resume_version_id UUID REFERENCES resume_versions(id),
    cover_letter_path TEXT,
    channel         TEXT,                      -- ats_direct | referral | recruiter | career_fair
    referral_contact_id UUID,                  -- FK to contacts, set if referred
    applied_at      TIMESTAMPTZ,
    minutes_after_posting INT,                 -- KPI: speed to apply
    status          TEXT DEFAULT 'submitted',
    -- submitted | oa | phone | onsite | offer | rejected | ghosted | withdrawn
    status_updated_at TIMESTAMPTZ DEFAULT now(),
    rejection_stage TEXT,
    notes           TEXT
);

CREATE TABLE application_events (              -- full history, not just current status
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    application_id  UUID REFERENCES applications(id),
    event           TEXT NOT NULL,             -- submitted | oa_received | oa_done | recruiter_call | ...
    occurred_at     TIMESTAMPTZ DEFAULT now(),
    detail          TEXT
);

CREATE TABLE contacts (                        -- the recruiter/networking CRM
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    company_id      UUID REFERENCES companies(id),
    name            TEXT NOT NULL,
    role            TEXT,                      -- recruiter | engineer | hiring_manager | alumni
    relationship    TEXT,                      -- cold | alumni | met_event | mutual | friend
    email           TEXT,
    linkedin_url    TEXT,
    school_overlap  BOOLEAN DEFAULT FALSE,
    warmth          INT DEFAULT 0,             -- 0 cold .. 3 strong
    do_not_contact  BOOLEAN DEFAULT FALSE,
    created_at      TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE interactions (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    contact_id      UUID REFERENCES contacts(id),
    channel         TEXT,                      -- email | linkedin | irl | phone
    direction       TEXT,                      -- outbound | inbound
    summary         TEXT,
    occurred_at     TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE follow_ups (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    -- exactly one of these set:
    application_id  UUID REFERENCES applications(id),
    contact_id      UUID REFERENCES contacts(id),
    due_at          TIMESTAMPTZ NOT NULL,
    action          TEXT NOT NULL,             -- 'nudge recruiter', 'thank-you note', 'check portal', ...
    done            BOOLEAN DEFAULT FALSE,
    created_at      TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX follow_ups_due ON follow_ups(due_at) WHERE NOT done;

CREATE TABLE interviews (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    application_id  UUID REFERENCES applications(id),
    round           TEXT,                      -- oa | phone | virtual_onsite | final
    scheduled_at    TIMESTAMPTZ,
    format          TEXT,                      -- dsa | system_design | behavioral | practical
    outcome         TEXT,                      -- pending | pass | fail
    debrief         TEXT                       -- what was asked, what went wrong — gold for prep
);

CREATE TABLE prep_log (                        -- interview prep pipeline (§12)
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    kind            TEXT,                      -- leetcode | system_design | behavioral | mock
    ref             TEXT,                      -- problem slug / story id
    result          TEXT,                      -- solved | solved_with_hints | failed
    minutes         INT,
    next_review_at  TIMESTAMPTZ,               -- spaced repetition
    done_at         TIMESTAMPTZ DEFAULT now()
);
```

Key schema decisions:
- **`raw JSONB` on jobs**: never throw away source payloads; when you improve enrichment later you re-run it over history without re-fetching.
- **`application_events` separate from `applications.status`**: status is "current state," events are the audit log. Your funnel metrics come from events.
- **`minutes_after_posting`**: your single most actionable KPI. If median > 24h, fix ingestion before anything else.
- **`follow_ups` with partial index**: the "what do I do today" query is `SELECT * FROM follow_ups WHERE NOT done AND due_at < now() + interval '1 day'` and it must be instant.

---

# 4. Job Discovery Engine (the "minutes after posting" machine)

## 4.1 Why ATS public APIs beat scraping every board

Greenhouse, Lever, Ashby, and SmartRecruiters all expose **public, documented, unauthenticated JSON endpoints** for every company's job board. LinkedIn/Indeed/BuiltIn/Otta are mostly *aggregators of these same postings*, delayed by hours to days. Polling the source directly means:
- You see postings **before** the aggregators do.
- It's an API, not scraping — stable, legal, no bot arms race.
- One poller per ATS covers *hundreds of companies* — you just need their board tokens.

Endpoint reference:

| ATS | Endpoint pattern | Notes |
|---|---|---|
| Greenhouse | `https://boards-api.greenhouse.io/v1/boards/{token}/jobs?content=true` | `content=true` includes full JD HTML |
| Lever | `https://api.lever.co/v0/postings/{token}?mode=json` | JD in `description`/`lists` |
| Ashby | `https://api.ashbyhq.com/posting-api/job-board/{token}?includeCompensation=true` | POST also works via their posting API |
| SmartRecruiters | `https://api.smartrecruiters.com/v1/companies/{token}/postings` | Paginated; job detail at `.../postings/{id}` |
| Workday | No public API; per-tenant `.../wday/cxs/{tenant}/{site}/jobs` JSON endpoints exist but are undocumented and change | Prefer the company's email alerts + your Gmail parser (§4.5) for Workday shops |
| Taleo / iCIMS | No usable public API | Email alerts + Gmail parser |

**How you get board tokens:** the careers page URL usually contains it (`boards.greenhouse.io/stripe` → token `stripe`; `jobs.lever.co/scaleai` → `scaleai`; `jobs.ashbyhq.com/ramp` → `ramp`). Seed your watchlist from: MyVisaJobs top-sponsor lists, the SimplifyJobs new-grad repo's company set, and every company you've ever been interested in. Expect 200–600 tokens after a weekend of curation. Store in `data/watchlist.yaml`:

```yaml
greenhouse: [stripe, databricks, figma, duolingo, robinhood, ...]
lever:      [scaleai, palantir, ...]
ashby:      [ramp, linear, openai, ...]
smartrecruiters: [visa, bosch, ...]
```

## 4.2 Shared ingestion core (jobops/db.py + models)

```python
# jobops/db.py
import os, contextlib
import psycopg
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

pool = ConnectionPool(os.environ["DATABASE_URL"], min_size=1, max_size=5,
                      kwargs={"row_factory": dict_row})

@contextlib.contextmanager
def conn():
    with pool.connection() as c:
        yield c
```

```python
# jobops/ingest/common.py
import hashlib, re
from datetime import datetime, timezone
from jobops.db import conn

NEW_GRAD_PAT = re.compile(
    r"\b(new ?grad|university grad|entry.?level|early career|campus|"
    r"(software|swe).{0,20}(intern|20(2[6-9])))\b", re.I)
SENIOR_PAT = re.compile(r"\b(senior|staff|principal|lead|manager|director|sr\.?)\b", re.I)

def looks_new_grad(title: str, jd: str = "") -> bool:
    if SENIOR_PAT.search(title):
        return False
    return bool(NEW_GRAD_PAT.search(title) or NEW_GRAD_PAT.search(jd[:2000]))

def upsert_company(cur, name: str, ats_type: str, ats_token: str):
    norm = normalize_company(name)
    cur.execute("""
        INSERT INTO companies (name, name_normalized, ats_type, ats_token)
        VALUES (%s,%s,%s,%s)
        ON CONFLICT (name_normalized) DO UPDATE SET ats_token = EXCLUDED.ats_token
        RETURNING id""", (name, norm, ats_type, ats_token))
    return cur.fetchone()["id"]

def normalize_company(name: str) -> str:
    n = name.lower().strip()
    n = re.sub(r"[,\.]", "", n)
    n = re.sub(r"\b(inc|llc|ltd|corp|corporation|co|technologies|technology|labs|holdings|usa|us)\b", "", n)
    return re.sub(r"\s+", " ", n).strip()

def insert_job(source, external_id, company_id, title, location, url,
               description, posted_at, raw) -> str | None:
    """Returns job id if this is a NEW job, else None."""
    with conn() as c, c.cursor() as cur:
        cur.execute("""
            INSERT INTO jobs (source, external_id, company_id, title, location,
                              url, description, posted_at, raw, is_new_grad)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (source, external_id) DO NOTHING
            RETURNING id""",
            (source, external_id, company_id, title, location, url,
             description, posted_at, raw, looks_new_grad(title, description or "")))
        row = cur.fetchone()
        return row["id"] if row else None
```

## 4.3 The pollers

```python
# jobops/ingest/greenhouse.py
import httpx, yaml, json
from datetime import datetime
from jobops.db import conn
from jobops.ingest.common import upsert_company, insert_job
from jobops.notify.discord import notify_new_job

def poll_greenhouse(token: str, client: httpx.Client) -> list[str]:
    r = client.get(f"https://boards-api.greenhouse.io/v1/boards/{token}/jobs",
                   params={"content": "true"}, timeout=20)
    if r.status_code == 404:      # board renamed/removed — flag in watchlist review
        return []
    r.raise_for_status()
    data = r.json()
    new_ids = []
    with conn() as c, c.cursor() as cur:
        company_name = data["jobs"][0]["company_name"] if data["jobs"] else token
        company_id = upsert_company(cur, company_name, "greenhouse", token)
    for j in data.get("jobs", []):
        jid = insert_job(
            source="greenhouse",
            external_id=str(j["id"]),
            company_id=company_id,
            title=j["title"],
            location=(j.get("location") or {}).get("name"),
            url=j["absolute_url"],
            description=j.get("content"),          # HTML; strip later in enrichment
            posted_at=j.get("updated_at"),
            raw=json.dumps(j),
        )
        if jid:
            new_ids.append(jid)
    return new_ids

def run():
    watch = yaml.safe_load(open("data/watchlist.yaml"))
    with httpx.Client(headers={"User-Agent": "jobops/1.0 (personal job tracker)"}) as client:
        for token in watch.get("greenhouse", []):
            try:
                for jid in poll_greenhouse(token, client):
                    notify_new_job(jid)
            except Exception as e:
                print(f"[greenhouse:{token}] {e}")   # log & continue; never let one board kill the run

if __name__ == "__main__":
    run()
```

Lever and Ashby pollers are structurally identical — only the URL and field mapping change:

```python
# jobops/ingest/lever.py — field mapping only
# GET https://api.lever.co/v0/postings/{token}?mode=json  -> list of postings
# external_id = p["id"]; title = p["text"]; url = p["hostedUrl"]
# location = p.get("categories", {}).get("location")
# description = p.get("descriptionPlain") or p.get("description")
# posted_at = datetime.fromtimestamp(p["createdAt"]/1000, tz=timezone.utc)

# jobops/ingest/ashby.py
# GET https://api.ashbyhq.com/posting-api/job-board/{token}
# -> data["jobs"]: external_id=j["id"], title=j["title"], url=j["jobUrl"],
#    location=j.get("location"), description via j.get("descriptionHtml")
```

```python
# jobops/ingest/github_repos.py  — SimplifyJobs/New-Grad-Positions and friends
import httpx, re, json, os
from jobops.db import conn
from jobops.ingest.common import upsert_company, insert_job

REPOS = [
    ("SimplifyJobs", "New-Grad-Positions", ".github/scripts/listings.json"),
    # many repos keep a listings.json; fall back to parsing README.md tables if absent
]

def run():
    headers = {"Accept": "application/vnd.github.raw+json"}
    if tok := os.environ.get("GITHUB_TOKEN"):
        headers["Authorization"] = f"Bearer {tok}"     # 5000 req/h vs 60 unauthenticated
    with httpx.Client(headers=headers) as client:
        for owner, repo, path in REPOS:
            r = client.get(f"https://api.github.com/repos/{owner}/{repo}/contents/{path}")
            if r.status_code != 200:
                continue
            for item in r.json() if isinstance(r.json(), list) else json.loads(r.text):
                if not item.get("active", True):
                    continue
                with conn() as c, c.cursor() as cur:
                    cid = upsert_company(cur, item["company_name"], "other", "")
                insert_job("github_repo", item["id"], cid, item["title"],
                           ", ".join(item.get("locations", [])), item["url"],
                           None, item.get("date_posted"), json.dumps(item))
```

## 4.4 Scheduling with GitHub Actions (free, reliable)

```yaml
# .github/workflows/poll.yml
name: poll-jobs
on:
  schedule: [{cron: "*/10 * * * *"}]   # every 10 min (Actions may add jitter; fine)
  workflow_dispatch:
jobs:
  poll:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with: {python-version: "3.12", cache: pip}
      - run: pip install -e .
      - run: python -m jobops.ingest.greenhouse && python -m jobops.ingest.lever && python -m jobops.ingest.ashby && python -m jobops.ingest.github_repos
        env:
          DATABASE_URL: ${{ secrets.DATABASE_URL }}     # Supabase pooler URL
          DISCORD_WEBHOOK: ${{ secrets.DISCORD_WEBHOOK }}
          GITHUB_TOKEN: ${{ secrets.GH_PAT }}
```

This requires a **hosted** Postgres (Supabase free tier: use the connection-pooler URL). If you run Postgres locally instead, run pollers via cron/systemd on your machine and skip Actions.

## 4.5 Gmail alert parser (covers Workday/Taleo/iCIMS/LinkedIn alerts)

Set up native email alerts on LinkedIn, Indeed, and individual Workday career sites (most have "job alert" signup). Then parse them via the Gmail API — this is your net for everything without a public API.

```python
# jobops/ingest/gmail_alerts.py
# Setup: Google Cloud project -> enable Gmail API -> OAuth desktop credentials
#        -> run once interactively to store token.json (google-auth-oauthlib flow)
from googleapiclient.discovery import build
from google.oauth2.credentials import Credentials
from bs4 import BeautifulSoup
import base64, re

QUERY = 'newer_than:1d (from:jobalerts-noreply@linkedin.com OR from:*@myworkday.com OR subject:"job alert")'

def run():
    creds = Credentials.from_authorized_user_file("token.json",
              ["https://www.googleapis.com/auth/gmail.readonly"])
    svc = build("gmail", "v1", credentials=creds)
    msgs = svc.users().messages().list(userId="me", q=QUERY, maxResults=50).execute()
    for m in msgs.get("messages", []):
        full = svc.users().messages().get(userId="me", id=m["id"], format="full").execute()
        html = _body_html(full)
        for title, url in _extract_links(html):
            # insert with source='email', external_id=sha1(url) — dedup handles repeats
            ...

def _extract_links(html: str):
    soup = BeautifulSoup(html or "", "html.parser")
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if any(k in href for k in ("/jobs/view/", "myworkdayjobs.com", "/job/")):
            yield a.get_text(strip=True), re.sub(r"[?&]trk=.*$", "", href)
```

## 4.6 Notification: know within minutes

```python
# jobops/notify/discord.py
import httpx, os
from jobops.db import conn

def notify_new_job(job_id: str):
    with conn() as c, c.cursor() as cur:
        cur.execute("""SELECT j.title, j.url, j.location, j.is_new_grad,
                              co.name, co.sponsor_status, co.sponsor_score
                       FROM jobs j JOIN companies co ON co.id=j.company_id
                       WHERE j.id=%s""", (job_id,))
        j = cur.fetchone()
    if not j["is_new_grad"]:
        return                                # only ping for relevant roles
    badge = {"verified": "🟢", "likely": "🟡"}.get(j["sponsor_status"], "⚪")
    httpx.post(os.environ["DISCORD_WEBHOOK"], json={"content":
        f"{badge} **{j['name']}** — {j['title']} ({j['location']})\n{j['url']}"})
```

Result: phone push notification for every sponsor-verified new-grad posting, typically **within 10 minutes of it going live**, before it hits LinkedIn.

---

# 5. Sponsorship Classifier (public data, fully automated)

This is the highest-ROI component in the whole system: it converts "is this application a waste of my time?" from a 15-minute manual research task into a precomputed 🟢/🟡/⚪ badge.

## 5.1 Data sources (all public, all free)

1. **USCIS H-1B Employer Data Hub** — annual CSVs of every employer's H-1B petition counts (initial approvals/denials, continuing). Download from uscis.gov → "H-1B Employer Data Hub Files". This tells you *who actually sponsors and how much*.
2. **DOL LCA Disclosure Data** — quarterly Excel files of every Labor Condition Application: employer, job title, SOC code, worksite, wage. This tells you *whether they sponsor for software roles specifically* and at what wage.
3. **JD text itself** — regex + LLM pass for phrases like "unable to sponsor", "must be authorized without sponsorship" (→ hard `visa_flag='jd_says_no_sponsor'`, auto-skip) vs. silence vs. explicit "will sponsor".

MyVisaJobs/H1BGrader are just frontends over #1 and #2 — go to the source; it's more current and you can join it to your own tables.

## 5.2 ETL (migrations/002_sponsors.sql + jobops/etl/)

```sql
-- 002_sponsors.sql
CREATE TABLE sponsor_records (
    id            BIGSERIAL PRIMARY KEY,
    src           TEXT NOT NULL,               -- uscis_hub | dol_lca
    fiscal_year   INT,
    employer_raw  TEXT NOT NULL,
    employer_norm TEXT NOT NULL,
    initial_approvals INT, initial_denials INT,
    continuing_approvals INT,
    soc_code TEXT, job_title TEXT, wage NUMERIC, worksite_state TEXT
);
CREATE INDEX sponsor_norm_trgm ON sponsor_records USING gin (employer_norm gin_trgm_ops);
CREATE INDEX sponsor_norm_idx  ON sponsor_records(employer_norm);
```

```python
# jobops/etl/uscis_hub.py
# Manually download the yearly CSVs once (they're versioned files, not an API),
# drop into data/uscis/, then this loader is idempotent.
import csv, glob
from jobops.db import conn
from jobops.ingest.common import normalize_company

def run():
    with conn() as c, c.cursor() as cur:
        cur.execute("DELETE FROM sponsor_records WHERE src='uscis_hub'")
        for path in glob.glob("data/uscis/*.csv"):
            with open(path, newline="", encoding="utf-8-sig") as f:
                for row in csv.DictReader(f):
                    cur.execute("""INSERT INTO sponsor_records
                        (src, fiscal_year, employer_raw, employer_norm,
                         initial_approvals, initial_denials, continuing_approvals)
                        VALUES ('uscis_hub',%s,%s,%s,%s,%s,%s)""",
                        (int(row["Fiscal Year"]), row["Employer (Petitioner) Name"],
                         normalize_company(row["Employer (Petitioner) Name"]),
                         int(row.get("Initial Approval") or 0),
                         int(row.get("Initial Denial") or 0),
                         int(row.get("Continuing Approval") or 0)))
```

The DOL LCA loader is the same pattern over the quarterly disclosure Excel files (use `openpyxl` or convert to CSV first; keep only SOC codes 15-12xx (software/CS) to keep the table small and relevant).

## 5.3 Matching + scoring

```python
# jobops/enrich/sponsor_match.py
from rapidfuzz import fuzz
from jobops.db import conn

def score_company(cur, norm_name: str) -> tuple[float, str]:
    # 1) exact normalized match, 2) trigram candidates + rapidfuzz confirm
    cur.execute("""
        SELECT employer_norm,
               sum(initial_approvals) a, sum(initial_denials) d,
               max(fiscal_year) yr
        FROM sponsor_records
        WHERE employer_norm % %s          -- pg_trgm similarity operator
        GROUP BY employer_norm""", (norm_name,))
    best, best_sim = None, 0
    for row in cur.fetchall():
        sim = fuzz.token_sort_ratio(norm_name, row["employer_norm"]) / 100
        if sim > best_sim:
            best, best_sim = row, sim
    if not best or best_sim < 0.90:
        return 0.0, "unknown"
    a, d, yr = best["a"] or 0, best["d"] or 0, best["yr"]
    if a == 0:
        return 0.05, "unlikely"
    recency = 1.0 if yr >= current_fiscal_year() - 2 else 0.6
    volume  = min(a / 50, 1.0)            # 50+ recent approvals ≈ routine sponsor
    approval_rate = a / (a + d + 5)       # Laplace-smoothed: tiny sponsors must not
                                          # hit 'verified' off a perfect 3-sample rate
    score = round(0.5*volume + 0.3*approval_rate + 0.2*recency, 3)
    status = "verified" if score >= 0.5 else ("likely" if score >= 0.2 else "unlikely")
    return score, status

def run():
    with conn() as c, c.cursor() as cur:
        cur.execute("SELECT id, name_normalized FROM companies WHERE sponsor_status='unknown'")
        for co in cur.fetchall():
            s, status = score_company(cur, co["name_normalized"])
            cur.execute("UPDATE companies SET sponsor_score=%s, sponsor_status=%s WHERE id=%s",
                        (s, status, co["id"]))
```

**Caveats to encode in your head, not just the code:** subsidiaries file under different legal names (Google → "Google LLC" but also historical entities); startups too young to appear in the data may still sponsor (status `unknown` ≠ `no` — check their careers FAQ manually); and *H-1B history ≠ new-grad OPT willingness* (some sponsor only seniors). The badge triages; a 2-minute human check before applying settles edge cases. Also: **never let the system auto-answer visa questions on applications.** "Will you now or in the future require sponsorship?" — you answer that, truthfully ("Yes"), every time, by hand.

---

# 6. JD Enrichment: fit scoring, keyword extraction, visa flags (LLM pass)

Every new job with `is_new_grad=true` and sponsor status 🟢/🟡 gets one cheap LLM call that does three things at once. Store prompt version + raw output.

```python
# jobops/enrich/jd_score.py
import json, os
import anthropic
from bs4 import BeautifulSoup
from jobops.db import conn

client = anthropic.Anthropic()   # ANTHROPIC_API_KEY from env
PROFILE = open("data/profile_summary.txt").read()   # 300-word factual summary of your skills

PROMPT = """You are screening a job description for a specific candidate.

<candidate_profile>{profile}</candidate_profile>

<job_description>{jd}</job_description>

Return ONLY JSON:
{{
 "fit_score": 0.0-1.0,          // realistic interview-probability proxy, be harsh
 "fit_rationale": "one sentence",
 "keywords": ["8-15 concrete skills/technologies the JD emphasizes"],
 "visa_flag": "jd_says_no_sponsor" | "jd_silent" | "jd_says_yes",
 "knockouts": ["hard requirements candidate clearly lacks, e.g. 'requires MS degree'"]
}}"""

def enrich(job_id: str):
    with conn() as c, c.cursor() as cur:
        cur.execute("SELECT description FROM jobs WHERE id=%s", (job_id,))
        jd = BeautifulSoup(cur.fetchone()["description"] or "", "html.parser").get_text(" ")[:12000]
    msg = client.messages.create(
        model="claude-sonnet-4-6", max_tokens=800,
        messages=[{"role": "user", "content": PROMPT.format(profile=PROFILE, jd=jd)}])
    out = json.loads(msg.content[0].text.strip().removeprefix("```json").removesuffix("```"))
    with conn() as c, c.cursor() as cur:
        cur.execute("""UPDATE jobs SET fit_score=%s, fit_rationale=%s, keywords=%s,
                       visa_flag=%s, status='triaged' WHERE id=%s""",
                    (out["fit_score"], out["fit_rationale"], out["keywords"],
                     out["visa_flag"], job_id))
    return out
```

Triage policy (encode as a nightly job):
- `visa_flag = jd_says_no_sponsor` → `status='skipped'`, no notification. (Trust but verify: sample-check 20 of these manually once; regex/LLM false positives on phrases like "we sponsor visas" negated in context do happen.)
- `fit_score >= 0.55` and sponsor 🟢 → `status='queued'`, goes to your apply queue with priority = `fit_score × sponsor_score × freshness`.
- Everything else → stays `triaged`, visible in the dashboard backlog for manual promotion.

Cost: ~$0.01–0.02/job → a full season of enrichment costs less than one coffee/month.

---

# 7. Resume Automation

## 7.1 Master resume as structured data

One YAML file is the single source of truth. Every tailored version is a *selection and re-ordering* of this file — never new content.

```yaml
# data/master_resume.yaml  (abridged)
basics:
  name: Your Name
  email: you@school.edu
  links: {github: gh.com/you, linkedin: linkedin.com/in/you, site: you.dev}
education:
  - school: University of X
    degree: B.S. Computer Science
    grad: 2027-05
    gpa: "3.7"          # include if ≥3.5
    coursework: [Operating Systems, Distributed Systems, ML, Databases]
experience:
  - id: exp_acme_intern
    org: Acme Corp
    title: Software Engineering Intern
    dates: "May 2025 – Aug 2025"
    bullets:
      - id: b_acme_1
        text: "Built a Kafka-based event pipeline processing 2M events/day, cutting report latency from 6h to 15min"
        tags: [kafka, python, data-pipelines, aws]
      - id: b_acme_2
        text: "Wrote integration test harness in pytest raising coverage from 41% to 78%, catching 3 regressions pre-release"
        tags: [python, testing, ci]
projects:
  - id: proj_jobops
    name: JobOps — automated job-search platform
    bullets:
      - id: b_jo_1
        text: "Designed Postgres-backed ingestion system polling 400+ company job boards via public ATS APIs (FastAPI, Docker, GitHub Actions)"
        tags: [python, postgres, fastapi, docker, api-design]
skills:
  languages: [Python, TypeScript, Go, SQL, C++]
  tools: [PostgreSQL, Docker, AWS, Kafka, React, Playwright]
```

(Yes — **this project itself becomes your best resume project.** "Built a production data pipeline with ingestion, enrichment, LLM integration, and dashboards" is a stronger story than most class projects. Write it up.)

## 7.2 Tailoring with a hard no-fabrication guardrail

The LLM's job is **selection, ordering, and phrasing** — it may rephrase a bullet using its tags and the JD's vocabulary, but every fact (numbers, technologies, outcomes) must come from the YAML. Enforce it structurally: the model returns bullet **IDs plus optional rewrites**, and a validator rejects any rewrite that introduces numbers or proper nouns not present in the original.

```python
# jobops/apply/resume_tailor.py
import json, re, yaml, hashlib
import anthropic
from jobops.db import conn

client = anthropic.Anthropic()
MASTER = yaml.safe_load(open("data/master_resume.yaml"))

TAILOR_PROMPT = """Select and order resume content for this job. Rules:
- Choose the best 3-4 bullets per experience and 2 projects for THIS job's keywords.
- You may rephrase a bullet ONLY to adjust emphasis/vocabulary. NEVER add facts,
  numbers, technologies, or claims not present in the original bullet.
- Return JSON: {"experience_order":[...ids], "bullets": {"<bullet_id>":
  {"use": true/false, "rewrite": "text or null"}}, "skills_order": [...],
  "summary_line": "one optional headline line using only facts from the resume"}

<job_keywords>{keywords}</job_keywords>
<job_title>{title}</job_title>
<master_resume>{master}</master_resume>"""

NUMBER_OR_PROPER = re.compile(r"\d|[A-Z][a-zA-Z]+")

def _validate_rewrite(original: str, rewrite: str) -> bool:
    """Every number and capitalized token in the rewrite must appear in the original."""
    orig_tokens = set(NUMBER_OR_PROPER.findall(original))
    for tok in NUMBER_OR_PROPER.findall(rewrite):
        if tok not in orig_tokens and not tok.istitle() is False:
            if tok not in original:
                return False
    return True

def tailor(job_id: str) -> str:
    with conn() as c, c.cursor() as cur:
        cur.execute("SELECT title, keywords FROM jobs WHERE id=%s", (job_id,))
        job = cur.fetchone()
    msg = client.messages.create(model="claude-sonnet-4-6", max_tokens=2000,
        messages=[{"role":"user","content": TAILOR_PROMPT.format(
            keywords=job["keywords"], title=job["title"],
            master=json.dumps(MASTER))}])
    plan = json.loads(msg.content[0].text.strip().removeprefix("```json").removesuffix("```"))

    # apply plan against master, validating rewrites
    tailored = apply_plan(MASTER, plan, validator=_validate_rewrite)  # pure function, ~40 LOC
    with conn() as c, c.cursor() as cur:
        cur.execute("""INSERT INTO resume_versions
            (job_id, base_hash, content_json, diff_summary)
            VALUES (%s,%s,%s,%s) RETURNING id""",
            (job_id, hashlib.sha1(open("data/master_resume.yaml","rb").read()).hexdigest(),
             json.dumps(tailored), summarize_diff(MASTER, tailored)))
        return cur.fetchone()["id"]
```

## 7.3 Rendering to an ATS-safe PDF

Use **Typst** (`pip install typst`) — programmatic, fast, and produces clean single-column PDFs with a real text layer. ATS golden rules baked into the template: single column, no tables/text-boxes/graphics for content, standard section headers ("Experience", "Education", "Projects", "Skills"), standard fonts, `FirstLast_Company_Role.pdf` naming, and the exact keywords from the JD *where they're true*. Verify each render by extracting text back out (`pdfplumber`) and checking that every keyword you intended is present and the read order is sane — that extraction check *is* your ATS score:

```python
# jobops/apply/render.py (excerpt)
import typst, pdfplumber

def render(tailored: dict, out_path: str) -> None:
    typ_source = build_typst(tailored)          # template fn: dict -> .typ markup
    typst.compile(typ_source.encode(), output=out_path)

def ats_check(pdf_path: str, keywords: list[str]) -> tuple[int, list[str]]:
    text = " ".join(p.extract_text() or "" for p in pdfplumber.open(pdf_path).pages).lower()
    hits = [k for k in keywords if k.lower() in text]
    return len(hits), [k for k in keywords if k.lower() not in text]
```

Missing keywords report → you decide: is it missing because you lack it (fine, leave it) or because it's phrased differently ("Postgres" vs "PostgreSQL" — fix the master YAML to include both once).

## 7.4 Cover letters

Same pattern, lower stakes: template with 3 slots (why-this-company, 2 mapped experiences, close), LLM fills from master YAML + JD, **you skim for 30 seconds** before it's attached. Generate only when the application asks — cover letters move the needle mainly at small companies and mission-driven orgs; default off elsewhere.

---

# 8. Application Assist (prefill-and-pause, human submits)

Compress a Greenhouse/Lever application from ~15 min to ~3 without crossing into bot territory.

**Pattern:** Playwright launches a *headed* browser on your machine, navigates to the job URL from your queue, fills the boring invariant fields (name, email, phone, school, links, uploads the tailored PDF), then **stops** and hands you the mouse. You answer every free-text and every knockout/visa/demographic question yourself, review, and click submit. Then you press Enter in the terminal and it logs the application.

```python
# jobops/apply/prefill.py
import asyncio, yaml, sys
from datetime import datetime, timezone
from playwright.async_api import async_playwright
from jobops.db import conn

ME = yaml.safe_load(open("data/master_resume.yaml"))["basics"]

FIELD_MAP = {   # label-text → value; extend as you meet new forms
    "first name": ME["name"].split()[0],
    "last name":  ME["name"].split()[-1],
    "email": ME["email"],
    "phone": ME.get("phone", ""),
    "linkedin": ME["links"]["linkedin"],
    "github": ME["links"]["github"],
    "website": ME["links"].get("site", ""),
    "school": "University of X",
}

async def prefill(job_id: str):
    with conn() as c, c.cursor() as cur:
        cur.execute("""SELECT j.url, rv.pdf_path FROM jobs j
                       LEFT JOIN resume_versions rv ON rv.job_id=j.id
                       WHERE j.id=%s ORDER BY rv.created_at DESC LIMIT 1""", (job_id,))
        row = cur.fetchone()
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)   # headed: you are present
        page = await browser.new_page()
        await page.goto(row["url"])
        for label, value in FIELD_MAP.items():
            try:
                await page.get_by_label(label, exact=False).first.fill(value, timeout=1500)
            except Exception:
                pass                                        # field absent on this form: fine
        if row["pdf_path"]:
            try:
                await page.set_input_files("input[type=file]", row["pdf_path"], timeout=3000)
            except Exception:
                print("⚠ upload resume manually")
        print("Prefilled. Answer remaining questions (visa Qs YOURSELF), review, submit.")
        input("Press Enter AFTER you have submitted (or Ctrl+C to abort)... ")
        await browser.close()
    with conn() as c, c.cursor() as cur:
        cur.execute("""INSERT INTO applications (job_id, applied_at, channel,
                        minutes_after_posting)
                       SELECT id, now(), 'ats_direct',
                        EXTRACT(EPOCH FROM (now()-posted_at))/60 FROM jobs WHERE id=%s
                       ON CONFLICT (job_id) DO NOTHING""", (job_id,))
        cur.execute("UPDATE jobs SET status='applied' WHERE id=%s", (job_id,))
        cur.execute("""INSERT INTO follow_ups (application_id, due_at, action)
                       SELECT id, now() + interval '10 days', 'no response — check portal / nudge'
                       FROM applications WHERE job_id=%s""", (job_id,))

if __name__ == "__main__":
    asyncio.run(prefill(sys.argv[1]))
```

Also worth having: **Simplify's browser extension** (a legitimate consumer autofill product) covers the same ground with zero code — use it for one-offs, use your script for batch sessions where you also want auto-logging and the tailored PDF wired in. Your daily "apply session" becomes: dashboard shows queue sorted by priority → `python -m jobops.apply.resume_tailor <id> && python -m jobops.apply.prefill <id>` → review, submit, Enter → next. Sustainable pace: **10–20 quality applications/hour-and-a-half.**

Why not go fully unattended, one more time, concretely: knockout questions (visa status, work authorization, relocation, start date) vary per form, are legally significant, and a wrong auto-answer is worse than no application. The 90 seconds you spend per form is exactly the 90 seconds that matters.

---

# 9. Networking & Referral Machine (where offers actually come from)

Automate the *pipeline*, not the *relationship*. The system finds who to talk to, drafts what to say, schedules the cadence, and tracks everything. You personalize and send.

## 9.1 Sourcing contacts (manual-fast, not scraped)

- **Alumni**: your university's LinkedIn page → "Alumni" tab → filter by company + "software". Also your CS department's alumni Slack/Discord, and Handshake's alumni features. Log each into `contacts` with `school_overlap=true` (these convert 3–5× better than pure cold).
- **Recruiters**: when a 🟢 job arrives, spend 3 minutes finding the university/early-career recruiter for that company (LinkedIn search, by hand) and log them.
- **Engineers**: authors of the team's blog posts / conference talks / open-source repos — these give you a *real* personalization hook.
- **Events**: career fairs, hackathons, meetups → every conversation gets logged same-day with a note about what you discussed (this note is the personalization for the follow-up).

Entry friction matters, so make logging one command:

```python
# scripts/add_contact.py — `contact add "Jane Doe" --company stripe --role recruiter --met "grace hopper booth, discussed infra team"`
```

## 9.2 Cadence engine

```python
# jobops/crm/cadence.py — rules run nightly
RULES = [
    # (condition, days_after, action)
    ("new cold contact created",            0,  "send intro message (draft ready)"),
    ("outbound sent, no reply",             6,  "polite bump #1"),
    ("bump sent, no reply",                10,  "final bump, then park"),
    ("had a call/coffee",                   1,  "thank-you note"),
    ("application submitted w/ contact at company", 0, "ask contact about referral"),
    ("interview completed",                 0,  "thank-you to interviewer if email known"),
    ("offer/rejection at their company",    0,  "update contact, thank for help"),
]
# implementation: query interactions/applications, insert into follow_ups if not exists
```

## 9.3 Drafting (Gmail *drafts*, never auto-send)

```python
# jobops/crm/drafts.py
def create_draft(svc, to: str, subject: str, body: str):
    from email.mime.text import MIMEText
    import base64
    msg = MIMEText(body); msg["to"] = to; msg["subject"] = subject
    svc.users().drafts().create(userId="me", body={"message":
        {"raw": base64.urlsafe_b64encode(msg.as_bytes()).decode()}}).execute()
```

The LLM drafts from a template + the contact's `interactions` history + the job in question; you open Gmail, see 6 drafts waiting, personalize the first line of each (this is the part that can't be faked), send. Message templates that work for intl. new grads:

- **Alumni intro (LinkedIn/email, ≤90 words):** shared school hook → one specific thing about their team → *one* narrow ask ("would you be open to a 15-min chat about how you chose X team?"). Never open with the referral ask, and never open with visa questions.
- **Referral ask (only after any warm exchange, or direct if they've posted "happy to refer"):** link to the *specific* job ID, attach tailored resume, 2-line why-I-fit, make it 30-seconds-easy for them.
- **Recruiter after applying:** "Applied to [req #] this morning — I'm a 2027 new grad, [one-line hook matched to the JD]. Happy to share anything else useful." Short. No essay.
- **Visa timing:** disclose honestly whenever asked; don't lead with it in a first cold message — lead with fit.

Replies get parsed back in semi-automatically: a nightly Gmail query for messages from known contact emails creates `interactions(direction='inbound')` rows and clears pending follow-ups for that contact.

## 9.4 Referral tracking

`applications.channel='referral'` + `referral_contact_id` gives you the killer dashboard stat: **response rate by channel**. When you see referral at 25% vs cold at 3% in *your own data*, your daily time allocation fixes itself.

---

# 10. Calendar, Email, and Ops Glue

- **Calendar**: Google Calendar API — when an `interviews` row is created, insert an event with 2 reminders + a prep block 24h before (auto-created `prep_log` TODO tagged with the company's known interview style). Parse interview-confirmation emails (subject regexes for common ATS mailers) to create `interviews` rows semi-automatically — always with a confirm prompt, never silently.
- **Status-email parser**: nightly Gmail pass classifying inbound mail from ATS domains into {rejection, OA invite, interview invite, offer, other} (regex first, LLM for ambiguous) → proposes `application_events` updates → you approve in a 2-minute CLI review (`y/n/edit` per item). This keeps the funnel data honest with near-zero effort.
- **Backups**: `scripts/backup.sh` = nightly `pg_dump | gzip` → rclone to Google Drive/B2. Also commit `master_resume.yaml`, `stories.yaml`, templates, and migrations to a **private** git repo. Your DB after 3 months is irreplaceable.
- **Secrets**: everything via env vars / GitHub Actions secrets; nothing in git; `.env` in `.gitignore` from commit #1. If you expose the dashboard beyond localhost, put it behind Tailscale rather than adding auth code.
- **Logging/monitoring**: pollers print structured lines; a tiny `heartbeats` table gets a row per successful run; the dashboard shows "last successful poll per source" and turns red after 60 min of silence. That's all the monitoring this system needs.

---

# 11. Public Profile Optimization (GitHub / Portfolio / LinkedIn)

Not automation-heavy, but the system enforces it via weekly checklist items:

**GitHub** — recruiters spend 30–60 seconds: profile README with a 3-line pitch and pinned repos; pin 4–6 repos where each has a real README (what/why/demo GIF/how to run), CI badge, and recent commits. **JobOps itself should be one of them** (sanitize secrets; a public repo showing Postgres schema design, API integration, LLM guardrails, and CI is exceptional new-grad signal). Green squares matter less than 2–3 polished, *finished* things.

**Portfolio site** — one page, loads instantly, above the fold: name, "SWE, graduating May 2027", 3 project cards with live demos, resume PDF link, email. Skip the blog unless you'll actually write.

**LinkedIn** — headline = role + strongest concrete hook ("CS @ X '27 · built a 400-board job-data pipeline · ex-Acme SWE intern"), not "aspiring engineer". About = 4 lines. Featured = portfolio + best repo. Every experience entry mirrors resume bullets. Settings: "Open to work" (recruiters-only is fine), and turn ON creator-mode only if posting. Keep it hand-maintained; the system just reminds you monthly to sync it with `master_resume.yaml`.

---

# 12. Interview Prep Pipeline

The `prep_log` table + spaced repetition turns prep from vibes into a system.

- **DSA**: pick a canonical list (NeetCode 150). Each attempt logs `result` and schedules `next_review_at` (SM-2-lite: solved → ×2.5 interval; hints → 3 days; failed → tomorrow). Daily dashboard widget: "3 reviews due, 2 new". Target before peak season (Aug–Oct 2026 for 2027 grads): 120+ solved, 90% of mediums in <25 min.
- **Company-targeted**: when an OA/interview lands, a prep doc auto-generates: company's known styles (you maintain a small YAML of notes per company from your own debriefs + public interview-experience reading), your relevant `prep_log` weak spots, and the JD keywords to speak to.
- **Behavioral**: `data/stories.yaml` = 8–10 STAR stories tagged by trait (conflict, failure, leadership, ambiguity, impact). An LLM mock-interviewer prompt (paste 2 stories, have it grill you with follow-ups) is genuinely effective practice. Every real interview gets a same-day `interviews.debrief` — after 5 interviews, your debriefs are a better prep guide than any website.
- **System design (new-grad-lite)**: 1 case/week from a standard course; log it.

---

# 13. Dashboard & KPIs (migrations/003 + Streamlit)

```sql
-- 003_metrics_views.sql
CREATE VIEW funnel AS
SELECT count(*) FILTER (WHERE status IS NOT NULL)                       AS applied,
       count(*) FILTER (WHERE status IN ('oa','phone','onsite','offer')) AS responded,
       count(*) FILTER (WHERE status IN ('phone','onsite','offer'))      AS interviewing,
       count(*) FILTER (WHERE status = 'offer')                          AS offers
FROM applications;

CREATE VIEW channel_performance AS
SELECT channel, count(*) AS n,
       round(100.0*count(*) FILTER (WHERE status NOT IN ('submitted','ghosted','rejected'))
             / greatest(count(*),1), 1) AS positive_pct
FROM applications GROUP BY channel;

CREATE VIEW speed AS
SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY minutes_after_posting) AS median_minutes
FROM applications WHERE minutes_after_posting IS NOT NULL;

CREATE VIEW weekly_activity AS
SELECT date_trunc('week', applied_at) wk, count(*) apps,
       (SELECT count(*) FROM interactions i WHERE i.direction='outbound'
        AND date_trunc('week', i.occurred_at)=date_trunc('week', a.applied_at)) outreach
FROM applications a GROUP BY 1 ORDER BY 1 DESC;
```

```python
# jobops/dashboard/app.py  (streamlit run jobops/dashboard/app.py)
import streamlit as st, pandas as pd
from jobops.db import conn

st.set_page_config("JobOps", layout="wide")
with conn() as c:
    funnel  = pd.read_sql("SELECT * FROM funnel", c)
    chans   = pd.read_sql("SELECT * FROM channel_performance", c)
    queue   = pd.read_sql("""SELECT co.name, j.title, j.fit_score, co.sponsor_status, j.url
                             FROM jobs j JOIN companies co ON co.id=j.company_id
                             WHERE j.status='queued'
                             ORDER BY j.fit_score * co.sponsor_score DESC LIMIT 25""", c)
    due     = pd.read_sql("""SELECT due_at, action FROM follow_ups
                             WHERE NOT done AND due_at < now() + interval '1 day'
                             ORDER BY due_at""", c)

c1,c2,c3,c4 = st.columns(4)
c1.metric("Applied", int(funnel.applied[0]))
c2.metric("Response rate", f"{100*funnel.responded[0]/max(funnel.applied[0],1):.1f}%")
c3.metric("Interviewing", int(funnel.interviewing[0]))
c4.metric("Offers", int(funnel.offers[0]))
st.subheader("Today"); st.dataframe(due, use_container_width=True)
st.subheader("Apply queue (priority order)"); st.dataframe(queue, use_container_width=True)
st.subheader("Channel performance"); st.bar_chart(chans.set_index("channel")["positive_pct"])
```

**KPIs and healthy targets** (intl. new grad, 2026–27 season):

| KPI | Definition | Healthy | If below → action |
|---|---|---|---|
| Median apply speed | `speed` view | < 12h | Fix ingestion/watchlist coverage first |
| Sponsor-verified % | % of apps to 🟢 companies | > 80% | Tighten triage; stop wasting shots |
| Response rate (cold) | responded/applied, ats_direct | 3–8% | Resume rewrite; check `visa_flag` leakage; get feedback |
| Response rate (referral) | same, referral channel | 15%+ | If low: referral quality, not quantity |
| Referral share | % apps with referral | > 25% | Shift hours from applying to outreach |
| OA→interview | pass rate | > 50% | More timed DSA practice |
| Interview→offer | | > 20% after 5+ onsites | Debrief patterns → targeted prep |
| Outreach reply rate | inbound/outbound | > 15% | Personalization is too thin |

Weekly review ritual (30 min, Sunday): read every number, write one sentence on the biggest bottleneck, reallocate next week's hours to it. This loop is the "business" part of running it like a business.

---

# 14. Roadmap

| Phase | Weeks (calendar) | Hours | Deliverable | Milestone/exit criteria |
|---|---|---|---|---|
| 1. Foundation | 1 | 8–10 | Docker Compose (Postgres+Adminer), migrations 001–003 applied, repo scaffold, backups | `psql` shows all tables; nightly dump lands in Drive |
| 2. Tracking MVP | 1 | 6–8 | Manual CRUD scripts + Streamlit v0 | You log 5 real applications and 5 contacts through it |
| 3. Ingestion | 2 | 15–20 | Greenhouse/Lever/Ashby/GitHub pollers + watchlist (200+ tokens) + Discord notify + Actions cron | Push notification for a real new posting < 15 min after it appears |
| 4. Sponsorship ETL | 1 | 8–10 | USCIS + DOL loaders, matcher, badges on dashboard | 90%+ of your watchlist has non-unknown status; spot-check 20 by hand |
| 5. Enrichment + resume | 2 | 15–20 | jd_score, master_resume.yaml (the hard part is writing good bullets — budget real time), tailor + Typst render + ats_check | Tailored PDF for a real job in < 60s, passes keyword check, zero fabrications on manual audit |
| 6. Apply assist | 1 | 8–10 | prefill.py + follow-up auto-creation + Simplify installed | 10 real applications in one 90-min session, all logged with speed metric |
| 7. CRM + outreach | 2 | 10–12 | contacts CLI, cadence rules, Gmail drafts, inbound parser | 20 contacts loaded, first 10 personalized sends, replies auto-logged |
| 8. Prep pipeline | ongoing | 5 setup | prep_log + spaced repetition + stories.yaml | Daily review widget live; 8 STAR stories written |
| 9. Analytics polish | 1 | 4–6 | Full KPI views, weekly review doc template | First Sunday review completed with a real reallocation decision |

**Total build: ~80–100 hours** — a serious but bounded project, and (deliberately) also your best portfolio piece. Timing for a May 2027 grad: build Phases 1–6 in **spring/summer 2026**, so the machine is warm when new-grad postings open **August–September 2026**, which is when the majority of big-company 2027 new-grad hiring happens. Applying in August with a tuned system beats applying in November with a perfect one.

---

# 15. Daily Operating System (in-season, ~2.5–3.5 h/day around classes)

| Block | Time | What |
|---|---|---|
| Morning scan | 15 min | Dashboard: overnight 🟢 jobs, today's follow-ups, interviews. Triage queue. |
| Apply session | 60–90 min | Top of priority queue: tailor → prefill → review → submit. 8–15 apps. Referral-possible ones get the referral ask instead of instant apply. |
| Outreach | 30 min | Send the day's 3–6 drafted messages (personalize first lines), log event conversations, process replies. |
| Prep | 45–60 min | Spaced-rep reviews + new problems; behavioral reps 2×/week; interview-specific prep when scheduled. |
| Evening close | 10 min | Approve email-parser status updates, mark follow-ups done, tomorrow auto-plans itself. |
| Weekly (Sun) | 30 min | KPI review + reallocation. |
| Weekly (any) | 1–2 h | System maintenance: watchlist additions, broken pollers, template improvements. Cap it — the system serves the search, not vice versa. |

Off-season (now → summer 2026): flip the ratio — mostly building the system + projects + prep, light networking (alumni chats compound; start early), no mass applying until postings open.

---

# 16. Best Practices, Pitfalls, and Backup Plans

**Recruiter-side realities**
- Recruiters see application *timestamps*. Early + tailored reads as "on top of it"; the same resume at 3 companies' shared ATS reads fine, but 40 identical applications across one company's reqs reads as spam — **one best-fit req per company at a time** (two max).
- Never misrepresent work authorization to get past a knockout. It surfaces at offer/I-9 stage and burns the offer *and* the relationship. The whole point of the sponsorship classifier is that you never need to be tempted.
- Ghost rates are ~70% even for strong candidates. The system's job is to make ghosting cost you nothing emotionally: it's a row, follow-up fires in 10 days, move on.

**Technical hygiene**
- Rate-limit yourself even on public APIs: sequential polling, 1 req/board/10min, honest User-Agent, back off on 429/5xx. You want to be the client they never notice.
- Watchlist rot: boards get renamed. Weekly job flags 404-ing tokens.
- LLM outputs: always JSON-parse defensively, log prompt version, and keep the fabrication validator on. Audit 1 in 10 tailored resumes by hand forever.
- Handle the F-1 specifics outside the system too: talk to your DSO about CPT/OPT timelines *now*; know your OPT application window (up to 90 days pre-graduation); STEM OPT extension gives you 3 lottery attempts — mention "36 months of work authorization" framing when relevant in conversations, because many recruiters don't know it.

**Backup plans (build optionality, don't just hope)**
- Day-1 CPT is a red flag; don't plan around it. Do plan around: **STEM OPT (3 yrs) → 3 H-1B lottery shots**, cap-exempt employers (universities, nonprofits, research orgs — no lottery), Big-Tech-scale sponsors with global offices (transfer options if lottery fails), and Canada/EU-friendly companies as a parallel track. Tag companies in the DB with `lottery_backup` notes — this is queryable strategy, not anxiety.
- If cold response rate stays <2% after 100 sponsor-verified tailored apps: stop applying for two weeks, get 3 humans (career center, alumni engineer, recruiter contact) to tear down your resume, and shift 80% of hours to referrals. The data told you the bottleneck; believe it.

---

## Final note

You asked for a system that maximizes offer probability. The honest version of that system spends its automation budget on **speed, targeting, and volume-of-quality** — and deliberately keeps a human at the two points that decide outcomes: what goes on the resume, and what gets submitted. Build it, ship it publicly (sanitized), and it does double duty: it runs your search *and* it's the first line of your resume.