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

CREATE TABLE heartbeats (                      -- pollers/ETL write a row per successful run
    source          TEXT NOT NULL,
    ok              BOOLEAN NOT NULL,
    ran_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    detail          TEXT
);
CREATE INDEX heartbeats_source_ran_idx ON heartbeats(source, ran_at DESC);
