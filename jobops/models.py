"""Dataclasses mirroring the core tables (migrations/001_core.sql).

These exist for type clarity when passing rows between functions — they are
NOT an ORM layer. Field names and order match the schema columns; optional
columns default to None so partially-populated rows (e.g. pre-enrichment
jobs) are representable.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any
from uuid import UUID


@dataclass
class Company:
    name: str
    name_normalized: str
    id: UUID | None = None
    website: str | None = None
    ats_type: str | None = None          # greenhouse | lever | ashby | smartrecruiters | workday | other
    ats_token: str | None = None
    hq_location: str | None = None
    size_bucket: str | None = None       # startup | midsize | enterprise
    sponsor_score: float | None = None   # 0..1
    sponsor_status: str = "unknown"      # verified | likely | unlikely | no | unknown
    notes: str | None = None
    created_at: datetime | None = None


@dataclass
class Job:
    source: str                          # greenhouse | lever | ashby | smartrec | github_repo | email | rss | manual
    title: str
    url: str
    id: UUID | None = None
    company_id: UUID | None = None
    external_id: str | None = None
    location: str | None = None
    remote: bool | None = None
    description: str | None = None
    posted_at: datetime | None = None
    first_seen_at: datetime | None = None
    raw: dict[str, Any] | None = None
    # enrichment
    is_new_grad: bool | None = None
    fit_score: float | None = None       # 0..1
    fit_rationale: str | None = None
    keywords: list[str] = field(default_factory=list)
    visa_flag: str | None = None         # jd_says_no_sponsor | jd_silent | jd_says_yes
    status: str = "new"                  # new | triaged | queued | applied | skipped | expired


@dataclass
class Application:
    job_id: UUID
    id: UUID | None = None
    resume_version_id: UUID | None = None
    cover_letter_path: str | None = None
    channel: str | None = None           # ats_direct | referral | recruiter | career_fair
    referral_contact_id: UUID | None = None
    applied_at: datetime | None = None
    minutes_after_posting: int | None = None
    status: str = "submitted"            # submitted | oa | phone | onsite | offer | rejected | ghosted | withdrawn
    status_updated_at: datetime | None = None
    rejection_stage: str | None = None
    notes: str | None = None
