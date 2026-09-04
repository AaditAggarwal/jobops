"""Tests for board discovery's pure logic: token extraction, guessing, curation.

Token extraction is the load-bearing piece — it decides which boards enter the
watchlist from thousands of scraped URLs, and a sloppy pattern would fill the
poll cycle with garbage tokens.
"""

from __future__ import annotations

import pytest

from scripts.discover_boards import (
    extract_tokens,
    is_excluded,
    slug_candidates,
    us_relevant,
)


@pytest.mark.parametrize(
    "url,expected",
    [
        ("https://boards.greenhouse.io/stripe/jobs/12345", ("greenhouse", "stripe")),
        ("https://job-boards.greenhouse.io/databricks/jobs/7", ("greenhouse", "databricks")),
        ("https://boards.eu.greenhouse.io/wise/jobs/1", ("greenhouse", "wise")),
        ("https://boards-api.greenhouse.io/v1/boards/figma/jobs", ("greenhouse", "figma")),
        ("https://boards.greenhouse.io/embed/job_board?for=notion", ("greenhouse", "notion")),
        ("https://jobs.lever.co/ramp/abc-def", ("lever", "ramp")),
        ("https://jobs.eu.lever.co/monzo/x", ("lever", "monzo")),
        ("https://jobs.ashbyhq.com/linear/1234-5678", ("ashby", "linear")),
        ("https://jobs.smartrecruiters.com/Experian/999", ("smartrecruiters", "Experian")),
    ],
)
def test_extract_one_token(url, expected):
    assert expected in extract_tokens(url)


def test_extraction_is_case_folded_except_smartrecruiters():
    # SmartRecruiters tokens are case-sensitive in the API path; the others are not.
    assert ("greenhouse", "stripe") in extract_tokens("https://boards.greenhouse.io/Stripe/jobs/1")
    assert ("smartrecruiters", "Bosch") in extract_tokens("https://jobs.smartrecruiters.com/Bosch/2")


def test_stopwords_are_not_tokens():
    assert extract_tokens("https://boards.greenhouse.io/embed/job_board") == set()


def test_extract_from_a_markdown_blob():
    text = (
        "| Company | [Apply](https://jobs.ashbyhq.com/cursor/abc) |\n"
        "| Other   | [Apply](https://jobs.lever.co/plaid/xyz) |\n"
        "no link here\n"
    )
    assert extract_tokens(text) == {("ashby", "cursor"), ("lever", "plaid")}


def test_non_ats_urls_yield_nothing():
    assert extract_tokens("https://example.com/careers/software-engineer") == set()


@pytest.mark.parametrize(
    "name,slug,expected_first",
    [
        ("Modern Treasury", "modern-treasury", "modern-treasury"),
        ("Jane Street", "", "janestreet"),
        ("H1 Insights", "h1-insights", "h1insights"),
    ],
)
def test_slug_candidates(name, slug, expected_first):
    got = slug_candidates(name, slug)
    assert expected_first in got
    assert len(got) == len(set(got))  # no duplicate probing


def test_slug_candidates_handles_punctuation_and_empties():
    assert slug_candidates("", "") == []
    assert "wearedrw" not in slug_candidates("DRW")
    assert slug_candidates("Acme, Inc.")[0] == "acmeinc"


@pytest.mark.parametrize(
    "name,token",
    [
        ("Acme Defense Systems", "acme"),
        ("Global Staffing Partners", "acme"),
        ("Fine Company", "shieldai"),
        ("Fine Company", "insight-global"),
        ("Talent Acquisition Group", "acme"),
    ],
)
def test_excluded_candidates(name, token):
    # CLAUDE.md targeting rule: no clearance-gated employers, no staffing mills.
    assert is_excluded(name, token) is True


def test_ordinary_company_is_not_excluded():
    assert is_excluded("Stripe", "stripe") is False
    assert is_excluded("YC W22 (30 ppl)", "linear") is False


@pytest.mark.parametrize(
    "location",
    ["San Francisco, CA, USA", "Remote", "New York, NY", "", "Austin, Texas, United States"],
)
def test_us_relevant_locations(location):
    assert us_relevant(location) is True


@pytest.mark.parametrize("location", ["London, England, United Kingdom", "Bengaluru, India"])
def test_non_us_locations_filtered(location):
    assert us_relevant(location) is False
