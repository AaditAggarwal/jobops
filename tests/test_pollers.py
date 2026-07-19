"""Field-mapping tests for every poller against real (trimmed) API payloads.

Fixtures in tests/fixtures/ were fetched once from public boards
(Duolingo/Greenhouse, Palantir/Lever, Ramp/Ashby, ServiceNow/SmartRecruiters,
SimplifyJobs listings.json) and truncated. No live-API calls here.
"""

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from jobops.ingest import ashby, github_repos, greenhouse, lever, smartrecruiters

FIXTURES = Path(__file__).parent / "fixtures"


def load(name):
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


class TestGreenhouseMapping:
    def test_map_posting(self):
        j = load("greenhouse_sample.json")["jobs"][0]
        m = greenhouse.map_posting(j)
        assert m["external_id"] == str(j["id"])
        assert m["title"] == j["title"]
        assert m["location"] == j["location"]["name"]
        assert m["url"].startswith("https://")
        assert m["description"]  # content=true payload includes the JD
        assert isinstance(m["posted_at"], datetime)
        assert m["posted_at"].tzinfo is not None

    def test_posted_at_prefers_first_published(self):
        j = load("greenhouse_sample.json")["jobs"][0]
        j["first_published"] = "2026-01-02T00:00:00-05:00"
        j["updated_at"] = "2026-06-01T00:00:00-04:00"
        assert greenhouse.map_posting(j)["posted_at"].year == 2026
        assert greenhouse.map_posting(j)["posted_at"].month == 1

    def test_missing_location_is_none(self):
        j = load("greenhouse_sample.json")["jobs"][0]
        j["location"] = None
        assert greenhouse.map_posting(j)["location"] is None


class TestLeverMapping:
    def test_map_posting(self):
        p = load("lever_sample.json")[0]
        m = lever.map_posting(p)
        assert m["external_id"] == p["id"]
        assert m["title"] == p["text"]
        assert m["location"] == p["categories"]["location"]
        assert m["url"] == p["hostedUrl"]
        assert m["description"] == p["descriptionPlain"]
        # createdAt is ms-epoch
        assert m["posted_at"] == datetime.fromtimestamp(
            p["createdAt"] / 1000, tz=timezone.utc
        )

    def test_no_categories(self):
        p = load("lever_sample.json")[0]
        p["categories"] = None
        assert lever.map_posting(p)["location"] is None


class TestAshbyMapping:
    def test_map_posting(self):
        j = load("ashby_sample.json")["jobs"][0]
        m = ashby.map_posting(j)
        assert m["external_id"] == j["id"]
        assert m["title"] == j["title"]
        assert m["location"] == j["location"]
        assert m["url"] == j["jobUrl"]
        assert m["description"] == j["descriptionPlain"]
        assert isinstance(m["posted_at"], datetime)

    def test_falls_back_to_html_description(self):
        j = load("ashby_sample.json")["jobs"][0]
        j["descriptionPlain"] = None
        assert ashby.map_posting(j)["description"] == j["descriptionHtml"]


class TestSmartRecruitersMapping:
    def test_map_posting(self):
        item = load("smartrecruiters_sample.json")["content"][0]
        m = smartrecruiters.map_posting(item, "ServiceNow")
        assert m["external_id"] == str(item["id"])
        assert m["title"] == item["name"]
        assert m["location"] == item["location"]["fullLocation"]
        assert m["url"] == f"https://jobs.smartrecruiters.com/ServiceNow/{item['id']}"
        assert m["description"] is None  # list endpoint has no JD
        assert isinstance(m["posted_at"], datetime)

    def test_description_from_detail(self):
        detail = load("smartrecruiters_detail_sample.json")
        desc = smartrecruiters.description_from_detail(detail)
        assert len(desc) > 100
        # every non-empty section's text should be present
        for sec in detail["jobAd"]["sections"].values():
            if sec.get("text"):
                assert sec["text"][:50] in desc


class TestGithubListingMapping:
    def test_map_listing(self):
        item = load("github_listings_sample.json")[0]
        m = github_repos.map_listing(item)
        assert m["external_id"] == item["id"]
        assert m["title"] == item["title"]
        assert m["url"] == item["url"]
        assert m["location"] == ", ".join(item["locations"])
        assert m["posted_at"] == datetime.fromtimestamp(
            item["date_posted"], tz=timezone.utc
        )

    def test_empty_locations_is_none(self):
        item = load("github_listings_sample.json")[0]
        item["locations"] = []
        assert github_repos.map_listing(item)["location"] is None


@pytest.mark.parametrize(
    "fixture", ["greenhouse_sample.json", "lever_sample.json", "ashby_sample.json",
                "smartrecruiters_sample.json", "github_listings_sample.json"]
)
def test_all_fixture_records_map_without_error(fixture):
    """Every record in every fixture must map cleanly, not just the first."""
    data = load(fixture)
    if fixture.startswith("greenhouse") or fixture.startswith("ashby"):
        records = data["jobs"]
    elif fixture.startswith("smartrecruiters"):
        records = data["content"]
    else:
        records = data
    assert records
    for rec in records:
        if fixture.startswith("greenhouse"):
            m = greenhouse.map_posting(rec)
        elif fixture.startswith("lever"):
            m = lever.map_posting(rec)
        elif fixture.startswith("ashby"):
            m = ashby.map_posting(rec)
        elif fixture.startswith("smartrecruiters"):
            m = smartrecruiters.map_posting(rec, "ServiceNow")
        else:
            m = github_repos.map_listing(rec)
        assert m["external_id"] and m["title"] and m["url"]
