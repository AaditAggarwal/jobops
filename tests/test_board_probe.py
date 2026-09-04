"""Tests for the shared ATS board probe verdicts (pure logic only, no network)."""

from __future__ import annotations

import pytest

from jobops.ingest.board_probe import classify, count_postings


@pytest.mark.parametrize(
    "ats,payload,expected",
    [
        ("greenhouse", {"jobs": [{}, {}, {}]}, 3),
        ("ashby", {"jobs": [{}]}, 1),
        ("lever", [{}, {}], 2),
        ("smartrecruiters", {"totalFound": 42, "content": []}, 42),
        ("greenhouse", {}, 0),
        ("lever", {"unexpected": "shape"}, 0),
        ("ashby", None, 0),
    ],
)
def test_count_postings(ats, payload, expected):
    assert count_postings(ats, payload) == expected


def test_404_is_dead():
    assert classify("greenhouse", 404, None).alive is False


def test_live_board_with_zero_openings_stays_alive():
    # A real board between hiring waves must not be evicted from the watchlist.
    p = classify("greenhouse", 200, {"jobs": []})
    assert p.alive is True and p.count == 0


def test_smartrecruiters_zero_postings_is_dead():
    # SmartRecruiters 200s with totalFound 0 for unknown tokens; it never 404s.
    assert classify("smartrecruiters", 200, {"totalFound": 0}).alive is False


def test_smartrecruiters_with_postings_is_alive():
    assert classify("smartrecruiters", 200, {"totalFound": 7}).alive is True


@pytest.mark.parametrize("status", [429, 500, 502, 503])
def test_throttle_and_server_errors_are_inconclusive(status):
    # Inconclusive, never "dead" — a tarpitted probe must not delete a board.
    assert classify("greenhouse", status, None).alive is None


def test_other_4xx_is_dead():
    assert classify("lever", 403, None).alive is False


def test_lever_titles_come_from_text_field():
    # Lever names the title "text"; reading "title" scored every Lever board
    # as having zero engineering roles and silently dropped the provider.
    data = [{"text": "Software Engineer", "categories": {"location": "Austin, TX"}}]
    p = classify("lever", 200, data)
    assert p.swe == 1 and p.us == 1


def test_greenhouse_location_object():
    data = {"jobs": [
        {"title": "Backend Engineer", "location": {"name": "San Francisco, CA"}},
        {"title": "Backend Engineer", "location": {"name": "London, United Kingdom"}},
    ]}
    p = classify("greenhouse", 200, data)
    assert p.count == 2 and p.swe == 2 and p.us == 1


def test_ashby_plain_string_location():
    data = {"jobs": [{"title": "ML Engineer", "location": "Remote - US"}]}
    assert classify("ashby", 200, data).us == 1


def test_board_with_no_engineering_roles():
    data = {"jobs": [{"title": "Barista", "location": {"name": "Austin, TX"}},
                     {"title": "Store Manager", "location": {"name": "Austin, TX"}}]}
    p = classify("greenhouse", 200, data)
    assert p.alive is True and p.swe == 0 and p.us == 2


def test_non_us_board_scores_zero_us():
    data = {"jobs": [{"title": "Software Engineer", "location": "Bengaluru, India"},
                     {"title": "Software Engineer", "location": "Berlin, Germany"}]}
    assert classify("ashby", 200, data).us == 0
