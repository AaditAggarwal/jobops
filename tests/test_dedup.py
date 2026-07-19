"""Unit tests for the pure cross-source dedup decision logic."""

from datetime import datetime, timedelta, timezone

from jobops.enrich.dedup import find_duplicates

T0 = datetime(2026, 7, 1, 12, 0, tzinfo=timezone.utc)


def row(id, company="stripe", title="Software Engineer, New Grad",
        location="New York, NY", source="greenhouse", days=0.0):
    return {
        "id": id,
        "company_norm": company,
        "title": title,
        "location": location,
        "source": source,
        "first_seen_at": T0 + timedelta(days=days),
    }


def test_cross_source_duplicate_marks_later_row():
    rows = [
        row("a", source="greenhouse", days=0),
        row("b", source="github_repo", title="Software Engineer - New Grad", days=1),
    ]
    decisions = find_duplicates(rows)
    assert decisions == [("b", "a", "duplicate of a via greenhouse")]


def test_earliest_row_wins_regardless_of_input_order():
    rows = [
        row("late", days=2),
        row("early", days=0),
    ]
    (dup_id, kept_id, _note), = find_duplicates(rows)
    assert dup_id == "late" and kept_id == "early"


def test_dissimilar_titles_not_duplicates():
    rows = [
        row("a", title="Software Engineer, New Grad"),
        row("b", title="Product Manager, New Grad", days=1),
    ]
    assert find_duplicates(rows) == []


def test_below_title_threshold_not_duplicate():
    rows = [
        row("a", title="Software Engineer, Backend"),
        row("b", title="Software Engineer, Frontend", days=1),
    ]
    assert find_duplicates(rows) == []


def test_outside_14_day_window_not_duplicate():
    rows = [
        row("a", days=0),
        row("b", days=15),
    ]
    assert find_duplicates(rows) == []


def test_different_location_not_duplicate():
    rows = [
        row("a", location="New York, NY"),
        row("b", location="Seattle, WA", days=1),
    ]
    assert find_duplicates(rows) == []


def test_location_case_insensitive():
    rows = [
        row("a", location="Remote"),
        row("b", location="remote ", days=1),
    ]
    assert len(find_duplicates(rows)) == 1


def test_both_locations_none_can_match():
    rows = [
        row("a", location=None),
        row("b", location=None, days=1),
    ]
    assert len(find_duplicates(rows)) == 1


def test_one_location_none_does_not_match():
    rows = [
        row("a", location=None),
        row("b", location="New York, NY", days=1),
    ]
    assert find_duplicates(rows) == []


def test_different_companies_never_compared():
    rows = [
        row("a", company="stripe"),
        row("b", company="figma", days=1),
    ]
    assert find_duplicates(rows) == []


def test_triple_cluster_keeps_only_earliest():
    rows = [
        row("a", source="greenhouse", days=0),
        row("b", source="github_repo", days=1),
        row("c", source="email", days=2),
    ]
    decisions = find_duplicates(rows)
    assert {(d, k) for d, k, _ in decisions} == {("b", "a"), ("c", "a")}
