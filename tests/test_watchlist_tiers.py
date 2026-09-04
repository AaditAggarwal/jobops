"""Tests for cadence-tiered watchlist loading and raw-payload trimming."""

from __future__ import annotations

import pytest

from jobops.ingest import common
from jobops.ingest.common import load_watchlist, merge_watchlists, trim_raw


def write(path, data):
    path.write_text(data, encoding="utf-8")
    return path


@pytest.fixture
def tiers(tmp_path, monkeypatch):
    core = write(tmp_path / "core.yaml", "greenhouse: [stripe, figma]\nlever: [ramp]\n")
    tail = write(tmp_path / "tail.yaml", "greenhouse: [linear, STRIPE]\nashby: [cursor]\n")
    monkeypatch.setattr(common, "TIER_PATHS", {"core": core, "tail": tail})
    return core, tail


def test_core_tier_loads_only_core(tiers):
    assert load_watchlist("core") == {"greenhouse": ["stripe", "figma"], "lever": ["ramp"]}


def test_tail_tier_loads_only_tail(tiers):
    assert load_watchlist("tail") == {"greenhouse": ["linear", "STRIPE"], "ashby": ["cursor"]}


def test_all_merges_both_tiers_without_duplicates(tiers):
    merged = load_watchlist("all")
    # STRIPE duplicates stripe across tiers — one board must never be polled twice.
    assert merged["greenhouse"] == ["stripe", "figma", "linear"]
    assert merged["lever"] == ["ramp"] and merged["ashby"] == ["cursor"]


def test_tier_defaults_to_env(tiers, monkeypatch):
    monkeypatch.setenv("JOBOPS_TIER", "tail")
    assert "ashby" in load_watchlist()


def test_unknown_tier_is_an_error(tiers):
    with pytest.raises(ValueError):
        load_watchlist("middle")


def test_missing_tier_file_is_empty(tmp_path, monkeypatch):
    monkeypatch.setattr(common, "TIER_PATHS", {"core": tmp_path / "nope.yaml"})
    assert load_watchlist("core") == {}


def test_merge_preserves_first_tier_order():
    a = {"greenhouse": ["a", "b"]}
    b = {"greenhouse": ["b", "c"], "lever": ["d"]}
    assert merge_watchlists(a, b) == {"greenhouse": ["a", "b", "c"], "lever": ["d"]}


def test_trim_raw_drops_jd_duplicates_when_description_is_stored():
    raw = {"id": 1, "title": "SWE", "content": "x" * 5000, "department": "eng"}
    out = trim_raw(raw, "the stored jd")
    assert "content" not in out
    assert out["_trimmed_keys"] == ["content"]
    assert out["id"] == 1 and out["department"] == "eng"


def test_trim_raw_keeps_everything_when_no_description_was_stored():
    # Rows beyond a poller's detail cap have description NULL; their raw payload
    # is the only copy of the JD and must survive.
    raw = {"id": 1, "content": "the only copy"}
    assert trim_raw(raw, None) == raw
    assert trim_raw(raw, "") == raw


def test_trim_raw_is_a_noop_without_jd_keys():
    raw = {"id": 1, "title": "SWE"}
    assert trim_raw(raw, "jd") == raw


def test_trim_raw_handles_every_known_jd_key():
    raw = {"id": 1, "content": "a", "descriptionHtml": "b", "descriptionPlain": "c",
           "description": "d", "jobAd": "e", "plaintext": "f", "keep": "yes"}
    out = trim_raw(raw, "jd")
    assert set(out) == {"id", "keep", "_trimmed_keys"}
