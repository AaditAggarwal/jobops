"""Unit tests for the pure ingestion-core logic (no DB, no network)."""

import pytest

from jobops.ingest.common import looks_new_grad, normalize_company


class TestNormalizeCompany:
    @pytest.mark.parametrize(
        ("raw", "expected"),
        [
            ("Stripe, Inc.", "stripe"),
            ("stripe", "stripe"),
            ("Databricks Inc", "databricks"),
            ("Palantir Technologies", "palantir"),
            ("Bosch Group", "bosch"),
            ("Anduril Industries", "anduril industries"),
            ("The Trade Desk", "the trade desk"),
            ("Ramp Business Corporation", "ramp business"),
            ("Scale AI, Inc.", "scale ai"),
            ("Two Sigma Investments, LP", "two sigma investments lp"),
            ("  Figma   ", "figma"),
            ("DoorDash USA, LLC", "doordash"),
        ],
    )
    def test_normalization(self, raw, expected):
        assert normalize_company(raw) == expected

    def test_same_company_different_sources_collide(self):
        assert normalize_company("Stripe, Inc.") == normalize_company("Stripe")
        assert normalize_company("Duolingo, Inc.") == normalize_company("Duolingo")

    def test_suffix_only_inside_word_is_kept(self):
        # "co"/"corp" must only strip as whole words, not inside names
        assert normalize_company("Coinbase") == "coinbase"
        assert normalize_company("Costco") == "costco"
        assert normalize_company("Incode") == "incode"

    def test_punctuation_removed(self):
        assert normalize_company("O'Reilly Media") == "oreilly media"


class TestLooksNewGrad:
    # -- positives -----------------------------------------------------------
    @pytest.mark.parametrize(
        "title",
        [
            "Software Engineer, New Grad (2027)",
            "Software Engineer - New Grad",
            "New Graduate Software Engineer",
            "University Grad Software Engineer",
            "Entry Level Software Engineer",
            "Entry-Level Backend Engineer",
            "Early Career Software Developer",
            "Campus Hire - Software Engineering",
            "Software Engineer Intern (Summer 2027)",
            "SWE Intern",
            "Software Engineer 2026",
        ],
    )
    def test_positive_titles(self, title):
        assert looks_new_grad(title) is True

    # -- negatives -----------------------------------------------------------
    @pytest.mark.parametrize(
        "title",
        [
            "Sr. Software Engineer",
            "Senior Software Engineer",
            "Staff Software Engineer",
            "Principal Engineer",
            "Engineering Manager",
            "Director of Engineering",
            "Lead Software Engineer",
            "Software Engineer",           # no signal either way -> False
            "Backend Engineer, Payments",
            "Senior New Grad Program Manager",  # senior veto beats new-grad hit
            "Sr. Software Engineer, Early Career Programs",  # veto wins
        ],
    )
    def test_negative_titles(self, title):
        assert looks_new_grad(title) is False

    # -- JD fallback ---------------------------------------------------------
    def test_jd_signal_qualifies_when_title_is_neutral(self):
        jd = "We are looking for new grad engineers graduating in 2027."
        assert looks_new_grad("Software Engineer I", jd) is True

    def test_jd_signal_beyond_2000_chars_is_ignored(self):
        jd = ("x" * 2500) + " new grad"
        assert looks_new_grad("Software Engineer", jd) is False

    def test_senior_title_vetoes_even_with_new_grad_jd(self):
        jd = "Join our new grad program!"
        assert looks_new_grad("Senior Software Engineer", jd) is False

    def test_empty_inputs(self):
        assert looks_new_grad("") is False
        assert looks_new_grad("", "") is False
