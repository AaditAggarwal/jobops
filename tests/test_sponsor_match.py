"""Unit tests for the sponsorship scorer/matcher (pure parts, no DB).

One of the two highest-stakes pure functions in the system (CLAUDE.md):
a false 'verified' wastes application time; a false 'unknown' on a routine
sponsor buries a good target.
"""

import pytest

from jobops.enrich.sponsor_match import (
    MATCH_THRESHOLD,
    compute_score,
    current_fiscal_year,
    pick_best_match,
)
from jobops.etl.uscis_hub import parse_row

NOW_FY = 2026


class TestComputeScore:
    def test_routine_recent_sponsor_is_verified(self):
        # 200 approvals, few denials, current-year data -> clearly verified
        score, status = compute_score(200, 5, NOW_FY, NOW_FY)
        assert status == "verified"
        assert score >= 0.9

    def test_zero_approvals_is_unlikely(self):
        assert compute_score(0, 0, NOW_FY, NOW_FY) == (0.05, "unlikely")
        assert compute_score(0, 10, NOW_FY, NOW_FY) == (0.05, "unlikely")

    def test_negative_or_missing_treated_as_unlikely(self):
        assert compute_score(-1, 0, None, NOW_FY)[1] == "unlikely"

    def test_small_recent_sponsor_is_likely(self):
        # 5 approvals, no denials, recent: real but not routine
        score, status = compute_score(5, 0, NOW_FY, NOW_FY)
        assert status == "likely"
        assert 0.2 <= score < 0.5

    def test_volume_saturates_at_50(self):
        s50, _ = compute_score(50, 0, NOW_FY, NOW_FY)
        s500, _ = compute_score(500, 0, NOW_FY, NOW_FY)
        # volume capped for both; only the smoothed rate differs slightly
        assert abs(s50 - s500) < 0.03

    def test_single_approval_is_not_verified(self):
        # the regression that motivated rate smoothing: 1 clean recent
        # approval must not produce a 'verified' badge
        score, status = compute_score(1, 0, NOW_FY, NOW_FY)
        assert status == "likely"
        assert score < 0.5

    def test_denial_heavy_shop_scores_lower(self):
        clean, _ = compute_score(50, 0, NOW_FY, NOW_FY)
        denied, _ = compute_score(50, 50, NOW_FY, NOW_FY)
        assert denied < clean

    def test_stale_data_scores_lower_than_recent(self):
        recent, _ = compute_score(50, 0, NOW_FY, NOW_FY)
        stale, _ = compute_score(50, 0, NOW_FY - 5, NOW_FY)
        assert stale < recent

    def test_recency_window_is_two_fiscal_years(self):
        inside, _ = compute_score(50, 0, NOW_FY - 2, NOW_FY)
        outside, _ = compute_score(50, 0, NOW_FY - 3, NOW_FY)
        assert inside > outside

    def test_score_bounds(self):
        for a, d, fy in [(1, 0, NOW_FY), (10000, 0, NOW_FY), (1, 100, 2015)]:
            score, _ = compute_score(a, d, fy, NOW_FY)
            assert 0.0 <= score <= 1.0

    def test_status_thresholds(self):
        # engineered aggregates around the 0.5 / 0.2 boundaries
        assert compute_score(200, 0, NOW_FY, NOW_FY)[1] == "verified"
        assert compute_score(4, 0, NOW_FY - 5, NOW_FY)[1] == "likely"
        assert compute_score(1, 30, NOW_FY - 5, NOW_FY)[1] == "unlikely"


class TestPickBestMatch:
    def rows(self, *names):
        return [{"employer_norm": n, "a": 10, "d": 0, "yr": NOW_FY} for n in names]

    def test_exact_match_wins(self):
        best = pick_best_match("stripe", self.rows("square", "stripe"))
        assert best["employer_norm"] == "stripe"

    def test_word_order_ignored(self):
        best = pick_best_match("labs figma", self.rows("figma labs"))
        assert best is not None

    def test_below_threshold_returns_none(self):
        assert pick_best_match("stripe", self.rows("microsoft")) is None

    def test_similar_but_different_company_rejected(self):
        # the classic trap: near-identical strings, different employers
        assert pick_best_match("cognizant", self.rows("cognigent solutions")) is None

    def test_empty_candidates(self):
        assert pick_best_match("stripe", []) is None

    def test_threshold_is_strict(self):
        assert MATCH_THRESHOLD >= 0.90  # loosening this silently is a bug


class TestCurrentFiscalYear:
    def test_returns_plausible_year(self):
        assert 2025 <= current_fiscal_year() <= 2030


class TestUscisParseRow:
    def test_modern_headers(self):
        row = {"Fiscal Year": "2025", "Employer (Petitioner) Name": "STRIPE, INC.",
               "Initial Approval": "12", "Initial Denial": "1",
               "Continuing Approval": "30"}
        assert parse_row(row) == (2025, "STRIPE, INC.", 12, 1, 30)

    def test_lowercase_and_alternate_headers(self):
        row = {"fiscal_year": "2024", "Employer": "Acme Corp",
               "initial_approval": "3", "initial_denial": "",
               "continuing_approval": None}
        assert parse_row(row) == (2024, "Acme Corp", 3, 0, 0)

    def test_comma_separated_numbers(self):
        row = {"Fiscal Year": "2025", "Employer (Petitioner) Name": "Big Co",
               "Initial Approval": "1,234", "Initial Denial": "0",
               "Continuing Approval": "2,500"}
        assert parse_row(row)[2] == 1234
        assert parse_row(row)[4] == 2500

    def test_blank_employer_row_skipped(self):
        assert parse_row({"Fiscal Year": "2025",
                          "Employer (Petitioner) Name": "  "}) is None
        assert parse_row({"Fiscal Year": "2025"}) is None

    def test_garbage_numbers_dont_crash(self):
        row = {"Fiscal Year": "abc", "Employer (Petitioner) Name": "X Co",
               "Initial Approval": "N/A", "Initial Denial": "-",
               "Continuing Approval": "?"}
        fy, name, ia, idn, ca = parse_row(row)
        assert fy is None and (ia, idn, ca) == (0, 0, 0)
