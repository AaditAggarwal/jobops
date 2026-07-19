"""Unit tests for scripts/migrate.py ordering/parsing logic (no DB needed)."""

from pathlib import Path

import pytest

from migrate import migration_number, order_migrations, pending_migrations


def paths(*names: str) -> list[Path]:
    return [Path(n) for n in names]


class TestMigrationNumber:
    def test_parses_three_digit_prefix(self):
        assert migration_number("001_core.sql") == 1
        assert migration_number("042_sponsors_v2.sql") == 42

    @pytest.mark.parametrize(
        "bad",
        [
            "core.sql",            # no prefix
            "1_core.sql",          # not three digits
            "001-core.sql",        # dash instead of underscore
            "001_Core.sql",        # uppercase
            "001_core.txt",        # wrong extension
            "001_.sql",            # empty name part
        ],
    )
    def test_rejects_bad_filenames(self, bad):
        with pytest.raises(ValueError, match="bad migration filename"):
            migration_number(bad)


class TestOrderMigrations:
    def test_sorts_by_numeric_prefix_not_lexically(self):
        got = order_migrations(paths("010_ten.sql", "002_two.sql", "001_one.sql"))
        assert [p.name for p in got] == ["001_one.sql", "002_two.sql", "010_ten.sql"]

    def test_rejects_duplicate_numbers(self):
        with pytest.raises(ValueError, match="duplicate migration number 002"):
            order_migrations(paths("002_a.sql", "002_b.sql"))

    def test_empty_is_fine(self):
        assert order_migrations([]) == []


class TestPendingMigrations:
    def test_filters_applied_and_keeps_order(self):
        all_paths = paths("003_c.sql", "001_a.sql", "002_b.sql")
        got = pending_migrations(all_paths, applied={"001_a.sql"})
        assert [p.name for p in got] == ["002_b.sql", "003_c.sql"]

    def test_all_applied_means_nothing_pending(self):
        all_paths = paths("001_a.sql", "002_b.sql")
        assert pending_migrations(all_paths, {"001_a.sql", "002_b.sql"}) == []
