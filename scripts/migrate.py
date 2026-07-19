"""Apply migrations/*.sql in order, tracking them in schema_migrations.

Usage: uv run python scripts/migrate.py

Idempotent: already-applied migrations (keyed by filename) are skipped, so
re-running is a no-op. Each migration runs in its own transaction together
with its schema_migrations insert — a failed migration leaves no partial
record. Migrations are forward-only and never edited after being applied.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

import psycopg

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from jobops.db import database_url  # noqa: E402

MIGRATIONS_DIR = Path(__file__).resolve().parent.parent / "migrations"

MIGRATION_NAME_RE = re.compile(r"^(\d{3})_[a-z0-9_]+\.sql$")


def migration_number(filename: str) -> int:
    """Return the numeric prefix of a migration filename, e.g. 1 for '001_core.sql'.

    Raises ValueError if the filename doesn't match NNN_name.sql — a
    misnamed file would silently break ordering, so it's a hard error.
    """
    m = MIGRATION_NAME_RE.match(filename)
    if not m:
        raise ValueError(
            f"bad migration filename {filename!r}: expected NNN_name.sql "
            "(three-digit prefix, lowercase snake_case)"
        )
    return int(m.group(1))


def order_migrations(paths: list[Path]) -> list[Path]:
    """Sort migration files by numeric prefix, rejecting duplicate numbers."""
    ordered = sorted(paths, key=lambda p: migration_number(p.name))
    seen: dict[int, str] = {}
    for p in ordered:
        n = migration_number(p.name)
        if n in seen:
            raise ValueError(
                f"duplicate migration number {n:03d}: {seen[n]} and {p.name}"
            )
        seen[n] = p.name
    return ordered


def pending_migrations(all_paths: list[Path], applied: set[str]) -> list[Path]:
    """Return ordered migrations whose filenames are not in `applied`."""
    return [p for p in order_migrations(all_paths) if p.name not in applied]


def run(migrations_dir: Path = MIGRATIONS_DIR) -> int:
    """Apply all pending migrations; return the number applied."""
    paths = list(migrations_dir.glob("*.sql"))
    if not paths:
        print(f"[migrate] no .sql files in {migrations_dir}")
        return 0

    with psycopg.connect(database_url()) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS schema_migrations (
                filename   TEXT PRIMARY KEY,
                applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
            )
            """
        )
        conn.commit()

        rows = conn.execute("SELECT filename FROM schema_migrations").fetchall()
        applied = {r[0] for r in rows}

        todo = pending_migrations(paths, applied)
        if not todo:
            print(f"[migrate] up to date ({len(applied)} applied)")
            return 0

        for path in todo:
            print(f"[migrate] applying {path.name} ...", end=" ")
            try:
                conn.execute(path.read_text(encoding="utf-8"))
                conn.execute(
                    "INSERT INTO schema_migrations (filename) VALUES (%s)",
                    (path.name,),
                )
                conn.commit()
            except Exception:
                conn.rollback()
                print("FAILED")
                raise
            print("ok")

    print(f"[migrate] applied {len(todo)} migration(s)")
    return len(todo)


if __name__ == "__main__":
    run()
