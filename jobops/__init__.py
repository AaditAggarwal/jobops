"""jobops — a personal job-search operating system.

Importing the package loads `.env` into the process environment so that
`uv run python -m jobops.<module>` behaves locally the way it does in CI, where
the same variables arrive as Actions secrets. Without this, a local run silently
fell back to the docker-compose DATABASE_URL default and failed against whatever
Postgres happened to be listening on localhost.

Real environment variables always win: `.env` only fills in what is missing, so
CI secrets can never be shadowed by a stale local file.
"""

from __future__ import annotations

import os
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
ENV_PATH = REPO_ROOT / ".env"


def load_env_file(path: Path | None = None) -> int:
    """Fill missing environment variables from a KEY=VALUE file; returns how many were set.

    Tolerates comments, blank lines, `export ` prefixes, and quoted values.
    A missing file is not an error — CI has no .env and needs none.
    """
    path = path or ENV_PATH
    if not path.exists():
        return 0
    loaded = 0
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.removeprefix("export ").strip()
        if key and key not in os.environ:
            os.environ[key] = value.strip().strip('"').strip("'")
            loaded += 1
    return loaded


load_env_file()
