"""Make scripts/ importable so tests can unit-test migrate.py's pure logic."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))
