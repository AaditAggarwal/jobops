#!/usr/bin/env bash
# Nightly Postgres backup: pg_dump | gzip into backups/, keep the newest 14.
# Usage: DATABASE_URL=... scripts/backup.sh   (defaults to local docker URL)
set -euo pipefail

DATABASE_URL="${DATABASE_URL:-postgresql://jobops:jobops@localhost:5432/jobops}"
BACKUP_DIR="$(dirname "$0")/../backups"
mkdir -p "$BACKUP_DIR"

STAMP="$(date +%Y-%m-%d_%H%M%S)"
OUT="$BACKUP_DIR/jobops_${STAMP}.sql.gz"

pg_dump "$DATABASE_URL" | gzip > "$OUT"
echo "[backup] wrote $OUT ($(du -h "$OUT" | cut -f1))"

# prune: keep the 14 newest dumps
ls -1t "$BACKUP_DIR"/jobops_*.sql.gz 2>/dev/null | tail -n +15 | while read -r old; do
    rm -f "$old"
    echo "[backup] pruned $old"
done
