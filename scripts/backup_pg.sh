#!/usr/bin/env bash
# backup_pg.sh — pg_dump ticks table to a compressed local backup
# Usage: ./scripts/backup_pg.sh [output_dir]
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ENV_FILE="$SCRIPT_DIR/../.env"

# Load env
set -o allexport
[[ -f "$ENV_FILE" ]] && source "$ENV_FILE"
set +o allexport

OUT_DIR="${1:-$SCRIPT_DIR/../data/backups}"
mkdir -p "$OUT_DIR"

STAMP=$(date +%Y%m%d_%H%M)
OUTFILE="$OUT_DIR/ticks_$STAMP.sql.gz"

echo "=== rithmic_engine PostgreSQL backup ==="
echo "  Host:   ${PG_HOST}:${PG_PORT}"
echo "  DB:     ${PG_DB}"
echo "  Output: $OUTFILE"

# Count rows before dump
ROW_COUNT=$(psql "postgresql://${PG_USER}:${PG_PASSWORD}@${PG_HOST}:${PG_PORT}/${PG_DB}" \
    -t -c "SELECT COUNT(*) FROM ticks" 2>/dev/null | tr -d ' ')
echo "  Rows:   ${ROW_COUNT}"

PGPASSWORD="$PG_PASSWORD" pg_dump \
    -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -d "$PG_DB" \
    -t ticks --data-only --column-inserts \
    | gzip -9 > "$OUTFILE"

SIZE=$(du -sh "$OUTFILE" | cut -f1)
echo "  Done:   $OUTFILE  ($SIZE)"
echo "  Rows backed up: $ROW_COUNT"
