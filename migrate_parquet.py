#!/usr/bin/env python3
"""
migrate_parquet.py — Bulk-load bot parquet tick data into rithmic_engine PostgreSQL.

Reads every YYYY-MM.parquet from the bot's data/parquet/trades/ directory and
loads it into the ticks hypertable using PostgreSQL COPY (fastest bulk method).

Usage:
    python migrate_parquet.py              # migrate all files
    python migrate_parquet.py --dry-run    # show what would be loaded
    python migrate_parquet.py --from 2025-01  # start from a specific month
    python migrate_parquet.py --month 2025-06 # single month only

Progress is saved after each file — safe to interrupt and resume.
Already-loaded months are skipped (idempotent).
"""
from __future__ import annotations

import argparse
import io
import json
import os
import sys
import time
from pathlib import Path

import pandas as pd
import psycopg2
import psycopg2.extras

# ── paths ──────────────────────────────────────────────────────────
ENGINE_DIR   = Path(__file__).parent
BOT_DIR      = ENGINE_DIR.parent / "bot"
PARQUET_DIR  = BOT_DIR / "data" / "parquet" / "trades"
PROGRESS_FILE = ENGINE_DIR / "data" / "migrate_progress.json"

BATCH_SIZE = 50_000  # rows per COPY batch


# ── env ────────────────────────────────────────────────────────────
def _load_env():
    env = ENGINE_DIR / ".env"
    if env.exists():
        for line in env.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                if k.strip() not in os.environ:
                    os.environ[k.strip()] = v.strip()


def _connect():
    return psycopg2.connect(
        host=os.environ.get("PG_HOST", "127.0.0.1"),
        port=int(os.environ.get("PG_PORT", "5432")),
        dbname=os.environ.get("PG_DB", "rithmic"),
        user=os.environ.get("PG_USER", "rithmic_user"),
        password=os.environ.get("PG_PASSWORD", ""),
    )


# ── progress ───────────────────────────────────────────────────────
def _load_progress() -> dict:
    if PROGRESS_FILE.exists():
        try:
            return json.loads(PROGRESS_FILE.read_text())
        except Exception:
            pass
    return {"completed": [], "total_inserted": 0}


def _save_progress(p: dict):
    PROGRESS_FILE.parent.mkdir(parents=True, exist_ok=True)
    tmp = PROGRESS_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(p, indent=2))
    os.replace(tmp, PROGRESS_FILE)


# ── fast COPY loader (no index during load) ────────────────────────
def _build_copy_buf(df: "pd.DataFrame") -> "io.StringIO":
    """Render a DataFrame slice to tab-separated text for COPY FROM STDIN."""
    buf = io.StringIO()
    for ts, px, sz, sd, ib in zip(
        df["ts_event"], df["price"], df["size"], df["side"], df["is_buy"],
    ):
        ts_us  = ts.floor("us").isoformat()
        sd_val = sd if pd.notna(sd) and sd in ("B", "A") else "\\N"
        ib_val = "t" if ib else "f"
        buf.write(f"{ts_us}\tNQ\tCME\t{float(px)}\t{int(sz)}\t{sd_val}\t{ib_val}\tdatabento\n")
    buf.seek(0)
    return buf


def _prep_df(path: Path) -> "pd.DataFrame | None":
    """Read and normalize a parquet file. Returns None if unusable."""
    df = pd.read_parquet(path)
    if df.empty:
        return None

    df["ts_event"] = pd.to_datetime(df["ts_event"], utc=True)

    if "aggressor_side" in df.columns:
        if "side" not in df.columns:
            df["side"] = df["aggressor_side"]
        if "is_buy" not in df.columns:
            df["is_buy"] = df["aggressor_side"] == "B"

    for col in ["ts_event", "price", "size", "side", "is_buy"]:
        if col not in df.columns:
            print(f"    WARN: missing column '{col}' — skipping")
            return None

    df["side"]   = df["side"].where(df["side"].isin(["B", "A"]), other=None)
    df["price"]  = df["price"].astype("float64")
    df["size"]   = df["size"].astype("int64")
    df["is_buy"] = df["is_buy"].astype("bool")

    # Deduplicate within the file: two real trades can share the same µs timestamp,
    # price and size (nanosecond precision floored to µs by TIMESTAMPTZ).
    # Keep the first occurrence — consistent with ON CONFLICT DO NOTHING behaviour.
    before = len(df)
    df["_ts_us"] = df["ts_event"].dt.floor("us")
    df = df.drop_duplicates(subset=["_ts_us", "price", "size"])
    df = df.drop(columns=["_ts_us"])
    dropped = before - len(df)
    if dropped:
        print(f"    dedup: dropped {dropped:,} within-file duplicates", flush=True)

    return df


def _load_file_fast(conn, path: Path) -> int:
    """Fast-path: direct COPY into ticks (no unique index, no dedup per batch).

    Called by main() after the unique index is dropped.  The index is rebuilt
    once at the end of all files.  This is 10-20× faster than the staging-table
    approach when loading a fresh table.
    """
    df = _prep_df(path)
    if df is None:
        return 0

    cur = conn.cursor()
    for i in range(0, len(df), BATCH_SIZE):
        chunk = df.iloc[i : i + BATCH_SIZE]
        buf   = _build_copy_buf(chunk)
        cur.copy_expert(
            "COPY ticks (ts_event,symbol,exchange,price,size,side,is_buy,source) "
            "FROM STDIN WITH (FORMAT text, NULL '\\N')",
            buf,
        )
    conn.commit()
    cur.close()
    return len(df)


# ── safe loader (with dedup via staging table) ─────────────────────
def _load_file(conn, path: Path, dry_run: bool = False) -> int:
    """Safe-path: stage then INSERT…ON CONFLICT DO NOTHING.

    Used when the unique index must stay live (incremental updates).
    """
    df = _prep_df(path)
    if df is None:
        return 0

    if dry_run:
        return len(df)

    total_inserted = 0
    cur = conn.cursor()

    cur.execute("DROP TABLE IF EXISTS _migrate_stage")
    cur.execute("""
        CREATE TEMP TABLE _migrate_stage (
            ts_event  TIMESTAMPTZ NOT NULL,
            symbol    VARCHAR(32),
            exchange  VARCHAR(32),
            price     DOUBLE PRECISION,
            size      BIGINT,
            side      CHAR(1),
            is_buy    BOOLEAN,
            source    VARCHAR(32)
        )
    """)
    conn.commit()

    for i in range(0, len(df), BATCH_SIZE):
        chunk = df.iloc[i : i + BATCH_SIZE]
        buf   = _build_copy_buf(chunk)
        cur.copy_expert(
            "COPY _migrate_stage (ts_event,symbol,exchange,price,size,side,is_buy,source) "
            "FROM STDIN WITH (FORMAT text, NULL '\\N')",
            buf,
        )
        cur.execute("""
            INSERT INTO ticks (ts_event, symbol, exchange, price, size, side, is_buy, source)
            SELECT ts_event, symbol, exchange, price, size, side, is_buy, source
            FROM _migrate_stage
            ON CONFLICT (symbol, exchange, ts_event, price, size) DO NOTHING
        """)
        inserted = cur.rowcount
        total_inserted += inserted if inserted >= 0 else len(chunk)
        cur.execute("TRUNCATE _migrate_stage")
        conn.commit()

    cur.close()
    return total_inserted


# ── main ───────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Migrate parquet ticks → PostgreSQL")
    parser.add_argument("--dry-run",    action="store_true", help="Show what would be loaded, don't write")
    parser.add_argument("--from",       dest="from_month", default=None, help="Start from YYYY-MM (inclusive)")
    parser.add_argument("--month",      default=None, help="Load a single month YYYY-MM")
    parser.add_argument("--reset",      action="store_true", help="Reset progress (re-load all files)")
    parser.add_argument("--fast",       action="store_true",
                        help="Fast bulk-load: drop unique index, COPY directly, rebuild index at end "
                             "(10-20× faster; use when loading a fresh table from scratch)")
    parser.add_argument("--parquet-dir", default=None, help="Override parquet trades directory path")
    args = parser.parse_args()

    _load_env()

    parquet_dir = Path(args.parquet_dir).expanduser() if args.parquet_dir else PARQUET_DIR
    files = sorted(parquet_dir.glob("*.parquet"))
    if not files:
        print(f"ERROR: no parquet files in {PARQUET_DIR}")
        sys.exit(1)

    # Filter files
    if args.month:
        files = [f for f in files if f.stem == args.month]
        if not files:
            print(f"ERROR: {args.month}.parquet not found")
            sys.exit(1)
    elif args.from_month:
        files = [f for f in files if f.stem >= args.from_month]

    # Load progress
    progress = {"completed": [], "total_inserted": 0} if args.reset else _load_progress()

    # Connect
    try:
        conn = _connect()
        print(f"Connected to PostgreSQL  "
              f"{os.environ.get('PG_HOST')}:{os.environ.get('PG_PORT','5432')}/"
              f"{os.environ.get('PG_DB','rithmic')}")
    except Exception as e:
        print(f"ERROR: PostgreSQL connection failed: {e}")
        sys.exit(1)

    # Schema bootstrap — recreate ticks if missing (e.g. after TimescaleDB chunk loss)
    cur = conn.cursor()
    cur.execute("""
        SELECT COUNT(*) FROM information_schema.tables
        WHERE table_schema='public' AND table_name='ticks'
    """)
    ticks_exists = cur.fetchone()[0] > 0
    if not ticks_exists:
        print("WARN: ticks table missing — recreating schema...")
        cur.execute("""
            CREATE TABLE ticks (
                ts_event  TIMESTAMPTZ      NOT NULL,
                symbol    VARCHAR(32)      NOT NULL DEFAULT 'NQ',
                exchange  VARCHAR(32)      NOT NULL DEFAULT 'CME',
                price     DOUBLE PRECISION NOT NULL,
                size      BIGINT           NOT NULL,
                side      CHAR(1),
                is_buy    BOOLEAN,
                source    VARCHAR(32)      DEFAULT 'amp_rithmic'
            )
        """)
        cur.execute(
            "SELECT create_hypertable('ticks','ts_event',"
            " if_not_exists => TRUE, migrate_data => TRUE)"
        )
        cur.execute("""
            CREATE UNIQUE INDEX idx_ticks_unique
                ON ticks(symbol, exchange, ts_event, price, size)
        """)
        conn.commit()
        print("  ticks table + hypertable + unique index created")
        # Reset progress since DB was wiped
        if not args.reset:
            print("  Progress reset (DB was wiped — must re-load all months)")
            progress = {"completed": [], "total_inserted": 0}
    cur.close()

    # Check existing tick count
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM ticks")
    existing = cur.fetchone()[0]
    cur.close()
    print(f"Existing ticks in DB: {existing:,}")
    print(f"Files to process: {len(files)}")
    if args.dry_run:
        print("DRY RUN — nothing will be written\n")

    # Fast-path setup: drop unique index so COPY doesn't pay per-row index cost
    use_fast = args.fast and not args.dry_run
    if use_fast:
        print("FAST mode: dropping idx_ticks_unique for bulk load (will rebuild at end)")
        cur = conn.cursor()
        cur.execute("DROP INDEX IF EXISTS idx_ticks_unique")
        conn.commit()
        cur.close()

    # Process files
    grand_total = 0
    t_start = time.monotonic()

    for i, f in enumerate(files, 1):
        month = f.stem

        if not args.dry_run and month in progress["completed"]:
            print(f"  [{i:02d}/{len(files)}] {month}  SKIP (already loaded)")
            continue

        size_mb = f.stat().st_size / 1024**2
        print(f"  [{i:02d}/{len(files)}] {month}  ({size_mb:.1f} MB)  ", end="", flush=True)

        t0 = time.monotonic()
        try:
            if use_fast:
                n = _load_file_fast(conn, f)
            else:
                n = _load_file(conn, f, dry_run=args.dry_run)
        except Exception as e:
            print(f"ERROR: {e}")
            conn.rollback()
            continue

        elapsed = time.monotonic() - t0
        rate = n / elapsed if elapsed > 0 else 0
        print(f"{n:>10,} rows  {elapsed:5.1f}s  ({rate:,.0f} rows/s)")

        grand_total += n
        if not args.dry_run:
            progress["completed"].append(month)
            progress["total_inserted"] += n
            _save_progress(progress)

    # Fast-path teardown: rebuild unique index.
    # Within-file duplicates are removed by _prep_df (pandas drop_duplicates).
    # Cross-file duplicates are extremely rare for Databento monthly files, but if
    # CREATE INDEX fails we emit a targeted fix script rather than a slow self-join.
    if use_fast:
        print("Rebuilding idx_ticks_unique (this may take a few minutes)...")
        t_idx = time.monotonic()
        cur = conn.cursor()
        try:
            cur.execute("""
                CREATE UNIQUE INDEX IF NOT EXISTS idx_ticks_unique
                    ON ticks(symbol, exchange, ts_event, price, size)
            """)
            conn.commit()
            print(f"  Index built in {time.monotonic() - t_idx:.1f}s")
        except Exception as e:
            conn.rollback()
            print(f"  WARN: index creation failed — cross-file duplicates exist: {e}")
            print("  Run this SQL to remove them, then re-run the index command:")
            print("""
    WITH ranked AS (
        SELECT ctid,
               ROW_NUMBER() OVER (
                   PARTITION BY symbol, exchange, ts_event, price, size
                   ORDER BY ctid
               ) AS rn
        FROM ticks
    )
    DELETE FROM ticks WHERE ctid IN (SELECT ctid FROM ranked WHERE rn > 1);
    CREATE UNIQUE INDEX IF NOT EXISTS idx_ticks_unique
        ON ticks(symbol, exchange, ts_event, price, size);
            """)
        cur.close()

    conn.close()

    total_elapsed = time.monotonic() - t_start
    print(f"\n{'─'*55}")
    if args.dry_run:
        print(f"  DRY RUN — would insert {grand_total:,} rows from {len(files)} files")
    else:
        print(f"  Done: {grand_total:,} rows inserted in {total_elapsed:.0f}s")
        print(f"  Progress saved to {PROGRESS_FILE}")
    print(f"{'─'*55}")


if __name__ == "__main__":
    main()
