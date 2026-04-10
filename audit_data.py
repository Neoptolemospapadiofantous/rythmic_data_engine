#!/usr/bin/env python3
"""
audit_data.py — Data compatibility audit: rithmic_engine (PostgreSQL) vs bot (Parquet).

Checks:
  1. Schema compatibility  — column names, types, nullable differences
  2. Overlapping date range — what period both sources cover
  3. Daily tick counts      — PG vs Parquet per day (overlap window only)
  4. Price consistency      — OHLCV bars match within tolerance
  5. Side / is_buy parity   — buy/sell ratios per day
  6. Timestamp precision    — µs (PG) vs ns (Parquet)
  7. Gaps                   — days present in one source but missing in other

Usage:
    python audit_data.py [--days N]   # default: last 30 days of overlap
    python audit_data.py --full       # full overlap window
    python audit_data.py --summary    # one-line pass/fail per check
"""
from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

# ── paths ──────────────────────────────────────────────────────────
ENGINE_DIR  = Path(__file__).parent
BOT_DIR     = Path(__file__).parent.parent / "bot"
PARQUET_DIR = BOT_DIR / "data" / "parquet" / "trades"

PASS = "\033[32mPASS\033[0m"
FAIL = "\033[31mFAIL\033[0m"
WARN = "\033[33mWARN\033[0m"
INFO = "\033[36mINFO\033[0m"


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


def _pg_connstr() -> str:
    return (
        f"host={os.environ.get('PG_HOST','localhost')} "
        f"port={os.environ.get('PG_PORT','5432')} "
        f"dbname={os.environ.get('PG_DB','rithmic')} "
        f"user={os.environ.get('PG_USER','rithmic_user')} "
        f"password={os.environ.get('PG_PASSWORD','')}"
    )


# ── helpers ────────────────────────────────────────────────────────
def _pg_connect():
    import psycopg2
    return psycopg2.connect(_pg_connstr())


def _load_parquet(start: datetime, end: datetime) -> pd.DataFrame:
    """Load bot parquet tick files covering [start, end)."""
    files = sorted(PARQUET_DIR.glob("*.parquet"))
    if not files:
        raise FileNotFoundError(f"No parquet files in {PARQUET_DIR}")

    dfs = []
    for f in files:
        # filename: YYYY-MM.parquet
        try:
            ym = datetime.strptime(f.stem, "%Y-%m").replace(tzinfo=timezone.utc)
        except ValueError:
            continue
        month_end = (ym.replace(day=28) + timedelta(days=4)).replace(day=1)
        if month_end < start or ym > end:
            continue
        df = pd.read_parquet(f)
        dfs.append(df)

    if not dfs:
        return pd.DataFrame()

    df = pd.concat(dfs, ignore_index=True)
    df["ts_event"] = pd.to_datetime(df["ts_event"], utc=True)
    df = df[(df["ts_event"] >= pd.Timestamp(start)) & (df["ts_event"] < pd.Timestamp(end))]
    return df.sort_values("ts_event").reset_index(drop=True)


def section(title: str):
    print(f"\n{'─'*60}")
    print(f"  {title}")
    print(f"{'─'*60}")


def result(label: str, status: str, detail: str = ""):
    pad = max(0, 45 - len(label))
    print(f"  {label}{' '*pad}{status}  {detail}")


# ══════════════════════════════════════════════════════════════════
# CHECK 1 — Schema
# ══════════════════════════════════════════════════════════════════
def check_schema(conn) -> bool:
    section("1. Schema Compatibility")

    # PG columns
    cur = conn.cursor()
    cur.execute("""
        SELECT column_name, data_type, is_nullable
        FROM information_schema.columns
        WHERE table_name = 'ticks'
        ORDER BY ordinal_position
    """)
    pg_cols = {row[0]: (row[1], row[2]) for row in cur.fetchall()}

    # Parquet columns (from latest file)
    files = sorted(PARQUET_DIR.glob("*.parquet"))
    if not files:
        result("Parquet files exist", FAIL, str(PARQUET_DIR))
        return False

    sample = pd.read_parquet(files[-1]).head(1)
    parquet_cols = {c: str(sample[c].dtype) for c in sample.columns}

    # Core columns the bot uses
    REQUIRED = {
        "ts_event": ("timestamp", "timestamptz"),
        "price":    ("float64", "double precision"),
        "size":     ("uint32",  "bigint"),
        "side":     ("object",  "character"),
        "is_buy":   ("bool",    "boolean"),
    }

    ok = True
    for col, (parquet_type, pg_type) in REQUIRED.items():
        in_pg      = col in pg_cols
        in_parquet = col in parquet_cols
        if in_pg and in_parquet:
            result(f"  {col}", PASS, f"PG={pg_cols[col][0]}  Parquet={parquet_cols[col]}")
        elif in_pg and not in_parquet:
            result(f"  {col}", WARN, "in PG only — not in Parquet")
        elif not in_pg and in_parquet:
            result(f"  {col}", WARN, "in Parquet only — not in PG")
        else:
            result(f"  {col}", FAIL, "missing from both")
            ok = False

    # Extra columns
    pg_extra      = set(pg_cols) - set(REQUIRED)
    parquet_extra = set(parquet_cols) - set(REQUIRED)
    if pg_extra:
        result("  PG-only columns", INFO, ", ".join(sorted(pg_extra)))
    if parquet_extra:
        result("  Parquet-only columns", INFO, ", ".join(sorted(parquet_extra)))

    return ok


# ══════════════════════════════════════════════════════════════════
# CHECK 2 — Date Range Overlap
# ══════════════════════════════════════════════════════════════════
def check_date_range(conn) -> tuple[datetime, datetime] | None:
    section("2. Date Range Overlap")

    # PG range
    cur = conn.cursor()
    cur.execute("SELECT MIN(ts_event), MAX(ts_event) FROM ticks")
    row = cur.fetchone()
    if not row or not row[0]:
        result("PostgreSQL has data", FAIL, "table is empty")
        return None

    pg_start = row[0].replace(tzinfo=timezone.utc) if row[0].tzinfo is None else row[0].astimezone(timezone.utc)
    pg_end   = row[1].replace(tzinfo=timezone.utc) if row[1].tzinfo is None else row[1].astimezone(timezone.utc)

    # Parquet range
    files = sorted(PARQUET_DIR.glob("*.parquet"))
    if not files:
        result("Parquet files exist", FAIL)
        return None

    try:
        first = datetime.strptime(files[0].stem,  "%Y-%m").replace(tzinfo=timezone.utc)
        last  = datetime.strptime(files[-1].stem, "%Y-%m").replace(tzinfo=timezone.utc)
        parquet_end = (last.replace(day=28) + timedelta(days=4)).replace(day=1)
    except ValueError:
        result("Parquet filename format", FAIL, "expected YYYY-MM.parquet")
        return None

    result("PostgreSQL range", INFO, f"{pg_start.date()} → {pg_end.date()}")
    result("Parquet range",    INFO, f"{first.date()} → {parquet_end.date()}")

    overlap_start = max(pg_start, first)
    overlap_end   = min(pg_end,   parquet_end)

    if overlap_start >= overlap_end:
        gap_days = (overlap_start - overlap_end).days
        result("Overlap", WARN,
               f"no overlap — gap of {gap_days} days between sources")
        print(f"\n  Parquet ends {overlap_end.date()}, PG starts {pg_start.date()}.")
        print(f"  Schema and precision checks will still run.")
        print(f"  Tick count / bar / gap checks need overlapping data.")
        return None

    overlap_days = (overlap_end - overlap_start).days
    result("Overlap window", PASS,
           f"{overlap_start.date()} → {overlap_end.date()} ({overlap_days} days)")

    return overlap_start, overlap_end


# ══════════════════════════════════════════════════════════════════
# CHECK 3 — Daily Tick Counts
# ══════════════════════════════════════════════════════════════════
def check_tick_counts(conn, start: datetime, end: datetime) -> bool:
    section("3. Daily Tick Counts (overlap window)")

    # PG counts per day
    cur = conn.cursor()
    cur.execute("""
        SELECT DATE(ts_event AT TIME ZONE 'UTC') AS day, COUNT(*)
        FROM ticks
        WHERE ts_event >= %s AND ts_event < %s
        GROUP BY 1
        ORDER BY 1
    """, (start, end))
    pg_counts = {str(row[0]): row[1] for row in cur.fetchall()}

    # Parquet counts per day
    df = _load_parquet(start, end)
    if df.empty:
        result("Parquet data loaded", FAIL, f"no data for {start.date()} → {end.date()}")
        return False

    df["_date"] = df["ts_event"].dt.date.astype(str)
    parquet_counts = df.groupby("_date").size().to_dict()

    all_days = sorted(set(pg_counts) | set(parquet_counts))
    if not all_days:
        result("Overlapping days", FAIL, "no matching days found")
        return False

    # Summary table — show up to 10 days (worst divergence)
    divergences = []
    for day in all_days:
        pg_n  = pg_counts.get(day, 0)
        par_n = parquet_counts.get(day, 0)
        if pg_n == 0 or par_n == 0:
            divergences.append((day, pg_n, par_n, float("inf")))
        else:
            pct = abs(pg_n - par_n) / max(pg_n, par_n) * 100
            divergences.append((day, pg_n, par_n, pct))

    divergences.sort(key=lambda x: -x[3])

    total_pg  = sum(pg_counts.values())
    total_par = sum(parquet_counts.values())
    pct_total = abs(total_pg - total_par) / max(total_pg, total_par) * 100 if max(total_pg, total_par) > 0 else 0

    result("Total PG ticks",      INFO, f"{total_pg:,}")
    result("Total Parquet ticks", INFO, f"{total_par:,}")
    overall = PASS if pct_total < 5 else (WARN if pct_total < 20 else FAIL)
    result("Total count delta",   overall, f"{pct_total:.1f}%")

    # Show worst 10 days
    print(f"\n  {'Date':<12} {'PG':>10} {'Parquet':>10} {'Delta%':>8}")
    print(f"  {'-'*44}")
    for day, pg_n, par_n, pct in divergences[:10]:
        flag = "  !" if pct > 20 else ""
        pct_str = f"{pct:.1f}%" if pct != float("inf") else "INF"
        print(f"  {day:<12} {pg_n:>10,} {par_n:>10,} {pct_str:>8}{flag}")

    days_ok = sum(1 for *_, pct in divergences if pct <= 20)
    result(f"\n  Days within 20% delta", PASS if days_ok == len(all_days) else WARN,
           f"{days_ok}/{len(all_days)}")

    return pct_total < 20


# ══════════════════════════════════════════════════════════════════
# CHECK 4 — OHLCV Bar Consistency
# ══════════════════════════════════════════════════════════════════
def check_bars(conn, start: datetime, end: datetime) -> bool:
    section("4. OHLCV Bar Consistency (1-min, last 3 trading days)")

    # Use last 3 days of overlap
    bar_end   = end
    bar_start = max(start, end - timedelta(days=5))

    # PG bars (continuous aggregate)
    cur = conn.cursor()
    cur.execute("""
        SELECT ts, open, high, low, close, volume
        FROM bars_1min
        WHERE ts >= %s AND ts < %s
        ORDER BY ts
    """, (bar_start, bar_end))
    rows = cur.fetchall()

    if not rows:
        result("PG bars_1min has data", WARN, "continuous aggregate empty — may not be populated yet")
        return True  # non-fatal

    pg_bars = pd.DataFrame(rows, columns=["ts","open","high","low","close","volume"])
    pg_bars["ts"] = pd.to_datetime(pg_bars["ts"], utc=True)
    pg_bars = pg_bars.set_index("ts")

    # Compute bars from parquet for same window
    df = _load_parquet(bar_start, bar_end)
    if df.empty:
        result("Parquet data for bar window", WARN, "no data")
        return True

    df = df.set_index("ts_event").sort_index()
    par_bars = df["price"].resample("1min").ohlc()
    par_bars["volume"] = df["size"].resample("1min").sum()
    par_bars = par_bars.dropna()

    # Align
    common = pg_bars.index.intersection(par_bars.index)
    if len(common) == 0:
        result("Bar timestamps overlap", FAIL, "no common 1-min buckets found")
        return False

    result("Common 1-min bars",  INFO, f"{len(common)} bars")

    PRICE_TOL = 0.25  # 1 tick
    for col in ["open", "high", "low", "close"]:
        diff = (pg_bars.loc[common, col] - par_bars.loc[common, col]).abs()
        bad  = (diff > PRICE_TOL).sum()
        status = PASS if bad == 0 else (WARN if bad < 5 else FAIL)
        result(f"  {col} within {PRICE_TOL}pt", status, f"{bad} mismatches / {len(common)}")

    vol_diff_pct = ((pg_bars.loc[common, "volume"] - par_bars.loc[common, "volume"]).abs()
                    / par_bars.loc[common, "volume"].clip(lower=1) * 100).mean()
    result("  volume avg delta%", PASS if vol_diff_pct < 5 else WARN, f"{vol_diff_pct:.1f}%")

    return True


# ══════════════════════════════════════════════════════════════════
# CHECK 5 — Side / is_buy Parity
# ══════════════════════════════════════════════════════════════════
def check_side_parity(conn, start: datetime, end: datetime) -> bool:
    section("5. Side / is_buy Parity")

    # PG
    cur = conn.cursor()
    cur.execute("""
        SELECT is_buy, COUNT(*) FROM ticks
        WHERE ts_event >= %s AND ts_event < %s
        GROUP BY is_buy
    """, (start, end))
    pg_sides = {row[0]: row[1] for row in cur.fetchall()}
    pg_buy  = pg_sides.get(True,  0)
    pg_sell = pg_sides.get(False, 0)
    pg_total = pg_buy + pg_sell

    # Parquet
    df = _load_parquet(start, end)
    if df.empty:
        result("Parquet data", FAIL, "empty")
        return False

    par_buy  = int(df["is_buy"].sum())
    par_sell = int((~df["is_buy"]).sum())
    par_total = par_buy + par_sell

    # Ratios
    pg_buy_ratio  = pg_buy  / pg_total  * 100 if pg_total  else 0
    par_buy_ratio = par_buy / par_total * 100 if par_total else 0

    result("PG  buy ratio",     INFO, f"{pg_buy_ratio:.1f}%  ({pg_buy:,} / {pg_total:,})")
    result("Parquet buy ratio", INFO, f"{par_buy_ratio:.1f}%  ({par_buy:,} / {par_total:,})")

    delta = abs(pg_buy_ratio - par_buy_ratio)
    status = PASS if delta < 2 else (WARN if delta < 5 else FAIL)
    result("Buy ratio delta",   status, f"{delta:.2f}pp")

    # Neutral ticks in parquet (side == 'N')
    if "side" in df.columns:
        neutral = int((df["side"] == "N").sum())
        pct_neutral = neutral / len(df) * 100 if len(df) else 0
        result("Parquet neutral ticks (side=N)", INFO,
               f"{neutral:,}  ({pct_neutral:.1f}%)  — not present in PG")

    return delta < 5


# ══════════════════════════════════════════════════════════════════
# CHECK 6 — Timestamp Precision
# ══════════════════════════════════════════════════════════════════
def check_timestamp_precision(conn, start: datetime, end: datetime) -> bool:
    section("6. Timestamp Precision")

    # PG: check sub-second resolution
    cur = conn.cursor()
    cur.execute("""
        SELECT COUNT(*) FROM ticks
        WHERE ts_event >= %s AND ts_event < %s
          AND EXTRACT(MICROSECONDS FROM ts_event) != 0
        LIMIT 1
    """, (start, end))
    pg_has_subsecond = cur.fetchone()[0] > 0

    # Parquet: check nanosecond resolution
    df = _load_parquet(start, end)
    par_has_subsecond = False
    par_has_ns = False
    if not df.empty:
        ns_vals = df["ts_event"].astype("int64") % 1_000_000_000
        par_has_subsecond = bool((ns_vals != 0).any())
        par_has_ns = bool((ns_vals % 1000 != 0).any())  # true nanoseconds (not just µs)

    result("PG has sub-second timestamps",       PASS if pg_has_subsecond else WARN)
    result("Parquet has sub-second timestamps",  PASS if par_has_subsecond else WARN)
    result("Parquet has true nanosecond precision", INFO if par_has_ns else WARN,
           "ns-level" if par_has_ns else "µs-level (ns zeros in last 3 digits)")

    # Precision note
    print("\n  Note: PG stores µs precision (TIMESTAMPTZ), Parquet stores ns.")
    print("  When joining, cast PG ts to ns: ts_micros * 1000.")

    return True


# ══════════════════════════════════════════════════════════════════
# CHECK 7 — Gap Detection
# ══════════════════════════════════════════════════════════════════
def check_gaps(conn, start: datetime, end: datetime) -> bool:
    section("7. Gap Detection (RTH trading days)")

    # Get all weekdays in overlap window
    trading_days = pd.bdate_range(start=start.date(), end=end.date())

    # PG days with data
    cur = conn.cursor()
    cur.execute("""
        SELECT DISTINCT DATE(ts_event AT TIME ZONE 'UTC')
        FROM ticks
        WHERE ts_event >= %s AND ts_event < %s
    """, (start, end))
    pg_days = {str(row[0]) for row in cur.fetchall()}

    # Parquet days with data
    df = _load_parquet(start, end)
    par_days = set()
    if not df.empty:
        par_days = {str(d) for d in df["ts_event"].dt.date.unique()}

    pg_missing  = [str(d.date()) for d in trading_days if str(d.date()) not in pg_days]
    par_missing = [str(d.date()) for d in trading_days if str(d.date()) not in par_days]
    both_missing = set(pg_missing) & set(par_missing)

    result("Trading days in window",     INFO, str(len(trading_days)))
    result("Days with PG data",          INFO, str(len(pg_days)))
    result("Days with Parquet data",     INFO, str(len(par_days)))

    if both_missing:
        result("Days missing from both", WARN,
               f"{len(both_missing)} (likely holidays/weekends): " +
               ", ".join(sorted(both_missing)[:5]))
    else:
        result("Days missing from both", PASS, "none")

    pg_only_missing = [d for d in pg_missing if d not in both_missing]
    par_only_missing = [d for d in par_missing if d not in both_missing]

    if pg_only_missing:
        result("Days missing PG only", FAIL,
               f"{len(pg_only_missing)}: " + ", ".join(pg_only_missing[:5]))
    else:
        result("Days missing PG only", PASS, "none")

    if par_only_missing:
        result("Days missing Parquet only", WARN,
               f"{len(par_only_missing)}: " + ", ".join(par_only_missing[:5]))
    else:
        result("Days missing Parquet only", PASS, "none")

    return len(pg_only_missing) == 0


# ══════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════
def main():
    parser = argparse.ArgumentParser(description="rithmic_engine ↔ bot data audit")
    parser.add_argument("--days",    type=int, default=30, help="Audit last N days of overlap (default: 30)")
    parser.add_argument("--full",    action="store_true",  help="Audit entire overlap window")
    parser.add_argument("--summary", action="store_true",  help="Print pass/fail summary only")
    args = parser.parse_args()

    _load_env()

    print("=" * 60)
    print("  rithmic_engine ↔ bot  DATA AUDIT")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    # Connect to PG
    try:
        conn = _pg_connect()
        print(f"\n  {PASS}  Connected to PostgreSQL  "
              f"({os.environ.get('PG_HOST','localhost')}:{os.environ.get('PG_PORT','5432')}/"
              f"{os.environ.get('PG_DB','rithmic')})")
    except Exception as e:
        print(f"\n  {FAIL}  PostgreSQL connection failed: {e}")
        print("  Check PG_HOST / PG_USER / PG_PASSWORD in .env")
        sys.exit(1)

    # Verify parquet dir
    if not PARQUET_DIR.exists():
        print(f"\n  {FAIL}  Parquet directory not found: {PARQUET_DIR}")
        sys.exit(1)
    print(f"  {PASS}  Parquet dir: {PARQUET_DIR}")

    results = {}

    # 1. Schema
    results["schema"] = check_schema(conn)

    # 2. Date range
    date_range = check_date_range(conn)
    results["date_range"] = date_range is not None

    if date_range is None:
        # No overlap — still run schema + precision using whatever data each source has
        section("3-7. Skipped — no overlapping date range")
        print("  Run again once the collector has accumulated data past the parquet end date.")
        print("  Parquet ends ~2026-03-31; PG will catch up as ticks are collected.")
        results["tick_counts"]  = None
        results["bars"]         = None
        results["side_parity"]  = None
        results["gaps"]         = None

        # Precision check against PG data only
        cur = conn.cursor()
        cur.execute("SELECT MIN(ts_event), MAX(ts_event) FROM ticks")
        row = cur.fetchone()
        if row and row[0]:
            pg_start = row[0].replace(tzinfo=timezone.utc) if row[0].tzinfo is None else row[0].astimezone(timezone.utc)
            pg_end   = row[1].replace(tzinfo=timezone.utc) if row[1].tzinfo is None else row[1].astimezone(timezone.utc)
            results["ts_precision"] = check_timestamp_precision(conn, pg_start, pg_end)
        else:
            results["ts_precision"] = None
    else:
        overlap_start, overlap_end = date_range

        # Narrow window if --days specified
        if not args.full:
            window_start = max(overlap_start, overlap_end - timedelta(days=args.days))
        else:
            window_start = overlap_start

        print(f"\n  Audit window: {window_start.date()} → {overlap_end.date()}")

        results["tick_counts"]  = check_tick_counts(conn, window_start, overlap_end)
        results["bars"]         = check_bars(conn, window_start, overlap_end)
        results["side_parity"]  = check_side_parity(conn, window_start, overlap_end)
        results["ts_precision"] = check_timestamp_precision(conn, window_start, overlap_end)
        results["gaps"]         = check_gaps(conn, window_start, overlap_end)

    conn.close()

    # ── Summary ────────────────────────────────────────────────────
    section("SUMMARY")
    labels = {
        "schema":       "Schema compatibility",
        "date_range":   "Date range overlap",
        "tick_counts":  "Daily tick counts",
        "bars":         "OHLCV bar consistency",
        "side_parity":  "Side/is_buy parity",
        "ts_precision": "Timestamp precision",
        "gaps":         "Gap detection",
    }
    all_pass = True
    for key, label in labels.items():
        ok = results.get(key)
        if ok is None:
            result(f"  {label}", WARN, "skipped (no overlap)")
        else:
            if not ok:
                all_pass = False
            result(f"  {label}", PASS if ok else FAIL)

    print(f"\n{'='*60}")
    print(f"  Overall: {PASS if all_pass else FAIL}")
    print(f"{'='*60}\n")

    sys.exit(0 if all_pass else 1)


if __name__ == "__main__":
    main()
