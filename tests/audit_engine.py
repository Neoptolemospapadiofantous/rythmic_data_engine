#!/usr/bin/env python3
"""
audit_engine.py — Comprehensive build + source + schema + data audit for rithmic_engine.

Sections:
  1. Build integrity     — binaries exist and are executable
  2. Source invariants   — grep checks (no compile needed)
  3. Proto integrity     — BBO/Depth message fields present
  4. PostgreSQL schema   — tables, columns, indexes
  5. Data health         — row counts, WAL state (requires PG)
  6. Config validation   — .env completeness
  7. Summary

Usage:
    python tests/audit_engine.py
    python tests/audit_engine.py --summary
    python tests/audit_engine.py --no-pg
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

# ── paths ──────────────────────────────────────────────────────────
ENGINE_DIR = Path(__file__).parent.parent
BUILD_DIR  = ENGINE_DIR / "build"
SRC_DIR    = ENGINE_DIR / "src"
PROTO_FILE = ENGINE_DIR / "proto" / "rithmic.proto"
ENV_FILE   = ENGINE_DIR / ".env"
WAL_FILE   = ENGINE_DIR / "data" / "wal.bin"

PASS = "\033[32mPASS\033[0m"
FAIL = "\033[31mFAIL\033[0m"
WARN = "\033[33mWARN\033[0m"
INFO = "\033[36mINFO\033[0m"


# ── env / pg helpers ───────────────────────────────────────────────
def _load_env():
    if ENV_FILE.exists():
        for line in ENV_FILE.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                if k.strip() not in os.environ:
                    os.environ[k.strip()] = v.strip()


def _pg_connect():
    import psycopg2
    return psycopg2.connect(
        host=os.environ.get("PG_HOST", "localhost"),
        port=int(os.environ.get("PG_PORT", "5432")),
        dbname=os.environ.get("PG_DB", "rithmic"),
        user=os.environ.get("PG_USER", "rithmic_user"),
        password=os.environ.get("PG_PASSWORD", ""),
        connect_timeout=5,
    )


# ── display helpers ────────────────────────────────────────────────
def section(title: str):
    print(f"\n{'─'*60}")
    print(f"  {title}")
    print(f"{'─'*60}")


def result(label: str, status: str, detail: str = ""):
    pad = max(0, 45 - len(label))
    print(f"  {label}{' ' * pad}{status}  {detail}")


# ── section 1: build integrity ─────────────────────────────────────
def check_build() -> bool:
    section("1. Build Integrity")
    ok = True

    binaries = [
        ("build/rithmic_engine",   True),
        ("build/test_db",          True),
        ("build/test_validator",   True),
    ]

    for rel, required in binaries:
        p = ENGINE_DIR / rel
        exists = p.exists()
        is_exe = exists and os.access(p, os.X_OK)
        if exists and is_exe:
            result(f"  {rel} exists + executable", PASS)
        elif exists:
            result(f"  {rel} exists but not executable", WARN if not required else FAIL)
            if required:
                ok = False
        else:
            result(f"  {rel} exists", FAIL if required else WARN, "not found")
            if required:
                ok = False

    # Proto generated artifact (path depends on cmake build dir layout)
    proto_candidates = [
        BUILD_DIR / "proto" / "rithmic.pb.cc",
        BUILD_DIR / "rithmic.pb.cc",
    ]
    proto_found = any(p.exists() for p in proto_candidates)
    result("  proto/rithmic.pb.cc generated", PASS if proto_found else WARN,
           "" if proto_found else "run cmake + make first")

    return ok


# ── section 2: source invariants ──────────────────────────────────
def _grep(path: Path, pattern: str) -> bool:
    """Return True if pattern appears in the file (simple substring match)."""
    try:
        return pattern in path.read_text()
    except OSError:
        return False


def check_source_invariants() -> bool:
    section("2. Source Code Invariants")
    ok = True

    db_cpp    = SRC_DIR / "db.cpp"
    client_cpp = SRC_DIR / "client.cpp"
    collector_cpp = SRC_DIR / "collector.cpp"
    migrate_py = ENGINE_DIR / "migrate_parquet.py"

    checks = [
        # (label, file, pattern, required)
        ("UNIQUE idx includes 'price'",
         db_cpp, "price, size", True),
        ("ON CONFLICT in write() uses (symbol,exchange,ts_event,price,size)",
         db_cpp, "ON CONFLICT (symbol, exchange, ts_event, price, size)", True),
        ("write_bbo() exists in db.cpp",
         db_cpp, "write_bbo", True),
        ("write_depth() exists in db.cpp",
         db_cpp, "write_depth", True),
        ("Template 151 handler (BBO dispatch) in client.cpp",
         client_cpp, "151", True),
        ("Template 160 handler (Depth dispatch) in client.cpp",
         client_cpp, "160", True),
        ("on_bbo_ callback in collector.cpp",
         collector_cpp, "on_bbo", True),
        ("on_depth_ callback in collector.cpp",
         collector_cpp, "on_depth", True),
        ("update_bits includes BBO (1 | 2) in client.cpp",
         client_cpp, "1 | 2", True),
        ("DataSentinel class in validator.hpp",
         SRC_DIR / "validator.hpp", "class DataSentinel", True),
        ("sentinel_->observe_tick() in collector.cpp",
         collector_cpp, "sentinel_->observe_tick", True),
        ("sentinel_->observe_bbo() in collector.cpp",
         collector_cpp, "sentinel_->observe_bbo", True),
        ("session tracking (start_session) in collector.cpp",
         collector_cpp, "start_session", True),
        ("quality_metrics table in db.cpp",
         db_cpp, "quality_metrics", True),
        ("sessions table in db.cpp",
         db_cpp, "CREATE TABLE IF NOT EXISTS sessions", True),
        ("loss_limits table in db.cpp",
         db_cpp, "loss_limits", True),
        ("gate_results table in db.cpp",
         db_cpp, "gate_results", True),
    ]

    for label, fpath, pattern, required in checks:
        if not fpath.exists():
            result(f"  {label}", WARN, f"{fpath.name} not found")
            continue
        found = _grep(fpath, pattern)
        status = PASS if found else (FAIL if required else WARN)
        result(f"  {label}", status)
        if not found and required:
            ok = False

    # migrate_parquet.py ON CONFLICT check (non-fatal — file may not exist)
    if migrate_py.exists():
        found = _grep(migrate_py, "(symbol, exchange, ts_event, price, size)")
        result("  migrate_parquet.py ON CONFLICT (5-col)",
               PASS if found else WARN,
               "" if found else "old narrow ON CONFLICT")
    else:
        result("  migrate_parquet.py ON CONFLICT (5-col)", INFO, "file not present")

    return ok


# ── section 3: proto integrity ─────────────────────────────────────
def check_proto() -> bool:
    section("3. Proto Integrity")
    ok = True

    if not PROTO_FILE.exists():
        result("  rithmic.proto exists", FAIL, str(PROTO_FILE))
        return False

    proto_text = PROTO_FILE.read_text()

    checks = [
        ("BestBidOffer message defined",       "BestBidOffer",     True),
        ("DepthByOrder message defined",        "DepthByOrder",     True),
        ("update_type field in DepthByOrder",   "update_type",      True),
        ("transaction_type field",              "transaction_type", True),
        ("source_nsecs field",                  "source_nsecs",     True),
    ]

    for label, pattern, required in checks:
        found = pattern in proto_text
        status = PASS if found else (FAIL if required else WARN)
        result(f"  {label}", status)
        if not found and required:
            ok = False

    return ok


# ── section 4: postgresql schema ──────────────────────────────────
def check_schema(conn) -> bool:
    section("4. PostgreSQL Schema")
    ok = True
    cur = conn.cursor()

    def col_exists(table: str, col: str) -> bool:
        cur.execute(
            "SELECT COUNT(*) FROM information_schema.columns"
            " WHERE table_name=%s AND column_name=%s",
            (table, col),
        )
        return cur.fetchone()[0] > 0

    def table_exists(table: str) -> bool:
        cur.execute(
            "SELECT COUNT(*) FROM information_schema.columns WHERE table_name=%s",
            (table,),
        )
        return cur.fetchone()[0] > 0

    # Tables
    table_checks = [
        ("ticks",            True),
        ("bbo",              True),
        ("depth_by_order",   True),
        ("audit_log",        True),
        ("quality_metrics",  True),
        ("sessions",         True),
        ("sentinel_alerts",  True),
        ("loss_limits",      True),
        ("trade_log",        True),
        ("daily_stats",      True),
        ("orders",           True),
        ("gate_results",     True),
        ("bars_1min",        False),  # TimescaleDB continuous aggregate — may not exist
    ]
    for tbl, required in table_checks:
        exists = table_exists(tbl)
        status = PASS if exists else (FAIL if required else WARN)
        result(f"  Table '{tbl}' exists", status,
               "" if exists else ("required" if required else "TimescaleDB not loaded?"))
        if not exists and required:
            ok = False

    # Column checks
    col_checks = [
        ("ticks",            ["ts_event", "price", "size", "side", "is_buy"]),
        ("bbo",              ["ts_event", "bid_price", "bid_size", "ask_price", "ask_size"]),
        ("depth_by_order",   ["ts_event", "source_ns", "update_type", "depth_price"]),
        ("quality_metrics",  ["ts", "metric", "value", "labels_json"]),
        ("sessions",         ["started_at", "tick_count", "rejected_count", "gap_count",
                              "strategy", "total_pnl", "sharpe", "max_drawdown", "profit_factor"]),
        ("sentinel_alerts",  ["ts", "check_name", "severity", "message", "value"]),
        ("loss_limits",      ["symbol", "daily_loss_limit", "weekly_loss_limit", "active"]),
        ("trade_log",        ["entry_time", "exit_time", "net_pnl", "exit_reason",
                              "strategy", "mode", "slippage", "points", "ticks", "created_at"]),
        ("daily_stats",      ["stat_date", "total_pnl", "trade_count", "win_rate", "profit_factor"]),
        ("orders",           ["order_type", "side", "status", "fill_price", "broker_order_id"]),
        ("audit_log",        ["source", "event", "severity", "details"]),
        ("gate_results",     ["gate_name", "status", "threshold", "actual_value"]),
    ]
    for tbl, cols in col_checks:
        if not table_exists(tbl):
            continue
        for col in cols:
            exists = col_exists(tbl, col)
            status = PASS if exists else FAIL
            result(f"  {tbl}.{col}", status)
            if not exists:
                ok = False

    # idx_ticks_unique includes price and size (regression guard)
    cur.execute(
        "SELECT indexdef FROM pg_indexes"
        " WHERE tablename='ticks' AND indexname='idx_ticks_unique'"
    )
    row = cur.fetchone()
    if row:
        indexdef = row[0]
        has_price = "price" in indexdef
        has_size  = "size" in indexdef
        status = PASS if (has_price and has_size) else FAIL
        result("  idx_ticks_unique includes price+size", status,
               "" if (has_price and has_size) else f"indexdef: {indexdef}")
        if not (has_price and has_size):
            ok = False
    else:
        result("  idx_ticks_unique exists", FAIL, "index not found")
        ok = False

    # idx_depth_unique on depth_by_order
    cur.execute(
        "SELECT COUNT(*) FROM pg_indexes"
        " WHERE tablename='depth_by_order' AND indexname='idx_depth_unique'"
    )
    count = cur.fetchone()[0]
    result("  idx_depth_unique on depth_by_order",
           PASS if count > 0 else WARN,
           "" if count > 0 else "not yet created (collect some depth data first)")

    # bars_1min has data — non-fatal warning
    try:
        cur.execute("SELECT COUNT(*) FROM bars_1min")
        bar_count = cur.fetchone()[0]
        status = PASS if bar_count > 0 else WARN
        result("  bars_1min has data", status,
               f"{bar_count:,} rows" if bar_count > 0 else "continuous aggregate empty")
    except Exception:
        result("  bars_1min has data", WARN, "table not accessible")

    return ok


# ── section 5: data health ─────────────────────────────────────────
def check_data_health(conn) -> bool:
    section("5. Data Health")
    ok = True
    cur = conn.cursor()

    # ticks — TimescaleDB hypertable: parent table has reltuples=0 (data is in chunks).
    # Use approximate_row_count() which sums chunks, or fall back to EXISTS for non-TimescaleDB.
    try:
        cur.execute("SELECT approximate_row_count('ticks')")
        tick_count = cur.fetchone()[0]
    except Exception:
        # Not TimescaleDB or function unavailable — fall back to pg_class
        conn.rollback()
        cur.execute("SELECT reltuples::bigint FROM pg_class WHERE relname = 'ticks'")
        tick_count = cur.fetchone()[0]

    if tick_count <= 0:
        # Last resort: cheap EXISTS
        cur.execute("SELECT EXISTS (SELECT 1 FROM ticks LIMIT 1)")
        has_data = cur.fetchone()[0]
        if has_data:
            result("  ticks total rows (approx)", WARN,
                   "stats unavailable — table has data (~270M expected); run ANALYZE")
            tick_count = 1  # mark as "has data" for date range check
        else:
            result("  ticks total rows (approx)", FAIL, "table empty")
            ok = False
            tick_count = 0
    else:
        TICKS_WARN_THRESHOLD = 100_000_000
        ticks_status = PASS if tick_count >= TICKS_WARN_THRESHOLD else WARN
        result("  ticks total rows (approx)", ticks_status,
               f"~{tick_count:,}" + (" — below 100M, re-migration may be needed" if ticks_status == WARN else ""))
        ok = ok and ticks_status != FAIL

    if tick_count > 0:
        cur.execute("SELECT MIN(ts_event)::text FROM ticks LIMIT 1")
        tick_min = cur.fetchone()[0]
        cur.execute("SELECT MAX(ts_event)::text FROM ticks WHERE ts_event > now() - interval '30 days'")
        tick_max = cur.fetchone()[0]
        result("  ticks date range", INFO, f"{tick_min or '?'}  →  {tick_max or '?'}")

    # bbo — small table, exact count is fine
    cur.execute("SELECT COUNT(*) FROM bbo")
    bbo_count = cur.fetchone()[0]
    result("  bbo total rows",
           PASS if bbo_count > 0 else WARN,
           f"{bbo_count:,}" if bbo_count > 0 else "0 — BBO collection not started yet")

    # depth_by_order — use estimate too (can grow large)
    cur.execute("SELECT reltuples::bigint FROM pg_class WHERE relname = 'depth_by_order'")
    depth_count = cur.fetchone()[0]
    result("  depth_by_order total rows (approx)",
           PASS if depth_count > 0 else WARN,
           f"~{depth_count:,}" if depth_count > 0 else "0 — depth collection not started yet")

    # audit_log
    cur.execute("SELECT COUNT(*), MAX(ts)::text FROM audit_log")
    row = cur.fetchone()
    audit_count, audit_latest = row
    result("  audit_log entries", INFO, f"{audit_count:,}")
    if audit_latest:
        result("  audit_log most recent", INFO, audit_latest)

    # WAL file check
    wal_candidates = [
        ENGINE_DIR / "ticks.wal",
        ENGINE_DIR / "data" / "wal.bin",
        WAL_FILE,
    ]
    wal_found = None
    for p in wal_candidates:
        if p.exists():
            wal_found = p
            break

    if wal_found:
        size_bytes = wal_found.stat().st_size
        size_kb = size_bytes / 1024
        threshold_bytes = 1024 * 1024  # 1 MB
        status = PASS if size_bytes == 0 else (WARN if size_bytes > threshold_bytes else INFO)
        result(f"  WAL file ({wal_found.name})", status,
               f"{size_kb:.1f} KB" + (" — unflushed data > 1 MB!" if size_bytes > threshold_bytes else ""))
    else:
        result("  WAL file", INFO, "not found (created on first tick batch)")

    return ok


# ── section 6: python data readability ────────────────────────────
def check_python_data(conn) -> bool:
    """Verify Python (pandas) can read ticks from PG and that data is sane."""
    section("6. Python Data Readability")
    ok = True

    try:
        import warnings
        warnings.filterwarnings("ignore", category=UserWarning, module="pandas")
        import pandas as pd
    except ImportError:
        result("  pandas importable", WARN, "pip install pandas — skipping section")
        return True  # not a hard failure

    result("  pandas importable", PASS)

    # ── pull a sample of recent ticks into a DataFrame ────────────────
    try:
        df = pd.read_sql(
            "SELECT ts_event, price, size, is_buy FROM ticks "
            "ORDER BY ts_event DESC LIMIT 1000",
            conn,
        )
        result("  pd.read_sql ticks (1000 rows)", PASS, f"{len(df)} rows loaded")
    except Exception as e:
        result("  pd.read_sql ticks", FAIL, str(e))
        return False

    if df.empty:
        result("  DataFrame non-empty", FAIL, "no rows returned")
        return False

    # ── column presence ───────────────────────────────────────────────
    for col in ("ts_event", "price", "size", "is_buy"):
        present = col in df.columns
        result(f"  column '{col}' present", PASS if present else FAIL)
        if not present:
            ok = False

    if not ok:
        return False

    # ── NQ price sanity (15 000 – 50 000) ────────────────────────────
    pmin, pmax = float(df["price"].min()), float(df["price"].max())
    price_ok = 15_000 < pmin and pmax < 50_000
    result("  price range sanity (15k–50k)",
           PASS if price_ok else FAIL,
           f"min={pmin:.2f}  max={pmax:.2f}")
    if not price_ok:
        ok = False

    # ── size sanity (1 – 50 000) ──────────────────────────────────────
    size_ok = int(df["size"].min()) >= 1 and int(df["size"].max()) <= 50_000
    result("  size range sanity (1–50k)",
           PASS if size_ok else FAIL,
           f"min={int(df['size'].min())}  max={int(df['size'].max())}")
    if not size_ok:
        ok = False

    # ── buy/sell ratio (40–60% buys expected on NQ) ──────────────────
    buy_pct = float(df["is_buy"].mean()) * 100
    ratio_ok = 30 <= buy_pct <= 70
    result("  buy/sell ratio (30–70% buys)",
           PASS if ratio_ok else WARN,
           f"{buy_pct:.1f}% buys in last 1000 ticks")

    # ── data freshness (last tick within 3 trading days) ─────────────
    import datetime
    latest = pd.to_datetime(df["ts_event"].max(), utc=True)
    now_utc = pd.Timestamp.now(tz="UTC")
    age_hours = (now_utc - latest).total_seconds() / 3600
    # NQ closes for ~1h/day + ~48h weekend, so 72h covers Mon open after weekend
    fresh = age_hours < 72
    result("  data freshness (last tick < 72h old)",
           PASS if fresh else WARN,
           f"{age_hours:.1f}h ago  ({latest.strftime('%Y-%m-%d %H:%M UTC')})")

    # ── timestamp dtype is datetime ───────────────────────────────────
    is_dt = pd.api.types.is_datetime64_any_dtype(df["ts_event"])
    result("  ts_event parsed as datetime",
           PASS if is_dt else WARN,
           str(df["ts_event"].dtype))

    # ── oldest tick in full table (date range check) ──────────────────
    try:
        cur = conn.cursor()
        cur.execute("SELECT MIN(ts_event)::text FROM ticks LIMIT 1")
        oldest = cur.fetchone()[0]
        result("  oldest tick in DB", INFO, oldest or "n/a")
    except Exception:
        pass

    return ok


# ── section 7: ml on/off comparison summary ───────────────────────
def check_ml_comparison() -> bool:
    """
    Show ML on vs off paper-session comparison from the comparison store.

    Non-fatal (returns True) when data is missing — this section is
    informational until at least 5 sessions per arm are collected.
    """
    section("7. ML On/Off Comparison (paper trading)")

    store_path = ENGINE_DIR / "data" / "ml_comparison" / "sessions.json"
    if not store_path.exists():
        result("  sessions.json present", WARN,
               "No comparison data yet — run: python scripts/pipeline_run.py --compare-ml")
        return True

    try:
        import json as _json
        records = _json.loads(store_path.read_text())
    except Exception as exc:
        result("  sessions.json readable", FAIL, str(exc))
        return False

    ml_on  = [r for r in records if r.get("ml_enabled")]
    ml_off = [r for r in records if not r.get("ml_enabled")]

    result("  sessions (ML ON)",  INFO, str(len(ml_on)))
    result("  sessions (ML OFF)", INFO, str(len(ml_off)))

    MIN_SESSIONS = 5
    if len(ml_on) < MIN_SESSIONS or len(ml_off) < MIN_SESSIONS:
        result(
            "  sufficient sessions (≥5 per arm)",
            WARN,
            f"ML-on={len(ml_on)}, ML-off={len(ml_off)} — need {MIN_SESSIONS} each",
        )
        return True  # not a hard failure — data is still accumulating

    result("  sufficient sessions (≥5 per arm)", PASS)

    def _avg(rows: list[dict], key: str) -> float:
        vals = [r[key] for r in rows if key in r]
        return sum(vals) / len(vals) if vals else 0.0

    avg_pnl_on  = _avg(ml_on,  "total_pnl")
    avg_pnl_off = _avg(ml_off, "total_pnl")
    delta       = avg_pnl_on - avg_pnl_off

    result("  avg P&L/session (ML ON)",  INFO, f"${avg_pnl_on:.2f}")
    result("  avg P&L/session (ML OFF)", INFO, f"${avg_pnl_off:.2f}")

    if delta > 0:
        result("  ML advantage",  PASS, f"+${delta:.2f}/session vs ML-off")
    elif delta < 0:
        result("  ML advantage",  WARN, f"ML-off outperforms by ${-delta:.2f}/session")
    else:
        result("  ML advantage",  INFO, "no difference detected")

    return True


# ── section 8: config validation ───────────────────────────────────
def check_config() -> bool:
    section("7. Config Validation (.env)")
    ok = True

    if not ENV_FILE.exists():
        result("  .env file exists", FAIL, str(ENV_FILE))
        return False

    required_keys = [
        ("PG_HOST",              None),
        ("PG_PORT",              "numeric"),
        ("PG_DB",                None),
        ("PG_USER",              None),
        ("PG_PASSWORD",          "non-empty"),
        ("RITHMIC_AMP_USER",     None),
        ("RITHMIC_AMP_PASSWORD", None),
        ("RITHMIC_SYMBOL",       "NQ"),
        ("RITHMIC_EXCHANGE",     "CME"),
    ]

    # Parse .env to a dict (don't rely on os.environ in case _load_env not called)
    env_vals: dict[str, str] = {}
    for line in ENV_FILE.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            env_vals[k.strip()] = v.strip()

    for key, expected in required_keys:
        val = env_vals.get(key) or os.environ.get(key, "")
        if not val:
            result(f"  {key}", FAIL, "not set")
            ok = False
            continue

        if expected == "numeric":
            if not val.isdigit():
                result(f"  {key}", FAIL, f"value '{val}' is not numeric")
                ok = False
            else:
                result(f"  {key}", PASS, val)
        elif expected == "non-empty":
            result(f"  {key}", PASS, "(set)")
        elif expected is not None:
            # expected is a specific required value
            if val == expected:
                result(f"  {key}", PASS, val)
            else:
                result(f"  {key}", WARN, f"'{val}' (expected '{expected}')")
        else:
            result(f"  {key}", PASS, val)

    return ok


# ── main ──────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="rithmic_engine audit suite")
    parser.add_argument("--summary", action="store_true",
                        help="Print one-line pass/fail per section")
    parser.add_argument("--no-pg", action="store_true",
                        help="Skip PostgreSQL sections (4 and 5)")
    args = parser.parse_args()

    _load_env()

    print("=" * 60)
    print("  rithmic_engine AUDIT")
    print("=" * 60)

    results: dict[str, bool | None] = {}

    results["build"]  = check_build()
    results["source"] = check_source_invariants()
    results["proto"]  = check_proto()

    if args.no_pg:
        results["schema"] = None
        results["data"]   = None
        results["pydata"] = None
        results["config"] = check_config()
        print(f"\n  {INFO}  Skipping sections 4+6 (--no-pg)")
    else:
        conn = None
        try:
            conn = _pg_connect()
            print(f"\n  {PASS}  PostgreSQL connected  "
                  f"({os.environ.get('PG_HOST','localhost')}:"
                  f"{os.environ.get('PG_PORT','5432')}/"
                  f"{os.environ.get('PG_DB','rithmic')})")
        except Exception as e:
            print(f"\n  {FAIL}  PostgreSQL connection failed: {e}")
            print("  Use --no-pg to skip schema/data checks.")
            results["schema"] = False
            results["data"]   = None

        if conn is not None:
            try:
                results["schema"]  = check_schema(conn)
                results["data"]    = check_data_health(conn)
                results["pydata"]  = check_python_data(conn)
            finally:
                conn.close()

    results["ml_cmp"] = check_ml_comparison()
    results["config"] = check_config()

    # ── summary ───────────────────────────────────────────────────
    section("9. Summary")
    labels = {
        "build":  "Build integrity",
        "source": "Source invariants",
        "proto":  "Proto integrity",
        "schema": "PostgreSQL schema",
        "data":   "Data health",
        "pydata": "Python data readability",
        "ml_cmp": "ML on/off comparison",
        "config": "Config validation",
    }

    all_pass = True
    for key, label in labels.items():
        val = results.get(key)
        if val is None:
            result(f"  {label}", WARN, "skipped")
        else:
            if not val:
                all_pass = False
            result(f"  {label}", PASS if val else FAIL)

    print(f"\n{'='*60}")
    print(f"  Overall: {PASS if all_pass else FAIL}")
    print(f"{'='*60}\n")

    sys.exit(0 if all_pass else 1)


if __name__ == "__main__":
    main()
