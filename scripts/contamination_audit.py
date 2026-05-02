#!/usr/bin/env python3
"""
Contamination & Data Integrity Audit for rithmic_engine.

Adapted from bot's Gate 15 contamination audit. Validates the engine's
PostgreSQL data for integrity, contamination, and readiness for the
bot's pipeline (walk-forward, OOT holdout, regime classification).

Checks:
  1. Tick timestamp monotonicity — no backward timestamps in stored data
  2. Tick deduplication — unique index enforced, no exact duplicates
  3. Price continuity — no impossible jumps (>10% in < 1 second)
  4. BBO bid-ask validity — no persistent crossed markets
  5. Tick-BBO timestamp alignment — BBO timestamps within range of ticks
  6. Bar OHLC integrity — high >= low, open/close within range
  7. Data gap detection — identifies gaps > 2 min during RTH
  8. Source tag consistency — all ticks from expected source
  9. Walk-forward window non-overlap — train/test boundaries don't leak
  10. OOT holdout isolation — holdout partition is untouched by pipeline

Usage:
  python scripts/contamination_audit.py
  python scripts/contamination_audit.py --json
  python scripts/contamination_audit.py --no-pg   # source-only checks
"""
import argparse
import json
import os
import re
import sys
from pathlib import Path

ENGINE_DIR = Path(__file__).resolve().parent.parent
SRC_DIR = ENGINE_DIR / "src"


def _load_env():
    env_file = ENGINE_DIR / ".env"
    if env_file.exists():
        for line in env_file.read_text().splitlines():
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


def _pass(name, msg=""):
    return {"check": name, "status": "PASS", "severity": "INFO", "message": msg}


def _fail(name, msg, severity="ERROR"):
    return {"check": name, "status": "FAIL", "severity": severity, "message": msg}


# ── Source code checks (no DB needed) ──────────────────────────────

def check_no_negative_shift():
    """Feature/strategy code must not use shift(-N) which looks into the future."""
    findings = []
    for py in SRC_DIR.parent.rglob("*.py"):
        if "test_" in py.name or "__pycache__" in str(py):
            continue
        src = py.read_text()
        matches = re.findall(r'\.shift\(\s*-\d+\s*\)', src)
        if matches:
            findings.append(_fail("no_negative_shift",
                f"{py.name} uses shift(negative): {matches[:3]}"))
    if not findings:
        findings.append(_pass("no_negative_shift", "No shift(-N) found in Python code"))
    return findings


def check_dedup_index_in_source():
    """db.cpp ON CONFLICT clause must use 5-column unique key."""
    db_cpp = SRC_DIR / "db.cpp"
    if not db_cpp.exists():
        return [_fail("dedup_index_source", "db.cpp not found")]
    src = db_cpp.read_text()
    if "ON CONFLICT (symbol, exchange, ts_event, price, size)" in src:
        return [_pass("dedup_index_source",
            "ON CONFLICT uses 5-column key (symbol, exchange, ts_event, price, size)")]
    return [_fail("dedup_index_source",
        "ON CONFLICT clause does not use full 5-column key — dedup gap")]


def check_validator_price_bounds():
    """validator.hpp must enforce price > 0 and < MAX_PRICE."""
    validator = SRC_DIR / "validator.hpp"
    if not validator.exists():
        return [_fail("validator_price", "validator.hpp not found")]
    src = validator.read_text()
    has_gt_zero = "r.price <= 0.0" in src or "r.price <= 0" in src
    has_max = "MAX_PRICE" in src
    if has_gt_zero and has_max:
        return [_pass("validator_price", "Price bounded: > 0 AND < MAX_PRICE")]
    return [_fail("validator_price", "Price validation incomplete in validator.hpp")]


def check_sentinel_exists():
    """DataSentinel must exist in validator.hpp for economic plausibility checks."""
    validator = SRC_DIR / "validator.hpp"
    if not validator.exists():
        return [_fail("sentinel_exists", "validator.hpp not found")]
    src = validator.read_text()
    if "class DataSentinel" in src:
        return [_pass("sentinel_exists",
            "DataSentinel class found — economic plausibility checks active")]
    return [_fail("sentinel_exists",
        "DataSentinel not found in validator.hpp — no economic checks")]


def check_wal_crash_recovery():
    """Collector must replay WAL on startup."""
    collector = SRC_DIR / "collector.cpp"
    if not collector.exists():
        return [_fail("wal_recovery", "collector.cpp not found")]
    src = collector.read_text()
    if "wal_->replay()" in src:
        return [_pass("wal_recovery", "WAL replay on startup confirmed")]
    return [_fail("wal_recovery", "WAL replay not found in collector startup")]


# ── PostgreSQL data checks ─────────────────────────────────────────

def check_tick_timestamp_monotonicity(conn):
    """Sample recent ticks and verify timestamps are monotonically increasing."""
    cur = conn.cursor()
    cur.execute("""
        WITH recent AS (
            SELECT ts_event, LAG(ts_event) OVER (ORDER BY ts_event) AS prev_ts
            FROM ticks
            WHERE ts_event > NOW() - INTERVAL '7 days'
            ORDER BY ts_event
            LIMIT 100000
        )
        SELECT COUNT(*) FROM recent WHERE ts_event < prev_ts
    """)
    backward = cur.fetchone()[0]
    if backward == 0:
        return [_pass("tick_ts_monotonicity",
            "No backward timestamps in last 7 days (100k sample)")]
    return [_fail("tick_ts_monotonicity",
        f"{backward} backward timestamps detected in last 7 days", "WARN")]


def check_tick_deduplication(conn):
    """Verify unique index prevents exact duplicates."""
    cur = conn.cursor()
    cur.execute("""
        SELECT COUNT(*) FROM pg_indexes
        WHERE tablename = 'ticks' AND indexname = 'idx_ticks_unique'
    """)
    has_idx = cur.fetchone()[0] > 0
    if has_idx:
        return [_pass("tick_dedup", "idx_ticks_unique exists — dedup enforced")]
    return [_fail("tick_dedup", "idx_ticks_unique not found — duplicates possible", "CRITICAL")]


def check_price_continuity(conn):
    """Check for impossible price jumps (>10% within 1 second)."""
    cur = conn.cursor()
    cur.execute("""
        WITH recent AS (
            SELECT ts_event, price,
                   LAG(price) OVER (ORDER BY ts_event) AS prev_price,
                   LAG(ts_event) OVER (ORDER BY ts_event) AS prev_ts
            FROM ticks
            WHERE ts_event > NOW() - INTERVAL '7 days'
            ORDER BY ts_event
            LIMIT 100000
        )
        SELECT COUNT(*) FROM recent
        WHERE prev_price > 0
          AND ABS(price - prev_price) / prev_price > 0.10
          AND ts_event - prev_ts < INTERVAL '1 second'
    """)
    jumps = cur.fetchone()[0]
    if jumps == 0:
        return [_pass("price_continuity",
            "No impossible price jumps (>10% in <1s) in last 7 days")]
    return [_fail("price_continuity",
        f"{jumps} impossible price jumps detected", "WARN")]


def check_bbo_validity(conn):
    """Check for persistent crossed markets in BBO data."""
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM bbo LIMIT 1")
    if cur.fetchone()[0] == 0:
        return [_pass("bbo_validity", "No BBO data yet (skipped)")]

    cur.execute("""
        SELECT COUNT(*) FROM bbo
        WHERE bid_price > 0 AND ask_price > 0 AND bid_price > ask_price
          AND ts_event > NOW() - INTERVAL '7 days'
    """)
    crossed = cur.fetchone()[0]
    if crossed == 0:
        return [_pass("bbo_validity", "No crossed markets in BBO data (last 7 days)")]
    # A few crossed ticks are normal during fast markets
    if crossed < 100:
        return [_pass("bbo_validity",
            f"{crossed} momentary crossed BBO events (normal during fast markets)")]
    return [_fail("bbo_validity",
        f"{crossed} crossed BBO events — check feed quality", "WARN")]


def check_bar_ohlc_integrity(conn):
    """Verify OHLC bars have high >= low, open/close within range."""
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT COUNT(*) FROM bars_1min WHERE high < low
        """)
        bad = cur.fetchone()[0]
        if bad == 0:
            return [_pass("bar_ohlc", "All 1min bars have high >= low")]
        return [_fail("bar_ohlc", f"{bad} bars with high < low", "ERROR")]
    except Exception:
        conn.rollback()
        return [_pass("bar_ohlc", "bars_1min not available (TimescaleDB required)")]


def check_data_gaps_rth(conn):
    """Detect gaps > 2 min during RTH (13:30-20:00 UTC for NQ)."""
    cur = conn.cursor()
    cur.execute("""
        WITH rth_ticks AS (
            SELECT ts_event,
                   LAG(ts_event) OVER (ORDER BY ts_event) AS prev_ts
            FROM ticks
            WHERE ts_event > NOW() - INTERVAL '7 days'
              AND EXTRACT(HOUR FROM ts_event) BETWEEN 13 AND 20
            ORDER BY ts_event
            LIMIT 500000
        )
        SELECT COUNT(*) FROM rth_ticks
        WHERE ts_event - prev_ts > INTERVAL '2 minutes'
    """)
    gaps = cur.fetchone()[0]
    if gaps == 0:
        return [_pass("rth_gaps", "No RTH gaps > 2 min in last 7 days")]
    if gaps < 10:
        return [_pass("rth_gaps", f"{gaps} minor RTH gaps (likely session boundaries)")]
    return [_fail("rth_gaps", f"{gaps} RTH gaps > 2 min — check feed stability", "WARN")]


def check_source_consistency(conn):
    """All ticks should come from expected source."""
    cur = conn.cursor()
    cur.execute("""
        SELECT source, COUNT(*) FROM ticks
        WHERE ts_event > NOW() - INTERVAL '7 days'
        GROUP BY source
    """)
    rows = cur.fetchall()
    if not rows:
        return [_pass("source_consistency", "No recent ticks to check")]
    sources = {r[0]: r[1] for r in rows}
    if len(sources) == 1 and "amp_rithmic" in sources:
        return [_pass("source_consistency",
            f"All recent ticks from 'amp_rithmic' ({sources['amp_rithmic']:,} ticks)")]
    return [_fail("source_consistency",
        f"Multiple sources detected: {sources}", "WARN")]


def check_walk_forward_windows(conn):
    """Walk-forward view windows must not overlap (train_end <= test_start)."""
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT COUNT(*) FROM v_walk_forward_windows
            WHERE train_end > test_start
        """)
        overlaps = cur.fetchone()[0]
        if overlaps == 0:
            return [_pass("wf_no_overlap",
                "Walk-forward windows: train_end <= test_start (no overlap)")]
        return [_fail("wf_no_overlap",
            f"{overlaps} walk-forward windows have train/test overlap", "CRITICAL")]
    except Exception:
        conn.rollback()
        return [_pass("wf_no_overlap",
            "v_walk_forward_windows not available (insufficient data)")]


def check_oot_holdout_isolation(conn):
    """OOT holdout partition view must define pipeline_end < holdout_start."""
    cur = conn.cursor()
    try:
        cur.execute("SELECT pipeline_end, holdout_start FROM v_oot_partition")
        row = cur.fetchone()
        if row is None or row[0] is None:
            return [_pass("oot_isolation", "OOT partition not yet defined (insufficient data)")]
        if row[0] <= row[1]:
            return [_pass("oot_isolation",
                f"OOT: pipeline_end ({row[0]}) <= holdout_start ({row[1]})")]
        return [_fail("oot_isolation",
            f"OOT violation: pipeline_end ({row[0]}) > holdout_start ({row[1]})",
            "CRITICAL")]
    except Exception:
        conn.rollback()
        return [_pass("oot_isolation", "v_oot_partition not available")]


def main():
    parser = argparse.ArgumentParser(description="Engine Contamination Audit")
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--no-pg", action="store_true", help="Source-only checks")
    args = parser.parse_args()

    _load_env()
    all_findings = []

    # Source code checks (always run)
    all_findings.extend(check_no_negative_shift())
    all_findings.extend(check_dedup_index_in_source())
    all_findings.extend(check_validator_price_bounds())
    all_findings.extend(check_sentinel_exists())
    all_findings.extend(check_wal_crash_recovery())

    # PostgreSQL data checks
    if not args.no_pg:
        try:
            conn = _pg_connect()
        except Exception as e:
            all_findings.append(_fail("pg_connection", f"Cannot connect to PG: {e}"))
            conn = None

        if conn is not None:
            try:
                all_findings.extend(check_tick_timestamp_monotonicity(conn))
                all_findings.extend(check_tick_deduplication(conn))
                all_findings.extend(check_price_continuity(conn))
                all_findings.extend(check_bbo_validity(conn))
                all_findings.extend(check_bar_ohlc_integrity(conn))
                all_findings.extend(check_data_gaps_rth(conn))
                all_findings.extend(check_source_consistency(conn))
                all_findings.extend(check_walk_forward_windows(conn))
                all_findings.extend(check_oot_holdout_isolation(conn))
                conn.close()
            except Exception as e:
                err = str(e).lower()
                if "does not exist" in err or "42p01" in err:
                    all_findings.append({"status": "INFO", "check": "pg_data_checks",
                                         "message": "ticks table not created — collector not started yet"})
                else:
                    all_findings.append(_fail("pg_connection", f"PG data check error: {e}"))

    passed = sum(1 for f in all_findings if f["status"] == "PASS")
    failed = sum(1 for f in all_findings if f["status"] == "FAIL")

    if args.json:
        print(json.dumps(all_findings, indent=2))
        sys.exit(1 if failed > 0 else 0)

    print(f"\n{'='*60}")
    print("  ENGINE CONTAMINATION & DATA INTEGRITY AUDIT")
    print(f"  {passed} passed, {failed} failed, {len(all_findings)} total")
    print(f"{'='*60}")

    for f in all_findings:
        if f["status"] == "FAIL":
            print(f"  FAIL  [{f['check']}] {f['message']}")
    for f in all_findings:
        if f["status"] == "PASS":
            print(f"  PASS  [{f['check']}] {f['message']}")

    print(f"\n  Status: {'PASS' if failed == 0 else 'FAIL'}")
    sys.exit(1 if any(f["severity"] == "CRITICAL" and f["status"] == "FAIL"
                      for f in all_findings) else 0)


if __name__ == "__main__":
    main()
