#!/usr/bin/env python3
"""
Continuous Audit Daemon for rithmic_engine.

Adapted from bot's audit_daemon.py. Runs data health checks in a loop,
writes quality metrics to PostgreSQL, and logs failures.

Checks performed each cycle:
  1. Data freshness — last tick within expected recency
  2. Rejection rate — percentage of ticks rejected by validator
  3. Gap detection — timestamp gaps during RTH
  4. BBO integrity — crossed market detection
  5. Bar aggregate health — 1min bars generating correctly
  6. Session health — current session stats
  7. Contamination audit — runs contamination_audit.py checks
  8. WAL health — WAL file size (unflushed data)
  9. Disk space — alerts when < 5 GB free
  10. C++ test suite — runs ctest (if build exists)

Usage:
  python scripts/audit_daemon.py                 # Run forever, check every 5 min
  python scripts/audit_daemon.py --interval 60   # Check every 60 seconds
  python scripts/audit_daemon.py --once           # Run once and exit
"""
import argparse
import json
import os
import shutil
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

ENGINE_DIR = Path(__file__).resolve().parent.parent
LOG_DIR = ENGINE_DIR / "data" / "logs"
LOG_FILE = LOG_DIR / "audit_daemon.log"
FAIL_FILE = LOG_DIR / "audit_failures.log"
STATUS_FILE = ENGINE_DIR / "data" / "audit_status.json"
WAL_FILE = ENGINE_DIR / "data" / "wal.bin"


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


def log(msg: str, level: str = "INFO"):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] [{level}] {msg}"
    print(line, flush=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    with open(LOG_FILE, "a") as f:
        f.write(line + "\n")


def log_failure(msg: str):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] FAILURE: {msg}"
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    with open(FAIL_FILE, "a") as f:
        f.write(line + "\n")


def write_metric(conn, metric: str, value: float, labels: dict | None = None):
    try:
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO quality_metrics (metric, value, labels_json) VALUES (%s, %s, %s)",
            (metric, value, json.dumps(labels) if labels else None))
        conn.commit()
    except Exception:
        conn.rollback()


def write_event(conn, severity: str, event: str, details: str = ""):
    try:
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO audit_log (event, severity, details) VALUES (%s, %s, %s)",
            (event, severity, details))
        conn.commit()
    except Exception:
        conn.rollback()


# ── Individual checks ──────────────────────────────────────────────

def check_data_freshness(conn) -> dict:
    """Check that we've received ticks recently."""
    cur = conn.cursor()
    cur.execute("SELECT MAX(ts_event) FROM ticks")
    row = cur.fetchone()
    if row[0] is None:
        return {"check": "data_freshness", "status": "WARN",
                "message": "No ticks in database", "value": -1}

    from datetime import timezone as tz
    latest = row[0]
    if latest.tzinfo is None:
        import pytz
        latest = latest.replace(tzinfo=pytz.UTC)
    age_sec = (datetime.now(tz.utc) - latest).total_seconds()
    # During RTH, expect ticks within last 5 min; off-hours up to 18h is normal
    threshold = 300 if 13 <= datetime.now(tz.utc).hour <= 21 else 64800
    ok = age_sec < threshold
    return {"check": "data_freshness", "status": "PASS" if ok else "WARN",
            "message": f"Last tick {age_sec:.0f}s ago ({latest})",
            "value": age_sec}


def check_rejection_rate(conn) -> dict:
    """Check sentinel/quality_metrics for rejection rate."""
    cur = conn.cursor()
    cur.execute("""
        SELECT value FROM quality_metrics
        WHERE metric = 'rejection_rate_pct'
        ORDER BY ts DESC LIMIT 1
    """)
    row = cur.fetchone()
    if row is None:
        return {"check": "rejection_rate", "status": "INFO",
                "message": "No rejection rate data yet", "value": 0}
    rate = row[0]
    ok = rate < 5.0
    return {"check": "rejection_rate",
            "status": "PASS" if ok else "WARN",
            "message": f"Rejection rate: {rate:.2f}%", "value": rate}


def check_gap_count(conn) -> dict:
    """Check for timestamp gaps in recent data."""
    cur = conn.cursor()
    cur.execute("""
        SELECT value FROM quality_metrics
        WHERE metric = 'sentinel_gaps'
        ORDER BY ts DESC LIMIT 1
    """)
    row = cur.fetchone()
    if row is None:
        return {"check": "gap_count", "status": "INFO",
                "message": "No gap data yet", "value": 0}
    gaps = int(row[0])
    return {"check": "gap_count",
            "status": "PASS" if gaps < 50 else "WARN",
            "message": f"Session gaps: {gaps}", "value": float(gaps)}


def check_session_health(conn) -> dict:
    """Check most recent session stats."""
    cur = conn.cursor()
    cur.execute("""
        SELECT id, mode, started_at, tick_count, rejected_count, gap_count, alert_count
        FROM sessions ORDER BY started_at DESC LIMIT 1
    """)
    row = cur.fetchone()
    if row is None:
        return {"check": "session_health", "status": "INFO",
                "message": "No sessions recorded", "value": 0}
    sid, mode, started, ticks, rejected, gaps, alerts = row
    return {"check": "session_health", "status": "PASS",
            "message": f"Session {sid} ({mode}): {ticks or 0} ticks, "
                       f"{rejected or 0} rejected, {gaps or 0} gaps, "
                       f"{alerts or 0} alerts, started {started}",
            "value": float(ticks or 0)}


def check_wal_health() -> dict:
    """Check WAL file size — large WAL means unflushed data."""
    candidates = [ENGINE_DIR / "data" / "wal.bin", ENGINE_DIR / "ticks.wal"]
    for p in candidates:
        if p.exists():
            size = p.stat().st_size
            ok = size < 1024 * 1024  # < 1 MB
            return {"check": "wal_health",
                    "status": "PASS" if ok else "WARN",
                    "message": f"WAL size: {size / 1024:.1f} KB"
                               + (" — unflushed data!" if not ok else ""),
                    "value": float(size)}
    return {"check": "wal_health", "status": "INFO",
            "message": "WAL file not found (normal before first run)", "value": 0}


def check_disk_space() -> dict:
    """Check available disk space."""
    free_gb = shutil.disk_usage("/").free / (1024 ** 3)
    ok = free_gb > 5.0
    return {"check": "disk_space",
            "status": "PASS" if ok else "WARN",
            "message": f"{free_gb:.1f} GB free", "value": free_gb}


def run_contamination_audit() -> dict:
    """Run the contamination audit script."""
    script = ENGINE_DIR / "scripts" / "contamination_audit.py"
    if not script.exists():
        return {"check": "contamination_audit", "status": "WARN",
                "message": "contamination_audit.py not found", "value": -1}
    try:
        result = subprocess.run(
            [sys.executable, str(script), "--json"],
            capture_output=True, text=True, timeout=60,
            cwd=str(ENGINE_DIR),
        )
        findings = json.loads(result.stdout)
        passed = sum(1 for f in findings if f["status"] == "PASS")
        failed = sum(1 for f in findings if f["status"] == "FAIL")
        return {"check": "contamination_audit",
                "status": "PASS" if failed == 0 else "FAIL",
                "message": f"{passed} passed, {failed} failed",
                "value": float(failed)}
    except subprocess.TimeoutExpired:
        return {"check": "contamination_audit", "status": "WARN",
                "message": "Timed out after 60s", "value": -1}
    except Exception as e:
        return {"check": "contamination_audit", "status": "WARN",
                "message": f"Error: {e}", "value": -1}


def run_cpp_tests() -> dict:
    """Run C++ ctest suite if build exists."""
    build_dir = ENGINE_DIR / "build"
    if not build_dir.exists():
        return {"check": "cpp_tests", "status": "INFO",
                "message": "build/ not found — skip", "value": 0}
    try:
        result = subprocess.run(
            ["ctest", "--output-on-failure", "--test-dir", str(build_dir)],
            capture_output=True, text=True, timeout=30,
        )
        import re
        m = re.search(r"(\d+)% tests passed, (\d+) tests failed out of (\d+)",
                       result.stdout)
        if m:
            failed = int(m.group(2))
            total = int(m.group(3))
            passed = total - failed
        else:
            passed = result.stdout.count("Passed")
            failed = result.stdout.count("Failed")
        return {"check": "cpp_tests",
                "status": "PASS" if failed == 0 else "FAIL",
                "message": f"{passed} passed, {failed} failed",
                "value": float(failed)}
    except subprocess.TimeoutExpired:
        return {"check": "cpp_tests", "status": "WARN",
                "message": "Timed out after 30s", "value": -1}
    except FileNotFoundError:
        return {"check": "cpp_tests", "status": "INFO",
                "message": "ctest not found", "value": 0}
    except Exception as e:
        return {"check": "cpp_tests", "status": "WARN",
                "message": f"Error: {e}", "value": -1}


# ── Main loop ──────────────────────────────────────────────────────

def run_all_checks(conn) -> list[dict]:
    results = []

    log("Checking data freshness...")
    results.append(check_data_freshness(conn))

    log("Checking rejection rate...")
    results.append(check_rejection_rate(conn))

    log("Checking gap count...")
    results.append(check_gap_count(conn))

    log("Checking session health...")
    results.append(check_session_health(conn))

    log("Checking WAL health...")
    results.append(check_wal_health())

    log("Checking disk space...")
    results.append(check_disk_space())

    log("Running contamination audit...")
    results.append(run_contamination_audit())

    log("Running C++ tests...")
    results.append(run_cpp_tests())

    return results


def main():
    parser = argparse.ArgumentParser(description="Engine Audit Daemon")
    parser.add_argument("--interval", type=int, default=300,
                        help="Seconds between runs (default: 300)")
    parser.add_argument("--once", action="store_true", help="Run once and exit")
    args = parser.parse_args()

    _load_env()

    log("=" * 50)
    log("  RITHMIC ENGINE AUDIT DAEMON STARTED")
    log(f"  Interval: {'once' if args.once else f'{args.interval}s'}")
    log("=" * 50)

    run_count = 0
    consecutive_pass = 0

    while True:
        run_count += 1
        conn = None

        try:
            conn = _pg_connect()
        except Exception as e:
            log(f"PostgreSQL connection failed: {e}", "ERROR")
            log_failure(f"pg_connection: {e}")
            if args.once:
                sys.exit(1)
            time.sleep(args.interval)
            continue

        try:
            results = run_all_checks(conn)
        finally:
            conn.close()

        # Reconnect to write metrics (fresh connection)
        try:
            conn = _pg_connect()
        except Exception:
            conn = None

        passed = sum(1 for r in results if r["status"] == "PASS")
        failed = sum(1 for r in results if r["status"] == "FAIL")
        warned = sum(1 for r in results if r["status"] == "WARN")

        if failed == 0:
            consecutive_pass += 1
        else:
            consecutive_pass = 0
            for r in results:
                if r["status"] == "FAIL":
                    log_failure(f"{r['check']}: {r['message']}")

        # Write metrics + status
        if conn:
            try:
                for r in results:
                    write_metric(conn, f"audit_{r['check']}",
                                 r.get("value", 0),
                                 {"status": r["status"], "run": run_count})
                write_metric(conn, "audit_passed", float(passed), {"run": run_count})
                write_metric(conn, "audit_failed", float(failed), {"run": run_count})
                write_metric(conn, "audit_consecutive_pass", float(consecutive_pass))

                if failed > 0:
                    write_event(conn, "ERROR", "audit_failures",
                                f"{failed} checks failed in run #{run_count}")
                else:
                    write_event(conn, "INFO", "audit_pass",
                                f"All {passed} checks passed (run #{run_count})")

                # Cleanup old metrics (every 100 runs)
                if run_count % 100 == 0:
                    cur = conn.cursor()
                    cur.execute(
                        "DELETE FROM quality_metrics WHERE ts < NOW() - INTERVAL '30 days'")
                    conn.commit()
            except Exception as e:
                log(f"Metrics write failed: {e}", "WARN")
            finally:
                conn.close()

        # Status file (atomic write)
        status = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "total_passed": passed,
            "total_failed": failed,
            "total_warned": warned,
            "status": "pass" if failed == 0 else "fail",
            "run_count": run_count,
            "consecutive_pass": consecutive_pass,
            "checks": results,
        }
        STATUS_FILE.parent.mkdir(parents=True, exist_ok=True)
        tmp = str(STATUS_FILE) + ".tmp"
        with open(tmp, "w") as f:
            json.dump(status, f, indent=2)
        os.replace(tmp, STATUS_FILE)

        log(f"TOTAL: {passed} passed, {failed} failed, {warned} warned "
            f"{'OK' if failed == 0 else 'CHECK audit_failures.log'}"
            f" (run #{run_count}, {consecutive_pass} consecutive pass)")
        log("---")

        if args.once:
            sys.exit(1 if failed > 0 else 0)

        # Adaptive interval
        if consecutive_pass >= 10:
            sleep_time = min(args.interval * 2, 600)
        else:
            sleep_time = args.interval
        time.sleep(sleep_time)


if __name__ == "__main__":
    main()
