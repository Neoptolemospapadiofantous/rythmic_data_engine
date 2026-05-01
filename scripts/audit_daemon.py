#!/usr/bin/env python3
"""
Continuous Audit Daemon for rithmic_engine.

Runs data health checks in a loop, enforces escalation rules, fires Slack
alerts, and writes quality metrics to PostgreSQL.

Escalation policy (from quality_rules/escalation.yaml):
  3 WARNs from same check in 60 min  →  escalate to ERROR, alert once
  ERROR unresolved for 30 min         →  escalate to CRITICAL, alert once
  CRITICAL on trading_constants       →  write data/AUDIT_HALT + alert
  2 consecutive clean passes          →  auto-resolve, alert once

Checks each cycle:
  1. data_freshness         — last tick within expected recency
  2. rejection_rate         — percentage of ticks rejected
  3. gap_count              — timestamp gaps during RTH
  4. session_health         — current session stats
  5. wal_health             — WAL file size (unflushed data)
  6. disk_space             — free disk space
  7. contamination_audit    — runs contamination_audit.py
  8. cpp_tests              — runs ctest
  9. trading_constants      — point_value, tick_value, symbol, commission_rt
  10. pnl_sanity            — recent trades within expected PnL range

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
AUDIT_HALT = ENGINE_DIR / "data" / "AUDIT_HALT"


def _load_env():
    env_file = ENGINE_DIR / ".env"
    if env_file.exists():
        for line in env_file.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                if k.strip() not in os.environ:
                    os.environ[k.strip()] = v.strip()


def _load_live_config() -> dict | None:
    cfg_path = ENGINE_DIR / "config" / "live_config.json"
    try:
        return json.loads(cfg_path.read_text())
    except Exception:
        return None


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


def _send_alert(message: str) -> None:
    """Fire a Slack webhook.  Non-fatal — never let alert failure break the daemon."""
    url = os.environ.get("SLACK_WEBHOOK_URL", "")
    if not url:
        return
    try:
        import urllib.request
        payload = json.dumps({"text": message}).encode()
        req = urllib.request.Request(url, data=payload,
                                     headers={"Content-Type": "application/json"})
        urllib.request.urlopen(req, timeout=8)
    except Exception:
        pass


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


# ── Escalation engine ──────────────────────────────────────────────

class EscalationEngine:
    """Enforces quality_rules/escalation.yaml policy at runtime.

    Tracks per-check WARN history and ERROR age in memory. Fires alerts
    exactly once per transition (no alert storms). Auto-resolves after
    two consecutive fully-clean cycles.
    """

    WARN_WINDOW_SEC = 3600    # 60-min WARN accumulation window
    WARN_THRESHOLD = 3        # WARNs needed to escalate to ERROR
    ERROR_CRITICAL_SEC = 1800 # 30 min unresolved ERROR → CRITICAL
    CLEAN_RESOLVE = 2         # consecutive clean passes to auto-resolve

    # Checks whose CRITICAL state warrants an AUDIT_HALT sentinel
    HALT_CHECKS = {"trading_constants"}

    def __init__(self):
        self._warn_ts: dict[str, list[float]] = {}
        self._error_since: dict[str, float] = {}
        self._alerted: set[str] = set()
        self._consecutive_clean: int = 0

    def process(self, results: list[dict], conn) -> list[dict]:
        now = time.time()
        escalated: list[dict] = []
        any_bad = False

        for raw in results:
            r = dict(raw)
            check = r["check"]
            status = r["status"]

            # ── WARN accumulation → ERROR ──────────────────────────
            if status == "WARN":
                hist = self._warn_ts.setdefault(check, [])
                hist.append(now)
                hist[:] = [t for t in hist if now - t < self.WARN_WINDOW_SEC]

                if len(hist) >= self.WARN_THRESHOLD:
                    r["status"] = "ERROR"
                    r["message"] += f" [escalated: {len(hist)}× WARN in 60 min]"
                    key = f"warn_esc:{check}"
                    if key not in self._alerted:
                        self._alerted.add(key)
                        _send_alert(
                            f":warning: *AUDIT ESCALATED* `{check}` WARN→ERROR "
                            f"({len(hist)} WARNs in 60 min)\n{r['message']}")
                        if conn:
                            write_event(conn, "ERROR", "audit_warn_escalated",
                                        f"{check}: {r['message']}")
                    if check not in self._error_since:
                        self._error_since[check] = now

            # ── ERROR age → CRITICAL ───────────────────────────────
            if r["status"] in ("FAIL", "ERROR", "CRITICAL"):
                any_bad = True
                if check not in self._error_since:
                    self._error_since[check] = now

                elapsed = now - self._error_since[check]
                if elapsed >= self.ERROR_CRITICAL_SEC and r["status"] != "CRITICAL":
                    r["status"] = "CRITICAL"
                    r["message"] += f" [CRITICAL: unresolved {elapsed / 60:.0f} min]"

                if r["status"] == "CRITICAL":
                    key = f"crit:{check}"
                    if key not in self._alerted:
                        self._alerted.add(key)
                        _send_alert(
                            f":red_circle: *AUDIT CRITICAL* `{check}` "
                            f"unresolved {elapsed / 60:.0f} min\n{r['message']}")
                        if conn:
                            write_event(conn, "CRITICAL", "audit_critical",
                                        f"{check}: {r['message']}")

                    # Write AUDIT_HALT for trading-safety checks
                    if check in self.HALT_CHECKS and not AUDIT_HALT.exists():
                        AUDIT_HALT.parent.mkdir(parents=True, exist_ok=True)
                        AUDIT_HALT.write_text(
                            json.dumps({
                                "check": check,
                                "message": r["message"],
                                "timestamp": datetime.now(timezone.utc).isoformat(),
                            }))
                        log(f"AUDIT_HALT written for {check}", "CRITICAL")
                        _send_alert(
                            f":octagonal_sign: *AUDIT_HALT written* — `{check}` CRITICAL. "
                            f"live_trader will refuse to start until resolved and "
                            f"data/AUDIT_HALT is removed.")

            escalated.append(r)

        # ── Auto-resolve after CLEAN_RESOLVE consecutive clean passes ──
        if not any_bad:
            self._consecutive_clean += 1
            if self._consecutive_clean >= self.CLEAN_RESOLVE and self._error_since:
                resolved = list(self._error_since.keys())
                for check in resolved:
                    _send_alert(
                        f":white_check_mark: *AUDIT RESOLVED* `{check}` "
                        f"cleared after {self.CLEAN_RESOLVE} clean passes")
                    if conn:
                        write_event(conn, "INFO", "audit_resolved",
                                    f"{check}: auto-resolved after {self.CLEAN_RESOLVE} clean passes")
                self._error_since.clear()
                self._warn_ts.clear()
                self._alerted.clear()
                self._consecutive_clean = 0
        else:
            self._consecutive_clean = 0

        return escalated


# ── Individual checks ──────────────────────────────────────────────

def check_data_freshness(conn) -> dict:
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
    threshold = 300 if 13 <= datetime.now(tz.utc).hour <= 21 else 64800
    ok = age_sec < threshold
    return {"check": "data_freshness", "status": "PASS" if ok else "WARN",
            "message": f"Last tick {age_sec:.0f}s ago ({latest})",
            "value": age_sec}


def check_rejection_rate(conn) -> dict:
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
    return {"check": "rejection_rate",
            "status": "PASS" if rate < 5.0 else "WARN",
            "message": f"Rejection rate: {rate:.2f}%", "value": rate}


def check_gap_count(conn) -> dict:
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
            "message": (f"Session {sid} ({mode}): {ticks or 0} ticks, "
                        f"{rejected or 0} rejected, {gaps or 0} gaps, "
                        f"{alerts or 0} alerts, started {started}"),
            "value": float(ticks or 0)}


def check_wal_health() -> dict:
    candidates = [ENGINE_DIR / "data" / "wal.bin", ENGINE_DIR / "ticks.wal"]
    for p in candidates:
        if p.exists():
            size = p.stat().st_size
            ok = size < 1024 * 1024
            return {"check": "wal_health",
                    "status": "PASS" if ok else "WARN",
                    "message": f"WAL size: {size / 1024:.1f} KB"
                               + (" — unflushed data!" if not ok else ""),
                    "value": float(size)}
    return {"check": "wal_health", "status": "INFO",
            "message": "WAL file not found (normal before first run)", "value": 0}


def check_disk_space() -> dict:
    free_gb = shutil.disk_usage("/").free / (1024 ** 3)
    return {"check": "disk_space",
            "status": "PASS" if free_gb > 5.0 else "WARN",
            "message": f"{free_gb:.1f} GB free", "value": free_gb}


def run_contamination_audit() -> dict:
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
    build_dir = ENGINE_DIR / "build"
    if not build_dir.exists():
        return {"check": "cpp_tests", "status": "INFO",
                "message": "build/ not found — skip", "value": 0}
    try:
        result = subprocess.run(
            ["ctest", "--output-on-failure", "--test-dir", str(build_dir)],
            capture_output=True, text=True, timeout=120,
        )
        import re
        m = re.search(r"(\d+)% tests passed, (\d+) tests failed out of (\d+)",
                      result.stdout)
        if m:
            failed = int(m.group(2))
            passed = int(m.group(3)) - failed
        else:
            passed = result.stdout.count("Passed")
            failed = result.stdout.count("Failed")
        return {"check": "cpp_tests",
                "status": "PASS" if failed == 0 else "FAIL",
                "message": f"{passed} passed, {failed} failed",
                "value": float(failed)}
    except subprocess.TimeoutExpired:
        return {"check": "cpp_tests", "status": "WARN",
                "message": "Timed out after 120s", "value": -1}
    except FileNotFoundError:
        return {"check": "cpp_tests", "status": "INFO",
                "message": "ctest not found", "value": 0}
    except Exception as e:
        return {"check": "cpp_tests", "status": "WARN",
                "message": f"Error: {e}", "value": -1}


def check_trading_constants(live_cfg: dict | None) -> dict:
    """Verify point_value, tick_value, symbol, commission_rt match MNQ spec.

    Any mismatch is immediately CRITICAL — a wrong point_value silently
    scales PnL by 10x, making this the highest-severity check in the daemon.
    """
    if live_cfg is None:
        return {"check": "trading_constants", "status": "WARN",
                "message": "live_config.json not loaded — cannot verify constants",
                "value": -1}

    errors: list[str] = []
    warnings: list[str] = []
    pv = live_cfg.get("point_value")
    tv = live_cfg.get("tick_value")
    sym = live_cfg.get("symbol", "")
    comm = live_cfg.get("commission_rt")

    if pv is None:
        warnings.append("point_value not set in config")
    elif abs(float(pv) - 2.0) > 0.001:
        errors.append(f"point_value={pv} (must be 2.0 for MNQ)")

    if tv is None:
        warnings.append("tick_value not set in config")
    elif abs(float(tv) - 0.50) > 0.001:
        errors.append(f"tick_value={tv} (must be 0.50 for MNQ)")

    if sym and sym not in ("MNQ", ""):
        errors.append(f"symbol={sym!r} (expected MNQ)")
    if comm is not None and abs(float(comm) - 4.0) > 0.01:
        errors.append(f"commission_rt={comm} (expected 4.0)")

    if errors:
        return {"check": "trading_constants", "status": "CRITICAL",
                "message": "MISMATCH: " + "; ".join(errors),
                "value": float(len(errors))}
    if warnings:
        return {"check": "trading_constants", "status": "WARN",
                "message": "Missing constants: " + "; ".join(warnings),
                "value": float(len(warnings))}
    return {"check": "trading_constants", "status": "PASS",
            "message": f"point_value={pv} tick_value={tv} symbol={sym!r} commission_rt={comm}",
            "value": 0.0}


def check_pnl_sanity(conn) -> dict:
    """Flag any trade in the last 24h with |PnL| > $500 (implausible for 1 MNQ contract).

    Rationale: 500 points × $2/point = $1,000. A single trade exceeding $500 PnL
    on 1 contract suggests a point_value misconfiguration.
    """
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT id, pnl_usd FROM live_trades
            WHERE exit_time > NOW() - INTERVAL '24 hours'
              AND ABS(pnl_usd) > 500
            ORDER BY ABS(pnl_usd) DESC LIMIT 5
        """)
        rows = cur.fetchall()
    except Exception as e:
        return {"check": "pnl_sanity", "status": "WARN",
                "message": f"Query error: {e}", "value": -1}

    if rows:
        details = ", ".join(f"trade {r[0]}: ${r[1]:.0f}" for r in rows)
        return {"check": "pnl_sanity", "status": "WARN",
                "message": f"Unusually large PnL last 24h: {details}",
                "value": float(abs(rows[0][1]))}
    return {"check": "pnl_sanity", "status": "PASS",
            "message": "All recent trades within normal PnL range (<$500)", "value": 0.0}


# ── Main loop ──────────────────────────────────────────────────────

def run_all_checks(conn, live_cfg: dict | None) -> list[dict]:
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

    log("Checking trading constants...")
    results.append(check_trading_constants(live_cfg))

    log("Checking PnL sanity...")
    results.append(check_pnl_sanity(conn))

    return results


def main():
    parser = argparse.ArgumentParser(description="Engine Audit Daemon")
    parser.add_argument("--interval", type=int, default=300,
                        help="Seconds between runs (default: 300)")
    parser.add_argument("--once", action="store_true", help="Run once and exit")
    args = parser.parse_args()

    _load_env()

    log("=" * 60)
    log("  RITHMIC ENGINE AUDIT DAEMON STARTED")
    log(f"  Interval: {'once' if args.once else f'{args.interval}s'}")
    log("=" * 60)

    escalation = EscalationEngine()
    run_count = 0
    consecutive_pass = 0

    while True:
        run_count += 1
        conn = None
        live_cfg = _load_live_config()

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
            raw_results = run_all_checks(conn, live_cfg)
        finally:
            conn.close()

        # Re-connect for escalation writes and metrics
        try:
            conn = _pg_connect()
        except Exception:
            conn = None

        # Apply escalation rules
        results = escalation.process(raw_results, conn)

        passed = sum(1 for r in results if r["status"] == "PASS")
        failed = sum(1 for r in results if r["status"] in ("FAIL", "ERROR", "CRITICAL"))
        warned = sum(1 for r in results if r["status"] == "WARN")
        info = sum(1 for r in results if r["status"] == "INFO")

        if failed == 0:
            consecutive_pass += 1
        else:
            consecutive_pass = 0
            for r in results:
                if r["status"] in ("FAIL", "ERROR", "CRITICAL"):
                    log_failure(f"{r['check']}: {r['message']}")

        # Write metrics + audit events
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

                if run_count % 100 == 0:
                    cur = conn.cursor()
                    cur.execute(
                        "DELETE FROM quality_metrics WHERE ts < NOW() - INTERVAL '30 days'")
                    conn.commit()
            except Exception as e:
                log(f"Metrics write failed: {e}", "WARN")
            finally:
                conn.close()

        # Atomic status file
        status = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "total_passed": passed,
            "total_failed": failed,
            "total_warned": warned,
            "total_info": info,
            "status": "pass" if failed == 0 else "fail",
            "run_count": run_count,
            "consecutive_pass": consecutive_pass,
            "audit_halt": AUDIT_HALT.exists(),
            "checks": results,
        }
        STATUS_FILE.parent.mkdir(parents=True, exist_ok=True)
        tmp = str(STATUS_FILE) + ".tmp"
        with open(tmp, "w") as f:
            json.dump(status, f, indent=2)
        os.replace(tmp, STATUS_FILE)

        suffix = " *** AUDIT_HALT ACTIVE ***" if AUDIT_HALT.exists() else ""
        log(f"TOTAL: {passed} passed, {failed} failed, {warned} warned "
            f"{'OK' if failed == 0 else 'CHECK audit_failures.log'}"
            f" (run #{run_count}, {consecutive_pass} consecutive pass){suffix}")
        log("---")

        if args.once:
            sys.exit(1 if failed > 0 else 0)

        # Adaptive interval: after 10 consecutive passes, slow down off-hours work
        sleep_time = min(args.interval * 2, 600) if consecutive_pass >= 10 else args.interval
        time.sleep(sleep_time)


if __name__ == "__main__":
    main()
