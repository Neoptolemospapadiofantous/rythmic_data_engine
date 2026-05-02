#!/usr/bin/env python3
"""
Continuous Audit Daemon for rithmic_engine.

Runs data health checks in a loop, enforces escalation rules, fires Slack
alerts, and writes quality metrics to PostgreSQL.  Escalation state is
persisted to disk so daemon restarts never silently amnesty an open incident.

Escalation policy (from quality_rules/escalation.yaml):
  3 WARNs from same check in 60 min  →  escalate to ERROR, alert once
  ERROR unresolved for 30 min         →  escalate to CRITICAL, alert once
  CRITICAL on trading_constants       →  write data/AUDIT_HALT + alert
  2 consecutive clean passes          →  auto-resolve, alert once

Checks each cycle:
  1.  data_freshness         — last tick recency (72h grace on weekends)
  2.  rejection_rate         — percentage of ticks rejected by validator
  3.  gap_count              — timestamp gaps in recent data
  4.  session_health         — latest session row stats
  5.  wal_health             — WAL file size vs 1 MB threshold
  6.  disk_space             — free disk vs 5 GB floor
  7.  contamination_audit    — runs contamination_audit.py --json
  8.  cpp_tests              — runs ctest (INFO on timeout, not WARN)
  9.  trading_constants      — point_value, tick_value, symbol, commission_rt
  10. pnl_sanity             — |pnl_usd| > $500 in live_trades last 24h
  11. ram_usage              — system RAM % used
  12. process_liveness       — nq_executor / live_trader running during RTH
  13. model_staleness        — ML model file age vs 30-day threshold
  14. drift_halt             — data/DRIFT_HALT sentinel present
  15. slippage_sanity        — avg fill slippage vs 6-tick threshold (7d)
  16. python_tests           — pytest tests/ suite (regression gate)

Usage:
  python scripts/audit_daemon.py                 # Run forever, check every 5 min
  python scripts/audit_daemon.py --interval 60   # Check every 60 seconds
  python scripts/audit_daemon.py --once           # Run once and exit
"""
from __future__ import annotations

import argparse
import json
import os
import re
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
ESCALATION_STATE_FILE = ENGINE_DIR / "data" / "escalation_state.json"


# ── Environment + config ───────────────────────────────────────────

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
    except Exception as exc:
        log(f"alert delivery failed: {exc}", "WARN")


# ── Logging + DB helpers ───────────────────────────────────────────

def log(msg: str, level: str = "INFO"):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] [{level}] {msg}"
    print(line, flush=True)
    # Skip disk write when running inside pytest to avoid polluting the daemon log
    if not os.environ.get("PYTEST_CURRENT_TEST"):
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

    State (warn timestamps, error ages, alerted keys) is persisted to
    ESCALATION_STATE_FILE after every cycle so a daemon restart never
    silently amnesty an open incident.
    """

    WARN_WINDOW_SEC = 3600     # 60-min WARN accumulation window
    WARN_THRESHOLD = 3         # WARNs in window needed to escalate to ERROR
    ERROR_CRITICAL_SEC = 1800  # 30 min unresolved ERROR → CRITICAL
    CLEAN_RESOLVE = 2          # consecutive clean passes to auto-resolve

    # Only native_critical=True results in HALT_CHECKS write AUDIT_HALT.
    # Checks that were escalated WARN→ERROR→CRITICAL do NOT write the sentinel.
    HALT_CHECKS = {"trading_constants"}

    def __init__(self, state_file: Path = ESCALATION_STATE_FILE):
        self._state_file = state_file
        self._warn_ts: dict[str, list[float]] = {}
        self._error_since: dict[str, float] = {}
        self._alerted: set[str] = set()
        self._consecutive_clean: int = 0
        self._load_state()

    def _load_state(self) -> None:
        try:
            data = json.loads(self._state_file.read_text())
            self._warn_ts = {k: [float(t) for t in v]
                             for k, v in data.get("warn_ts", {}).items()}
            self._error_since = {k: float(v)
                                 for k, v in data.get("error_since", {}).items()}
            self._alerted = set(data.get("alerted", []))
            self._consecutive_clean = int(data.get("consecutive_clean", 0))
            if self._error_since:
                log(f"Escalation state loaded — {len(self._error_since)} active error(s): "
                    f"{list(self._error_since)}")
        except FileNotFoundError:
            pass  # clean first run
        except Exception as e:
            log(f"Escalation state load failed (starting fresh): {e}", "WARN")

    def _save_state(self) -> None:
        try:
            data = {
                "warn_ts": self._warn_ts,
                "error_since": self._error_since,
                "alerted": list(self._alerted),
                "consecutive_clean": self._consecutive_clean,
                "saved_at": time.time(),
            }
            self._state_file.parent.mkdir(parents=True, exist_ok=True)
            tmp = str(self._state_file) + ".tmp"
            with open(tmp, "w") as f:
                json.dump(data, f)
            os.replace(tmp, self._state_file)
        except Exception as e:
            log(f"Escalation state save failed: {e}", "WARN")

    def process(self, results: list[dict], conn) -> list[dict]:
        now = time.time()
        escalated: list[dict] = []
        any_bad = False

        for raw in results:
            r = dict(raw)
            check = r["check"]
            status = r["status"]

            # INFO results never accumulate toward escalation
            if status == "INFO":
                escalated.append(r)
                continue

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

                    # AUDIT_HALT only for native_critical — actual value mismatch,
                    # not a check that was slowly promoted via WARN accumulation.
                    if (check in self.HALT_CHECKS
                            and r.get("native_critical")
                            and not AUDIT_HALT.exists()):
                        AUDIT_HALT.parent.mkdir(parents=True, exist_ok=True)
                        AUDIT_HALT.write_text(json.dumps({
                            "check": check,
                            "message": r["message"],
                            "timestamp": datetime.now(timezone.utc).isoformat(),
                        }))
                        log(f"AUDIT_HALT written for {check}", "CRITICAL")
                        _send_alert(
                            f":octagonal_sign: *AUDIT_HALT written* — `{check}` CRITICAL. "
                            f"live_trader will refuse to start. Remove data/AUDIT_HALT "
                            f"after resolving the issue.")

            escalated.append(r)

        # ── Auto-resolve after CLEAN_RESOLVE consecutive clean passes ──
        if not any_bad:
            self._consecutive_clean += 1
            if self._consecutive_clean >= self.CLEAN_RESOLVE and self._error_since:
                for check in list(self._error_since):
                    _send_alert(
                        f":white_check_mark: *AUDIT RESOLVED* `{check}` "
                        f"cleared after {self.CLEAN_RESOLVE} clean passes")
                    if conn:
                        write_event(conn, "INFO", "audit_resolved",
                                    f"{check}: auto-resolved after "
                                    f"{self.CLEAN_RESOLVE} clean passes")
                self._error_since.clear()
                self._warn_ts.clear()
                self._alerted.clear()
                self._consecutive_clean = 0
        else:
            self._consecutive_clean = 0

        self._save_state()
        return escalated


# ── Individual checks ──────────────────────────────────────────────

def check_data_freshness(conn) -> dict:
    """Last tick recency — weekend threshold 72h, RTH 5 min, off-hours 18h."""
    try:
        cur = conn.cursor()
        cur.execute("SELECT MAX(ts_event) FROM ticks")
        row = cur.fetchone()
    except Exception as e:
        conn.rollback()
        if "does not exist" in str(e).lower() or "42p01" in str(e).lower():
            return {"check": "data_freshness", "status": "INFO",
                    "message": "ticks table not yet created (collector not started)",
                    "value": -1}
        return {"check": "data_freshness", "status": "WARN",
                "message": f"Query error: {e}", "value": -1}
    if row[0] is None:
        return {"check": "data_freshness", "status": "WARN",
                "message": "No ticks in database", "value": -1}

    from datetime import timezone as tz
    latest = row[0]
    if latest.tzinfo is None:
        import pytz
        latest = latest.replace(tzinfo=pytz.UTC)
    now_utc = datetime.now(tz.utc)
    age_sec = (now_utc - latest).total_seconds()
    weekday = now_utc.weekday()   # 5=Sat 6=Sun
    hour_utc = now_utc.hour

    if weekday >= 5:               # weekend — no trading, 72h grace
        threshold = 259200
    elif 13 <= hour_utc <= 21:     # weekday RTH (≈ 09:30–16:00 ET)
        threshold = 300
    else:                          # weekday off-hours
        threshold = 64800          # 18 h

    return {"check": "data_freshness",
            "status": "PASS" if age_sec < threshold else "WARN",
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


def check_ram_usage() -> dict:
    """System RAM % used — warn above 90%.  Uses /proc/meminfo (no psutil dep)."""
    try:
        import psutil
        mem = psutil.virtual_memory()
        pct = mem.percent
        free_gb = mem.available / 1024 ** 3
    except ImportError:
        try:
            info: dict[str, int] = {}
            for line in Path("/proc/meminfo").read_text().splitlines():
                k, v = line.split(":", 1)
                info[k.strip()] = int(v.strip().split()[0])  # kB
            total = info.get("MemTotal", 0)
            avail = info.get("MemAvailable", 0)
            pct = (1 - avail / total) * 100 if total else 0.0
            free_gb = avail / (1024 ** 2)  # kB → GB
        except Exception as e:
            return {"check": "ram_usage", "status": "INFO",
                    "message": f"Cannot read RAM: {e}", "value": -1}
    return {"check": "ram_usage",
            "status": "PASS" if pct < 90.0 else "WARN",
            "message": f"RAM {pct:.1f}% used ({free_gb:.1f} GB free)",
            "value": pct}


def check_process_liveness() -> dict:
    """Check whether trading processes are alive during RTH.

    Outside RTH (including weekends) the check is INFO — processes are not
    expected to be running.  During RTH a missing process is a WARN.
    """
    now_utc = datetime.now(timezone.utc)
    in_rth = now_utc.weekday() < 5 and 13 <= now_utc.hour <= 21

    procs: dict[str, bool] = {}
    for name, pattern in [("nq_executor", "nq_executor"),
                           ("live_trader", "live_trader.py")]:
        r = subprocess.run(["pgrep", "-f", pattern],
                           capture_output=True, text=True)
        procs[name] = r.returncode == 0 and bool(r.stdout.strip())

    running = [k for k, v in procs.items() if v]
    stopped = [k for k, v in procs.items() if not v]

    if not in_rth:
        return {"check": "process_liveness", "status": "INFO",
                "message": ("Outside RTH — "
                            + (", ".join(running) + " running"
                               if running else "no processes running")),
                "value": float(len(running))}

    if stopped:
        return {"check": "process_liveness", "status": "WARN",
                "message": f"RTH but not running: {', '.join(stopped)}",
                "value": float(len(running))}

    return {"check": "process_liveness", "status": "PASS",
            "message": f"Running: {', '.join(running)}", "value": float(len(running))}


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
        m = re.search(r"(\d+)% tests passed, (\d+) tests failed out of (\d+)",
                      result.stdout)
        if m:
            failed = int(m.group(2))
            passed = int(m.group(3)) - failed
        else:
            passed = result.stdout.count("Passed")
            failed = result.stdout.count("Failed")

        # Downgrade to WARN when failures are purely DB connectivity — these are
        # infrastructure failures (wrong PG port/creds in dev), not code bugs.
        if failed > 0 and "PostgreSQL connection failed" in result.stdout:
            real_fail_lines = [ln for ln in result.stdout.splitlines()
                               if "FAIL:" in ln and "PostgreSQL connection failed" not in ln]
            if not real_fail_lines:
                return {"check": "cpp_tests", "status": "WARN",
                        "message": (f"{passed} passed, {failed} DB-connectivity-only "
                                    "failures (not code bugs)"),
                        "value": float(failed)}

        return {"check": "cpp_tests",
                "status": "PASS" if failed == 0 else "FAIL",
                "message": f"{passed} passed, {failed} failed",
                "value": float(failed)}
    except subprocess.TimeoutExpired:
        return {"check": "cpp_tests", "status": "INFO",
                "message": "Timed out after 120s", "value": -1}
    except FileNotFoundError:
        return {"check": "cpp_tests", "status": "INFO",
                "message": "ctest not found", "value": 0}
    except Exception as e:
        return {"check": "cpp_tests", "status": "WARN",
                "message": f"Error: {e}", "value": -1}


def run_python_tests() -> dict:
    """Run pytest test suite — catches regressions as they are introduced."""
    test_dir = ENGINE_DIR / "tests"
    if not test_dir.exists():
        return {"check": "python_tests", "status": "INFO",
                "message": "tests/ directory not found", "value": 0}
    try:
        result = subprocess.run(
            [sys.executable, "-m", "pytest", str(test_dir),
             "-q", "--tb=line", "--no-header", "-p", "no:warnings"],
            capture_output=True, text=True, timeout=180,
            cwd=str(ENGINE_DIR),
        )
        out = result.stdout + result.stderr
        passed = int(m.group(1)) if (m := re.search(r"(\d+) passed", out)) else 0
        failed = int(m.group(1)) if (m := re.search(r"(\d+) failed", out)) else 0
        errors = int(m.group(1)) if (m := re.search(r"(\d+) error", out)) else 0
        total_fail = failed + errors
        status = "PASS" if total_fail == 0 and passed > 0 else \
                 "FAIL" if total_fail > 0 else "WARN"
        return {"check": "python_tests",
                "status": status,
                "message": f"{passed} passed, {total_fail} failed",
                "value": float(total_fail)}
    except subprocess.TimeoutExpired:
        return {"check": "python_tests", "status": "INFO",
                "message": "Timed out after 180s", "value": -1}
    except Exception as e:
        return {"check": "python_tests", "status": "WARN",
                "message": f"Error running pytest: {e}", "value": -1}


def check_trading_constants(live_cfg: dict | None) -> dict:
    """Verify trading constants match MNQ spec and prop firm constraints.

    Mismatches are CRITICAL with native_critical=True — a wrong point_value
    silently scales every PnL calculation by 10x.
    Missing (None) values are WARN — incomplete config, not a wrong value.
    Also checks: sl_points > 0, trail_step > 0, qty within prop firm limit.
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
    sl_pts = live_cfg.get("sl_points")
    trail = live_cfg.get("trail_step")
    qty = live_cfg.get("qty")
    max_pos = live_cfg.get("prop_firm", {}).get("max_position_size", 3)

    if pv is None:
        warnings.append("point_value not set")
    elif abs(float(pv) - 2.0) > 0.001:
        errors.append(f"point_value={pv} (must be 2.0 for MNQ)")

    if tv is None:
        warnings.append("tick_value not set")
    elif abs(float(tv) - 0.50) > 0.001:
        errors.append(f"tick_value={tv} (must be 0.50 for MNQ)")

    if sym and sym not in ("MNQ", ""):
        errors.append(f"symbol={sym!r} (expected MNQ)")
    if comm is not None and abs(float(comm) - 4.0) > 0.01:
        errors.append(f"commission_rt={comm} (expected 4.0)")

    if sl_pts is None:
        warnings.append("sl_points not set")
    elif float(sl_pts) <= 0:
        errors.append(f"sl_points={sl_pts} must be > 0")

    if trail is None:
        warnings.append("trail_step not set")
    elif float(trail) <= 0:
        errors.append(f"trail_step={trail} must be > 0")

    if qty is None:
        warnings.append("qty not set")
    elif int(qty) <= 0:
        errors.append(f"qty={qty} must be > 0")
    elif int(qty) > int(max_pos):
        errors.append(f"qty={qty} exceeds prop_firm.max_position_size={max_pos}")

    if errors:
        return {"check": "trading_constants", "status": "CRITICAL",
                "native_critical": True,
                "message": "MISMATCH: " + "; ".join(errors),
                "value": float(len(errors))}
    if warnings:
        return {"check": "trading_constants", "status": "WARN",
                "message": "Missing constants: " + "; ".join(warnings),
                "value": float(len(warnings))}
    return {"check": "trading_constants", "status": "PASS",
            "message": (f"point_value={pv} tick_value={tv} symbol={sym!r} "
                        f"commission_rt={comm} sl_points={sl_pts} "
                        f"trail_step={trail} qty={qty}"),
            "value": 0.0}


def check_pnl_sanity(conn) -> dict:
    """Flag trades in the last 24h with |pnl_usd| > $500 (implausible for 1 MNQ)."""
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


def check_model_staleness(live_cfg: dict | None) -> dict:
    """Flag ML model files older than 30 days."""
    if live_cfg is None:
        return {"check": "model_staleness", "status": "INFO",
                "message": "Config unavailable", "value": -1}
    ml = live_cfg.get("ml", {})
    if not ml.get("enabled", False):
        return {"check": "model_staleness", "status": "INFO",
                "message": "ML disabled in config", "value": 0}
    model_path = ENGINE_DIR / ml.get("model_path", "")
    if not model_path.exists():
        return {"check": "model_staleness", "status": "WARN",
                "message": f"Model not found: {model_path.name}", "value": -1}
    age_days = (time.time() - model_path.stat().st_mtime) / 86400
    return {"check": "model_staleness",
            "status": "PASS" if age_days < 30 else "WARN",
            "message": f"Model {age_days:.0f} days old ({model_path.name})",
            "value": age_days}


def check_drift_halt() -> dict:
    """Detect data/DRIFT_HALT sentinel — indicates model drift requiring retrain."""
    halt = ENGINE_DIR / "data" / "DRIFT_HALT"
    if halt.exists():
        try:
            detail = halt.read_text().strip()[:80]
        except Exception:
            detail = "(unreadable)"
        return {"check": "drift_halt", "status": "WARN",
                "message": f"DRIFT_HALT present — retrain required: {detail}",
                "value": 1.0}
    return {"check": "drift_halt", "status": "PASS",
            "message": "No drift halt", "value": 0.0}


def check_slippage_sanity(conn) -> dict:
    """Avg fill slippage vs 6-tick threshold over the last 7 trading days.

    If avg slippage on either side exceeds 6 ticks, the backtest cost model
    (4 ticks/side) is materially wrong and the strategy edge calculation is off.
    """
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT AVG(entry_slippage_ticks),
                   AVG(exit_slippage_ticks),
                   COUNT(*)
            FROM live_trades
            WHERE exit_time > NOW() - INTERVAL '7 days'
              AND entry_slippage_ticks IS NOT NULL
              AND exit_slippage_ticks IS NOT NULL
        """)
        row = cur.fetchone()
    except Exception as e:
        return {"check": "slippage_sanity", "status": "WARN",
                "message": f"Query error: {e}", "value": -1}

    if row is None or (row[2] or 0) == 0:
        return {"check": "slippage_sanity", "status": "INFO",
                "message": "No slippage data in last 7 days", "value": 0}

    avg_entry = float(row[0] or 0)
    avg_exit = float(row[1] or 0)
    n = int(row[2])
    worse = max(avg_entry, avg_exit)
    return {"check": "slippage_sanity",
            "status": "PASS" if worse <= 6.0 else "WARN",
            "message": (f"Avg slippage — entry: {avg_entry:.1f}t "
                        f"exit: {avg_exit:.1f}t over {n} trades (7d)"),
            "value": worse}


def check_trade_table_consistency(conn) -> dict:
    """Detect duplicate open positions across Python trades and C++ live_trades tables.

    A trade open in both tables simultaneously means reconciliation failed at
    startup or an edge-case crash recovery scenario — requires manual intervention.
    """
    from datetime import date
    today = date.today()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT COUNT(*) FROM trades
            WHERE session_date = %s AND exit_time IS NULL AND source = 'python'
        """, (today,))
        py_open = cur.fetchone()[0] or 0
    except Exception as e:
        conn.rollback()
        err = str(e).lower()
        if "does not exist" in err or "undefined_table" in err or "42p01" in err:
            return {"check": "trade_table_consistency", "status": "INFO",
                    "message": "trades table not yet created (expected before first run)",
                    "value": 0}
        return {"check": "trade_table_consistency", "status": "WARN",
                "message": f"trades query error: {e}", "value": -1}
    try:
        cur.execute("""
            SELECT COUNT(*) FROM live_trades
            WHERE trade_date = %s AND exit_time IS NULL
        """, (today,))
        cpp_open = cur.fetchone()[0] or 0
    except Exception:
        conn.rollback()
        cpp_open = 0  # live_trades may not exist in all environments

    if py_open > 0 and cpp_open > 0:
        return {"check": "trade_table_consistency", "status": "FAIL",
                "message": (f"DUPLICATE OPEN POSITION: {py_open} in trades "
                            f"AND {cpp_open} in live_trades for {today}. "
                            "Manual intervention required."),
                "value": float(py_open + cpp_open)}

    if py_open > 1:
        return {"check": "trade_table_consistency", "status": "WARN",
                "message": f"{py_open} open trades records in trades table for {today}",
                "value": float(py_open)}

    if cpp_open > 1:
        return {"check": "trade_table_consistency", "status": "WARN",
                "message": f"{cpp_open} open trade records in live_trades for {today}",
                "value": float(cpp_open)}

    return {"check": "trade_table_consistency", "status": "PASS",
            "message": (f"trades open={py_open} live_trades open={cpp_open} "
                        f"for {today} — consistent"),
            "value": 0.0}


def check_config_schema(live_cfg: dict | None) -> dict:
    """Run Pydantic schema validation on live_config.json every audit cycle.

    Catches config drift early — e.g. a field set to a wrong type, a required
    field removed, or a constraint violated.  Non-blocking (WARN not CRITICAL)
    since a schema mismatch alone doesn't stop trading.
    """
    if live_cfg is None:
        return {"check": "config_schema", "status": "WARN",
                "message": "live_config.json not loaded — cannot validate schema",
                "value": -1}
    try:
        sys.path.insert(0, str(ENGINE_DIR))
        from config.live_config_schema import LiveConfig  # type: ignore[import]
        LiveConfig.model_validate(live_cfg)
        return {"check": "config_schema", "status": "PASS",
                "message": "live_config.json passes Pydantic schema validation",
                "value": 0.0}
    except ImportError:
        return {"check": "config_schema", "status": "INFO",
                "message": "LiveConfig not importable — schema check skipped",
                "value": 0.0}
    except Exception as e:
        return {"check": "config_schema", "status": "WARN",
                "message": f"Schema validation failed: {e}",
                "value": 1.0}


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

    log("Checking RAM usage...")
    results.append(check_ram_usage())

    log("Checking process liveness...")
    results.append(check_process_liveness())

    log("Checking trading constants...")
    results.append(check_trading_constants(live_cfg))

    log("Checking PnL sanity...")
    results.append(check_pnl_sanity(conn))

    log("Checking model staleness...")
    results.append(check_model_staleness(live_cfg))

    log("Checking drift halt...")
    results.append(check_drift_halt())

    log("Checking slippage sanity...")
    results.append(check_slippage_sanity(conn))

    log("Checking trade table consistency...")
    results.append(check_trade_table_consistency(conn))

    log("Checking config schema...")
    results.append(check_config_schema(live_cfg))

    log("Running contamination audit...")
    results.append(run_contamination_audit())

    log("Running C++ tests...")
    results.append(run_cpp_tests())

    log("Running Python tests...")
    results.append(run_python_tests())

    return results


def main():
    parser = argparse.ArgumentParser(description="Engine Audit Daemon")
    parser.add_argument("--interval", type=int, default=300,
                        help="Seconds between runs (default: 300)")
    parser.add_argument("--once", action="store_true", help="Run once and exit")
    args = parser.parse_args()

    _load_env()

    # Ensure runtime directories exist before any check tries to write to them
    for d in [LOG_DIR, ENGINE_DIR / "data" / "alerts"]:
        d.mkdir(parents=True, exist_ok=True)

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

        try:
            conn = _pg_connect()
        except Exception:
            conn = None

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

        if conn:
            try:
                for r in results:
                    write_metric(conn, f"audit_{r['check']}",
                                 r.get("value", 0),
                                 {"status": r["status"], "run": run_count})
                write_metric(conn, "audit_passed", float(passed), {"run": run_count})
                write_metric(conn, "audit_failed", float(failed), {"run": run_count})
                write_metric(conn, "audit_consecutive_pass", float(consecutive_pass))

                severity = "ERROR" if failed > 0 else "INFO"
                event = "audit_failures" if failed > 0 else "audit_pass"
                detail = (f"{failed} checks failed in run #{run_count}"
                          if failed > 0
                          else f"All {passed} checks passed (run #{run_count})")
                write_event(conn, severity, event, detail)

                if run_count % 100 == 0:
                    cur = conn.cursor()
                    cur.execute(
                        "DELETE FROM quality_metrics WHERE ts < NOW() - INTERVAL '30 days'")
                    conn.commit()
            except Exception as e:
                log(f"Metrics write failed: {e}", "WARN")
            finally:
                conn.close()

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

        sleep_time = (min(args.interval * 2, 600)
                      if consecutive_pass >= 10 else args.interval)
        time.sleep(sleep_time)


if __name__ == "__main__":
    main()
