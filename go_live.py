#!/usr/bin/env python3
"""
go_live.py — Formal paper→live promotion script for the NQ ORB trading system.

Usage
-----
    python go_live.py              # Run all pre-flight gates, print results
    python go_live.py --confirm-live   # Promote to live if all gates pass

Gates (all must pass before promotion)
---------------------------------------
A. NO_DEPLOY lockfile not present
B. config/live_config.json exists and is valid JSON
C. dry_run is currently True (must be in paper mode before promoting)
D. PostgreSQL connection succeeds
E. Rithmic SSL cert file exists
F. ML model file exists (when ml.enabled is True)
G. Disk space > 5 GB on working directory filesystem
H. No data/DRIFT_HALT file present
I. Prop firm limits set (daily_loss_limit > 0, max_position_size > 0)
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import shutil
import sys
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

log = logging.getLogger(__name__)

# ── constants ─────────────────────────────────────────────────────────────────

CONFIG_PATH   = Path("config/live_config.json")
DRIFT_HALT    = Path("data/DRIFT_HALT")
DISK_MIN_BYTES = 5 * 1024 ** 3   # 5 GiB

_GREEN  = "\033[32m"
_RED    = "\033[31m"
_YELLOW = "\033[33m"
_RESET  = "\033[0m"

_PASS = f"{_GREEN}PASS{_RESET}"
_FAIL = f"{_RED}FAIL{_RESET}"
_SKIP = f"{_YELLOW}SKIP{_RESET}"


# ── gate result dataclass ─────────────────────────────────────────────────────

@dataclass
class GateResult:
    label: str
    passed: bool
    detail: str = ""

    def display(self) -> str:
        status = _PASS if self.passed else _FAIL
        pad = max(0, 52 - len(self.label))
        detail = f"  {self.detail}" if self.detail else ""
        return f"  {self.label}{' ' * pad}{status}{detail}"


# ── database helper ───────────────────────────────────────────────────────────

def _check_db_connection(cfg: dict) -> tuple[bool, str]:
    """Attempt a PostgreSQL connection using environment variables from config."""
    db_cfg = cfg.get("db", {})

    def _env(key_name: str, default: str = "") -> str:
        env_key = db_cfg.get(key_name, "")
        return os.environ.get(env_key, default)

    host     = _env("host_env",     "localhost")
    port     = int(_env("port_env", "5432") or 5432)
    dbname   = _env("dbname_env",   "rithmic")
    user     = _env("user_env",     "rithmic_user")
    password = _env("password_env", "")
    timeout  = int(db_cfg.get("connect_timeout", 10))

    try:
        import psycopg2
        conn = psycopg2.connect(
            host=host,
            port=port,
            dbname=dbname,
            user=user,
            password=password,
            connect_timeout=timeout,
        )
        conn.close()
        return True, f"{host}:{port}/{dbname}"
    except Exception as exc:
        return False, str(exc)


# ── alert helper ──────────────────────────────────────────────────────────────

def _send_alert(cfg: dict, message: str) -> None:
    """Send an alert if configured.  Failures are logged but do not abort."""
    alert_cfg = cfg.get("alerts", {})
    if not alert_cfg.get("enabled", False):
        log.info("Alert (not sent — alerts disabled): %s", message)
        return

    webhook_env = alert_cfg.get("slack_webhook_env", "SLACK_WEBHOOK_URL")
    webhook_url = os.environ.get(webhook_env, "")
    if not webhook_url:
        log.warning("Alert webhook env var '%s' not set — skipping alert", webhook_env)
        return

    try:
        import urllib.request
        payload = json.dumps({"text": message}).encode()
        req = urllib.request.Request(
            webhook_url,
            data=payload,
            headers={"Content-Type": "application/json"},
        )
        urllib.request.urlopen(req, timeout=10)
    except Exception as exc:
        log.warning("Alert delivery failed (non-fatal): %s", exc)


# ── individual gate checks ────────────────────────────────────────────────────

def _gate_no_deploy(cfg: dict) -> GateResult:
    lock_path = Path(cfg.get("no_deploy_path", "NO_DEPLOY"))
    if lock_path.exists():
        try:
            data = json.loads(lock_path.read_text())
            reason = data.get("reason", "(unknown)")
            ts     = data.get("timestamp", "")
            detail = f"{reason} (at {ts})" if ts else reason
        except (json.JSONDecodeError, OSError):
            detail = lock_path.read_text().strip()[:120] or "(empty lockfile)"
        return GateResult("A. NO_DEPLOY lockfile absent", False, detail)
    return GateResult("A. NO_DEPLOY lockfile absent", True)


def _gate_config_valid(cfg: dict) -> GateResult:
    # Already parsed at this point; this gate was implicitly satisfied by _load_config().
    return GateResult("B. config/live_config.json valid JSON", True)


def _gate_dry_run(cfg: dict) -> GateResult:
    dry = cfg.get("dry_run")
    if dry is True:
        return GateResult("C. dry_run currently True (paper mode)", True)
    return GateResult(
        "C. dry_run currently True (paper mode)",
        False,
        f"dry_run={dry!r} — system is already in live mode or misconfigured",
    )


def _gate_db(cfg: dict) -> GateResult:
    ok, detail = _check_db_connection(cfg)
    return GateResult("D. PostgreSQL connection", ok, detail)


def _gate_cert(cfg: dict) -> GateResult:
    cert = Path(cfg.get("rithmic", {}).get("ssl_cert_path", ""))
    if cert and cert.exists():
        return GateResult("E. Rithmic SSL cert file exists", True, str(cert))
    return GateResult("E. Rithmic SSL cert file exists", False, f"not found: {cert}")


def _gate_ml_model(cfg: dict) -> GateResult:
    ml = cfg.get("ml", {})
    if not ml.get("enabled", False):
        return GateResult("F. ML model file exists", True, "ML disabled — skipped")
    model_path = Path(ml.get("model_path", ""))
    if model_path and model_path.exists():
        return GateResult("F. ML model file exists", True, str(model_path))
    return GateResult("F. ML model file exists", False, f"not found: {model_path}")


def _gate_disk_space(_cfg: dict) -> GateResult:
    usage = shutil.disk_usage(".")
    free_gb = usage.free / 1024 ** 3
    ok = usage.free >= DISK_MIN_BYTES
    return GateResult(
        "G. Disk space > 5 GB",
        ok,
        f"{free_gb:.1f} GB free" + ("" if ok else " — below 5 GB minimum"),
    )


def _gate_drift_halt(_cfg: dict) -> GateResult:
    if DRIFT_HALT.exists():
        try:
            detail = DRIFT_HALT.read_text().strip()[:120]
        except OSError:
            detail = "(unreadable)"
        return GateResult("H. No DRIFT_HALT file", False, detail)
    return GateResult("H. No DRIFT_HALT file", True)


def _gate_prop_firm(cfg: dict) -> GateResult:
    pf = cfg.get("prop_firm", {})
    dll = float(pf.get("daily_loss_limit", 0) or 0)
    mps = float(pf.get("max_position_size", 0) or 0)
    if dll > 0 and mps > 0:
        return GateResult(
            "I. Prop firm limits set",
            True,
            f"DLL={dll:,.0f}  max_pos={mps:.0f}",
        )
    missing = []
    if dll <= 0:
        missing.append("daily_loss_limit=0")
    if mps <= 0:
        missing.append("max_position_size=0")
    return GateResult("I. Prop firm limits set", False, ", ".join(missing))


_ALL_GATES = [
    _gate_no_deploy,
    _gate_config_valid,
    _gate_dry_run,
    _gate_db,
    _gate_cert,
    _gate_ml_model,
    _gate_disk_space,
    _gate_drift_halt,
    _gate_prop_firm,
]


# ── config loader ─────────────────────────────────────────────────────────────

def _load_config() -> Optional[dict]:
    """Load and parse config/live_config.json.  Returns None on any failure."""
    if not CONFIG_PATH.exists():
        return None
    try:
        return json.loads(CONFIG_PATH.read_text())
    except (json.JSONDecodeError, OSError):
        return None


# ── atomic config promotion ───────────────────────────────────────────────────

def _promote_config(cfg: dict) -> None:
    """Write dry_run: false atomically to config/live_config.json."""
    cfg["dry_run"] = False
    updated_text = json.dumps(cfg, indent=2)
    # Write to a temp file in the same directory, then os.replace for atomicity.
    tmp_fd, tmp_path = tempfile.mkstemp(
        dir=CONFIG_PATH.parent,
        prefix=".live_config_tmp_",
        suffix=".json",
    )
    try:
        with os.fdopen(tmp_fd, "w") as fh:
            fh.write(updated_text)
        os.replace(tmp_path, CONFIG_PATH)
    except Exception:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise


# ── public entry points ───────────────────────────────────────────────────────

def run_preflight(args: list[str]) -> tuple[int, list[GateResult]]:
    """Run all pre-flight gates.

    Returns (exit_code, gate_results).  exit_code 0 means all gates passed
    (promotion may or may not have happened depending on --confirm-live).
    """
    parsed = _build_parser().parse_args(args)

    cfg = _load_config()
    if cfg is None:
        print(
            f"{_FAIL}  Cannot load config/live_config.json — "
            "file missing or invalid JSON.",
            file=sys.stderr,
        )
        return 1, []

    # Gate A (NO_DEPLOY) is checked separately so we can fail fast and clearly.
    no_deploy_result = _gate_no_deploy(cfg)
    if not no_deploy_result.passed:
        print("\n  Pre-flight aborted: NO_DEPLOY lockfile is active.")
        print(no_deploy_result.display())
        print(
            "\n  Clear with:  python scripts/no_deploy.py clear "
            "--authorized-by <operator-name>\n"
        )
        return 1, [no_deploy_result]

    # Run remaining gates
    results: list[GateResult] = [no_deploy_result]
    for gate_fn in _ALL_GATES[1:]:
        results.append(gate_fn(cfg))

    # Print checklist
    print("\n  ── Pre-flight checklist ──────────────────────────────────────")
    for r in results:
        print(r.display())
    print()

    all_passed = all(r.passed for r in results)

    if not all_passed:
        failed = [r.label for r in results if not r.passed]
        print(f"  {_FAIL}  {len(failed)} gate(s) failed. Live trading NOT enabled.\n")
        return 1, results

    if not parsed.confirm_live:
        print(
            f"  {_PASS}  All checks passed.\n"
            "  Add --confirm-live to enable live trading.\n"
        )
        return 0, results

    # Promote
    try:
        _promote_config(cfg)
    except Exception as exc:
        print(f"  {_FAIL}  Failed to write config: {exc}\n", file=sys.stderr)
        return 1, results

    _send_alert(cfg, "LIVE TRADING ENABLED via go_live.py — monitor closely.")
    print(
        f"  {_GREEN}LIVE TRADING ENABLED{_RESET} — "
        "config/live_config.json updated.  Monitor closely.\n"
    )
    log.warning("Live trading enabled via go_live.py")
    return 0, results


def main(args: Optional[list[str]] = None) -> None:
    if args is None:
        args = sys.argv[1:]
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s  %(message)s",
        datefmt="%H:%M:%S",
    )
    code, _ = run_preflight(args)
    sys.exit(code)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="go_live.py",
        description=(
            "Formal paper→live promotion script.\n\n"
            "Runs all pre-flight safety gates before enabling live trading.\n"
            "Without --confirm-live the script is read-only (safe to run any time).\n\n"
            "Gates:\n"
            "  A. NO_DEPLOY lockfile absent\n"
            "  B. config/live_config.json valid JSON\n"
            "  C. dry_run currently True\n"
            "  D. PostgreSQL connection\n"
            "  E. Rithmic SSL cert file exists\n"
            "  F. ML model file exists (when enabled)\n"
            "  G. Disk space > 5 GB\n"
            "  H. No data/DRIFT_HALT file\n"
            "  I. Prop firm limits set (daily_loss_limit > 0, max_position_size > 0)\n"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--confirm-live",
        action="store_true",
        default=False,
        help=(
            "REQUIRED to actually enable live trading. "
            "Without this flag the script is read-only. "
            "All gates must pass before this flag has any effect."
        ),
    )
    return parser


if __name__ == "__main__":
    main()
