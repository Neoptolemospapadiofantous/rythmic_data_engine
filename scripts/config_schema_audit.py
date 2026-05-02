#!/usr/bin/env python3
"""
Config Schema Audit — validates live_config.json against the Pydantic schema.

Catches:
  - Missing required keys (e.g. 'trale_route' typo → 'trade_route' missing)
  - Wrong types (string where float expected)
  - MNQ invariants (point_value=2.0, tick_size=0.25, symbol=MNQ)
  - trade_route != 'simulator'
  - Flat/nested consistency (max_daily_trades, trailing_drawdown_cap, etc.)
  - sl_points vs orb.stop_loss_ticks agreement

Usage:
  python scripts/config_schema_audit.py           # validate live_config.json
  python scripts/config_schema_audit.py --json    # JSON output
"""
import argparse
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
CONFIG_PATH = PROJECT_ROOT / "config" / "live_config.json"
sys.path.insert(0, str(PROJECT_ROOT))


def _result(check: str, status: str, severity: str, msg: str) -> dict:
    return {"check": check, "status": status, "severity": severity, "message": msg}


def run_audit() -> list[dict]:
    findings = []

    try:
        from config.live_config_schema import validate_config
    except ImportError as e:
        findings.append(_result("schema_import", "FAIL", "CRITICAL",
            f"Cannot import config schema: {e} — run: pip install pydantic"))
        return findings

    if not CONFIG_PATH.exists():
        findings.append(_result("schema_file", "FAIL", "CRITICAL",
            f"live_config.json not found at {CONFIG_PATH}"))
        return findings

    ok, errors = validate_config(CONFIG_PATH)

    if ok:
        findings.append(_result("config_schema", "PASS", "INFO",
            "live_config.json passes full Pydantic schema validation"))
    else:
        # Parse pydantic error lines into individual findings
        current_field = None
        for line in errors:
            line = line.strip()
            if not line:
                continue
            # pydantic v2 errors look like: "  field_name\n    Value error, ..."
            if line.startswith("Value error,") or line.startswith("value is not"):
                msg = line.replace("Value error, ", "")
                check_id = f"schema_{current_field or 'unknown'}"
                findings.append(_result(check_id, "FAIL", "CRITICAL",
                    f"[{current_field or '?'}] {msg}"))
            elif line and not line[0].isdigit() and "validation error" not in line.lower():
                current_field = line.split("\n")[0].strip().rstrip("[]").replace(" ", "_")

        if not any(f["status"] == "FAIL" for f in findings):
            # Fallback: just emit the raw error block as one finding
            findings.append(_result("config_schema", "FAIL", "CRITICAL",
                "live_config.json schema violations:\n" + "\n".join(errors[:20])))

    return findings


def main() -> int:
    parser = argparse.ArgumentParser(description="Config Schema Audit — live_config.json validation")
    parser.add_argument("--json", action="store_true", help="JSON output")
    args = parser.parse_args()

    findings = run_audit()

    passed = sum(1 for f in findings if f["status"] == "PASS")
    failed = sum(1 for f in findings if f["status"] == "FAIL")
    warned = sum(1 for f in findings if f["status"] == "WARN")

    if args.json:
        print(json.dumps(findings, indent=2))
        return 1 if failed > 0 else 0

    print(f"\n{'='*60}")
    print("  CONFIG SCHEMA AUDIT")
    print(f"  {passed} passed, {failed} failed, {warned} warnings, {len(findings)} total")
    print(f"{'='*60}")

    for f in findings:
        tag = {"PASS": "PASS", "FAIL": "FAIL", "WARN": "WARN"}.get(f["status"], "INFO")
        print(f"  {tag}  [{f['check']}] {f['message']}")

    print(f"\n  Status: {'FAIL' if failed > 0 else 'PASS'}")
    return 1 if failed > 0 else 0


if __name__ == "__main__":
    sys.exit(main())
