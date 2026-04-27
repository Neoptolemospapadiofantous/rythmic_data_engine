#!/usr/bin/env python3
"""
Cross-System Audit — verifies constant consistency between C++ and Python for MNQ ORB.

Checks:
  1. NQ_TICK_VALUE in orb_config.hpp (5.0 is NQ, MNQ should be 0.50 — latency logger is 10x inflated)
  2. OrbConfig::point_value default in orb_config.hpp (expected 2.0 for MNQ)
  3. live_config.json symbol vs C++ OrbConfig symbol default (INFO — runtime override is fine)
  4. live_trader.py point_value fallback default (20.0 = NQ, FAIL — missing config causes 10x error)
  5. strategy/micro_orb.py hardcoded point_value outside comments (20.0 = NQ, WARN)

Usage:
  python scripts/cross_system_audit.py           # run all checks
  python scripts/cross_system_audit.py --json    # JSON output
"""
import argparse
import json
import re
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent

import yaml

RULES_PATH = PROJECT_ROOT / "quality_rules" / "cross_system.yaml"
CONFIG_PATH = PROJECT_ROOT / "config" / "live_config.json"
ORB_CONFIG_HPP = PROJECT_ROOT / "src" / "execution" / "orb_config.hpp"
LIVE_TRADER_PY = PROJECT_ROOT / "live_trader.py"
MICRO_ORB_PY = PROJECT_ROOT / "strategy" / "micro_orb.py"

MNQ_TICK_VALUE = 0.50   # MNQ: $0.50/tick
MNQ_POINT_VALUE = 2.0   # MNQ: $2.00/point


def _pass(check: str, msg: str) -> dict:
    return {"check": check, "severity": "INFO", "message": msg, "status": "PASS"}


def _fail(check: str, msg: str) -> dict:
    return {"check": check, "severity": "CRITICAL", "message": msg, "status": "FAIL"}


def _warn(check: str, msg: str) -> dict:
    return {"check": check, "severity": "WARN", "message": msg, "status": "WARN"}


def _info(check: str, msg: str) -> dict:
    return {"check": check, "severity": "INFO", "message": msg, "status": "INFO"}


def check_cpp_tick_value() -> list[dict]:
    """Check 1: MNQ_TICK_VALUE in orb_config.hpp and latency_logger.hpp uses it."""
    findings = []
    if not ORB_CONFIG_HPP.exists():
        findings.append(_fail("cpp_mnq_tick_value",
            f"orb_config.hpp not found at {ORB_CONFIG_HPP}"))
        return findings

    config_content = ORB_CONFIG_HPP.read_text()

    # Check MNQ_TICK_VALUE constant exists and equals 0.50
    m = re.search(r'inline constexpr double MNQ_TICK_VALUE\s*=\s*([\d.]+)', config_content)
    if not m:
        findings.append(_fail("cpp_mnq_tick_value",
            "MNQ_TICK_VALUE not found in orb_config.hpp — "
            "latency logger slippage_usd may be wrong for MNQ"))
    else:
        val = float(m.group(1))
        if abs(val - MNQ_TICK_VALUE) < 0.001:
            findings.append(_pass("cpp_mnq_tick_value",
                f"MNQ_TICK_VALUE={val} correct (${val:.2f}/tick for MNQ)"))
        else:
            findings.append(_fail("cpp_mnq_tick_value",
                f"MNQ_TICK_VALUE={val} — expected {MNQ_TICK_VALUE} for MNQ"))

    # Check latency_logger.hpp uses MNQ_TICK_VALUE (not NQ_TICK_VALUE) for slippage_usd
    lat_path = PROJECT_ROOT / "src" / "execution" / "latency_logger.hpp"
    if lat_path.exists():
        lat_content = lat_path.read_text()
        # Look for slippage_usd = ... * <value>
        slippage_m = re.search(r'slippage_usd\s*=\s*r\.slippage_ticks\s*\*\s*(\w+)', lat_content)
        if slippage_m:
            used_const = slippage_m.group(1)
            if used_const == "MNQ_TICK_VALUE":
                findings.append(_pass("latency_logger_uses_mnq",
                    f"latency_logger.hpp uses MNQ_TICK_VALUE for slippage_usd (correct for MNQ)"))
            elif used_const == "NQ_TICK_VALUE":
                findings.append(_fail("latency_logger_uses_mnq",
                    f"latency_logger.hpp uses NQ_TICK_VALUE ($5.00) for slippage_usd — "
                    f"should use MNQ_TICK_VALUE ($0.50) — 10x inflated for MNQ trades"))
            else:
                findings.append(_warn("latency_logger_uses_mnq",
                    f"latency_logger.hpp slippage_usd uses '{used_const}' — verify this is correct for MNQ"))
        else:
            findings.append(_warn("latency_logger_uses_mnq",
                "Could not find slippage_usd assignment pattern in latency_logger.hpp"))

    return findings


def check_cpp_point_value() -> list[dict]:
    """Check 2: OrbConfig::point_value default in orb_config.hpp."""
    findings = []
    if not ORB_CONFIG_HPP.exists():
        return [_fail("cpp_point_value", f"orb_config.hpp not found at {ORB_CONFIG_HPP}")]

    content = ORB_CONFIG_HPP.read_text()
    m = re.search(r'double\s+point_value\s*=\s*([\d.]+)', content)
    if not m:
        findings.append(_warn("cpp_point_value",
            "OrbConfig::point_value not found in orb_config.hpp"))
        return findings

    val = float(m.group(1))
    if abs(val - MNQ_POINT_VALUE) < 0.001:
        findings.append(_pass("cpp_point_value",
            f"OrbConfig::point_value default={val} correct for MNQ"))
    else:
        findings.append(_fail("cpp_point_value",
            f"OrbConfig::point_value default={val} — expected {MNQ_POINT_VALUE} for MNQ "
            f"(NQ value is 20.0)"))

    return findings


def check_symbol_consistency() -> list[dict]:
    """Check 3: live_config.json symbol vs C++ OrbConfig symbol default."""
    findings = []

    if not CONFIG_PATH.exists():
        return [_fail("symbol_consistency", f"live_config.json not found")]

    with open(CONFIG_PATH) as f:
        cfg = json.load(f)

    py_symbol = cfg.get("symbol", "")

    if not ORB_CONFIG_HPP.exists():
        findings.append(_info("symbol_consistency",
            f"Cannot check C++ symbol default — orb_config.hpp missing"))
        return findings

    content = ORB_CONFIG_HPP.read_text()
    m = re.search(r'std::string\s+symbol\s*=\s*["\'](\w+)["\']', content)
    cpp_symbol = m.group(1) if m else None

    if cpp_symbol is None:
        findings.append(_info("symbol_consistency",
            f"C++ OrbConfig::symbol default not found — runtime config overrides apply"))
    elif py_symbol == cpp_symbol:
        findings.append(_pass("symbol_consistency",
            f"Symbol match: live_config={py_symbol}, C++ default={cpp_symbol}"))
    else:
        # Both Python config and C++ override at runtime — this is expected
        findings.append(_info("symbol_consistency",
            f"Symbol differs: live_config={py_symbol}, C++ default={cpp_symbol} — "
            f"C++ reads symbol from config at runtime (not a crash bug, INFO only)"))

    return findings


def check_python_point_value_default() -> list[dict]:
    """Check 4: live_trader.py point_value fallback default."""
    findings = []
    if not LIVE_TRADER_PY.exists():
        return [_fail("python_point_value_default", f"live_trader.py not found")]

    content = LIVE_TRADER_PY.read_text()
    # Find all get("point_value", <default>) patterns
    matches = list(re.finditer(
        r'get\s*\(\s*["\']point_value["\']\s*,\s*([\d.]+)\s*\)',
        content
    ))

    if not matches:
        findings.append(_pass("python_point_value_default",
            "No hardcoded point_value fallback found in live_trader.py"))
        return findings

    for m in matches:
        val = float(m.group(1))
        # Find line number
        line_no = content[:m.start()].count('\n') + 1
        if abs(val - 20.0) < 0.001:
            findings.append(_fail("python_point_value_default",
                f"live_trader.py:{line_no}: point_value fallback default={val} is NQ value — "
                f"will cause 10x PnL error if orb.point_value missing from config "
                f"(expected {MNQ_POINT_VALUE} for MNQ)"))
        elif abs(val - MNQ_POINT_VALUE) < 0.001:
            findings.append(_pass("python_point_value_default",
                f"live_trader.py:{line_no}: point_value fallback={val} correct for MNQ"))
        else:
            findings.append(_warn("python_point_value_default",
                f"live_trader.py:{line_no}: point_value fallback={val} unexpected"))

    return findings


def check_micro_orb_point_value() -> list[dict]:
    """Check 5: strategy/micro_orb.py hardcoded point_value outside comments."""
    findings = []
    if not MICRO_ORB_PY.exists():
        return [_info("micro_orb_point_value", "strategy/micro_orb.py not found — skipping")]

    lines = MICRO_ORB_PY.read_text().splitlines()
    found_any = False

    for line_no, line in enumerate(lines, 1):
        stripped = line.strip()
        # Skip pure comment lines
        if stripped.startswith('#'):
            continue

        # Find 20.0 in non-comment context
        # Check in get("point_value", 20.0) or similar patterns
        m = re.search(r'get\s*\(\s*["\']point_value["\']\s*,\s*(20\.0)\s*\)', line)
        if m:
            found_any = True
            findings.append(_fail("micro_orb_point_value",
                f"strategy/micro_orb.py:{line_no}: point_value fallback={m.group(1)} is NQ "
                f"value — should be {MNQ_POINT_VALUE} for MNQ (line: {stripped[:80]!r})"))
            continue

        # Find raw 20.0 assignment to point_value outside docstrings
        m = re.search(r'point_value\s*[=:]\s*20\.0\b', line)
        if m:
            found_any = True
            # Check if it's in a docstring-like context (indented string content)
            if '"""' not in line and "'''" not in line and not stripped.startswith('"') and not stripped.startswith("'"):
                findings.append(_warn("micro_orb_point_value",
                    f"strategy/micro_orb.py:{line_no}: hardcoded point_value=20.0 (NQ) — "
                    f"should be {MNQ_POINT_VALUE} (MNQ) if not in docstring (line: {stripped[:80]!r})"))

    if not found_any:
        findings.append(_pass("micro_orb_point_value",
            "No hardcoded point_value=20.0 (NQ) found outside comments in micro_orb.py"))

    return findings


def run_audit() -> list[dict]:
    findings: list[dict] = []
    findings.extend(check_cpp_tick_value())
    findings.extend(check_cpp_point_value())
    findings.extend(check_symbol_consistency())
    findings.extend(check_python_point_value_default())
    findings.extend(check_micro_orb_point_value())
    return findings


def main():
    parser = argparse.ArgumentParser(description="Cross-System Audit — MNQ/NQ constant consistency")
    parser.add_argument("--json", action="store_true", help="JSON output")
    args = parser.parse_args()

    # Load YAML if available (graceful skip if Builder 2 hasn't created it yet)
    if RULES_PATH.exists():
        with open(RULES_PATH) as f:
            _rules = yaml.safe_load(f)  # reserved for future rule-driven checks
    else:
        pass  # scripts do not depend on YAML for their core logic

    findings = run_audit()

    passed = sum(1 for f in findings if f["status"] == "PASS")
    failed = sum(1 for f in findings if f["status"] == "FAIL")
    warned = sum(1 for f in findings if f["status"] == "WARN")

    if args.json:
        print(json.dumps(findings, indent=2))
        sys.exit(1 if failed > 0 else 0)

    print(f"\n{'='*60}")
    print(f"  CROSS-SYSTEM AUDIT")
    print(f"  {passed} passed, {failed} failed, {warned} warnings, {len(findings)} total")
    print(f"{'='*60}")

    for f in findings:
        if f["status"] == "FAIL":
            print(f"  FAIL  [{f['check']}] {f['message']}")
    for f in findings:
        if f["status"] == "WARN":
            print(f"  WARN  [{f['check']}] {f['message']}")
    for f in findings:
        if f["status"] == "INFO":
            print(f"  INFO  [{f['check']}] {f['message']}")
    for f in findings:
        if f["status"] == "PASS":
            print(f"  PASS  [{f['check']}] {f['message']}")

    print(f"\n  Status: {'FAIL' if failed > 0 else 'PASS'}")
    sys.exit(1 if failed > 0 else 0)


if __name__ == "__main__":
    main()
