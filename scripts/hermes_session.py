#!/usr/bin/env python3
"""
hermes_session.py — Local improvement loop runner.

Runs all quality gates (tests, mypy, ruff, audit) locally and writes a
findings report that the Hermes agent (Claude) uses to decide what to fix
or improve next.

Usage:
    python scripts/hermes_session.py          # full check
    python scripts/hermes_session.py --fast   # skip slow audit/cpp checks
    python scripts/hermes_session.py --json   # machine-readable output
"""
from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

ENGINE_DIR = Path(__file__).resolve().parent.parent
FINDINGS_FILE = ENGINE_DIR / "data" / "hermes_findings.json"
SESSION_LOG = ENGINE_DIR / "data" / "logs" / "hermes_session.log"


def _run(cmd: list[str], timeout: int = 120, cwd: Path = ENGINE_DIR) -> tuple[int, str]:
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout, cwd=str(cwd))
        return r.returncode, r.stdout + r.stderr
    except subprocess.TimeoutExpired:
        return -1, f"TIMEOUT after {timeout}s"
    except FileNotFoundError:
        return -2, f"Command not found: {cmd[0]}"


def check_tests(fast: bool) -> dict:
    mark = "fast or feature_parity or preflight or live_trader" if fast else None
    cmd = [sys.executable, "-m", "pytest", "-q", "--tb=short", "--no-header", "-p", "no:warnings"]
    if mark:
        cmd += ["-m", mark]
    t0 = time.monotonic()
    code, out = _run(cmd, timeout=60 if fast else 180)
    elapsed = time.monotonic() - t0

    passed = int(m.group(1)) if (m := re.search(r"(\d+) passed", out)) else 0
    failed = int(m.group(1)) if (m := re.search(r"(\d+) failed", out)) else 0
    errors = int(m.group(1)) if (m := re.search(r"(\d+) error", out)) else 0
    skipped = int(m.group(1)) if (m := re.search(r"(\d+) skipped", out)) else 0
    total_fail = failed + errors

    failures = []
    for line in out.splitlines():
        if line.startswith("FAILED ") or "ERROR " in line[:10]:
            failures.append(line.strip())

    return {
        "check": "tests_fast" if fast else "tests_full",
        "status": "PASS" if total_fail == 0 and passed > 0 else "FAIL",
        "passed": passed, "failed": total_fail, "skipped": skipped,
        "elapsed_s": round(elapsed, 1),
        "failures": failures[:20],
        "message": f"{passed} passed, {total_fail} failed, {skipped} skipped in {elapsed:.1f}s",
    }


def check_mypy() -> dict:
    targets = ["live_trader.py", "models.py", "go_live.py",
               "scripts/audit_daemon.py", "strategy/features.py"]
    existing = [t for t in targets if (ENGINE_DIR / t).exists()]
    code, out = _run(
        [sys.executable, "-m", "mypy", "--ignore-missing-imports",
         "--disable-error-code=import-untyped", "--no-error-summary", "--no-pretty"] + existing,
        timeout=60,
    )
    errors = [ln for ln in out.splitlines() if ": error:" in ln]
    return {
        "check": "mypy",
        "status": "PASS" if not errors else "FAIL",
        "error_count": len(errors),
        "errors": errors[:10],
        "message": f"{len(errors)} type error(s)" if errors else f"clean ({len(existing)} files)",
    }


def check_ruff() -> dict:
    code, out = _run(
        [sys.executable, "-m", "ruff", "check", ".",
         "--select=F,E7,E9,W6", "--exclude=build", "--quiet"],
        timeout=30,
    )
    lines = [ln for ln in out.splitlines() if ln.strip() and not ln.startswith(" ")]
    return {
        "check": "ruff",
        "status": "PASS" if code == 0 else "FAIL",
        "issue_count": len(lines),
        "issues": lines[:10],
        "message": f"{len(lines)} issue(s): {lines[0][:80]}" if lines else "clean",
    }


def check_audit() -> dict:
    code, out = _run(
        [sys.executable, "scripts/audit_daemon.py", "--once"],
        timeout=300,
    )
    passed = int(m.group(1)) if (m := re.search(r"TOTAL: (\d+) passed", out)) else 0
    failed = int(m.group(1)) if (m := re.search(r"(\d+) failed", out)) else 0
    warned = int(m.group(1)) if (m := re.search(r"(\d+) warned", out)) else 0
    return {
        "check": "audit",
        "status": "PASS" if failed == 0 else "FAIL",
        "passed": passed, "failed": failed, "warned": warned,
        "message": f"{passed} passed, {failed} failed, {warned} warned",
    }


def git_status() -> dict:
    _, diff = _run(["git", "diff", "--stat", "HEAD"], timeout=10)
    _, log = _run(["git", "log", "--oneline", "-5"], timeout=10)
    _, status = _run(["git", "status", "--short"], timeout=10)
    changed = [ln for ln in status.splitlines() if ln.strip()]
    return {
        "check": "git",
        "uncommitted_files": len(changed),
        "changed": changed[:20],
        "recent_commits": [ln.strip() for ln in log.strip().splitlines()[:5]],
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--fast", action="store_true", help="Skip full test + audit")
    parser.add_argument("--json", action="store_true", help="Machine-readable output")
    args = parser.parse_args()

    started = datetime.now(tz=timezone.utc)
    results = []

    print(f"\n{'='*60}")
    print(f"  HERMES SESSION  {started.strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"{'='*60}\n")

    steps = [
        ("Tests", lambda: check_tests(fast=args.fast)),
        ("Mypy", check_mypy),
        ("Ruff", check_ruff),
    ]
    if not args.fast:
        steps.append(("Audit", check_audit))

    for name, fn in steps:
        print(f"  Running {name}...", end="", flush=True)
        r = fn()
        results.append(r)
        icon = "✓" if r["status"] == "PASS" else "✗"
        print(f"\r  [{icon}] {name:<8} {r['message']}")
        if r["status"] == "FAIL":
            for detail in r.get("failures", r.get("errors", r.get("issues", []))):
                print(f"          {detail}")

    git = git_status()

    # Summary
    passed = sum(1 for r in results if r["status"] == "PASS")
    failed = sum(1 for r in results if r["status"] == "FAIL")
    overall = "PASS" if failed == 0 else "FAIL"

    print(f"\n  {'─'*56}")
    print(f"  Overall: {overall}  ({passed} pass, {failed} fail)")
    print(f"  Uncommitted: {git['uncommitted_files']} file(s)")
    if git["changed"]:
        for f in git["changed"][:5]:
            print(f"    {f}")
    print(f"{'='*60}\n")

    # Write findings for Hermes agent
    findings = {
        "timestamp": started.isoformat(),
        "overall": overall,
        "checks": results,
        "git": git,
        "next_action": (
            "fix_failures" if failed > 0 else
            "find_improvements"
        ),
    }
    FINDINGS_FILE.parent.mkdir(parents=True, exist_ok=True)
    FINDINGS_FILE.write_text(json.dumps(findings, indent=2))

    # Append to session log
    SESSION_LOG.parent.mkdir(parents=True, exist_ok=True)
    with SESSION_LOG.open("a") as f:
        f.write(f"{started.isoformat()} {overall} tests={results[0]['message']}\n")

    if args.json:
        print(json.dumps(findings, indent=2))

    sys.exit(0 if overall == "PASS" else 1)


if __name__ == "__main__":
    main()
