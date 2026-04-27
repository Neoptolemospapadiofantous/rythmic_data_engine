#!/usr/bin/env python3
"""
C++ Standards Check — enforces cpp_standards.yaml rules against C++ source files.

For each rule with check: regex_absent, scans matching *.cpp/*.hpp files and flags
lines where the pattern is found. For check: regex_present, flags files missing the
required pattern. Exit 1 on any ERROR severity finding.

Usage:
  python scripts/cpp_standards_check.py           # run all checks
  python scripts/cpp_standards_check.py --json    # JSON output
  python scripts/cpp_standards_check.py --errors-only  # ERROR/CRITICAL only
"""
import argparse
import fnmatch
import json
import re
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent

import yaml

RULES_PATH = PROJECT_ROOT / "quality_rules" / "cpp_standards.yaml"


def _resolve_scope(scope_str: str, root: Path) -> list[Path]:
    """Expand comma-separated glob patterns relative to root."""
    files: list[Path] = []
    seen: set[Path] = set()
    for pattern in scope_str.split(","):
        pattern = pattern.strip()
        for p in root.glob(pattern):
            if p.is_file() and p not in seen:
                files.append(p)
                seen.add(p)
    return files


def _is_excluded(path: Path, exclude_patterns: list[str], root: Path) -> bool:
    name = path.name
    rel = str(path.relative_to(root))
    for pattern in exclude_patterns:
        if fnmatch.fnmatch(name, pattern):
            return True
        if fnmatch.fnmatch(rel, pattern):
            return True
        # Also match build/* prefix
        if pattern == "build/*" and rel.startswith("build/"):
            return True
    return False


def _scan_regex_absent(rule: dict, root: Path) -> list[dict]:
    """Flag every file:line where a forbidden pattern appears."""
    findings = []
    rule_id = rule["id"]
    severity = rule.get("severity", "ERROR")
    pattern = rule.get("pattern", "")
    scope_str = rule.get("scope", "src/**/*.cpp")
    exclude = rule.get("exclude", [])
    message = rule.get("message", f"Rule {rule_id} violated")
    known_violations = {
        v.get("file", "") for v in rule.get("known_violations", [])
    }

    try:
        rx = re.compile(pattern)
    except re.error as e:
        findings.append({
            "rule_id": rule_id, "severity": "WARN",
            "message": f"Invalid regex in {rule_id}: {e}", "status": "WARN",
            "file": None, "line": None,
        })
        return findings

    files = _resolve_scope(scope_str, root)
    for path in files:
        if _is_excluded(path, exclude, root):
            continue
        rel = str(path.relative_to(root))
        try:
            text = path.read_text(errors="replace")
        except OSError:
            continue
        in_block_comment = False
        for line_no, line in enumerate(text.splitlines(), 1):
            stripped = line.strip()
            # Track block comments /* ... */
            if in_block_comment:
                if "*/" in stripped:
                    in_block_comment = False
                continue
            if "/*" in stripped and "*/" not in stripped:
                in_block_comment = True
                continue
            # Skip pure comment lines — only check code-bearing lines
            if stripped.startswith("//") or stripped.startswith("*"):
                continue
            # Strip inline comment suffix (everything from first // not inside quotes)
            code_part = re.sub(r'\s*//.*$', '', line)
            # Skip string literal content: remove quoted strings before pattern match
            code_no_strings = re.sub(r'"[^"\\]*(?:\\.[^"\\]*)*"', '""', code_part)
            if rx.search(code_no_strings):
                is_known = any(
                    fnmatch.fnmatch(rel, kv) or rel == kv
                    for kv in known_violations
                )
                status = "WARN" if is_known else severity
                findings.append({
                    "rule_id": rule_id, "severity": severity,
                    "message": f"{message} — found at {rel}:{line_no}: {line.strip()[:100]!r}",
                    "status": status,
                    "file": rel, "line": line_no,
                })

    return findings


def _scan_regex_present(rule: dict, root: Path) -> list[dict]:
    """Verify every matched file contains the required pattern."""
    findings = []
    rule_id = rule["id"]
    severity = rule.get("severity", "ERROR")
    pattern = rule.get("pattern", "")
    scope_str = rule.get("scope", "src/**/*.hpp")
    exclude = rule.get("exclude", [])
    message = rule.get("message", f"Rule {rule_id}: required pattern missing")

    try:
        rx = re.compile(pattern)
    except re.error as e:
        findings.append({
            "rule_id": rule_id, "severity": "WARN",
            "message": f"Invalid regex in {rule_id}: {e}", "status": "WARN",
            "file": None, "line": None,
        })
        return findings

    files = _resolve_scope(scope_str, root)
    for path in files:
        if _is_excluded(path, exclude, root):
            continue
        rel = str(path.relative_to(root))
        try:
            text = path.read_text(errors="replace")
        except OSError:
            continue
        if not rx.search(text):
            findings.append({
                "rule_id": rule_id, "severity": severity,
                "message": f"{message} — missing in {rel}",
                "status": severity,
                "file": rel, "line": None,
            })

    return findings


def run_rules(rules_data: dict, root: Path) -> list[dict]:
    findings = []
    for rule in rules_data.get("rules", []):
        check = rule.get("check", "")
        if check == "regex_absent":
            result = _scan_regex_absent(rule, root)
            if not result:
                findings.append({
                    "rule_id": rule["id"], "severity": "INFO",
                    "message": f"[{rule['id']}] {rule.get('name', '')} — no violations found",
                    "status": "PASS", "file": None, "line": None,
                })
            else:
                findings.extend(result)
        elif check == "regex_present":
            result = _scan_regex_present(rule, root)
            if not result:
                findings.append({
                    "rule_id": rule["id"], "severity": "INFO",
                    "message": f"[{rule['id']}] {rule.get('name', '')} — all files compliant",
                    "status": "PASS", "file": None, "line": None,
                })
            else:
                findings.extend(result)
        elif check == "manual_review":
            known = rule.get("known_violations", [])
            if known:
                for kv in known:
                    kv_id = kv.get("id", rule["id"])
                    findings.append({
                        "rule_id": rule["id"], "severity": rule.get("severity", "ERROR"),
                        "message": (f"[{rule['id']}] {rule.get('name', '')} — "
                                    f"known violation {kv_id} in {kv.get('file', '?')}: "
                                    f"{kv.get('detail', kv.get('impact', ''))}"),
                        "status": "WARN",
                        "file": kv.get("file"), "line": None,
                    })
            else:
                findings.append({
                    "rule_id": rule["id"], "severity": "INFO",
                    "message": (f"[{rule['id']}] {rule.get('name', '')} — "
                                f"manual review: {rule.get('description', '')}"),
                    "status": "INFO", "file": None, "line": None,
                })
        else:
            findings.append({
                "rule_id": rule["id"], "severity": "INFO",
                "message": f"[{rule['id']}] Unknown check type: {check!r} — skipping",
                "status": "SKIP", "file": None, "line": None,
            })
    return findings


def main():
    parser = argparse.ArgumentParser(description="C++ Standards Check")
    parser.add_argument("--json", action="store_true", help="JSON output")
    parser.add_argument("--errors-only", action="store_true",
                        help="Only report ERROR and CRITICAL findings")
    args = parser.parse_args()

    if not RULES_PATH.exists():
        msg = f"Rules file not found: {RULES_PATH}"
        if args.json:
            print(json.dumps([{"rule_id": "load", "status": "SKIP",
                               "severity": "WARN", "message": msg}], indent=2))
        else:
            print(f"SKIP: {msg}")
        sys.exit(0)

    with open(RULES_PATH) as f:
        rules_data = yaml.safe_load(f)

    findings = run_rules(rules_data, PROJECT_ROOT)

    if args.errors_only:
        findings = [f for f in findings if f["severity"] in ("ERROR", "CRITICAL")]

    errors = sum(
        1 for f in findings
        if f["severity"] in ("ERROR", "CRITICAL")
        and f["status"] not in ("PASS", "INFO", "SKIP", "WARN")
    )
    passed = sum(1 for f in findings if f["status"] == "PASS")
    warns = sum(1 for f in findings if f["status"] == "WARN")

    if args.json:
        print(json.dumps(findings, indent=2))
        sys.exit(1 if errors > 0 else 0)

    print(f"\n{'='*60}")
    print(f"  C++ STANDARDS CHECK")
    print(f"  {passed} passed, {errors} errors, {warns} warnings, {len(findings)} total")
    print(f"{'='*60}")

    for f in findings:
        sev = f.get("severity", "")
        st = f.get("status", "")
        if sev in ("ERROR", "CRITICAL") and st not in ("PASS", "INFO", "SKIP", "WARN"):
            print(f"  ERROR [{f['rule_id']}] {f['message']}")
    for f in findings:
        if f["status"] == "WARN":
            print(f"  WARN  [{f['rule_id']}] {f['message']}")
    for f in findings:
        if f["status"] == "INFO":
            print(f"  INFO  [{f['rule_id']}] {f['message']}")
    for f in findings:
        if f["status"] == "PASS":
            print(f"  PASS  [{f['rule_id']}] {f['message']}")

    print(f"\n  Status: {'FAIL' if errors > 0 else 'PASS'}")
    sys.exit(1 if errors > 0 else 0)


if __name__ == "__main__":
    main()
