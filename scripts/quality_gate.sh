#!/bin/bash
# quality_gate.sh — run all audit scripts as a single quality gate
# Exit 0 only if ALL checks pass.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

echo '============================================================'
echo '  QUALITY GATE'
echo '============================================================'

FAILED=0

run_check() {
    local label="$1"
    shift
    echo ""
    echo "--- $label ---"
    if python3 "$@"; then
        echo "  → $label: PASS"
    else
        echo "  → $label: FAIL"
        FAILED=$((FAILED + 1))
    fi
}

run_check "Formula Audit"          "$SCRIPT_DIR/formula_audit.py"
run_check "Cross-System Audit"     "$SCRIPT_DIR/cross_system_audit.py"
run_check "Config Schema"          "$SCRIPT_DIR/config_schema_audit.py"
run_check "Python Standards"       "$SCRIPT_DIR/python_standards_check.py"
run_check "C++ Standards"          "$SCRIPT_DIR/cpp_standards_check.py"

echo ""
echo "--- C++ Build Check ---"
if bash "$SCRIPT_DIR/cpp_build_check.sh"; then
    echo "  → C++ Build Check: PASS"
else
    echo "  → C++ Build Check: FAIL"
    FAILED=$((FAILED + 1))
fi

echo ""
echo '============================================================'
if [ "$FAILED" -eq 0 ]; then
    echo '  ALL GATES PASSED'
    echo '============================================================'
    exit 0
else
    echo "  $FAILED GATE(S) FAILED"
    echo '============================================================'
    exit 1
fi
