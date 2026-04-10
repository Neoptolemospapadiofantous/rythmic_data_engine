#!/usr/bin/env bash
# run_tests.sh — Run the complete rithmic_engine test suite
set -euo pipefail
cd "$(dirname "$0")/.."

PASS=0; FAIL=0

run() {
    local name=$1; shift
    printf "%-40s " "$name"
    if "$@" > /tmp/re_test_out.txt 2>&1; then
        echo "PASS"; ((PASS++))
    else
        echo "FAIL"; ((FAIL++))
        cat /tmp/re_test_out.txt
    fi
}

echo "=== rithmic_engine test suite ==="
echo ""

# C++ tests (require build)
if [ -f build/test_db ]; then
    run "test_db (DB + schema)"        ./build/test_db
    run "test_validator (unit tests)"  ./build/test_validator
else
    echo "WARN: build/test_db not found — run: cd build && make"
fi

# Python audit
run "audit_engine.py --no-pg"     python tests/audit_engine.py --no-pg
run "audit_data.py --summary"     python audit_data.py --summary 2>/dev/null || true

echo ""
echo "=== Results: $PASS passed, $FAIL failed ==="
[ $FAIL -eq 0 ]
