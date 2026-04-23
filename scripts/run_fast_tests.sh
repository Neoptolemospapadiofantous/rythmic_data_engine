#!/usr/bin/env bash
# run_fast_tests.sh — Fast feedback loop: pure unit tests, no I/O, <2s total.
#
# Run before every commit to catch regressions in <2 seconds.
#
# Usage:
#   ./scripts/run_fast_tests.sh           # fast marker suite (default)
#   ./scripts/run_fast_tests.sh --all     # full test suite including slow/subprocess
#   ./scripts/run_fast_tests.sh --parity  # feature_parity + orb_parity only

set -euo pipefail
cd "$(dirname "$0")/.."

PYTHON="${PYTHON:-python3}"

case "${1:-}" in
    --all)
        echo "Running full test suite..."
        exec "$PYTHON" -m pytest tests/ scripts/kill_test_suite.py -q
        ;;
    --parity)
        echo "Running parity tests (feature_parity + orb_parity)..."
        exec "$PYTHON" -m pytest -m "feature_parity or orb_parity" -q
        ;;
    *)
        # Default: fast-marked pure unit tests — no subprocess, no DB, <2s
        exec "$PYTHON" -m pytest -m fast -q
        ;;
esac
