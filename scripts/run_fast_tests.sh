#!/usr/bin/env bash
# run_fast_tests.sh — Fast feedback loop: runs only 'fast' and 'preflight' unit tests.
#
# Target: <2 seconds total. No subprocess calls, no DB, no C++ binary required.
# Use this before every commit to catch regressions instantly.
#
# Usage:
#   ./scripts/run_fast_tests.sh           # fast + preflight (default)
#   ./scripts/run_fast_tests.sh --all     # full test suite (slower)
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
        # Default: fast unit tests — no I/O, no subprocess, <2s total
        exec "$PYTHON" -m pytest \
            tests/test_feature_parity.py \
            tests/test_orb_parity.py \
            tests/test_preflight.py \
            tests/test_live_trader.py \
            -m "not (slow or cpp or live)" \
            -q --tb=short
        ;;
esac
