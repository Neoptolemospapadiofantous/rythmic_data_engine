"""
tests/conftest.py — pytest configuration for the rithmic_engine test suite.

Test tiers (select with -m <marker>):
    fast           pure unit tests, no I/O, <0.1s each — run these before every commit
    slow           subprocess / state-machine tests, <30s
    live           requires live Rithmic connection — excluded from CI by default
    cpp            requires build/orb_strategy binary — skipped if absent
    orb_parity     C++/Python ORB signal agreement
    feature_parity backtest vs live feature vector agreement
    preflight      go_live.py gate logic
    live_trader    live_trader.py state machine and SIGTERM handling
    audit          rithmic_engine source/schema integrity

Execution tiers:
    pytest -m fast -q                       # <2s pre-commit gate
    pytest -m "not live" -q                 # full CI suite (default)
    pytest -m "feature_parity or preflight" # parity + preflight only
    pytest -n auto -m "not live" -q         # parallel CI (pytest-xdist required)
"""
from __future__ import annotations

from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).parent.parent
_ORB_BINARY = _REPO_ROOT / "build" / "orb_strategy"

# ── Per-marker timeout enforcement ────────────────────────────────────────────
#
# fast tests get a tight 2s budget — if one hangs it's a bug, not a slow test.
# slow tests inherit the global 30s from pytest.ini.

def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    for item in items:
        if item.get_closest_marker("fast") and not item.get_closest_marker("timeout"):
            item.add_marker(pytest.mark.timeout(2))

        # Auto-skip cpp tests when the binary hasn't been built yet
        if item.get_closest_marker("cpp") and not _ORB_BINARY.exists():
            item.add_marker(
                pytest.mark.skip(reason=f"C++ binary absent: {_ORB_BINARY} — run: cd build && make orb_strategy")
            )


# ── Session-start dep warnings ────────────────────────────────────────────────

def pytest_sessionstart(session: pytest.Session) -> None:
    try:
        import xdist  # noqa: F401
    except ImportError:
        print(
            "\nHint: install pytest-xdist for parallel execution: "
            "pip install pytest-xdist\n"
            "Then run: pytest -n auto -m 'not live' -q\n"
        )
