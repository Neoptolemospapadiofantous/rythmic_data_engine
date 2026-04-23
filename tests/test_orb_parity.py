"""
CI parity test: Python MicroORBStrategy vs C++ OrbStrategy (build/orb_strategy).

Six identical 1-min OHLCV bars are fed through both implementations; the test
asserts that they produce the same signal direction and stop-loss level.
The C++ section is skipped gracefully when build/orb_strategy is absent so
CI does not hard-fail before the binary has been compiled.

Bar scenario (all times Eastern, 2024-03-15)
─────────────────────────────────────────────
  Bars 1-5 (09:30–09:34): build the 5-min opening range.
    Range high = max(highs) = 17510.0
    Range low  = min(lows)  = 17490.0
  Bar 6 (09:35): close = 17520 > range high → LONG signal expected.
    Entry   = 17520.0
    SL      = orb_high − stop_loss_ticks × tick_size = 17510 − 4.0 = 17506.0
    Target  = entry + target_ticks × tick_size = 17520 + 12.0 = 17532.0
"""

import json
import os
import subprocess
import sys
import unittest
import zoneinfo
from dataclasses import asdict
from datetime import datetime, timedelta, date, time
from pathlib import Path
from typing import Optional

# ── repository root on sys.path ───────────────────────────────────────────────
REPO_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(REPO_ROOT))

ET = zoneinfo.ZoneInfo("America/New_York")

# ── test scenario constants ───────────────────────────────────────────────────
_SESSION_DATE = date(2024, 3, 15)
_RTH_OPEN_ET  = time(9, 30, tzinfo=ET)

TICK_SIZE    = 0.25
SL_TICKS     = 16
TARGET_TICKS = 48
ORB_HIGH     = 17510.0
ORB_LOW      = 17490.0
# SL for LONG = orb_high - sl_ticks * tick_size  (see MicroORBStrategy._check_breakout)
EXPECTED_SL      = round(ORB_HIGH - SL_TICKS * TICK_SIZE, 4)   # 17506.0
EXPECTED_ENTRY   = 17520.0
EXPECTED_TARGET  = round(EXPECTED_ENTRY + TARGET_TICKS * TICK_SIZE, 4)  # 17532.0

CONFIG_PATH  = REPO_ROOT / "config" / "live_config.json"
CPP_BINARY   = REPO_ROOT / "build" / "orb_strategy"


def _bar(minute_offset: int, high: float, low: float,
         open_: float = 17500.0, close: float = 17500.0,
         volume: int = 1000) -> dict:
    ts = datetime.combine(_SESSION_DATE, time(9, 30), tzinfo=ET) + timedelta(minutes=minute_offset)
    return {"ts": ts, "open": open_, "high": high, "low": low, "close": close, "volume": volume}


# 5 range bars (minutes 0-4) followed by 1 breakout bar (minute 5)
RANGE_BARS = [
    _bar(0, high=17510.0, low=17490.0),  # bar 1: sets range high and low
    _bar(1, high=17505.0, low=17495.0),  # bar 2
    _bar(2, high=17508.0, low=17492.0),  # bar 3
    _bar(3, high=17503.0, low=17497.0),  # bar 4
    _bar(4, high=17506.0, low=17494.0),  # bar 5 — range locked after this
]
BREAKOUT_BAR = _bar(5, high=17525.0, low=17515.0,
                    open_=17515.0, close=17520.0, volume=500)
ALL_BARS = RANGE_BARS + [BREAKOUT_BAR]


def _load_config() -> dict:
    with CONFIG_PATH.open() as fh:
        return json.load(fh)


# ── serialise bars for subprocess (datetimes → ISO strings) ──────────────────

def _bars_to_json(bars: list[dict]) -> bytes:
    serialisable = [
        {
            "ts": b["ts"].isoformat(),
            "open": b["open"],
            "high": b["high"],
            "low": b["low"],
            "close": b["close"],
            "volume": b["volume"],
        }
        for b in bars
    ]
    return json.dumps(serialisable).encode()


# ── helpers ───────────────────────────────────────────────────────────────────

def _run_python(bars: list[dict]):
    """Feed bars into MicroORBStrategy; return last Signal (may be None)."""
    from strategy.micro_orb import MicroORBStrategy
    strategy = MicroORBStrategy(_load_config())
    result = None
    for bar in bars:
        sig = strategy.on_bar(bar)
        if sig is not None:
            result = sig
    return result, strategy


def _run_cpp(bars: list[dict]) -> Optional[dict]:
    """
    Invoke build/orb_strategy with bars as JSON on stdin.
    Returns parsed JSON output dict, or None if binary is absent.
    Raises RuntimeError on non-zero exit.
    """
    if not CPP_BINARY.exists():
        return None
    proc = subprocess.run(
        [str(CPP_BINARY), "--config", str(CONFIG_PATH)],
        input=_bars_to_json(bars),
        capture_output=True,
        timeout=15,
        env=os.environ.copy(),
    )
    if proc.returncode != 0:
        raise RuntimeError(
            f"orb_strategy exited {proc.returncode}:\n{proc.stderr.decode()}"
        )
    return json.loads(proc.stdout)


# ── Python-only tests (always run) ───────────────────────────────────────────

class TestPythonORBSignal(unittest.TestCase):
    """Python MicroORBStrategy: correct signal and SL on the 6-bar scenario."""

    @classmethod
    def setUpClass(cls):
        try:
            from strategy.micro_orb import MicroORBStrategy  # noqa: F401
        except ImportError:
            raise unittest.SkipTest("strategy.micro_orb not importable")

    def _run_all(self):
        signal, strategy = _run_python(ALL_BARS)
        return signal, strategy

    def test_breakout_produces_long_signal(self):
        signal, _ = self._run_all()
        self.assertIsNotNone(signal, "No signal returned after breakout bar")
        self.assertEqual(signal.direction, "LONG",
                         f"Expected LONG, got {signal.direction!r}")

    def test_stop_loss_at_orb_high_minus_sl_ticks(self):
        signal, _ = self._run_all()
        self.assertIsNotNone(signal, "No signal returned")
        self.assertAlmostEqual(
            signal.stop_loss, EXPECTED_SL, places=4,
            msg=f"SL mismatch: expected {EXPECTED_SL}, got {signal.stop_loss}",
        )

    def test_entry_price_equals_breakout_close(self):
        signal, _ = self._run_all()
        self.assertIsNotNone(signal)
        self.assertAlmostEqual(
            signal.entry_price, EXPECTED_ENTRY, places=4,
            msg=f"Entry mismatch: expected {EXPECTED_ENTRY}, got {signal.entry_price}",
        )

    def test_no_signal_during_orb_period(self):
        """Bars 1-5 are inside the ORB window — no signal must be emitted."""
        from strategy.micro_orb import MicroORBStrategy
        strategy = MicroORBStrategy(_load_config())
        for i, bar in enumerate(RANGE_BARS):
            sig = strategy.on_bar(bar)
            self.assertIsNone(sig, f"Unexpected signal from range bar {i + 1}: {sig}")

    def test_range_locked_correctly_after_5_bars(self):
        """After 5 range bars, orb_high and orb_low must reflect the actual range."""
        from strategy.micro_orb import MicroORBStrategy, StrategyState
        strategy = MicroORBStrategy(_load_config())
        for bar in RANGE_BARS:
            strategy.on_bar(bar)
        self.assertEqual(strategy.state, StrategyState.WATCHING,
                         f"Expected WATCHING state, got {strategy.state}")
        self.assertAlmostEqual(strategy.orb_high, ORB_HIGH, places=4,
                               msg=f"orb_high wrong: {strategy.orb_high}")
        self.assertAlmostEqual(strategy.orb_low, ORB_LOW, places=4,
                               msg=f"orb_low wrong: {strategy.orb_low}")

    def test_state_is_in_position_after_breakout(self):
        from strategy.micro_orb import StrategyState
        _, strategy = self._run_all()
        self.assertEqual(strategy.state, StrategyState.IN_POSITION,
                         f"Expected IN_POSITION, got {strategy.state}")


# ── C++ / Python parity tests (skipped if binary absent) ─────────────────────

class TestCppPythonParity(unittest.TestCase):
    """C++ and Python ORB implementations must agree on signal and SL."""

    @classmethod
    def setUpClass(cls):
        if not CPP_BINARY.exists():
            raise unittest.SkipTest(
                f"C++ binary not found at {CPP_BINARY}.\n"
                "Build it first: cmake --build build --target orb_strategy"
            )
        try:
            from strategy.micro_orb import MicroORBStrategy  # noqa: F401
        except ImportError:
            raise unittest.SkipTest("strategy.micro_orb not importable")

    def _results(self):
        python_signal, _ = _run_python(ALL_BARS)
        cpp_result       = _run_cpp(ALL_BARS)
        return python_signal, cpp_result

    def test_signal_direction_parity(self):
        python_sig, cpp_r = self._results()
        self.assertIsNotNone(python_sig, "Python produced no signal")
        self.assertIsNotNone(cpp_r,      "C++ produced no output")
        self.assertEqual(
            python_sig.direction, cpp_r["signal"],
            f"Direction mismatch — Python: {python_sig.direction!r}, "
            f"C++: {cpp_r['signal']!r}",
        )

    def test_stop_loss_parity(self):
        python_sig, cpp_r = self._results()
        self.assertIsNotNone(python_sig, "Python produced no signal")
        self.assertIsNotNone(cpp_r,      "C++ produced no output")
        self.assertAlmostEqual(
            python_sig.stop_loss, cpp_r["stop_loss"], places=4,
            msg=(
                f"SL mismatch — Python: {python_sig.stop_loss}, "
                f"C++: {cpp_r['stop_loss']}"
            ),
        )

    def test_no_signal_during_range_parity(self):
        """Both implementations must stay silent through the ORB window (bars 1-5)."""
        from strategy.micro_orb import MicroORBStrategy
        strategy = MicroORBStrategy(_load_config())
        for i, bar in enumerate(RANGE_BARS):
            sig = strategy.on_bar(bar)
            self.assertIsNone(sig, f"Python signalled on range bar {i + 1}: {sig}")

        cpp_r = _run_cpp(RANGE_BARS)
        if cpp_r is not None:
            self.assertIsNone(
                cpp_r.get("signal"),
                f"C++ signalled during range window: {cpp_r}",
            )


if __name__ == "__main__":
    unittest.main(verbosity=2)
