"""
test_feature_parity.py — Contract test ensuring backtest and live feature computation
produce identical values for all 74 features.

This test acts as the interface CONTRACT between:
  - backtest module: compute_features(bars) → dict
  - live_trader module: compute_live_features(bars) → dict  (Builder 3 must implement)

Both sides are loaded via importorskip so this test is safe to run before either
module exists — it will skip gracefully rather than error.

Run with:
    pytest tests/test_feature_parity.py -v
    pytest tests/test_feature_parity.py -v -m feature_parity
"""
from __future__ import annotations

import math
from typing import Any

import pytest

# ---------------------------------------------------------------------------
# Feature contract: exactly 74 names that both engines must produce.
# Builder 3 must implement compute_live_features() returning all 74 keys.
# The backtest compute_features() must also return all 74 keys.
# ---------------------------------------------------------------------------

FEATURE_NAMES: list[str] = [
    # ORB core (6)
    "orb_high",
    "orb_low",
    "orb_range",
    "orb_midpoint",
    "price_above_orb",
    "price_below_orb",
    # Volume / order-flow (9)
    "volume_ratio",
    "vwap",
    "vwap_deviation",
    "volume_delta",
    "cum_delta",
    "volume_ma_20",
    "relative_volume",
    "vwap_upper_band",
    "vwap_lower_band",
    # Price action (6)
    "bar_range",
    "bar_direction",
    "close_vs_open",
    "high_of_day",
    "low_of_day",
    "distance_to_vwap",
    # Time / session (7)
    "minutes_since_open",
    "is_power_hour",
    "session_quarter",
    "session_half",
    "pre_market_gap",
    "opening_range_vol",
    "time_to_close",
    # Moving averages (8)
    "ema_3",
    "ema_5",
    "ema_10",
    "ema_20",
    "ema_50",
    "sma_10",
    "sma_20",
    "sma_50",
    # Momentum (12)
    "rsi_7",
    "rsi_14",
    "rsi_21",
    "macd_line",
    "macd_signal",
    "macd_histogram",
    "stoch_k",
    "stoch_d",
    "cci_20",
    "williams_r",
    "mfi_14",
    "bb_position",
    # Bollinger Bands (3)
    "bb_upper",
    "bb_lower",
    "bb_width",
    # Volatility (6)
    "atr_7",
    "atr_14",
    "hist_vol_20",
    "true_range",
    "keltner_upper",
    "keltner_lower",
    # Previous session (4)
    "prev_day_high",
    "prev_day_low",
    "prev_day_close",
    "overnight_gap",
    # ORB distance / confirmation (4)
    "distance_to_orb_high",
    "distance_to_orb_low",
    "orb_breakout_confirmed",
    "price_vs_orb_midpoint",
    # Tick / aggression (9)
    "tick_direction_last5",
    "large_tick_count",
    "bid_ask_imbalance",
    "tick_sum",
    "uptick_count",
    "downtick_count",
    "imbalance_ratio",
    "trade_aggression",
    "delta_divergence",
]

assert len(FEATURE_NAMES) == 74, f"Expected 74 features, got {len(FEATURE_NAMES)}"
FEATURE_NAMES_SET: frozenset[str] = frozenset(FEATURE_NAMES)

# ---------------------------------------------------------------------------
# Synthetic input: 30 one-minute OHLCV bars (open → 9:30 ET on a typical NQ day)
# ---------------------------------------------------------------------------

def _make_synthetic_bars(n: int = 30) -> list[dict[str, Any]]:
    """Generate deterministic synthetic 1-minute OHLCV bars for testing."""
    import datetime

    base_price = 21_000.0
    base_time = datetime.datetime(2024, 3, 15, 9, 30, 0)
    bars: list[dict[str, Any]] = []

    for i in range(n):
        # Mild sine wave drift so features have non-trivial values
        drift = math.sin(i * 0.3) * 20.0
        open_ = base_price + drift + i * 0.5
        close = open_ + math.cos(i * 0.4) * 8.0
        high = max(open_, close) + abs(math.sin(i * 0.7)) * 5.0
        low = min(open_, close) - abs(math.cos(i * 0.7)) * 5.0
        volume = 1000 + int(abs(math.sin(i * 0.2)) * 500)
        bars.append(
            {
                "timestamp": base_time + datetime.timedelta(minutes=i),
                "open": round(open_, 2),
                "high": round(high, 2),
                "low": round(low, 2),
                "close": round(close, 2),
                "volume": volume,
                # Simulate bid/ask imbalance
                "bid_volume": int(volume * 0.45),
                "ask_volume": int(volume * 0.55),
            }
        )
    return bars


SYNTHETIC_BARS: list[dict[str, Any]] = _make_synthetic_bars(30)


# ---------------------------------------------------------------------------
# Contract test helpers
# ---------------------------------------------------------------------------

def _assert_feature_keys(features: dict[str, Any], source: str) -> None:
    """Assert the feature dict contains exactly the 74 contracted keys."""
    actual = frozenset(features.keys())
    missing = FEATURE_NAMES_SET - actual
    extra = actual - FEATURE_NAMES_SET

    assert not missing, (
        f"{source}: missing {len(missing)} feature(s): {sorted(missing)}"
    )
    assert not extra, (
        f"{source}: unexpected extra feature(s): {sorted(extra)}"
    )


def _assert_features_equal(
    backtest: dict[str, Any],
    live: dict[str, Any],
    tol: float = 1e-6,
) -> None:
    """Assert every feature value matches within tolerance."""
    mismatches: list[str] = []
    for name in FEATURE_NAMES:
        bv = backtest[name]
        lv = live[name]
        if bv is None and lv is None:
            continue
        if bv is None or lv is None:
            mismatches.append(f"{name}: backtest={bv!r}  live={lv!r}")
            continue
        # Boolean features: exact match
        if isinstance(bv, bool) or isinstance(lv, bool):
            if bool(bv) != bool(lv):
                mismatches.append(f"{name}: backtest={bv}  live={lv}")
        else:
            try:
                diff = abs(float(bv) - float(lv))
                if diff > tol:
                    mismatches.append(
                        f"{name}: backtest={bv:.8f}  live={lv:.8f}  diff={diff:.2e}"
                    )
            except (TypeError, ValueError):
                if bv != lv:
                    mismatches.append(f"{name}: backtest={bv!r}  live={lv!r}")

    assert not mismatches, (
        f"{len(mismatches)} feature(s) differ between backtest and live:\n"
        + "\n".join(f"  {m}" for m in mismatches)
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

@pytest.mark.feature_parity
def test_feature_names_count() -> None:
    """FEATURE_NAMES must contain exactly 74 distinct entries."""
    assert len(FEATURE_NAMES) == 74
    assert len(set(FEATURE_NAMES)) == 74, "Duplicate feature names detected"


@pytest.mark.feature_parity
def test_feature_names_are_strings() -> None:
    """Every entry in FEATURE_NAMES must be a non-empty lowercase string."""
    for name in FEATURE_NAMES:
        assert isinstance(name, str) and name, f"Invalid feature name: {name!r}"
        assert name == name.lower(), f"Feature name not lowercase: {name!r}"
        assert " " not in name, f"Feature name contains spaces: {name!r}"


@pytest.mark.feature_parity
def test_backtest_feature_keys() -> None:
    """Backtest compute_features() must return exactly the 74 contracted keys."""
    backtest_mod = pytest.importorskip(
        "backtest",
        reason="backtest module not yet present — skipping backtest key check",
    )
    compute = getattr(backtest_mod, "compute_features", None)
    if compute is None:
        pytest.skip("backtest.compute_features not found")

    features = compute(SYNTHETIC_BARS)
    _assert_feature_keys(features, "backtest.compute_features")


@pytest.mark.feature_parity
def test_live_feature_keys() -> None:
    """Live compute_live_features() must return exactly the 74 contracted keys."""
    live_mod = pytest.importorskip(
        "live_trader",
        reason="live_trader module not yet present — skipping live key check",
    )
    compute = getattr(live_mod, "compute_live_features", None)
    if compute is None:
        pytest.skip("live_trader.compute_live_features not found")

    features = compute(SYNTHETIC_BARS)
    _assert_feature_keys(features, "live_trader.compute_live_features")


@pytest.mark.feature_parity
def test_feature_parity_backtest_vs_live() -> None:
    """Given identical input bars, backtest and live features must match within 1e-6."""
    backtest_mod = pytest.importorskip(
        "backtest",
        reason="backtest module not yet present — skipping parity check",
    )
    live_mod = pytest.importorskip(
        "live_trader",
        reason="live_trader module not yet present — skipping parity check",
    )

    compute_bt = getattr(backtest_mod, "compute_features", None)
    compute_live = getattr(live_mod, "compute_live_features", None)

    if compute_bt is None:
        pytest.skip("backtest.compute_features not found")
    if compute_live is None:
        pytest.skip("live_trader.compute_live_features not found")

    backtest_features = compute_bt(SYNTHETIC_BARS)
    live_features = compute_live(SYNTHETIC_BARS)

    _assert_feature_keys(backtest_features, "backtest")
    _assert_feature_keys(live_features, "live")
    _assert_features_equal(backtest_features, live_features)


@pytest.mark.feature_parity
def test_feature_parity_multiple_bar_lengths() -> None:
    """Parity must hold for various bar-history lengths (edge cases)."""
    backtest_mod = pytest.importorskip(
        "backtest",
        reason="backtest module not yet present",
    )
    live_mod = pytest.importorskip(
        "live_trader",
        reason="live_trader module not yet present",
    )

    compute_bt = getattr(backtest_mod, "compute_features", None)
    compute_live = getattr(live_mod, "compute_live_features", None)

    if compute_bt is None or compute_live is None:
        pytest.skip("compute_features / compute_live_features not found")

    for n_bars in (10, 20, 30, 50):
        bars = _make_synthetic_bars(n_bars)
        bt_feats = compute_bt(bars)
        lv_feats = compute_live(bars)
        _assert_features_equal(bt_feats, lv_feats)


@pytest.mark.feature_parity
def test_synthetic_bars_structure() -> None:
    """Sanity check: synthetic bars have the expected shape and value ranges."""
    bars = _make_synthetic_bars(30)
    assert len(bars) == 30
    for bar in bars:
        assert bar["low"] <= bar["open"] <= bar["high"]
        assert bar["low"] <= bar["close"] <= bar["high"]
        assert bar["volume"] > 0
        assert 18_000 < bar["close"] < 25_000, "price out of NQ range"
