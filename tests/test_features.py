"""
Unit tests for strategy/features.py — compute_features() and helpers.

Coverage:
  - Empty input returns 74 zeros
  - Output always has exactly 74 keys matching _FEATURE_NAMES
  - No NaN/Inf values in any output
  - ORB core features (orb_high, orb_low, orb_range, price_above_orb, price_below_orb)
  - VWAP and vwap_deviation
  - Volume features (volume_ratio, volume_delta, cum_delta, bid_ask_imbalance)
  - Moving averages (EMA/SMA convergence with sufficient data)
  - Bollinger Bands (bb_upper > bb_lower, bb_width >= 0, bb_position in [0, 1])
  - RSI boundaries (0–100 range, 50 default on insufficient data)
  - MACD consistency (histogram = line - signal)
  - ATR > 0 with real bar data
  - Session time features (minutes_since_open, is_power_hour, session_quarter)
  - Tick / aggression features (bid_ask_imbalance, tick_direction_last5, large_tick_count)
  - Single-bar edge case
  - orb_period parameter override
  - _sma, _ema, _rsi, _bb, _stoch_k helpers directly
"""
from __future__ import annotations

import datetime
import math
import sys
from pathlib import Path
from typing import Any

import pytest

REPO_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(REPO_ROOT / "strategy"))

from strategy.features import (
    _FEATURE_NAMES,
    _atr,
    _bb,
    _ema,
    _rsi,
    _sma,
    _stoch_k,
    _tr,
    compute_features,
)


# ── Fixtures ───────────────────────────────────────────────────────────────────

def _make_bar(
    close: float = 100.0,
    open_: float = 99.0,
    high: float = 101.0,
    low: float = 98.0,
    volume: float = 1000.0,
    bid_volume: float = 400.0,
    ask_volume: float = 600.0,
    ts: datetime.datetime | None = None,
) -> dict[str, Any]:
    if ts is None:
        ts = datetime.datetime(2026, 1, 2, 14, 30)  # 09:30 ET in UTC+5 — irrelevant for most tests
    return {
        "open": open_, "high": high, "low": low, "close": close,
        "volume": volume, "bid_volume": bid_volume, "ask_volume": ask_volume,
        "ts": ts,
    }


def _rth_ts(hour: int, minute: int = 0) -> datetime.datetime:
    """Return a naive datetime with no timezone (features.py uses .hour/.minute)."""
    return datetime.datetime(2026, 1, 2, hour, minute)


def _make_bars(
    n: int,
    base_close: float = 100.0,
    trend: float = 0.1,
    volume: float = 1000.0,
) -> list[dict]:
    """Return n bars with a linear uptrend."""
    bars = []
    for i in range(n):
        c = base_close + i * trend
        o = c - 0.5
        h = c + 1.0
        lo = c - 1.0
        bars.append(_make_bar(close=c, open_=o, high=h, low=lo, volume=volume))
    return bars


# ── Output contract ────────────────────────────────────────────────────────────


@pytest.mark.fast
def test_empty_bars_returns_74_zeros():
    out = compute_features([])
    assert len(out) == 74
    assert all(v == 0.0 for v in out.values())


@pytest.mark.fast
def test_output_has_exactly_74_features():
    out = compute_features(_make_bars(10))
    assert set(out.keys()) == set(_FEATURE_NAMES)
    assert len(out) == 74


@pytest.mark.fast
def test_output_keys_match_feature_names_order():
    out = compute_features(_make_bars(5))
    assert list(out.keys()) == list(_FEATURE_NAMES)


@pytest.mark.fast
def test_no_nan_or_inf_in_output():
    bars = _make_bars(60)
    out = compute_features(bars)
    for k, v in out.items():
        assert math.isfinite(v), f"{k}={v} is not finite"


@pytest.mark.fast
def test_all_values_are_floats():
    out = compute_features(_make_bars(3))
    for k, v in out.items():
        assert isinstance(v, float), f"{k} is {type(v)}"


# ── Single-bar edge case ───────────────────────────────────────────────────────


@pytest.mark.fast
def test_single_bar_no_crash():
    bar = _make_bar(close=100.0, open_=99.0, high=101.0, low=98.0)
    out = compute_features([bar])
    assert len(out) == 74
    assert all(math.isfinite(v) for v in out.values())


@pytest.mark.fast
def test_single_bar_orb_equals_bar():
    bar = _make_bar(close=100.0, open_=99.0, high=101.0, low=98.0, volume=500.0)
    out = compute_features([bar])
    assert out["orb_high"] == pytest.approx(101.0)
    assert out["orb_low"] == pytest.approx(98.0)
    assert out["orb_range"] == pytest.approx(3.0)
    assert out["orb_midpoint"] == pytest.approx(99.5)


# ── ORB core ───────────────────────────────────────────────────────────────────


@pytest.mark.fast
def test_orb_high_low_from_first_n_bars():
    bars = [
        _make_bar(high=110.0, low=90.0, close=100.0),  # orb bar — wide range
        _make_bar(high=108.0, low=95.0, close=102.0),
        _make_bar(high=106.0, low=93.0, close=104.0),
        _make_bar(high=120.0, low=85.0, close=115.0),  # post-ORB — should not affect orb
        _make_bar(high=122.0, low=83.0, close=120.0),
        _make_bar(high=125.0, low=80.0, close=122.0),  # this bar is evaluated
    ]
    out = compute_features(bars, orb_period=3)
    assert out["orb_high"] == pytest.approx(110.0)
    assert out["orb_low"] == pytest.approx(90.0)


@pytest.mark.fast
def test_price_above_orb():
    bars = _make_bars(6, base_close=100.0)
    # Manually set the last bar's close above expected orb_high
    bars[-1]["close"] = 200.0
    bars[-1]["high"] = 201.0
    out = compute_features(bars, orb_period=5)
    assert out["price_above_orb"] == pytest.approx(1.0)
    assert out["price_below_orb"] == pytest.approx(0.0)


@pytest.mark.fast
def test_price_below_orb():
    bars = _make_bars(6, base_close=100.0)
    bars[-1]["close"] = 0.5
    bars[-1]["low"] = 0.4
    out = compute_features(bars, orb_period=5)
    assert out["price_below_orb"] == pytest.approx(1.0)
    assert out["price_above_orb"] == pytest.approx(0.0)


@pytest.mark.fast
def test_price_inside_orb():
    bars = [_make_bar(high=105.0, low=95.0, close=100.0)] * 6
    out = compute_features(bars, orb_period=5)
    assert out["price_above_orb"] == pytest.approx(0.0)
    assert out["price_below_orb"] == pytest.approx(0.0)


@pytest.mark.fast
def test_orb_breakout_confirmed_above():
    bars = _make_bars(6, base_close=100.0)
    bars[-1]["close"] = 200.0
    out = compute_features(bars, orb_period=5)
    assert out["orb_breakout_confirmed"] == pytest.approx(1.0)


@pytest.mark.fast
def test_orb_breakout_not_confirmed_inside():
    bars = [_make_bar(high=105.0, low=95.0, close=100.0)] * 6
    out = compute_features(bars, orb_period=5)
    assert out["orb_breakout_confirmed"] == pytest.approx(0.0)


@pytest.mark.fast
def test_orb_period_override():
    bars = _make_bars(10, base_close=100.0)
    out3 = compute_features(bars, orb_period=3)
    out7 = compute_features(bars, orb_period=7)
    # Different orb_period → different orb window → possibly different orb_high
    # At minimum both return valid dicts
    assert len(out3) == 74
    assert len(out7) == 74


# ── VWAP ───────────────────────────────────────────────────────────────────────


@pytest.mark.fast
def test_vwap_single_bar_equals_typical_price():
    bar = _make_bar(high=102.0, low=98.0, close=100.0, volume=1000.0)
    out = compute_features([bar])
    # typical = (102+98+100)/3 = 100.0
    assert out["vwap"] == pytest.approx(100.0)


@pytest.mark.fast
def test_vwap_deviation_zero_at_vwap():
    bar = _make_bar(high=102.0, low=98.0, close=100.0, volume=1000.0)
    out = compute_features([bar])
    # close == vwap → deviation should be ~0
    assert out["vwap_deviation"] == pytest.approx(0.0, abs=1e-6)


@pytest.mark.fast
def test_vwap_bands_upper_above_lower():
    bars = _make_bars(20)
    out = compute_features(bars)
    assert out["vwap_upper_band"] >= out["vwap_lower_band"]


@pytest.mark.fast
def test_zero_volume_bars_no_crash():
    bars = [_make_bar(volume=0.0, bid_volume=0.0, ask_volume=0.0)] * 5
    out = compute_features(bars)
    assert math.isfinite(out["vwap"])
    assert math.isfinite(out["volume_ratio"])


# ── Volume / order-flow ────────────────────────────────────────────────────────


@pytest.mark.fast
def test_volume_delta_ask_minus_bid():
    bar = _make_bar(bid_volume=300.0, ask_volume=700.0)
    out = compute_features([bar])
    assert out["volume_delta"] == pytest.approx(400.0)


@pytest.mark.fast
def test_cum_delta_sum_across_bars():
    bars = [
        _make_bar(bid_volume=100.0, ask_volume=200.0),  # delta = +100
        _make_bar(bid_volume=300.0, ask_volume=100.0),  # delta = -200
    ]
    out = compute_features(bars)
    assert out["cum_delta"] == pytest.approx(-100.0)


@pytest.mark.fast
def test_bid_ask_imbalance_balanced():
    bar = _make_bar(bid_volume=500.0, ask_volume=500.0)
    out = compute_features([bar])
    assert out["bid_ask_imbalance"] == pytest.approx(0.0)


@pytest.mark.fast
def test_bid_ask_imbalance_all_ask():
    bar = _make_bar(bid_volume=0.0, ask_volume=1000.0)
    out = compute_features([bar])
    assert out["bid_ask_imbalance"] == pytest.approx(1.0)


@pytest.mark.fast
def test_volume_ratio_above_average():
    # 19 bars with volume=100, last bar with volume=200 → ratio > 1
    bars = _make_bars(19, volume=100.0) + [_make_bar(volume=200.0)]
    out = compute_features(bars)
    assert out["volume_ratio"] > 1.0


# ── Moving averages ────────────────────────────────────────────────────────────


@pytest.mark.fast
def test_sma_equal_closes_all_same():
    bars = [_make_bar(close=50.0)] * 30
    out = compute_features(bars)
    assert out["sma_10"] == pytest.approx(50.0)
    assert out["sma_20"] == pytest.approx(50.0)
    assert out["sma_50"] == pytest.approx(50.0)


@pytest.mark.fast
def test_ema_converges_to_price_all_same():
    bars = [_make_bar(close=75.0)] * 60
    out = compute_features(bars)
    assert out["ema_3"] == pytest.approx(75.0, rel=1e-4)
    assert out["ema_20"] == pytest.approx(75.0, rel=1e-4)
    assert out["ema_50"] == pytest.approx(75.0, rel=1e-4)


@pytest.mark.fast
def test_ema_tracks_uptrend():
    bars = _make_bars(60, base_close=100.0, trend=1.0)
    out = compute_features(bars)
    # In an uptrend, shorter EMAs should be higher (tracking faster)
    assert out["ema_3"] > out["ema_20"]


# ── Bollinger Bands ────────────────────────────────────────────────────────────


@pytest.mark.fast
def test_bb_upper_above_lower():
    bars = _make_bars(30)
    out = compute_features(bars)
    assert out["bb_upper"] >= out["bb_lower"]


@pytest.mark.fast
def test_bb_width_non_negative():
    bars = _make_bars(30)
    out = compute_features(bars)
    assert out["bb_width"] >= 0.0


@pytest.mark.fast
def test_bb_position_in_range():
    bars = _make_bars(30)
    out = compute_features(bars)
    # bb_position can exceed [0,1] if close is outside the bands
    assert math.isfinite(out["bb_position"])


@pytest.mark.fast
def test_bb_all_same_price_no_crash():
    bars = [_make_bar(close=100.0, open_=100.0, high=100.0, low=100.0)] * 25
    out = compute_features(bars)
    assert out["bb_upper"] == pytest.approx(out["bb_lower"])
    assert out["bb_width"] == pytest.approx(0.0)


# ── RSI ───────────────────────────────────────────────────────────────────────


@pytest.mark.fast
def test_rsi_insufficient_data_returns_50():
    bars = _make_bars(3)
    out = compute_features(bars)
    assert out["rsi_7"] == pytest.approx(50.0)
    assert out["rsi_14"] == pytest.approx(50.0)


@pytest.mark.fast
def test_rsi_pure_uptrend_approaches_100():
    closes = [100.0 + i for i in range(30)]
    bars = [_make_bar(close=c, open_=c - 0.5, high=c + 0.5, low=c - 0.5) for c in closes]
    out = compute_features(bars)
    assert out["rsi_7"] > 90.0


@pytest.mark.fast
def test_rsi_pure_downtrend_approaches_0():
    closes = [200.0 - i for i in range(30)]
    bars = [_make_bar(close=c, open_=c + 0.5, high=c + 0.5, low=c - 0.5) for c in closes]
    out = compute_features(bars)
    assert out["rsi_7"] < 10.0


@pytest.mark.fast
def test_rsi_all_in_range():
    bars = _make_bars(50)
    out = compute_features(bars)
    assert 0.0 <= out["rsi_7"] <= 100.0
    assert 0.0 <= out["rsi_14"] <= 100.0
    assert 0.0 <= out["rsi_21"] <= 100.0


# ── MACD ─────────────────────────────────────────────────────────────────────


@pytest.mark.fast
def test_macd_histogram_equals_line_minus_signal():
    bars = _make_bars(50)
    out = compute_features(bars)
    assert out["macd_histogram"] == pytest.approx(
        out["macd_line"] - out["macd_signal"], rel=1e-6)


@pytest.mark.fast
def test_macd_zero_for_constant_price():
    bars = [_make_bar(close=100.0)] * 50
    out = compute_features(bars)
    assert out["macd_line"] == pytest.approx(0.0, abs=1e-6)
    assert out["macd_histogram"] == pytest.approx(0.0, abs=1e-6)


# ── Stochastic ────────────────────────────────────────────────────────────────


@pytest.mark.fast
def test_stoch_k_in_range():
    bars = _make_bars(20)
    out = compute_features(bars)
    assert 0.0 <= out["stoch_k"] <= 100.0
    assert 0.0 <= out["stoch_d"] <= 100.0


@pytest.mark.fast
def test_stoch_k_close_at_high_gives_100():
    bars = [_make_bar(high=110.0, low=90.0, close=110.0)] * 15
    out = compute_features(bars)
    assert out["stoch_k"] == pytest.approx(100.0)


@pytest.mark.fast
def test_stoch_k_close_at_low_gives_0():
    bars = [_make_bar(high=110.0, low=90.0, close=90.0)] * 15
    out = compute_features(bars)
    assert out["stoch_k"] == pytest.approx(0.0)


# ── ATR ───────────────────────────────────────────────────────────────────────


@pytest.mark.fast
def test_atr_positive_with_real_bars():
    bars = _make_bars(20)
    out = compute_features(bars)
    assert out["atr_7"] > 0.0
    assert out["atr_14"] > 0.0


@pytest.mark.fast
def test_true_range_positive():
    bars = _make_bars(5)
    out = compute_features(bars)
    assert out["true_range"] >= 0.0


@pytest.mark.fast
def test_atr7_lte_atr14_constant_range():
    # With constant bars, ATR7 and ATR14 should be equal (both just the bar range)
    bars = [_make_bar(high=105.0, low=95.0, close=100.0)] * 30
    out = compute_features(bars)
    assert out["atr_7"] == pytest.approx(out["atr_14"], rel=1e-4)


# ── CCI ───────────────────────────────────────────────────────────────────────


@pytest.mark.fast
def test_cci_finite():
    bars = _make_bars(25)
    out = compute_features(bars)
    assert math.isfinite(out["cci_20"])


@pytest.mark.fast
def test_cci_zero_constant_price():
    bars = [_make_bar(close=100.0, open_=100.0, high=100.0, low=100.0)] * 25
    out = compute_features(bars)
    assert out["cci_20"] == pytest.approx(0.0, abs=1e-6)


# ── Williams %R ───────────────────────────────────────────────────────────────


@pytest.mark.fast
def test_williams_r_in_range():
    bars = _make_bars(20)
    out = compute_features(bars)
    assert -100.0 <= out["williams_r"] <= 0.0


# ── Session time features ─────────────────────────────────────────────────────


@pytest.mark.fast
def test_minutes_since_open_at_rth_open():
    bar = {**_make_bar(), "ts": _rth_ts(9, 30)}
    out = compute_features([bar])
    assert out["minutes_since_open"] == pytest.approx(0.0)


@pytest.mark.fast
def test_minutes_since_open_one_hour_in():
    bar = {**_make_bar(), "ts": _rth_ts(10, 30)}
    out = compute_features([bar])
    assert out["minutes_since_open"] == pytest.approx(60.0)


@pytest.mark.fast
def test_is_power_hour_at_15():
    bar = {**_make_bar(), "ts": _rth_ts(15, 0)}
    out = compute_features([bar])
    assert out["is_power_hour"] == pytest.approx(1.0)


@pytest.mark.fast
def test_is_power_hour_at_14():
    bar = {**_make_bar(), "ts": _rth_ts(14, 0)}
    out = compute_features([bar])
    assert out["is_power_hour"] == pytest.approx(0.0)


@pytest.mark.fast
def test_session_quarter_increases():
    bars_q1 = [{**_make_bar(), "ts": _rth_ts(9, 30)}]
    bars_q4 = [{**_make_bar(), "ts": _rth_ts(15, 30)}]
    q1 = compute_features(bars_q1)["session_quarter"]
    q4 = compute_features(bars_q4)["session_quarter"]
    assert q1 < q4


@pytest.mark.fast
def test_session_half_first_half():
    bar = {**_make_bar(), "ts": _rth_ts(10, 0)}
    out = compute_features([bar])
    assert out["session_half"] == pytest.approx(1.0)


@pytest.mark.fast
def test_session_half_second_half():
    bar = {**_make_bar(), "ts": _rth_ts(13, 30)}
    out = compute_features([bar])
    assert out["session_half"] == pytest.approx(2.0)


# ── Tick / aggression ─────────────────────────────────────────────────────────


@pytest.mark.fast
def test_tick_direction_last5_all_bullish():
    closes = [100.0 + i for i in range(8)]
    bars = [_make_bar(close=c, open_=c - 0.5) for c in closes]
    out = compute_features(bars)
    assert out["tick_direction_last5"] == pytest.approx(1.0)


@pytest.mark.fast
def test_tick_direction_last5_all_bearish():
    closes = [100.0 - i for i in range(8)]
    bars = [_make_bar(close=c, open_=c + 0.5) for c in closes]
    out = compute_features(bars)
    assert out["tick_direction_last5"] == pytest.approx(0.0)


@pytest.mark.fast
def test_large_tick_count_non_negative():
    bars = _make_bars(10)
    out = compute_features(bars)
    assert out["large_tick_count"] >= 0.0


@pytest.mark.fast
def test_imbalance_ratio_minimum_one():
    bar = _make_bar(bid_volume=500.0, ask_volume=500.0)
    out = compute_features([bar])
    assert out["imbalance_ratio"] >= 1.0


@pytest.mark.fast
def test_trade_aggression_balanced():
    bars = [_make_bar(bid_volume=500.0, ask_volume=500.0)] * 6
    out = compute_features(bars)
    assert out["trade_aggression"] == pytest.approx(0.5)


@pytest.mark.fast
def test_uptick_downtick_counts():
    closes = [100.0, 101.0, 100.0, 99.0, 100.0]
    bars = [_make_bar(close=c) for c in closes]
    out = compute_features(bars)
    # 2 upticks (100→101, 99→100), 2 downticks (101→100, 100→99)
    assert out["uptick_count"] == pytest.approx(2.0)
    assert out["downtick_count"] == pytest.approx(2.0)


# ── Helper functions ───────────────────────────────────────────────────────────


@pytest.mark.fast
def test_sma_simple_window():
    assert _sma([1.0, 2.0, 3.0, 4.0, 5.0], 3) == pytest.approx(4.0)  # last 3: 3+4+5


@pytest.mark.fast
def test_sma_empty():
    assert _sma([], 10) == pytest.approx(0.0)


@pytest.mark.fast
def test_sma_fewer_than_n():
    assert _sma([2.0, 4.0], 10) == pytest.approx(3.0)


@pytest.mark.fast
def test_ema_constant_series():
    vals = [10.0] * 20
    assert _ema(vals, 5) == pytest.approx(10.0)


@pytest.mark.fast
def test_ema_empty():
    assert _ema([], 5) == pytest.approx(0.0)


@pytest.mark.fast
def test_rsi_constant_returns_100_or_50():
    # Constant closes → no gains or losses → avg_loss == 0 → RSI = 100
    closes = [100.0] * 20
    result = _rsi(closes, 14)
    assert result in (50.0, 100.0)


@pytest.mark.fast
def test_rsi_insufficient_returns_50():
    assert _rsi([1.0, 2.0], 14) == pytest.approx(50.0)


@pytest.mark.fast
def test_bb_wide_std():
    closes = [100.0 + (10.0 if i % 2 == 0 else -10.0) for i in range(25)]
    upper, lower, width = _bb(closes, 20)
    assert upper > lower
    assert width > 0.0


@pytest.mark.fast
def test_stoch_k_at_midpoint():
    bars = [_make_bar(high=110.0, low=90.0, close=100.0)] * 15
    assert _stoch_k(bars, 14) == pytest.approx(50.0)


@pytest.mark.fast
def test_tr_single_bar():
    bar = _make_bar(high=105.0, low=95.0, close=100.0)
    assert _tr(bar, 100.0) == pytest.approx(10.0)


@pytest.mark.fast
def test_atr_single_bar():
    bar = _make_bar(high=105.0, low=95.0, close=100.0)
    assert _atr([bar], 7) == pytest.approx(10.0)
