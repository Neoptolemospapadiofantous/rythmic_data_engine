"""
strategy/features.py — Shared feature engineering for NQ ORB.

Public API:
    compute_features(bars: list[dict]) -> dict[str, float]

Computes exactly 74 features from a list of 1-minute OHLCV bars.
Each bar must contain: timestamp, open, high, low, close, volume.
Optional keys: bid_volume, ask_volume (default to 0 when absent).

Feature values default to 0.0 when insufficient data is available.
No NaN or None values are returned.
"""
from __future__ import annotations

import datetime
import math
from typing import Any

# ORB period in bars (first N 1-minute bars define the opening range)
ORB_PERIOD = 5
# RTH session boundaries (Eastern Time)
_RTH_OPEN_MINUTES = 9 * 60 + 30   # 09:30 → 570
_RTH_CLOSE_MINUTES = 16 * 60       # 16:00 → 960
_RTH_TOTAL_MINUTES = _RTH_CLOSE_MINUTES - _RTH_OPEN_MINUTES  # 390

# Canonical feature order (74 features)
_FEATURE_NAMES: tuple[str, ...] = (
    # ORB core (6)
    "orb_high", "orb_low", "orb_range", "orb_midpoint",
    "price_above_orb", "price_below_orb",
    # Volume / order-flow (9)
    "volume_ratio", "vwap", "vwap_deviation", "volume_delta", "cum_delta",
    "volume_ma_20", "relative_volume", "vwap_upper_band", "vwap_lower_band",
    # Price action (6)
    "bar_range", "bar_direction", "close_vs_open",
    "high_of_day", "low_of_day", "distance_to_vwap",
    # Time / session (7)
    "minutes_since_open", "is_power_hour", "session_quarter", "session_half",
    "pre_market_gap", "opening_range_vol", "time_to_close",
    # Moving averages (8)
    "ema_3", "ema_5", "ema_10", "ema_20", "ema_50",
    "sma_10", "sma_20", "sma_50",
    # Momentum (12)
    "rsi_7", "rsi_14", "rsi_21",
    "macd_line", "macd_signal", "macd_histogram",
    "stoch_k", "stoch_d", "cci_20", "williams_r", "mfi_14", "bb_position",
    # Bollinger Bands (3)
    "bb_upper", "bb_lower", "bb_width",
    # Volatility (6)
    "atr_7", "atr_14", "hist_vol_20", "true_range",
    "keltner_upper", "keltner_lower",
    # Previous session (4)
    "prev_day_high", "prev_day_low", "prev_day_close", "overnight_gap",
    # ORB distance / confirmation (4)
    "distance_to_orb_high", "distance_to_orb_low",
    "orb_breakout_confirmed", "price_vs_orb_midpoint",
    # Tick / aggression (9)
    "tick_direction_last5", "large_tick_count", "bid_ask_imbalance",
    "tick_sum", "uptick_count", "downtick_count",
    "imbalance_ratio", "trade_aggression", "delta_divergence",
)

assert len(_FEATURE_NAMES) == 74

# ── private helpers ───────────────────────────────────────────────────────────

def _f(bar: dict, key: str) -> float:
    return float(bar[key])


def _fget(bar: dict, key: str, default: float = 0.0) -> float:
    return float(bar.get(key, default))


def _ts(bar: dict) -> datetime.datetime:
    # Accept both 'timestamp' (test/backtest) and 'ts' (production live bars from DB)
    ts = bar.get("timestamp") or bar.get("ts") or datetime.datetime.utcnow()
    if isinstance(ts, (int, float)):
        return datetime.datetime.utcfromtimestamp(ts)
    return ts


def _sma(values: list[float], n: int) -> float:
    if not values:
        return 0.0
    window = values[-n:] if len(values) >= n else values
    return sum(window) / len(window)


def _ema(values: list[float], n: int) -> float:
    """EMA using standard 2/(n+1) smoothing. Seeds on SMA of first min(n, len) values."""
    if not values:
        return 0.0
    k = 2.0 / (n + 1.0)
    start = min(n, len(values))
    ema = sum(values[:start]) / start
    for v in values[start:]:
        ema = v * k + ema * (1.0 - k)
    return ema


def _tr(bar: dict, prev_close: float) -> float:
    h, lo = _f(bar, "high"), _f(bar, "low")
    return max(h - lo, abs(h - prev_close), abs(lo - prev_close))


def _atr(bars: list[dict], n: int) -> float:
    """Wilder-smoothed ATR over n periods."""
    if len(bars) < 2:
        bar = bars[-1] if bars else {}
        return float(bar.get("high", 0)) - float(bar.get("low", 0)) if bar else 0.0
    trs: list[float] = [_tr(bars[i], _f(bars[i - 1], "close")) for i in range(1, len(bars))]
    start = min(n, len(trs))
    atr_val = sum(trs[:start]) / start
    k = 1.0 / n
    for tr in trs[start:]:
        atr_val = tr * k + atr_val * (1.0 - k)
    return atr_val


def _rsi(closes: list[float], n: int) -> float:
    """RSI using Wilder's smoothing. Returns 50.0 when insufficient data."""
    if len(closes) < n + 1:
        return 50.0
    changes = [closes[i] - closes[i - 1] for i in range(1, len(closes))]
    gains = [max(0.0, c) for c in changes]
    losses = [max(0.0, -c) for c in changes]
    avg_gain = sum(gains[:n]) / n
    avg_loss = sum(losses[:n]) / n
    for i in range(n, len(changes)):
        avg_gain = (avg_gain * (n - 1) + gains[i]) / n
        avg_loss = (avg_loss * (n - 1) + losses[i]) / n
    if avg_loss == 0.0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))


def _bb(closes: list[float], n: int = 20, multiplier: float = 2.0) -> tuple[float, float, float]:
    """Bollinger Bands. Returns (upper, lower, width_pct)."""
    sma = _sma(closes, n)
    window = closes[-n:] if len(closes) >= n else closes
    if len(window) < 2:
        return sma, sma, 0.0
    variance = sum((v - sma) ** 2 for v in window) / len(window)
    std = math.sqrt(variance)
    upper = sma + multiplier * std
    lower = sma - multiplier * std
    width = (upper - lower) / sma if sma != 0.0 else 0.0
    return upper, lower, width


def _stoch_k(bars: list[dict], n: int = 14) -> float:
    """Raw %K for the most recent bar."""
    window = bars[-n:] if len(bars) >= n else bars
    ph = max(_f(b, "high") for b in window)
    pl = min(_f(b, "low") for b in window)
    close = _f(bars[-1], "close")
    return (close - pl) / (ph - pl) * 100.0 if ph != pl else 50.0


def _zero_safe(d: dict[str, Any]) -> dict[str, float]:
    """Ensure every value is a float, replacing None/NaN with 0.0."""
    out: dict[str, float] = {}
    for k, v in d.items():
        try:
            fv = float(v)
            out[k] = 0.0 if math.isnan(fv) or math.isinf(fv) else fv
        except (TypeError, ValueError):
            out[k] = 0.0
    return out


# ── main public function ──────────────────────────────────────────────────────

def compute_features(bars: list[dict], orb_period: int = ORB_PERIOD) -> dict[str, float]:
    """Compute 74 features from a list of 1-minute OHLCV bars.

    Args:
        bars: non-empty list of bar dicts. Each bar must contain:
              timestamp (datetime or epoch float), open, high, low, close, volume.
              Optional: bid_volume, ask_volume (default 0).
        orb_period: number of 1-minute bars that define the opening range.
                    Must match orb_period_minutes from live_config.json.
                    Defaults to the module constant ORB_PERIOD (5) for
                    back-compat, but live callers should always pass the
                    configured value explicitly.

    Returns:
        Dict of exactly 74 feature name → float value.
        All values are finite floats; 0.0 is returned for any feature where
        there is insufficient history.
    """
    if not bars:
        return {k: 0.0 for k in _FEATURE_NAMES}

    n = len(bars)
    bar = bars[-1]

    closes: list[float] = [_f(b, "close") for b in bars]
    opens_: list[float] = [_f(b, "open") for b in bars]
    highs: list[float] = [_f(b, "high") for b in bars]
    lows: list[float] = [_f(b, "low") for b in bars]
    vols: list[float] = [_f(b, "volume") for b in bars]
    bid_vols: list[float] = [_fget(b, "bid_volume") for b in bars]
    ask_vols: list[float] = [_fget(b, "ask_volume") for b in bars]

    close = closes[-1]
    open_ = opens_[-1]
    high = highs[-1]
    low = lows[-1]
    volume = vols[-1]
    cur_bid_vol = bid_vols[-1]
    cur_ask_vol = ask_vols[-1]

    # ── ORB core ──────────────────────────────────────────────────────────────
    orb_count = min(orb_period, n)
    orb_bars = bars[:orb_count]
    orb_high = max(_f(b, "high") for b in orb_bars)
    orb_low = min(_f(b, "low") for b in orb_bars)
    orb_range = orb_high - orb_low
    orb_midpoint = (orb_high + orb_low) / 2.0
    price_above_orb = 1.0 if close > orb_high else 0.0
    price_below_orb = 1.0 if close < orb_low else 0.0

    # ── VWAP ─────────────────────────────────────────────────────────────────
    typical_prices: list[float] = [(highs[i] + lows[i] + closes[i]) / 3.0 for i in range(n)]
    vol_sum = sum(vols)
    if vol_sum > 0:
        vwap = sum(tp * v for tp, v in zip(typical_prices, vols)) / vol_sum
        vwap_var = sum(((tp - vwap) ** 2) * v for tp, v in zip(typical_prices, vols)) / vol_sum
        vwap_std = math.sqrt(vwap_var)
    else:
        vwap = close
        vwap_std = 0.0

    # ── Volume / order-flow ───────────────────────────────────────────────────
    vol_ma_20 = _sma(vols, 20)
    volume_ratio = volume / vol_ma_20 if vol_ma_20 > 0.0 else 1.0
    relative_volume = volume_ratio
    vwap_deviation = (close - vwap) / vwap if vwap != 0.0 else 0.0
    vwap_upper_band = vwap + 2.0 * vwap_std
    vwap_lower_band = vwap - 2.0 * vwap_std
    volume_delta = cur_ask_vol - cur_bid_vol
    cum_delta = sum(av - bv for av, bv in zip(ask_vols, bid_vols))

    # ── Price action ──────────────────────────────────────────────────────────
    bar_range = high - low
    bar_direction = (1.0 if close > open_ else (-1.0 if close < open_ else 0.0))
    close_vs_open = (close - open_) / open_ if open_ != 0.0 else 0.0
    high_of_day = max(highs)
    low_of_day = min(lows)
    distance_to_vwap = close - vwap

    # ── Time / session ────────────────────────────────────────────────────────
    ts = _ts(bar)
    try:
        bar_total_minutes = ts.hour * 60 + ts.minute
        minutes_since_open = float(max(0, bar_total_minutes - _RTH_OPEN_MINUTES))
        time_to_close = float(max(0, _RTH_CLOSE_MINUTES - bar_total_minutes))
        is_power_hour = 1.0 if ts.hour == 15 else 0.0
        quarter_len = _RTH_TOTAL_MINUTES / 4.0
        session_quarter = float(min(4, int(minutes_since_open / quarter_len) + 1))
        session_half = 1.0 if minutes_since_open < _RTH_TOTAL_MINUTES / 2.0 else 2.0
    except (AttributeError, TypeError):
        minutes_since_open = float(n - 1)
        time_to_close = float(max(0, _RTH_TOTAL_MINUTES - (n - 1)))
        is_power_hour = 0.0
        session_quarter = 1.0
        session_half = 1.0

    opening_range_vol = sum(_f(b, "volume") for b in orb_bars)
    # prev_day_* features require cross-session data not available in the bars window.
    # They are intentionally zeroed so the ML model sees a stable sentinel (0.0) rather
    # than opens_[0] - opens_[0] = 0 masquerading as a real value.
    prev_day_close = 0.0
    prev_day_high = 0.0
    prev_day_low = 0.0
    pre_market_gap = 0.0
    overnight_gap = 0.0

    # ── Moving averages ───────────────────────────────────────────────────────
    ema_3 = _ema(closes, 3)
    ema_5 = _ema(closes, 5)
    ema_10 = _ema(closes, 10)
    ema_20 = _ema(closes, 20)
    ema_50 = _ema(closes, 50)
    sma_10 = _sma(closes, 10)
    sma_20 = _sma(closes, 20)
    sma_50 = _sma(closes, 50)

    # ── Bollinger Bands ───────────────────────────────────────────────────────
    bb_upper, bb_lower, bb_width = _bb(closes, 20)
    bb_range = bb_upper - bb_lower
    bb_position = (close - bb_lower) / bb_range if bb_range > 0.0 else 0.5

    # ── Momentum ─────────────────────────────────────────────────────────────
    rsi_7 = _rsi(closes, 7)
    rsi_14 = _rsi(closes, 14)
    rsi_21 = _rsi(closes, 21)

    # MACD(12, 26, 9): single O(N) pass — carry EMA state forward bar by bar.
    # The old loop called _ema(closes[:i+1]) inside a for-i loop, making it O(N²).
    _k12, _k26 = 2.0 / 13.0, 2.0 / 27.0
    _e12 = _e26 = closes[0]
    macd_hist_vals: list[float] = []
    for _c in closes:
        _e12 = _c * _k12 + _e12 * (1.0 - _k12)
        _e26 = _c * _k26 + _e26 * (1.0 - _k26)
        macd_hist_vals.append(_e12 - _e26)
    macd_line = macd_hist_vals[-1]
    macd_signal = _ema(macd_hist_vals, 9)
    macd_histogram = macd_line - macd_signal

    # Stochastic(14, 3)
    stoch_k = _stoch_k(bars, 14)
    stoch_k_series: list[float] = [_stoch_k(bars[: i + 1], 14) for i in range(max(0, n - 3), n)]
    stoch_d = sum(stoch_k_series) / len(stoch_k_series) if stoch_k_series else 50.0

    # CCI(20): (typical_price - sma(tp, 20)) / (0.015 * mean_deviation)
    tp_sma_20 = _sma(typical_prices, 20)
    tp_window = typical_prices[-20:] if n >= 20 else typical_prices
    mean_dev = sum(abs(tp - tp_sma_20) for tp in tp_window) / len(tp_window)
    cci_20 = (typical_prices[-1] - tp_sma_20) / (0.015 * mean_dev) if mean_dev > 0.0 else 0.0

    # Williams %R(14)
    stoch_win = bars[-14:] if n >= 14 else bars
    ph14 = max(_f(b, "high") for b in stoch_win)
    pl14 = min(_f(b, "low") for b in stoch_win)
    williams_r = -((ph14 - close) / (ph14 - pl14) * 100.0) if ph14 != pl14 else -50.0

    # MFI(14): Money Flow Index
    if n >= 2:
        raw_mf: list[tuple[float, bool]] = []
        for i in range(1, n):
            tp_cur = typical_prices[i]
            tp_prev = typical_prices[i - 1]
            raw_mf.append((tp_cur * vols[i], tp_cur >= tp_prev))
        recent_mf = raw_mf[-(14 - 1):]
        pos_flow = sum(mf for mf, pos in recent_mf if pos)
        neg_flow = sum(mf for mf, pos in recent_mf if not pos)
        if neg_flow == 0.0:
            mfi_14 = 100.0
        elif pos_flow == 0.0:
            mfi_14 = 0.0
        else:
            mfi_14 = 100.0 - (100.0 / (1.0 + pos_flow / neg_flow))
    else:
        mfi_14 = 50.0

    # ── Volatility ────────────────────────────────────────────────────────────
    atr_7 = _atr(bars, 7)
    atr_14 = _atr(bars, 14)
    true_range = _tr(bar, closes[-2]) if n >= 2 else high - low

    # Historical volatility: annualised std of log-returns (252 days × 390 min/day)
    if n >= 2:
        log_rets = [math.log(closes[i] / closes[i - 1])
                    for i in range(1, n) if closes[i - 1] > 0.0 and closes[i] > 0.0]
        if len(log_rets) >= 2:
            lr_window = log_rets[-20:] if len(log_rets) >= 20 else log_rets
            lr_mean = sum(lr_window) / len(lr_window)
            lr_var = sum((r - lr_mean) ** 2 for r in lr_window) / len(lr_window)
            hist_vol_20 = math.sqrt(lr_var) * math.sqrt(252.0 * 390.0)
        else:
            hist_vol_20 = 0.0
    else:
        hist_vol_20 = 0.0

    # Keltner Channels: EMA(20) ± 2 × ATR(10)
    atr_10 = _atr(bars, 10)
    keltner_upper = ema_20 + 2.0 * atr_10
    keltner_lower = ema_20 - 2.0 * atr_10

    # ── ORB distance / confirmation ───────────────────────────────────────────
    distance_to_orb_high = close - orb_high
    distance_to_orb_low = close - orb_low
    orb_breakout_confirmed = 1.0 if (close > orb_high or close < orb_low) else 0.0
    price_vs_orb_midpoint = close - orb_midpoint

    # ── Tick / aggression ─────────────────────────────────────────────────────
    last5_bars = bars[-5:] if n >= 5 else bars
    tick_direction_last5 = sum(
        1 for b in last5_bars if _f(b, "close") > _f(b, "open")
    ) / len(last5_bars)

    ranges = [highs[i] - lows[i] for i in range(n)]
    avg_range = sum(ranges) / n if n > 0 else 0.0
    large_tick_count = float(sum(1 for r in ranges if r > avg_range * 1.5))

    total_ba = cur_bid_vol + cur_ask_vol
    bid_ask_imbalance = (cur_ask_vol - cur_bid_vol) / total_ba if total_ba > 0.0 else 0.0

    tick_sum = 0.0
    uptick_count = 0.0
    downtick_count = 0.0
    for i in range(1, n):
        if closes[i] > closes[i - 1]:
            tick_sum += 1.0
            uptick_count += 1.0
        elif closes[i] < closes[i - 1]:
            tick_sum -= 1.0
            downtick_count += 1.0

    if cur_bid_vol > 0.0 and cur_ask_vol > 0.0:
        imbalance_ratio = max(cur_ask_vol / cur_bid_vol, cur_bid_vol / cur_ask_vol)
    elif total_ba == 0.0:
        imbalance_ratio = 1.0
    else:
        imbalance_ratio = 2.0

    last5_ask_sum = sum(_fget(b, "ask_volume") for b in last5_bars)
    last5_bid_sum = sum(_fget(b, "bid_volume") for b in last5_bars)
    last5_total = last5_ask_sum + last5_bid_sum
    trade_aggression = last5_ask_sum / last5_total if last5_total > 0.0 else 0.5

    deltas = [ask_vols[i] - bid_vols[i] for i in range(n)]
    delta_ema5 = _ema(deltas, 5)
    first_close = closes[0]
    price_dir = (1.0 if close > first_close else (-1.0 if close < first_close else 0.0))
    delta_divergence = delta_ema5 * price_dir

    raw = {
        "orb_high": orb_high,
        "orb_low": orb_low,
        "orb_range": orb_range,
        "orb_midpoint": orb_midpoint,
        "price_above_orb": price_above_orb,
        "price_below_orb": price_below_orb,
        "volume_ratio": volume_ratio,
        "vwap": vwap,
        "vwap_deviation": vwap_deviation,
        "volume_delta": volume_delta,
        "cum_delta": cum_delta,
        "volume_ma_20": vol_ma_20,
        "relative_volume": relative_volume,
        "vwap_upper_band": vwap_upper_band,
        "vwap_lower_band": vwap_lower_band,
        "bar_range": bar_range,
        "bar_direction": bar_direction,
        "close_vs_open": close_vs_open,
        "high_of_day": high_of_day,
        "low_of_day": low_of_day,
        "distance_to_vwap": distance_to_vwap,
        "minutes_since_open": minutes_since_open,
        "is_power_hour": is_power_hour,
        "session_quarter": session_quarter,
        "session_half": session_half,
        "pre_market_gap": pre_market_gap,
        "opening_range_vol": opening_range_vol,
        "time_to_close": time_to_close,
        "ema_3": ema_3,
        "ema_5": ema_5,
        "ema_10": ema_10,
        "ema_20": ema_20,
        "ema_50": ema_50,
        "sma_10": sma_10,
        "sma_20": sma_20,
        "sma_50": sma_50,
        "rsi_7": rsi_7,
        "rsi_14": rsi_14,
        "rsi_21": rsi_21,
        "macd_line": macd_line,
        "macd_signal": macd_signal,
        "macd_histogram": macd_histogram,
        "stoch_k": stoch_k,
        "stoch_d": stoch_d,
        "cci_20": cci_20,
        "williams_r": williams_r,
        "mfi_14": mfi_14,
        "bb_position": bb_position,
        "bb_upper": bb_upper,
        "bb_lower": bb_lower,
        "bb_width": bb_width,
        "atr_7": atr_7,
        "atr_14": atr_14,
        "hist_vol_20": hist_vol_20,
        "true_range": true_range,
        "keltner_upper": keltner_upper,
        "keltner_lower": keltner_lower,
        "prev_day_high": prev_day_high,
        "prev_day_low": prev_day_low,
        "prev_day_close": prev_day_close,
        "overnight_gap": overnight_gap,
        "distance_to_orb_high": distance_to_orb_high,
        "distance_to_orb_low": distance_to_orb_low,
        "orb_breakout_confirmed": orb_breakout_confirmed,
        "price_vs_orb_midpoint": price_vs_orb_midpoint,
        "tick_direction_last5": tick_direction_last5,
        "large_tick_count": large_tick_count,
        "bid_ask_imbalance": bid_ask_imbalance,
        "tick_sum": tick_sum,
        "uptick_count": uptick_count,
        "downtick_count": downtick_count,
        "imbalance_ratio": imbalance_ratio,
        "trade_aggression": trade_aggression,
        "delta_divergence": delta_divergence,
    }
    return _zero_safe(raw)
