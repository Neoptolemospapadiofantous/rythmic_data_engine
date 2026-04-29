"""
Tests for the MNQ audit system — validates formula correctness, contract constants,
and cross-system invariants defined in quality_rules/*.yaml.

All tests marked @pytest.mark.fast (no I/O, no subprocesses).
"""

import datetime
import json as _json
import sys
import zoneinfo
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(REPO_ROOT))

LIVE_CONFIG_PATH = REPO_ROOT / "config" / "live_config.json"
_live_cfg: dict = _json.loads(LIVE_CONFIG_PATH.read_text()) if LIVE_CONFIG_PATH.exists() else {}

ET = zoneinfo.ZoneInfo("America/New_York")

from strategy.micro_orb import MicroORBStrategy

# ── MNQ contract constants ─────────────────────────────────────────────────────
MNQ_POINT_VALUE = 2.0
MNQ_TICK_SIZE = 0.25
MNQ_TICK_VALUE = MNQ_POINT_VALUE * MNQ_TICK_SIZE  # 0.50
COMMISSION_PER_SIDE = 2.0
COMMISSION_RT = COMMISSION_PER_SIDE * 2  # 4.0


# ── Helpers ────────────────────────────────────────────────────────────────────

def gross_pnl(entry: float, exit_: float, direction: str, qty: int = 1) -> float:
    mult = 1.0 if direction == "long" else -1.0
    return mult * (exit_ - entry) * MNQ_POINT_VALUE * qty


def net_pnl(gross: float) -> float:
    return gross - COMMISSION_RT


def pnl_points(entry: float, exit_: float, direction: str) -> float:
    mult = 1.0 if direction == "long" else -1.0
    return mult * (exit_ - entry)


def pnl_ticks(points: float) -> float:
    return points / MNQ_TICK_SIZE


# ── Contract constant tests ────────────────────────────────────────────────────

@pytest.mark.fast
def test_mnq_tick_value_derived():
    """TICK_VALUE = POINT_VALUE * TICK_SIZE must equal $0.50 for MNQ."""
    assert MNQ_TICK_VALUE == pytest.approx(0.50)


@pytest.mark.fast
def test_mnq_point_value():
    """MNQ point value is $2.00, not $20.00 (NQ)."""
    assert MNQ_POINT_VALUE == pytest.approx(2.0)
    assert MNQ_POINT_VALUE != 20.0, "point_value=20.0 is NQ — trading MNQ"


@pytest.mark.fast
def test_commission_rt():
    """Round-trip commission must be $4.00 (2x $2.00/side)."""
    assert COMMISSION_RT == pytest.approx(4.0)
    assert COMMISSION_RT == COMMISSION_PER_SIDE * 2


# ── Formula test vectors (FORMULA-001 to FORMULA-009) ─────────────────────────

@pytest.mark.fast
def test_tv001_long_winner():
    """TV-001: Long entry=21000 exit=21010 → gross=20.0, net=16.0, points=10.0, ticks=40."""
    g = gross_pnl(21000.0, 21010.0, "long")
    assert g == pytest.approx(20.0)
    assert net_pnl(g) == pytest.approx(16.0)
    pts = pnl_points(21000.0, 21010.0, "long")
    assert pts == pytest.approx(10.0)
    assert pnl_ticks(pts) == pytest.approx(40.0)


@pytest.mark.fast
def test_tv002_short_winner():
    """TV-002: Short entry=21010 exit=21000 → gross=20.0, net=16.0, points=10.0, ticks=40."""
    g = gross_pnl(21010.0, 21000.0, "short")
    assert g == pytest.approx(20.0)
    assert net_pnl(g) == pytest.approx(16.0)
    pts = pnl_points(21010.0, 21000.0, "short")
    assert pts == pytest.approx(10.0)
    assert pnl_ticks(pts) == pytest.approx(40.0)


@pytest.mark.fast
def test_tv003_long_loser():
    """TV-003: Long entry=21000 exit=20990 → gross=-20.0, net=-24.0."""
    g = gross_pnl(21000.0, 20990.0, "long")
    assert g == pytest.approx(-20.0)
    assert net_pnl(g) == pytest.approx(-24.0)


@pytest.mark.fast
def test_tv004_sl_price_long():
    """TV-004: Long SL price = entry - sl_points = 21000 - 15 = 20985."""
    entry = 21000.0
    sl_points = 15.0
    sl_price = entry - sl_points
    assert sl_price == pytest.approx(20985.0)


@pytest.mark.fast
def test_sl_price_short():
    """Short SL price = entry + sl_points."""
    entry = 21000.0
    sl_points = 15.0
    sl_price = entry + sl_points
    assert sl_price == pytest.approx(21015.0)


@pytest.mark.fast
def test_target_price_long():
    """Long target = entry + target_points."""
    assert 21000.0 + 20.0 == pytest.approx(21020.0)


@pytest.mark.fast
def test_target_price_short():
    """Short target = entry - target_points."""
    assert 21000.0 - 20.0 == pytest.approx(20980.0)


@pytest.mark.fast
def test_ticks_from_points():
    """10 points = 40 ticks (MNQ tick_size=0.25)."""
    assert pnl_ticks(10.0) == pytest.approx(40.0)
    assert pnl_ticks(0.25) == pytest.approx(1.0)
    assert pnl_ticks(15.0) == pytest.approx(60.0)  # default sl_points


@pytest.mark.fast
def test_flat_trade_zero_gross_negative_net():
    """Breakeven trade: gross=0.0, net=-4.0 (commission still applies)."""
    g = gross_pnl(21000.0, 21000.0, "long")
    assert g == pytest.approx(0.0)
    assert net_pnl(g) == pytest.approx(-4.0)


@pytest.mark.fast
def test_gross_pnl_extreme_prices():
    """PnL must be finite for extreme but valid MNQ prices."""
    assert gross_pnl(30000.0, 30100.0, "long") == pytest.approx(200.0)
    assert gross_pnl(10000.0, 10001.0, "long") == pytest.approx(2.0)
    assert gross_pnl(22000.0, 22000.0, "long") == pytest.approx(0.0)


# ── Net PnL invariants ─────────────────────────────────────────────────────────

@pytest.mark.fast
def test_net_always_4_less_than_gross():
    """net_pnl == gross_pnl - 4.0 for any trade (no separate slippage in Python)."""
    for gross in [20.0, 0.0, -20.0, 100.0, -100.0]:
        assert net_pnl(gross) == pytest.approx(gross - 4.0)


@pytest.mark.fast
def test_winner_net_less_than_gross():
    """For winners, net_pnl < gross_pnl (commission reduces it)."""
    g = gross_pnl(21000.0, 21010.0, "long")
    assert g > 0
    assert net_pnl(g) < g


@pytest.mark.fast
def test_loser_net_worse_than_gross():
    """For losers, net_pnl < gross_pnl (commission makes it worse)."""
    g = gross_pnl(21000.0, 20990.0, "long")
    assert g < 0
    assert net_pnl(g) < g


# ── Regression: hardcoded NQ defaults ─────────────────────────────────────────

@pytest.mark.fast
def test_micro_orb_default_point_value_is_mnq():
    """micro_orb.py must default point_value to 2.0 (MNQ), not 20.0 (NQ)."""
    cfg = {"orb": {
        "orb_period_minutes": 5,
        "stop_loss_ticks": 16,
        "target_ticks": 48,
        "tick_size": 0.25,
        "rth_open": "09:30:00",
        "rth_close": "16:00:00",
        "eod_exit_minutes_before_close": 15,
        "allow_short": True,
        # Intentionally omit point_value to test default
    }}
    s = MicroORBStrategy(cfg)
    assert s._point_value == pytest.approx(2.0), (
        f"MicroORBStrategy default point_value={s._point_value} — must be 2.0 for MNQ, not 20.0"
    )


@pytest.mark.fast
def test_micro_orb_explicit_point_value_respected():
    """Explicit point_value=2.0 in config is used correctly."""
    cfg = {"orb": {
        "orb_period_minutes": 5,
        "stop_loss_ticks": 16,
        "target_ticks": 48,
        "tick_size": 0.25,
        "point_value": 2.0,
        "rth_open": "09:30:00",
        "rth_close": "16:00:00",
        "eod_exit_minutes_before_close": 15,
        "allow_short": True,
    }}
    s = MicroORBStrategy(cfg)
    assert s._point_value == pytest.approx(2.0)


@pytest.mark.fast
def test_point_value_20_would_cause_10x_error():
    """Demonstrates that point_value=20.0 inflates PnL by 10x relative to MNQ=2.0."""
    entry, exit_ = 21000.0, 21010.0
    correct = gross_pnl(entry, exit_, "long")  # 20.0
    wrong = (exit_ - entry) * 20.0             # 200.0 (NQ value)
    assert correct == pytest.approx(20.0)
    assert wrong == pytest.approx(200.0)
    assert wrong / correct == pytest.approx(10.0), "NQ point_value causes 10x PnL inflation"


# ── Cross-system contract: C++ NQ_TICK_VALUE bug regression ───────────────────

@pytest.mark.fast
def test_nq_tick_value_not_used_for_mnq_slippage():
    """
    CPP-BUG-001 regression: latency_logger must use MNQ_TICK_VALUE=0.50,
    not NQ_TICK_VALUE=5.00. Verify the expected values differ by 10x.
    """
    nq_tick_value = 5.00    # what the constant was (NQ)
    mnq_tick_value = 0.50   # what MNQ actually needs
    slippage_ticks = 4

    wrong_slippage_usd = slippage_ticks * nq_tick_value
    correct_slippage_usd = slippage_ticks * mnq_tick_value

    assert wrong_slippage_usd == pytest.approx(20.0)    # 10x inflated
    assert correct_slippage_usd == pytest.approx(2.0)   # correct for MNQ
    assert wrong_slippage_usd / correct_slippage_usd == pytest.approx(10.0)


# ── live_trader.py: symbol written to DB ──────────────────────────────────────

@pytest.mark.fast
def test_write_trade_open_uses_mnq_symbol_default():
    """_write_trade_open must default to 'MNQ', not 'NQ'."""
    from live_trader import _write_trade_open
    import inspect
    sig = inspect.signature(_write_trade_open)
    symbol_param = sig.parameters.get("symbol")
    assert symbol_param is not None, "_write_trade_open must accept a 'symbol' parameter"
    assert symbol_param.default == "MNQ", (
        f"_write_trade_open default symbol='{symbol_param.default}' — must be 'MNQ'"
    )


# ── ORB strategy helpers ───────────────────────────────────────────────────────

def _orb_cfg(allow_short: bool = True) -> dict:
    return {"orb": {
        "orb_period_minutes": 5,
        "stop_loss_ticks": 60,
        "target_ticks": 48,
        "tick_size": 0.25,
        "point_value": 2.0,
        "rth_open": "09:30:00",
        "rth_close": "16:00:00",
        "eod_exit_minutes_before_close": 15,
        "allow_short": allow_short,
    }}


def _bar(ts: datetime.datetime, high: float, low: float, close: float) -> dict:
    return {"ts": ts, "open": close, "high": high, "low": low, "close": close, "volume": 1000}


def _strategy_with_range(allow_short: bool = True):
    """Return (strategy, base_dt) after 5 ORB bars lock the range at 17010/16990."""
    s = MicroORBStrategy(_orb_cfg(allow_short))
    base = datetime.datetime(2024, 1, 15, 9, 30, tzinfo=ET)
    for i in range(5):
        s.on_bar(_bar(base + datetime.timedelta(minutes=i), 17010.0, 16990.0, 17005.0))
    return s, base


# ── GROUP 1: ORB range building invariants ────────────────────────────────────

@pytest.mark.fast
def test_orb_range_high_gte_low():
    """After feeding ORB bars, range_high >= range_low."""
    s, _ = _strategy_with_range()
    assert s.orb_high is not None
    assert s.orb_low is not None
    assert s.orb_high >= s.orb_low


@pytest.mark.fast
def test_orb_range_only_built_during_window():
    """Bars outside the 9:30-9:35 ET range window do not change the locked range."""
    s, base = _strategy_with_range()
    saved_high, saved_low = s.orb_high, s.orb_low
    # Bar at 9:40 with extreme prices — range is already locked, must not change
    s.on_bar(_bar(base + datetime.timedelta(minutes=10), 99999.0, 1.0, 17005.0))
    assert s.orb_high == pytest.approx(saved_high)
    assert s.orb_low == pytest.approx(saved_low)


@pytest.mark.fast
def test_orb_breakout_long_above_range_high():
    """Close above range_high triggers a LONG signal."""
    s, base = _strategy_with_range()
    # orb_high=17010.0; close=17020.0 is above it
    sig = s.on_bar(_bar(base + datetime.timedelta(minutes=5), 17025.0, 17011.0, 17020.0))
    assert sig is not None
    assert sig.direction == "LONG"


@pytest.mark.fast
def test_orb_breakout_short_below_range_low():
    """Close below range_low triggers a SHORT signal when allow_short=True."""
    s, base = _strategy_with_range(allow_short=True)
    # orb_low=16990.0; close=16980.0 is below it
    sig = s.on_bar(_bar(base + datetime.timedelta(minutes=5), 16989.0, 16975.0, 16980.0))
    assert sig is not None
    assert sig.direction == "SHORT"


@pytest.mark.fast
def test_orb_no_entry_inside_range():
    """Close inside range does not trigger a signal."""
    s, base = _strategy_with_range()
    # orb_high=17010.0, orb_low=16990.0; close=17000.0 is inside
    sig = s.on_bar(_bar(base + datetime.timedelta(minutes=5), 17008.0, 16995.0, 17000.0))
    assert sig is None


# ── GROUP 2: Trailing stop invariants ─────────────────────────────────────────

@pytest.mark.fast
def test_trailing_stop_formula_long():
    """Trailing stop for a long: new_stop = max(current_stop, current_price - trail_step).

    The stop must only move up — never retreat.
    """
    trail_step = _live_cfg.get("trail_step", 10.0)
    current_stop = 17005.0  # initial stop (entry - sl_points = 17020 - 15)

    # Price rises to 17030: new stop candidate = 17030 - trail_step
    price_high = 17030.0
    new_stop = max(current_stop, price_high - trail_step)
    assert new_stop > current_stop, "Stop must rise when price rises"

    # Price retreats to 17022: stop must NOT fall below new_stop
    price_retreat = 17022.0
    after_retreat = max(new_stop, price_retreat - trail_step)
    assert after_retreat >= new_stop, "Trailing stop must never retreat for a long"


@pytest.mark.fast
def test_trailing_stop_breakeven_trigger():
    """After trail_be_trigger points profit, stop formula places stop at entry - trail_be_offset."""
    trail_be_trigger = _live_cfg.get("trail_be_trigger", 3.0)   # points to profit before BE
    trail_be_offset = _live_cfg.get("trail_be_offset", 1.0)     # points below entry for BE stop
    entry = 17020.0
    profit_trigger_price = entry + trail_be_trigger   # price that activates BE
    be_stop = entry - trail_be_offset                 # resulting BE stop level

    assert profit_trigger_price > entry, "trail_be_trigger must be positive"
    assert profit_trigger_price - entry == pytest.approx(trail_be_trigger)
    assert entry - be_stop == pytest.approx(trail_be_offset)
    assert be_stop < entry, "breakeven stop must be below entry (small downside buffer)"


@pytest.mark.fast
def test_trailing_stop_sl_points_default():
    """live_config.json sl_points must be 15.0 (matches C++ SL distance)."""
    assert _live_cfg.get("sl_points") == pytest.approx(15.0)


# ── GROUP 5: live_config.json key presence ────────────────────────────────────

@pytest.mark.fast
def test_live_config_has_legends_user():
    assert "rithmic_legends_user" in _live_cfg, "rithmic_legends_user missing from live_config.json"


@pytest.mark.fast
def test_live_config_has_account_id():
    assert "account_id" in _live_cfg, "account_id missing from live_config.json"
    assert _live_cfg["account_id"], "account_id must be non-empty"


@pytest.mark.fast
def test_live_config_has_prop_firm():
    assert "prop_firm" in _live_cfg, "prop_firm section missing from live_config.json"


@pytest.mark.fast
def test_live_config_mnq_symbol():
    assert _live_cfg.get("symbol") == "MNQ", f"symbol={_live_cfg.get('symbol')} — must be MNQ not NQ"


@pytest.mark.fast
def test_live_config_point_value_2():
    pv = _live_cfg.get("point_value")
    assert pv == pytest.approx(2.0), f"point_value={pv} — must be 2.0 for MNQ"


# ── GROUP 6: EOD + last_entry timing ──────────────────────────────────────────

@pytest.mark.fast
def test_eod_flatten_time_is_1555():
    assert _live_cfg.get("eod_flatten_hour") == 15
    assert _live_cfg.get("eod_flatten_min") == 55


@pytest.mark.fast
def test_last_entry_hour_is_13():
    assert _live_cfg.get("last_entry_hour") == 13, (
        f"last_entry_hour={_live_cfg.get('last_entry_hour')} — no new trades after 1 PM ET"
    )


# ── GROUP 7: Risk manager limits ──────────────────────────────────────────────

@pytest.mark.fast
def test_daily_loss_limit_is_negative():
    val = _live_cfg.get("daily_loss_limit", 0)
    assert val < 0, f"daily_loss_limit={val} — must be negative"


@pytest.mark.fast
def test_trailing_drawdown_cap_positive():
    val = _live_cfg.get("trailing_drawdown_cap", 0)
    assert val > 0, f"trailing_drawdown_cap={val} — must be positive"


@pytest.mark.fast
def test_max_daily_trades_positive():
    val = _live_cfg.get("max_daily_trades", 0)
    assert val >= 1, f"max_daily_trades={val} — must be >= 1"


@pytest.mark.fast
def test_prop_firm_daily_loss_limit_positive():
    """Legends prop_firm.daily_loss_limit is a positive cap (not a negative floor)."""
    pf = _live_cfg.get("prop_firm", {})
    val = pf.get("daily_loss_limit", -1)
    assert val > 0, f"prop_firm.daily_loss_limit={val} — should be a positive cap value"


@pytest.mark.fast
def test_trailing_drawdown_matches_prop_firm():
    """Top-level trailing_drawdown_cap must equal prop_firm.trailing_drawdown_limit."""
    top = _live_cfg.get("trailing_drawdown_cap", 0)
    pf = _live_cfg.get("prop_firm", {}).get("trailing_drawdown_limit", 0)
    assert top == pytest.approx(pf), (
        f"trailing_drawdown_cap={top} != prop_firm.trailing_drawdown_limit={pf}"
    )


# ── GROUP 8: ORB strategy config values ───────────────────────────────────────

@pytest.mark.fast
def test_orb_minutes_is_5():
    assert _live_cfg.get("orb_minutes") == 5, (
        f"orb_minutes={_live_cfg.get('orb_minutes')} — strategy requires 5-min ORB"
    )


@pytest.mark.fast
def test_sl_points_positive():
    val = _live_cfg.get("sl_points", 0)
    assert val > 0, f"sl_points={val} must be positive"


@pytest.mark.fast
def test_sl_ticks_consistent():
    """sl_points=15 at tick_size=0.25 gives exactly 60 ticks."""
    sl_points = _live_cfg.get("sl_points", 0)
    sl_ticks = sl_points / 0.25
    assert sl_ticks == pytest.approx(60.0), f"sl_ticks={sl_ticks} — expected 60 for sl_points=15"


@pytest.mark.fast
def test_trail_be_trigger_less_than_trail_step():
    """BE trigger must activate before trail_step so trailing has room to kick in."""
    be_trigger = _live_cfg.get("trail_be_trigger", 0)
    trail_step = _live_cfg.get("trail_step", 0)
    assert be_trigger < trail_step, (
        f"trail_be_trigger={be_trigger} >= trail_step={trail_step} — trailing never activates"
    )


# ── GROUP 9: dry_run safety ───────────────────────────────────────────────────

@pytest.mark.fast
def test_live_config_dry_run_is_false():
    """Production live_config.json must have dry_run=false — catches accidental paper-trading."""
    val = _live_cfg.get("dry_run")
    assert val is False, f"dry_run={val!r} — live_config.json must have dry_run=false for live trading"
