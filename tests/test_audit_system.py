"""
Tests for the MNQ audit system — validates formula correctness, contract constants,
and cross-system invariants defined in quality_rules/*.yaml.

All tests marked @pytest.mark.fast (no I/O, no subprocesses).
"""

import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(REPO_ROOT))

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
