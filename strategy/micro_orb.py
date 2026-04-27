"""
MicroORBStrategy — Opening Range Breakout for NQ futures.

State machine: WAITING → ORB_BUILDING → WATCHING → IN_POSITION → FLAT

Doctest example (verifiable with: python -m doctest strategy/micro_orb.py -v):

    >>> from strategy.micro_orb import MicroORBStrategy, StrategyState
    >>> import datetime, zoneinfo
    >>> ET = zoneinfo.ZoneInfo("America/New_York")
    >>> cfg = {"orb": {"orb_period_minutes": 5, "stop_loss_ticks": 16,
    ...     "target_ticks": 48, "tick_size": 0.25, "point_value": 2.0,
    ...     "rth_open": "09:30:00", "rth_close": "16:00:00",
    ...     "eod_exit_minutes_before_close": 15, "allow_short": True}}
    >>> s = MicroORBStrategy(cfg)
    >>> base_dt = datetime.datetime(2024, 1, 15, 9, 30, tzinfo=ET)

Feed 5 bars to build the ORB range (09:30–09:35):

    >>> for i in range(5):
    ...     bar = {"ts": base_dt + datetime.timedelta(minutes=i),
    ...            "open": 17000.0, "high": 17010.0 + i,
    ...            "low": 16990.0 - i, "close": 17005.0, "volume": 1000}
    ...     sig = s.on_bar(bar)
    >>> s.state.name
    'WATCHING'
    >>> s.orb_high is not None
    True

Feed a breakout bar (close above orb_high):

    >>> breakout_bar = {"ts": base_dt + datetime.timedelta(minutes=5),
    ...     "open": 17014.25, "high": 17020.0, "low": 17013.0,
    ...     "close": 17020.0, "volume": 500}
    >>> sig = s.on_bar(breakout_bar)
    >>> sig is not None
    True
    >>> sig.direction
    'LONG'
"""

from __future__ import annotations

import datetime
import zoneinfo
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Optional


ET = zoneinfo.ZoneInfo("America/New_York")


class StrategyState(Enum):
    WAITING = auto()
    ORB_BUILDING = auto()
    WATCHING = auto()
    IN_POSITION = auto()
    FLAT = auto()


@dataclass
class Signal:
    """Trade signal emitted by MicroORBStrategy.on_bar()."""
    direction: str          # 'LONG' or 'SHORT'
    entry_price: float
    stop_loss: float
    target: float
    bar_ts: datetime.datetime


@dataclass
class _Position:
    direction: str
    entry_price: float
    stop_loss: float
    target: float
    entry_ts: datetime.datetime
    highest_price: float = field(init=False)
    lowest_price: float = field(init=False)

    def __post_init__(self) -> None:
        self.highest_price = self.entry_price
        self.lowest_price = self.entry_price


class MicroORBStrategy:
    """ORB strategy: builds range over first N RTH minutes, trades breakout.

    Args:
        config: full live_config.json dict (reads config["orb"] section).
    """

    def __init__(self, config: dict) -> None:
        orb = config["orb"]
        self._range_minutes: int = int(orb["orb_period_minutes"])
        self._sl_ticks: int = int(orb["stop_loss_ticks"])
        self._target_ticks: int = int(orb["target_ticks"])
        self._tick: float = float(orb["tick_size"])
        self._point_value: float = float(orb.get("point_value", 2.0))
        self._allow_short: bool = bool(orb.get("allow_short", True))
        self._eod_cutoff_minutes: int = int(orb.get("eod_exit_minutes_before_close", 15))

        rth_open = orb.get("rth_open", "09:30:00")
        rth_close = orb.get("rth_close", "16:00:00")
        self._rth_open = datetime.time.fromisoformat(rth_open)
        self._rth_close = datetime.time.fromisoformat(rth_close)

        self.state: StrategyState = StrategyState.WAITING
        self.orb_high: Optional[float] = None
        self.orb_low: Optional[float] = None
        self._range_bars: list[dict] = []
        self._position: Optional[_Position] = None
        self._session_date: Optional[datetime.date] = None

    # ── public API ────────────────────────────────────────────────────

    def on_bar(self, bar: dict) -> Optional[Signal]:
        """Process a completed 1-min OHLCV bar.

        Args:
            bar: dict with keys ts (datetime, tz-aware ET), open, high, low, close, volume.

        Returns:
            Signal if a new entry should be taken, else None.
        """
        ts: datetime.datetime = bar["ts"]
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=ET)
        ts_et = ts.astimezone(ET)
        bar_time = ts_et.time()

        # New session reset
        if self._session_date != ts_et.date():
            self._reset_session(ts_et.date())

        # Outside RTH — ignore
        if bar_time < self._rth_open or bar_time >= self._rth_close:
            return None

        # EOD cutoff — no new entries
        eod_cutoff = (datetime.datetime.combine(ts_et.date(), self._rth_close, tzinfo=ET)
                      - datetime.timedelta(minutes=self._eod_cutoff_minutes)).time()
        if bar_time >= eod_cutoff and self.state != StrategyState.IN_POSITION:
            return None

        if self.state == StrategyState.WAITING:
            self.state = StrategyState.ORB_BUILDING
            self._range_bars = [bar]
        elif self.state == StrategyState.ORB_BUILDING:
            self._range_bars.append(bar)
            # Lock range after collecting exactly N range bars (1-min bars)
            if len(self._range_bars) >= self._range_minutes:
                self._lock_range()
                self.state = StrategyState.WATCHING
        elif self.state == StrategyState.WATCHING:
            return self._check_breakout(bar, ts_et)
        # IN_POSITION and FLAT: on_bar does not exit; use on_tick for that

        return None

    def on_tick(self, tick: dict) -> Optional[str]:
        """Process a live tick while in position.

        Args:
            tick: dict with keys price (float) and ts (datetime).

        Returns:
            'EXIT' if stop-loss or target is hit, else None.
        """
        if self.state != StrategyState.IN_POSITION or self._position is None:
            return None

        price: float = float(tick["price"])
        pos = self._position

        # Update trailing reference price
        if pos.direction == "LONG":
            pos.highest_price = max(pos.highest_price, price)
            pos.stop_loss = self.update_stop(pos.highest_price)
            if price <= pos.stop_loss or price >= pos.target:
                self.state = StrategyState.FLAT
                return "EXIT"
        else:
            pos.lowest_price = min(pos.lowest_price, price)
            pos.stop_loss = self.update_stop(pos.lowest_price)
            if price >= pos.stop_loss or price <= pos.target:
                self.state = StrategyState.FLAT
                return "EXIT"

        return None

    def update_stop(self, reference_price: float) -> float:
        """Compute trailing stop level from reference (highest/lowest) price.

        For LONG: stop = reference_price - sl_ticks * tick_size
        For SHORT: stop = reference_price + sl_ticks * tick_size
        """
        if self._position is None:
            raise RuntimeError("update_stop called with no active position")
        sl_distance = self._sl_ticks * self._tick
        if self._position.direction == "LONG":
            return round(reference_price - sl_distance, 4)
        return round(reference_price + sl_distance, 4)

    def eod_flatten(self) -> bool:
        """Mark session as ended. Returns True if a position was open."""
        had_position = self.state == StrategyState.IN_POSITION
        self.state = StrategyState.FLAT
        return had_position

    def current_position(self) -> Optional[_Position]:
        """Return the active position dataclass, or None."""
        return self._position if self.state == StrategyState.IN_POSITION else None

    # ── internals ─────────────────────────────────────────────────────

    def _reset_session(self, date: datetime.date) -> None:
        self._session_date = date
        self.state = StrategyState.WAITING
        self.orb_high = None
        self.orb_low = None
        self._range_bars = []
        self._position = None

    def _lock_range(self) -> None:
        highs = [b["high"] for b in self._range_bars]
        lows = [b["low"] for b in self._range_bars]
        self.orb_high = max(highs)
        self.orb_low = min(lows)

    def _check_breakout(self, bar: dict, ts_et: datetime.datetime) -> Optional[Signal]:
        if self.orb_high is None or self.orb_low is None:
            return None

        close = float(bar["close"])
        signal: Optional[Signal] = None

        if close > self.orb_high:
            sl = round(self.orb_high - self._sl_ticks * self._tick, 4)
            target = round(close + self._target_ticks * self._tick, 4)
            signal = Signal("LONG", close, sl, target, ts_et)
        elif self._allow_short and close < self.orb_low:
            sl = round(self.orb_low + self._sl_ticks * self._tick, 4)
            target = round(close - self._target_ticks * self._tick, 4)
            signal = Signal("SHORT", close, sl, target, ts_et)

        if signal is not None:
            self._position = _Position(
                direction=signal.direction,
                entry_price=signal.entry_price,
                stop_loss=signal.stop_loss,
                target=signal.target,
                entry_ts=ts_et,
            )
            self.state = StrategyState.IN_POSITION

        return signal

    def _minutes_since_open(self, ts_et: datetime.datetime) -> int:
        open_dt = datetime.datetime.combine(ts_et.date(), self._rth_open, tzinfo=ET)
        return int((ts_et - open_dt).total_seconds() // 60)
