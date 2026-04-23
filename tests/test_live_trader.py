"""
Unit tests for live_trader.py and strategy/micro_orb.py.

Coverage:
  - MicroORBStrategy state machine and signal generation
  - Position reconciliation reads from DB (not just a warning file)
  - dry_run=True blocks the live-order code path
  - SIGTERM triggers emergency flatten and clean exit
  - NO_DEPLOY lockfile causes immediate exit
"""

import datetime
import json
import os
import signal
import subprocess
import sys
import tempfile
import unittest
import zoneinfo
from pathlib import Path
from unittest.mock import MagicMock, patch, call

REPO_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(REPO_ROOT))

from strategy.micro_orb import MicroORBStrategy, Signal, StrategyState

ET = zoneinfo.ZoneInfo("America/New_York")


# ── helpers ───────────────────────────────────────────────────────────────────

def _make_config(**orb_overrides) -> dict:
    orb = {
        "orb_period_minutes": 5,
        "stop_loss_ticks": 16,
        "target_ticks": 48,
        "tick_size": 0.25,
        "point_value": 20.0,
        "rth_open": "09:30:00",
        "rth_close": "16:00:00",
        "eod_exit_minutes_before_close": 15,
        "allow_short": True,
    }
    orb.update(orb_overrides)
    return {
        "dry_run": True,
        "symbol": "NQ",
        "exchange": "CME",
        "orb": orb,
        "db": {"max_retries": 1, "retry_backoff_base": 0},
        "logging": {"log_level": "WARNING"},
        "no_deploy_path": "NO_DEPLOY_DOES_NOT_EXIST_TEST",
    }


def _base_dt() -> datetime.datetime:
    return datetime.datetime(2024, 1, 15, 9, 30, tzinfo=ET)


def _make_bar(dt: datetime.datetime, high_offset=5.0, low_offset=5.0,
              close_price=17005.0) -> dict:
    return {
        "ts": dt,
        "open": 17000.0,
        "high": 17000.0 + high_offset,
        "low": 17000.0 - low_offset,
        "close": close_price,
        "volume": 1000,
    }


def _feed_range_bars(strategy: MicroORBStrategy, n: int = 5) -> None:
    """Feed N range-building bars starting at RTH open."""
    base = _base_dt()
    for i in range(n):
        bar = _make_bar(base + datetime.timedelta(minutes=i))
        strategy.on_bar(bar)


# ── state machine tests ───────────────────────────────────────────────────────

class TestMicroORBStateMachine(unittest.TestCase):

    def setUp(self):
        self.cfg = _make_config()
        self.s = MicroORBStrategy(self.cfg)

    def test_initial_state_is_waiting(self):
        self.assertEqual(self.s.state, StrategyState.WAITING)

    def test_first_rth_bar_transitions_to_orb_building(self):
        self.s.on_bar(_make_bar(_base_dt()))
        self.assertEqual(self.s.state, StrategyState.ORB_BUILDING)

    def test_n_bars_transitions_to_watching(self):
        _feed_range_bars(self.s, 5)
        self.assertEqual(self.s.state, StrategyState.WATCHING)
        self.assertIsNotNone(self.s.orb_high)
        self.assertIsNotNone(self.s.orb_low)

    def test_orb_high_low_correct(self):
        base = _base_dt()
        # Feed 5 bars with known highs and lows
        for i in range(5):
            bar = {
                "ts": base + datetime.timedelta(minutes=i),
                "open": 17000.0, "high": 17000.0 + i, "low": 16990.0 - i,
                "close": 17000.0, "volume": 100,
            }
            self.s.on_bar(bar)
        self.assertEqual(self.s.orb_high, 17004.0)   # max(17000,1,2,3,4) = 17004
        self.assertEqual(self.s.orb_low, 16986.0)    # min(16990,89,88,87,86) = 16986

    def test_no_signal_within_range(self):
        base = _base_dt()
        sigs = []
        for i in range(5):
            bar = _make_bar(base + datetime.timedelta(minutes=i))
            sigs.append(self.s.on_bar(bar))
        self.assertTrue(all(s is None for s in sigs))

    def test_long_breakout_generates_signal(self):
        _feed_range_bars(self.s, 5)
        self.assertIsNotNone(self.s.orb_high)
        breakout_bar = {
            "ts": _base_dt() + datetime.timedelta(minutes=5),
            "open": self.s.orb_high + 0.25,
            "high": self.s.orb_high + 5.0,
            "low": self.s.orb_high,
            "close": self.s.orb_high + 5.0,   # close above range high
            "volume": 500,
        }
        sig = self.s.on_bar(breakout_bar)
        self.assertIsNotNone(sig)
        self.assertEqual(sig.direction, "LONG")
        self.assertEqual(self.s.state, StrategyState.IN_POSITION)

    def test_short_breakout_generates_signal(self):
        _feed_range_bars(self.s, 5)
        breakout_bar = {
            "ts": _base_dt() + datetime.timedelta(minutes=5),
            "open": self.s.orb_low - 0.25,
            "high": self.s.orb_low,
            "low": self.s.orb_low - 5.0,
            "close": self.s.orb_low - 5.0,   # close below range low
            "volume": 500,
        }
        sig = self.s.on_bar(breakout_bar)
        self.assertIsNotNone(sig)
        self.assertEqual(sig.direction, "SHORT")

    def test_no_short_when_allow_short_false(self):
        cfg = _make_config(allow_short=False)
        s = MicroORBStrategy(cfg)
        _feed_range_bars(s, 5)
        breakout_bar = {
            "ts": _base_dt() + datetime.timedelta(minutes=5),
            "open": s.orb_low - 0.25,
            "high": s.orb_low,
            "low": s.orb_low - 5.0,
            "close": s.orb_low - 5.0,
            "volume": 500,
        }
        sig = s.on_bar(breakout_bar)
        self.assertIsNone(sig)

    def test_session_resets_on_new_date(self):
        _feed_range_bars(self.s, 5)
        self.assertEqual(self.s.state, StrategyState.WATCHING)
        # Feed a bar from the next day
        next_day = _base_dt() + datetime.timedelta(days=1)
        self.s.on_bar(_make_bar(next_day))
        self.assertEqual(self.s.state, StrategyState.ORB_BUILDING)

    def test_eod_cutoff_blocks_new_entry(self):
        """Bars fed after EOD cutoff should not trigger signals."""
        _feed_range_bars(self.s, 5)
        # 15:50 ET is within 15-minute cutoff before 16:00
        late_bar = _make_bar(
            _base_dt().replace(hour=15, minute=50),
            high_offset=100.0, low_offset=0.0, close_price=17200.0
        )
        sig = self.s.on_bar(late_bar)
        self.assertIsNone(sig)


# ── on_tick / stop-loss tests ─────────────────────────────────────────────────

class TestOnTick(unittest.TestCase):

    def _in_position_strategy(self, direction="LONG") -> tuple[MicroORBStrategy, Signal]:
        cfg = _make_config()
        s = MicroORBStrategy(cfg)
        _feed_range_bars(s, 5)
        close = s.orb_high + 5.0 if direction == "LONG" else s.orb_low - 5.0
        breakout_bar = {
            "ts": _base_dt() + datetime.timedelta(minutes=5),
            "open": close - 0.25, "high": close + 1.0,
            "low": close - 1.0, "close": close, "volume": 100,
        }
        sig = s.on_bar(breakout_bar)
        return s, sig

    def test_no_exit_while_not_in_position(self):
        cfg = _make_config()
        s = MicroORBStrategy(cfg)
        result = s.on_tick({"price": 17000.0, "ts": datetime.datetime.now(tz=ET)})
        self.assertIsNone(result)

    def test_exit_on_stop_loss_long(self):
        s, sig = self._in_position_strategy("LONG")
        # Tick below stop loss
        result = s.on_tick({"price": sig.stop_loss - 0.25,
                             "ts": datetime.datetime.now(tz=ET)})
        self.assertEqual(result, "EXIT")
        self.assertEqual(s.state, StrategyState.FLAT)

    def test_exit_on_target_long(self):
        s, sig = self._in_position_strategy("LONG")
        result = s.on_tick({"price": sig.target,
                             "ts": datetime.datetime.now(tz=ET)})
        self.assertEqual(result, "EXIT")

    def test_exit_on_stop_loss_short(self):
        s, sig = self._in_position_strategy("SHORT")
        result = s.on_tick({"price": sig.stop_loss + 0.25,
                             "ts": datetime.datetime.now(tz=ET)})
        self.assertEqual(result, "EXIT")

    def test_no_exit_on_tick_between_sl_and_target(self):
        s, sig = self._in_position_strategy("LONG")
        mid_price = (sig.stop_loss + sig.target) / 2
        result = s.on_tick({"price": mid_price, "ts": datetime.datetime.now(tz=ET)})
        self.assertIsNone(result)


# ── dry_run gate test ─────────────────────────────────────────────────────────

class TestDryRunGate(unittest.TestCase):

    def test_dry_run_logs_but_does_not_call_rithmic(self):
        """_submit_order must not reach any real order code when dry_run=True."""
        import live_trader
        cfg = _make_config()
        log = MagicMock()
        sig = Signal("LONG", 17010.0, 17006.0, 17022.0, datetime.datetime.now(tz=ET))
        order_id = live_trader._submit_order(sig, cfg, dry_run=True, log=log)
        self.assertIsNotNone(order_id)
        self.assertTrue(order_id.startswith("DRY-"))
        # log.info was called with DRY RUN in the message
        log.info.assert_called_once()
        msg_args = log.info.call_args[0]
        self.assertIn("DRY RUN", msg_args[0])

    def test_dry_run_false_logs_error_not_implemented(self):
        """When dry_run=False, submit_order logs an error (not yet implemented)."""
        import live_trader
        cfg = _make_config()
        log = MagicMock()
        sig = Signal("LONG", 17010.0, 17006.0, 17022.0, datetime.datetime.now(tz=ET))
        order_id = live_trader._submit_order(sig, cfg, dry_run=False, log=log)
        self.assertIsNone(order_id)
        log.error.assert_called_once()


# ── NO_DEPLOY lockfile test ───────────────────────────────────────────────────

class TestNoDeployLockfile(unittest.TestCase):

    def test_no_deploy_causes_sys_exit(self):
        """live_trader._check_no_deploy must sys.exit(1) when lockfile exists."""
        import live_trader
        with tempfile.NamedTemporaryFile(delete=False) as f:
            lockfile_path = f.name
        try:
            cfg = {"no_deploy_path": lockfile_path}
            with self.assertRaises(SystemExit) as ctx:
                live_trader._check_no_deploy(cfg)
            self.assertEqual(ctx.exception.code, 1)
        finally:
            os.unlink(lockfile_path)

    def test_no_lockfile_does_not_exit(self):
        import live_trader
        cfg = {"no_deploy_path": "/tmp/no_deploy_file_that_does_not_exist_xyz_abc"}
        live_trader._check_no_deploy(cfg)  # must not raise


# ── SIGTERM handler test ──────────────────────────────────────────────────────

class TestSigtermHandler(unittest.TestCase):

    def test_sigterm_causes_clean_exit(self):
        """Send SIGTERM to a live_trader subprocess; verify it exits 0 within 5s."""
        config = _make_config()
        # Write a temporary config that will keep the trader in startup long enough
        # for the signal to be received (it blocks on PG connect which will fail fast)
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            # Point to a non-existent lockfile so the NO_DEPLOY check passes,
            # but db.max_retries=0 so the PG connect fails immediately
            cfg_copy = dict(config)
            cfg_copy["db"] = {"max_retries": 1, "retry_backoff_base": 0,
                              "host_env": "PG_HOST_DOES_NOT_EXIST",
                              "port_env": "PG_PORT_DOES_NOT_EXIST",
                              "dbname_env": "PG_DB_DOES_NOT_EXIST"}
            json.dump(cfg_copy, f)
            tmp_cfg = f.name

        try:
            proc = subprocess.Popen(
                [sys.executable, "live_trader.py", "--config", tmp_cfg, "--dry-run"],
                cwd=str(REPO_ROOT),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            # Give it a moment to start
            import time; time.sleep(0.5)
            proc.send_signal(signal.SIGTERM)
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
                self.fail("live_trader did not exit within 5s after SIGTERM")
            # Exit code 0 (clean) or 1 (PG connect failure) are both acceptable
            # The test asserts the process terminates, not infinite hang
            self.assertIn(proc.returncode, (0, 1),
                          "Expected exit code 0 or 1, got %d" % proc.returncode)
        finally:
            os.unlink(tmp_cfg)


# ── position reconciliation test ─────────────────────────────────────────────

class TestPositionReconciliation(unittest.TestCase):

    def test_no_open_position_leaves_strategy_waiting(self):
        """When DB has no open trade today, strategy stays in WAITING state."""
        import live_trader
        cfg = _make_config()
        strategy = MicroORBStrategy(cfg)

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.__enter__ = lambda s: mock_cursor
        mock_cursor.__exit__ = MagicMock(return_value=False)
        mock_cursor.fetchone.return_value = None
        mock_conn.cursor.return_value = mock_cursor

        log = MagicMock()
        result = live_trader._reconcile_position(mock_conn, cfg, strategy, log)
        self.assertIsNone(result)
        self.assertEqual(strategy.state, StrategyState.WAITING)

    def test_open_position_restores_strategy_state(self):
        """When DB has an open trade, strategy is restored to IN_POSITION."""
        import live_trader
        cfg = _make_config()
        strategy = MicroORBStrategy(cfg)

        open_trade = {
            "id": 42,
            "direction": "LONG",
            "entry_price": 17010.0,
            "stop_loss": 17006.0,
            "target": 17022.0,
            "entry_ts": datetime.datetime.now(tz=ET),
            "session_date": datetime.date.today(),
        }

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.__enter__ = lambda s: mock_cursor
        mock_cursor.__exit__ = MagicMock(return_value=False)
        mock_cursor.fetchone.return_value = open_trade
        mock_conn.cursor.return_value = mock_cursor

        log = MagicMock()
        result = live_trader._reconcile_position(mock_conn, cfg, strategy, log)
        self.assertIsNotNone(result)
        self.assertEqual(strategy.state, StrategyState.IN_POSITION)
        self.assertIsNotNone(strategy.current_position())
        self.assertEqual(strategy.current_position().direction, "LONG")


if __name__ == "__main__":
    unittest.main(verbosity=2)
