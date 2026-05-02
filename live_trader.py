#!/usr/bin/env python3
"""
live_trader.py — Main entry point for NQ ORB live trading bot.

Usage:
    python3 live_trader.py [--config PATH] [--dry-run]

    --config PATH   Path to live_config.json (default: config/live_config.json)
    --dry-run       Override config dry_run=True regardless of config file value

Safety gates:
    - Exits immediately if NO_DEPLOY lockfile is present
    - dry_run=True logs would-be orders but never submits them
    - Position reconciliation queries the DB on startup; does NOT just write a warning file
    - SIGTERM / SIGINT trigger emergency flatten + clean session record + exit
"""

from __future__ import annotations

import argparse
import datetime
import json
import logging
import os
import signal
import socket
import sys
import time
import zoneinfo
from pathlib import Path
from typing import Optional

import psycopg2
import psycopg2.extras
try:
    from dotenv import load_dotenv
except ImportError:
    def load_dotenv(*args, **kwargs):  # type: ignore[misc]
        """Fallback: read .env manually if python-dotenv is not installed."""
        env_path = Path(".env")
        if not env_path.exists():
            return
        for line in env_path.read_text().splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, val = line.partition("=")
            os.environ.setdefault(key.strip(), val.strip())

from models import Trade, SessionSummary
from strategy import MicroORBStrategy, Signal
from strategy.micro_orb import StrategyState, _Position

# ── constants ─────────────────────────────────────────────────────────────────

ET = zoneinfo.ZoneInfo("America/New_York")
WATCHDOG_INTERVAL = 30          # seconds between sd_notify WATCHDOG pings
PG_POLL_INTERVAL = 5            # seconds between bar polls
TICK_POLL_INTERVAL = 0.5        # seconds between tick polls when in position

# ── structured logging ────────────────────────────────────────────────────────

class _JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        doc = {
            "ts": datetime.datetime.utcnow().isoformat() + "Z",
            "level": record.levelname,
            "msg": record.getMessage(),
        }
        if record.exc_info:
            doc["exc"] = self.formatException(record.exc_info)
        return json.dumps(doc)


def _setup_logging(config: dict) -> None:
    log_cfg = config.get("logging", {})
    level = getattr(logging, log_cfg.get("log_level", "INFO").upper(), logging.INFO)
    fmt = log_cfg.get("format", "json")
    log_dir = Path(log_cfg.get("log_dir", "data/logs"))
    log_dir.mkdir(parents=True, exist_ok=True)

    handlers: list[logging.Handler] = [logging.StreamHandler(sys.stdout)]
    fh = logging.FileHandler(log_dir / "live_trader.log")
    handlers.append(fh)

    for h in handlers:
        h.setFormatter(_JsonFormatter() if fmt == "json" else logging.Formatter(
            "%(asctime)s %(levelname)s %(message)s"))

    logging.basicConfig(level=level, handlers=handlers, force=True)


# ── sd_notify ─────────────────────────────────────────────────────────────────

def _sd_notify(msg: str) -> None:
    """Send a message to systemd's notification socket if available."""
    sock_path = os.environ.get("NOTIFY_SOCKET")
    if not sock_path:
        return
    try:
        with socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM) as s:
            s.sendto(msg.encode(), sock_path.lstrip("@"))
    except OSError:
        pass


# ── alerts ───────────────────────────────────────────────────────────────────

def _send_alert(config: dict, message: str) -> None:
    """Fire a Slack webhook if alerts are enabled and SLACK_WEBHOOK_URL is set.

    Failures are always non-fatal — never let an alert failure break the trader.
    Set alerts.enabled=true and SLACK_WEBHOOK_URL env var to activate.
    """
    alert_cfg = config.get("alerts", {})
    if not alert_cfg.get("enabled", False):
        return
    webhook_env = alert_cfg.get("slack_webhook_env", "SLACK_WEBHOOK_URL")
    url = os.environ.get(webhook_env, "")
    if not url:
        return
    try:
        import urllib.request
        payload = json.dumps({"text": message}).encode()
        req = urllib.request.Request(url, data=payload, headers={"Content-Type": "application/json"})
        urllib.request.urlopen(req, timeout=8)
    except Exception as exc:
        logging.getLogger("live_trader").warning("alert delivery failed: %s", exc)


# ── DB helpers ────────────────────────────────────────────────────────────────

def _pg_connect(config: dict):
    """Return a psycopg2 connection using env vars specified in config."""
    db_cfg = config.get("db", {})
    host = os.environ.get(db_cfg.get("host_env", "PG_HOST"), "127.0.0.1")
    port = int(os.environ.get(db_cfg.get("port_env", "PG_PORT"), "5433"))
    dbname = os.environ.get(db_cfg.get("dbname_env", "PG_DB"), "rithmic")
    user = os.environ.get(db_cfg.get("user_env", "PG_USER"), "postgres")
    password = os.environ.get(db_cfg.get("password_env", "PG_PASSWORD"), "")
    timeout = int(db_cfg.get("connect_timeout", 10))
    return psycopg2.connect(
        host=host, port=port, dbname=dbname, user=user, password=password,
        connect_timeout=timeout, cursor_factory=psycopg2.extras.RealDictCursor,
    )


def _pg_connect_with_retry(config: dict, log: logging.Logger):
    """Connect to PG with exponential backoff. Raises on exhaustion."""
    db_cfg = config.get("db", {})
    max_retries = int(db_cfg.get("max_retries", 5))
    base = float(db_cfg.get("retry_backoff_base", 2))
    for attempt in range(max_retries):
        try:
            return _pg_connect(config)
        except psycopg2.OperationalError as exc:
            wait = base ** attempt
            log.warning("PG connect attempt %d/%d failed: %s — retrying in %.0fs",
                        attempt + 1, max_retries, exc, wait)
            time.sleep(wait)
    if config.get("alerts", {}).get("on_connection_loss", True):
        _send_alert(config, ":red_circle: *live_trader* DB connection lost after max retries — trader halting")
    raise RuntimeError("PostgreSQL unavailable after %d attempts" % max_retries)



# ── position reconciliation ───────────────────────────────────────────────────

def _reconcile_position(conn, config: dict, strategy: MicroORBStrategy,
                        log: logging.Logger) -> Optional[dict]:
    """Query DB for any open position from today's session.

    Returns the open trade row dict if found and loads strategy state, else None.
    This is NOT a stub — it queries the trades table synchronously.
    """
    today = datetime.datetime.now(tz=ET).date()
    open_row = None
    with conn.cursor() as cur:
        # Check Python-managed trades table
        cur.execute("""
            SELECT *, 'python' AS _source FROM trades
            WHERE session_date = %s
              AND exit_time IS NULL
              AND source = 'python'
            ORDER BY entry_time DESC
            LIMIT 1
        """, (today,))
        open_row = cur.fetchone()

        # Also check C++ executor's live_trades table for open positions
        try:
            cur.execute("""
                SELECT id, direction, entry_price, stop_loss, target, entry_time
                FROM live_trades
                WHERE trade_date = %s
                  AND exit_time IS NULL
                ORDER BY entry_time DESC
                LIMIT 1
            """, (today,))
            cpp_row = cur.fetchone()
        except Exception:
            conn.rollback()  # reset aborted transaction so subsequent queries work
            cpp_row = None  # live_trades table may not exist in all environments

    if cpp_row is not None:
        if open_row is not None:
            log.critical(
                "position_reconciliation: DUPLICATE OPEN POSITION — "
                "python trades id=%s AND live_trades id=%s both open for %s. "
                "Manual intervention required.",
                open_row["id"], cpp_row["id"], today
            )
        else:
            log.warning(
                "position_reconciliation: C++ executor has open position in live_trades "
                "(id=%s direction=%s entry=%s) but no matching python trades record — "
                "possible crash recovery scenario",
                cpp_row["id"], cpp_row["direction"], cpp_row["entry_price"]
            )
    row = open_row

    if row is None:
        log.info("position_reconciliation: no open position found for %s", today)
        return None

    # Restore strategy state from DB record
    log.warning(
        "position_reconciliation: found open position id=%s direction=%s entry=%s sl=%s — restoring state",
        row["id"], row["direction"], row["entry_price"], row["stop_loss"],
    )

    strategy._position = strategy._make_position_from_db(row)  # type: ignore[attr-defined]
    strategy.state = StrategyState.IN_POSITION
    return dict(row)


# ── order submission ──────────────────────────────────────────────────────────

def _submit_order(signal: Signal, config: dict, dry_run: bool, log: logging.Logger) -> Optional[str]:
    """Submit order to Rithmic or log in dry-run mode.

    Returns a synthetic order_id string (or None on failure).
    """
    if dry_run:
        log.info("DRY RUN: would submit %s order entry=%s sl=%s target=%s",
                 signal.direction, signal.entry_price, signal.stop_loss, signal.target)
        return f"DRY-{datetime.datetime.now(datetime.timezone.utc).strftime('%H%M%S%f')}"

    # Live order submission is not implemented in the Python path.
    # The C++ executor (src/execution/executor_main.cpp) handles live orders.
    log.critical(
        "LIVE ORDER ATTEMPTED via Python path — this is not supported. "
        "Use the C++ executor for live trading. Halting process."
    )
    sys.exit(1)


# ── trade DB writes ───────────────────────────────────────────────────────────

def _write_trade_open(conn, session_date: datetime.date, signal: Signal,
                      order_id: str, dry_run: bool, symbol: str = "MNQ") -> int:
    t = Trade(
        session_date=session_date,
        symbol=symbol,
        direction=signal.direction,
        entry_price=signal.entry_price,
        stop_loss=signal.stop_loss,
        target=signal.target,
        entry_time=signal.bar_ts,
        dry_run=dry_run,
    )
    return t.save(conn)


def _cancel_trade_open(conn, trade_id: int) -> None:
    """Mark a pending trade record as cancelled (order submission failed)."""
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE trades SET exit_reason = 'ORDER_FAILED', exit_time = NOW() WHERE id = %s",
            (trade_id,)
        )
    conn.commit()


def _update_trade_order_id(conn, trade_id: int, order_id: str) -> None:
    try:
        with conn.cursor() as cur:
            cur.execute("UPDATE trades SET order_id = %s WHERE id = %s", (order_id, trade_id))
        conn.commit()
    except Exception:
        conn.rollback()  # reset aborted transaction; order_id column is optional


def _write_trade_close(conn, trade_id: int, exit_price: float, exit_ts: datetime.datetime,
                       exit_reason: str, point_value: float, commission_rt: float = 4.0) -> float:
    """Close a trade in the DB and return realized P&L in USD (0.0 if trade not found)."""
    with conn.cursor() as cur:
        cur.execute("SELECT direction, entry_price FROM trades WHERE id = %s", (trade_id,))
        row = cur.fetchone()
        if row is None:
            return 0.0
        direction = row["direction"]
        entry = float(row["entry_price"])
        pts = (exit_price - entry) if direction == "LONG" else (entry - exit_price)
        pnl_usd = pts * point_value - commission_rt  # $2/side × 2 sides MNQ RT
        if abs(pnl_usd) > 5000.0:
            import logging as _logging
            _logging.getLogger("live_trader").warning(
                "pnl_sanity_check WARN: pnl_usd=%.2f > $5000 threshold "
                "(pts=%.4f × point_value=%.2f − commission=%.2f) — "
                "check point_value config", pnl_usd, pts, point_value, commission_rt)
        cur.execute("""
            UPDATE trades
            SET exit_price = %s, exit_time = %s, pnl_points = %s,
                pnl = %s, exit_reason = %s, updated_at = NOW()
            WHERE id = %s
        """, (exit_price, exit_ts, pts, pnl_usd, exit_reason, trade_id))
    conn.commit()
    return pnl_usd


def _write_session_summary(conn, session_date: datetime.date, dry_run: bool,
                            exit_reason: str) -> None:
    trades = Trade.for_date(conn, session_date)
    completed = [t for t in trades if t.exit_time is not None]
    summary = SessionSummary.build_from_trades(completed)
    summary.notes = exit_reason
    summary.save(conn)


# ── poll helpers ──────────────────────────────────────────────────────────────

_BARS_SELECT = """\
    SELECT time_bucket('1 minute', ts_event) AS ts,
           first(price, ts_event)                           AS open,
           max(price)                                       AS high,
           min(price)                                       AS low,
           last(price, ts_event)                            AS close,
           sum(size)                                        AS volume,
           COALESCE(sum(size) FILTER (WHERE is_buy = true),  0) AS ask_volume,
           COALESCE(sum(size) FILTER (WHERE is_buy = false), 0) AS bid_volume
"""


def _poll_bars_since(conn, symbol: str, since_ts: datetime.datetime) -> list[dict]:
    """Return all completed 1-min bars from since_ts onwards, in chronological order.

    ask_volume = buy-initiated volume (aggressor hit the ask).
    bid_volume = sell-initiated volume (aggressor hit the bid).
    Both are used by features.py for order-flow imbalance signals.
    """
    with conn.cursor() as cur:
        cur.execute(
            _BARS_SELECT + """
            FROM   ticks
            WHERE  symbol = %s AND ts_event >= %s
            GROUP  BY ts
            ORDER  BY ts ASC
        """, (symbol, since_ts))
        return [dict(row) for row in cur.fetchall()]


def _poll_latest_bar(conn, symbol: str, since_ts: Optional[datetime.datetime]) -> Optional[dict]:
    """Return the most recent completed 1-min bar, or None if none newer than since_ts."""
    with conn.cursor() as cur:
        if since_ts:
            cur.execute(
                _BARS_SELECT + """
                FROM   ticks
                WHERE  symbol = %s AND ts_event >= %s
                  AND  time_bucket('1 minute', ts_event) > time_bucket('1 minute', %s)
                GROUP  BY ts
                ORDER  BY ts DESC
                LIMIT  1
            """, (symbol, since_ts, since_ts))
        else:
            cur.execute(
                _BARS_SELECT + """
                FROM   ticks
                WHERE  symbol = %s
                GROUP  BY ts
                ORDER  BY ts DESC
                LIMIT  1
            """, (symbol,))
        return cur.fetchone()


def _poll_latest_tick(conn, symbol: str, since_ts: Optional[datetime.datetime]) -> Optional[dict]:
    """Return the most recent tick newer than since_ts."""
    with conn.cursor() as cur:
        if since_ts:
            cur.execute("""
                SELECT ts_event AS ts, price
                FROM   ticks
                WHERE  symbol = %s AND ts_event > %s
                ORDER  BY ts_event DESC
                LIMIT  1
            """, (symbol, since_ts))
        else:
            cur.execute("""
                SELECT ts_event AS ts, price
                FROM   ticks
                WHERE  symbol = %s
                ORDER  BY ts_event DESC
                LIMIT  1
            """, (symbol,))
        return cur.fetchone()


# ── main trading loop ─────────────────────────────────────────────────────────

class LiveTrader:
    """Orchestrates the trading loop: bar feed → strategy → order → DB."""

    def __init__(self, config: dict, dry_run: bool) -> None:
        self._config = config
        self._dry_run = dry_run
        self._symbol: str = config.get("symbol", "MNQ")
        self._point_value: float = float(config["orb"].get("point_value", 2.0))
        self._log = logging.getLogger("live_trader")
        self._strategy = MicroORBStrategy(config)
        self._conn: Optional[object] = None
        self._running = False
        self._session_date: Optional[datetime.date] = None
        self._active_trade_id: Optional[int] = None
        self._last_bar_ts: Optional[datetime.datetime] = None
        self._pid_path: Path = Path(config.get("pid_path", "data/live_trader.pid"))
        self._state_path: Path = Path(config.get("state_path", "data/live_state.json"))
        self._daily_pnl: float = 0.0
        self._reconnect_failures: int = 0
        self._last_tick_ts_str: Optional[str] = None
        self._last_tick_ts: Optional[datetime.datetime] = None
        self._last_watchdog = time.monotonic()
        self._eod_flatten_done = False
        self._orb_bars: list[dict] = []

        # Register signal handlers for clean shutdown
        signal.signal(signal.SIGTERM, self._handle_shutdown)
        signal.signal(signal.SIGINT, self._handle_shutdown)

    # ── signal handlers ───────────────────────────────────────────────

    def _handle_shutdown(self, signum: int, frame) -> None:
        self._log.warning("shutdown signal %d received — emergency flatten", signum)
        had_position = self._strategy.state.name == "IN_POSITION"
        if had_position and self._config.get("alerts", {}).get("on_emergency_flatten", True):
            _send_alert(self._config,
                        f":rotating_light: *live_trader* SIGTERM on {self._symbol} — emergency flatten triggered")
        self._emergency_flatten("SIGNAL_%d" % signum)
        sys.exit(0)

    def _emergency_flatten(self, reason: str) -> None:
        """Flatten open position and write session record regardless of clean state."""
        # Clean up PID file so the service monitor knows this process is gone
        try:
            self._pid_path.unlink(missing_ok=True)
        except Exception:
            pass
        try:
            if (self._strategy.state.name == "IN_POSITION"
                    and self._active_trade_id is not None and self._conn is not None):
                latest_tick = _poll_latest_tick(self._conn, self._symbol, None)
                exit_price = (float(latest_tick["price"]) if latest_tick
                              else self._strategy.current_position().entry_price)
                exit_ts = datetime.datetime.now(tz=datetime.timezone.utc)
                _write_trade_close(self._conn, self._active_trade_id,
                                   exit_price, exit_ts, reason, self._point_value)
                self._active_trade_id = None
            self._strategy.eod_flatten()

            if self._conn is not None and self._session_date is not None:
                _write_session_summary(self._conn, self._session_date,
                                       self._dry_run, reason)
        except Exception as exc:
            self._log.error("emergency_flatten error: %s", exc)
            _send_alert(self._config,
                        f":octagonal_sign: *live_trader* emergency_flatten FAILED: {exc}. "
                        f"Manual position check required.")
            halt = Path("data/AUDIT_HALT")
            try:
                halt.parent.mkdir(parents=True, exist_ok=True)
                halt.write_text(json.dumps({
                    "check": "emergency_flatten",
                    "message": f"emergency_flatten failed: {exc}",
                    "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                }))
            except Exception:
                pass

    # ── startup ───────────────────────────────────────────────────────

    def start(self) -> None:
        self._running = True
        self._session_date = datetime.datetime.now(tz=ET).date()
        self._log.info("live_trader starting: symbol=%s dry_run=%s session=%s",
                       self._symbol, self._dry_run, self._session_date)

        # Write PID file so the UI kill-switch and service monitor can find this process
        self._pid_path.parent.mkdir(parents=True, exist_ok=True)
        self._pid_path.write_text(str(os.getpid()))

        self._conn = _pg_connect_with_retry(self._config, self._log)
        Trade.ensure_schema(self._conn)
        SessionSummary.ensure_schema(self._conn)

        # Real position reconciliation — queries DB, does NOT just write a warning file
        _reconcile_position(self._conn, self._config, self._strategy, self._log)

        self._write_state("CONNECTED")
        _sd_notify("READY=1")
        self._replay_historical_bars()
        self._log.info("startup complete — entering trading loop")
        self._loop()

    # ── main loop ─────────────────────────────────────────────────────

    def _loop(self) -> None:
        while self._running:
            now_et = datetime.datetime.now(tz=ET)

            self._maybe_watchdog()
            self._maybe_eod(now_et)

            if self._strategy.state == StrategyState.IN_POSITION:
                self._tick_loop()
            else:
                self._bar_loop()

            self._write_state()
            time.sleep(PG_POLL_INTERVAL)

    def _replay_historical_bars(self) -> None:
        """Replay completed bars from today's RTH open to synchronise the strategy state machine.

        Intentionally does NOT call _on_signal — historical bars should never trigger live
        order submission.  If an open position exists, _reconcile_position already restored it
        before this runs.  If a trade was taken and closed earlier today, replaying the
        breakout bar would create a phantom open trade; we suppress that by ignoring signals
        from replay entirely.
        """
        rth_open_str = self._config.get("orb", {}).get("rth_open", "09:30:00")
        today = datetime.datetime.now(tz=ET).date()
        rth_open_et = datetime.datetime.combine(
            today, datetime.time.fromisoformat(rth_open_str), tzinfo=ET)
        bars = _poll_bars_since(self._conn, self._symbol, rth_open_et)
        if not bars:
            return
        self._log.info("replaying %d historical bar(s) from %s ET", len(bars), rth_open_str)
        for bar in bars:
            self._orb_bars.append(dict(bar))
            if len(self._orb_bars) > 120:
                self._orb_bars = self._orb_bars[-120:]
            self._strategy.on_bar(dict(bar))  # state-machine update only; signal suppressed
            self._last_bar_ts = bar["ts"]
        self._write_state("CONNECTED")
        self._log.info("replay complete: strategy_state=%s orb_high=%s orb_low=%s",
                       self._strategy.state.name, self._strategy.orb_high, self._strategy.orb_low)

    _MAX_RECONNECT_FAILURES = 10

    def _bar_loop(self) -> None:
        try:
            bar = _poll_latest_bar(self._conn, self._symbol, self._last_bar_ts)
        except psycopg2.OperationalError as exc:
            self._reconnect_failures += 1
            self._log.warning("PG bar poll error (%d/%d): %s — reconnecting",
                              self._reconnect_failures, self._MAX_RECONNECT_FAILURES, exc)
            if self._reconnect_failures >= self._MAX_RECONNECT_FAILURES:
                self._log.critical(
                    "max reconnect failures (%d) reached — writing NO_DEPLOY and halting",
                    self._MAX_RECONNECT_FAILURES)
                _send_alert(self._config,
                            f":octagonal_sign: *live_trader* halted after "
                            f"{self._MAX_RECONNECT_FAILURES} consecutive DB reconnect failures")
                no_deploy = Path(self._config.get("no_deploy_path", "NO_DEPLOY"))
                no_deploy.touch()
                self._running = False
                return
            self._conn = _pg_connect_with_retry(self._config, self._log)
            return

        self._reconnect_failures = 0  # successful poll resets counter
        if bar is None:
            return
        if self._last_bar_ts and bar["ts"] <= self._last_bar_ts:
            return

        self._last_bar_ts = bar["ts"]
        self._orb_bars.append(dict(bar))
        if len(self._orb_bars) > 120:
            self._orb_bars = self._orb_bars[-120:]
        signal = self._strategy.on_bar(dict(bar))
        if signal is not None:
            self._on_signal(signal)

    def _tick_loop(self) -> None:
        """Poll ticks tightly while in position to manage SL."""
        deadline = time.monotonic() + PG_POLL_INTERVAL
        while time.monotonic() < deadline:
            try:
                tick = _poll_latest_tick(self._conn, self._symbol, self._last_tick_ts)
            except psycopg2.OperationalError as exc:
                self._log.warning("PG tick poll error: %s — reconnecting", exc)
                self._conn = _pg_connect_with_retry(self._config, self._log)
                break

            if tick is not None and (self._last_tick_ts is None or tick["ts"] > self._last_tick_ts):
                self._last_tick_ts = tick["ts"]
                self._last_tick_ts_str = tick["ts"].isoformat() if hasattr(tick["ts"], "isoformat") else str(tick["ts"])
                result = self._strategy.on_tick({"price": float(tick["price"]), "ts": tick["ts"]})
                if result == "EXIT":
                    self._on_exit(float(tick["price"]), tick["ts"], "SL_OR_TARGET")
                    break

            self._maybe_watchdog()
            time.sleep(TICK_POLL_INTERVAL)

    def _on_signal(self, signal: Signal) -> None:
        # Write DB record first so a Rithmic fill always has a corresponding DB entry
        pending_order_id = f"PENDING-{datetime.datetime.now(datetime.timezone.utc).strftime('%H%M%S%f')}"
        self._active_trade_id = _write_trade_open(
            self._conn, self._session_date, signal, pending_order_id, self._dry_run, self._symbol)
        order_id = _submit_order(signal, self._config, self._dry_run, self._log)
        if order_id is None:
            self._log.error("order submission failed — rolling back DB entry")
            # Mark as cancelled in DB rather than leaving orphaned
            _cancel_trade_open(self._conn, self._active_trade_id)
            self._active_trade_id = None
            return
        # Update DB record with real order_id
        _update_trade_order_id(self._conn, self._active_trade_id, order_id)
        self._log.info("trade_open id=%s direction=%s entry=%s sl=%s target=%s dry_run=%s",
                       self._active_trade_id, signal.direction, signal.entry_price,
                       signal.stop_loss, signal.target, self._dry_run)
        if not self._dry_run and self._config.get("alerts", {}).get("on_trade_fill", True):
            _send_alert(self._config,
                        f":zap: *{self._symbol}* ENTRY {signal.direction} @ {signal.entry_price} "
                        f"| SL {signal.stop_loss} | Target {signal.target}")
        self._write_state()

    def _on_exit(self, exit_price: float, exit_ts, exit_reason: str) -> None:
        if self._active_trade_id is not None:
            if not isinstance(exit_ts, datetime.datetime):
                exit_ts = datetime.datetime.now(tz=datetime.timezone.utc)
            realized_pnl = _write_trade_close(self._conn, self._active_trade_id,
                                              exit_price, exit_ts, exit_reason, self._point_value,
                                              float(self._config.get("commission_rt", 4.0)))
            self._daily_pnl += realized_pnl
            self._log.info("trade_close id=%s exit=%s reason=%s pnl=%.2f daily_pnl=%.2f",
                           self._active_trade_id, exit_price, exit_reason,
                           realized_pnl, self._daily_pnl)
            if not self._dry_run and self._config.get("alerts", {}).get("on_trade_fill", True):
                pnl_sign = "+" if realized_pnl >= 0 else ""
                _send_alert(self._config,
                            f":white_check_mark: *{self._symbol}* EXIT {exit_reason} @ {exit_price} "
                            f"| PnL {pnl_sign}{realized_pnl:.2f} | Day {pnl_sign}{self._daily_pnl:.2f}")
            self._active_trade_id = None
        self._write_state()

    def _maybe_eod(self, now_et: datetime.datetime) -> None:
        if self._eod_flatten_done:
            return
        eod_time = datetime.time.fromisoformat(
            self._config["orb"].get("rth_close", "16:00:00"))
        if now_et.time() >= eod_time:
            self._log.info("EOD: flattening all positions at %s", now_et)
            had_position = self._strategy.eod_flatten()
            if had_position and self._active_trade_id is not None:
                latest_tick = _poll_latest_tick(self._conn, self._symbol, None)
                exit_price = float(latest_tick["price"]) if latest_tick else 0.0
                self._on_exit(exit_price, datetime.datetime.now(tz=datetime.timezone.utc), "EOD_FLATTEN")
            _write_session_summary(self._conn, self._session_date,
                                   self._dry_run, "EOD_FLATTEN")
            self._eod_flatten_done = True
            self._running = False  # Stop after EOD

    def _maybe_watchdog(self) -> None:
        now = time.monotonic()
        if now - self._last_watchdog >= WATCHDOG_INTERVAL:
            _sd_notify("WATCHDOG=1")
            self._last_watchdog = now

    def _write_state(self, connection: str = "CONNECTED") -> None:
        """Write current position + connection state to data/live_state.json atomically.

        The UI reads this file for the live position display. Atomic write via
        .tmp + rename prevents partial reads.
        """
        try:
            pos = self._strategy.current_position()
            # Only fetch current price when in position (needed for unrealized PnL).
            # When flat this round-trip is wasted — latest is never used below.
            latest = _poll_latest_tick(self._conn, self._symbol, None) if (pos is not None and self._conn) else None
            if pos is not None:
                position_str = pos.direction
                entry_price = pos.entry_price
                sl = pos.stop_loss
                unrealized = 0.0
                if latest:
                    price = float(latest["price"])
                    if pos.direction == "LONG":
                        unrealized = (price - pos.entry_price) * self._point_value
                    else:
                        unrealized = (pos.entry_price - price) * self._point_value
            else:
                position_str = "FLAT"
                entry_price = None
                sl = None
                unrealized = 0.0

            orb_bars_out = [
                {
                    "ts": b["ts"].isoformat() if hasattr(b["ts"], "isoformat") else str(b["ts"]),
                    "open": float(b["open"]),
                    "high": float(b["high"]),
                    "low": float(b["low"]),
                    "close": float(b["close"]),
                    "volume": int(b.get("volume", 0)),
                }
                for b in self._orb_bars[-60:]
            ]
            state = {
                "position": position_str,
                "entry_price": entry_price,
                "sl": sl,
                "unrealized_pnl": round(unrealized, 2),
                "daily_pnl": round(self._daily_pnl, 2),
                "connection": connection,
                "reconnect_failures": self._reconnect_failures,
                "last_tick_ts": self._last_tick_ts_str,
                "ts": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                "orb_high": self._strategy.orb_high,
                "orb_low": self._strategy.orb_low,
                "strategy_state": self._strategy.state.name,
                "orb_minutes": self._config.get("orb", {}).get("orb_period_minutes", 15),
                "orb_bars": orb_bars_out,
            }
            self._state_path.parent.mkdir(parents=True, exist_ok=True)
            tmp = self._state_path.with_suffix(".tmp")
            tmp.write_text(json.dumps(state))
            tmp.rename(self._state_path)

            # Mirror strategy state to live_position so the Next.js dashboard sees it.
            # current_price is intentionally omitted — C++ executor owns that field.
            if self._conn:
                orb_set = self._strategy.orb_high is not None
                with self._conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO live_position
                            (session_date, instrument, strategy, state, direction,
                             entry_price, unrealized_pnl_usd,
                             sl_price, orb_high, orb_low, orb_set, last_updated)
                        VALUES
                            (%(session_date)s, %(instrument)s, %(strategy)s, %(state)s,
                             %(direction)s, %(entry_price)s, %(unrealized_pnl_usd)s,
                             %(sl_price)s, %(orb_high)s, %(orb_low)s, %(orb_set)s, NOW())
                        ON CONFLICT (session_date, instrument, strategy) DO UPDATE SET
                            state               = EXCLUDED.state,
                            direction           = EXCLUDED.direction,
                            entry_price         = EXCLUDED.entry_price,
                            unrealized_pnl_usd  = EXCLUDED.unrealized_pnl_usd,
                            sl_price            = EXCLUDED.sl_price,
                            orb_high            = EXCLUDED.orb_high,
                            orb_low             = EXCLUDED.orb_low,
                            orb_set             = EXCLUDED.orb_set,
                            last_updated        = NOW()
                    """, {
                        "session_date": self._session_date,
                        "instrument": self._symbol,
                        "strategy": "ORB_PY",
                        "state": position_str,
                        "direction": pos.direction if pos else None,
                        "entry_price": float(pos.entry_price) if pos else None,
                        "unrealized_pnl_usd": round(unrealized, 2) if pos else None,
                        "sl_price": float(pos.stop_loss) if pos else None,
                        "orb_high": float(self._strategy.orb_high) if self._strategy.orb_high is not None else None,
                        "orb_low": float(self._strategy.orb_low) if self._strategy.orb_low is not None else None,
                        "orb_set": orb_set,
                    })
                self._conn.commit()
        except Exception as exc:
            self._log.debug("_write_state error (non-fatal): %s", exc)
            try:
                self._conn.rollback()
            except Exception:
                pass


# ── feature computation (delegates to strategy.features for parity with backtest) ─

def compute_live_features(bars: list, config: dict | None = None) -> dict:
    """Compute the 74-feature dict for the given bar history.

    Delegates to strategy.features.compute_features() so that live and backtest
    always use the identical implementation — no drift possible.

    Args:
        bars: list of bar dicts with keys: timestamp, open, high, low, close,
              volume, bid_volume (optional), ask_volume (optional).
        config: live_config dict. Used to read orb_period_minutes so features
                match the strategy's actual ORB range. Defaults to 5 bars when None.

    Returns:
        dict mapping each of the 74 feature names to its computed value.
    """
    from strategy.features import compute_features  # noqa: PLC0415 — lazy import
    orb_period = int((config or {}).get("orb", {}).get("orb_period_minutes", 5))
    return compute_features(bars, orb_period=orb_period)


# ── position restoration helper (monkey-patched onto strategy) ────────────────

def _make_position_from_db(self, row: dict):
    """Restore a _Position from a DB trade row."""
    p = _Position(
        direction=row["direction"],
        entry_price=float(row["entry_price"]),
        stop_loss=float(row["stop_loss"]) if row.get("stop_loss") else 0.0,
        target=float(row["target"]) if row.get("target") else 0.0,
        entry_ts=row["entry_time"],
    )
    return p

MicroORBStrategy._make_position_from_db = _make_position_from_db  # type: ignore[attr-defined]


# ── entry point ───────────────────────────────────────────────────────────────

def _load_config(path: str) -> dict:
    with open(path) as f:
        cfg = json.load(f)
    try:
        from config.live_config_schema import LiveConfig
    except ModuleNotFoundError as e:
        raise SystemExit(f"Missing dependency for config validation: {e}. Run: pip install pydantic") from e
    try:
        LiveConfig.model_validate(cfg)
    except Exception as e:
        raise SystemExit(f"Config validation failed: {e}") from e
    return cfg


def _check_no_deploy(config: dict) -> None:
    lockfile = config.get("no_deploy_path", "NO_DEPLOY")
    if Path(lockfile).exists():
        print(f"ERROR: NO_DEPLOY lockfile present at '{lockfile}' — refusing to start.", file=sys.stderr)
        print("Diagnose the failure, resolve it, then remove the lockfile to proceed.", file=sys.stderr)
        sys.exit(1)


def _check_audit_halt() -> None:
    halt = Path("data/AUDIT_HALT")
    if halt.exists():
        try:
            detail = json.loads(halt.read_text()).get("message", "(see data/AUDIT_HALT)")
        except Exception:
            detail = halt.read_text().strip()[:200] or "(see data/AUDIT_HALT)"
        print("ERROR: AUDIT_HALT sentinel present — audit daemon flagged a critical issue:", file=sys.stderr)
        print(f"  {detail}", file=sys.stderr)
        print("Resolve the issue, then: rm data/AUDIT_HALT", file=sys.stderr)
        sys.exit(1)


def main() -> None:
    parser = argparse.ArgumentParser(description="NQ ORB live trader")
    parser.add_argument("--config", default="config/live_config.json",
                        help="Path to live_config.json")
    parser.add_argument("--dry-run", action="store_true",
                        help="Force dry_run=True regardless of config")
    args = parser.parse_args()

    load_dotenv()
    config = _load_config(args.config)
    _setup_logging(config)
    log = logging.getLogger("live_trader")

    _check_no_deploy(config)
    _check_audit_halt()

    dry_run = args.dry_run or bool(config.get("dry_run", True))
    if dry_run:
        log.info("dry_run=True — no real orders will be submitted")

    trader = LiveTrader(config, dry_run)
    trader.start()


if __name__ == "__main__":
    main()
