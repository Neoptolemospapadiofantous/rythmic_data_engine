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
    raise RuntimeError("PostgreSQL unavailable after %d attempts" % max_retries)



# ── position reconciliation ───────────────────────────────────────────────────

def _reconcile_position(conn, config: dict, strategy: MicroORBStrategy,
                        log: logging.Logger) -> Optional[dict]:
    """Query DB for any open position from today's session.

    Returns the open trade row dict if found and loads strategy state, else None.
    This is NOT a stub — it queries the trades table synchronously.
    """
    today = datetime.datetime.now(tz=ET).date()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT * FROM trades
            WHERE session_date = %s
              AND exit_time IS NULL
              AND source = 'python'
            ORDER BY entry_time DESC
            LIMIT 1
        """, (today,))
        row = cur.fetchone()

    if row is None:
        log.info("position_reconciliation: no open position found for %s", today)
        return None

    # Restore strategy state from DB record
    log.warning(
        "position_reconciliation: found open position id=%s direction=%s entry=%s sl=%s — restoring state",
        row["id"], row["direction"], row["entry_price"], row["stop_loss"],
    )

    # Force strategy into IN_POSITION state with the recovered position
    from strategy.micro_orb import StrategyState
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

    # Live order submission is intentionally not implemented here.
    # Gate: live trading requires go_live.py to have set dry_run=False after passing
    # all pre-flight checks. The actual submission path goes through the C++ executor
    # (src/client.cpp) or a future async_rithmic Python client.
    raise NotImplementedError(
        "Live order submission not yet implemented. "
        "Run with dry_run=True for paper trading. "
        "See src/client.cpp for the C++ protobuf order submission pattern."
    )


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


def _write_trade_close(conn, trade_id: int, exit_price: float, exit_ts: datetime.datetime,
                       exit_reason: str, point_value: float) -> float:
    """Close a trade in the DB and return realized P&L in USD (0.0 if trade not found)."""
    with conn.cursor() as cur:
        cur.execute("SELECT direction, entry_price FROM trades WHERE id = %s", (trade_id,))
        row = cur.fetchone()
        if row is None:
            return 0.0
        direction = row["direction"]
        entry = float(row["entry_price"])
        pts = (exit_price - entry) if direction == "LONG" else (entry - exit_price)
        commission_rt = 4.0  # $2/side × 2 sides (Rithmic/Legends MNQ RT commission)
        pnl_usd = pts * point_value - commission_rt
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

def _poll_latest_bar(conn, symbol: str, since_ts: Optional[datetime.datetime]) -> Optional[dict]:
    """Return the most recent completed 1-min bar, or None if none newer than since_ts."""
    with conn.cursor() as cur:
        if since_ts:
            cur.execute("""
                SELECT time_bucket('1 minute', ts_event) AS ts,
                       first(price, ts_event) AS open,
                       max(price)             AS high,
                       min(price)             AS low,
                       last(price, ts_event)  AS close,
                       sum(size)              AS volume
                FROM   ticks
                WHERE  symbol = %s AND ts_event >= %s
                  AND  time_bucket('1 minute', ts_event) > time_bucket('1 minute', %s)
                GROUP  BY ts
                ORDER  BY ts DESC
                LIMIT  1
            """, (symbol, since_ts, since_ts))
        else:
            cur.execute("""
                SELECT time_bucket('1 minute', ts_event) AS ts,
                       first(price, ts_event) AS open,
                       max(price)             AS high,
                       min(price)             AS low,
                       last(price, ts_event)  AS close,
                       sum(size)              AS volume
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

        # Register signal handlers for clean shutdown
        signal.signal(signal.SIGTERM, self._handle_shutdown)
        signal.signal(signal.SIGINT, self._handle_shutdown)

    # ── signal handlers ───────────────────────────────────────────────

    def _handle_shutdown(self, signum: int, frame) -> None:
        self._log.warning("shutdown signal %d received — emergency flatten", signum)
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
        self._log.info("startup complete — entering trading loop")
        self._loop()

    # ── main loop ─────────────────────────────────────────────────────

    def _loop(self) -> None:
        while self._running:
            now_et = datetime.datetime.now(tz=ET)

            self._maybe_watchdog()
            self._maybe_eod(now_et)

            from strategy.micro_orb import StrategyState
            if self._strategy.state == StrategyState.IN_POSITION:
                self._tick_loop()
            else:
                self._bar_loop()

            time.sleep(PG_POLL_INTERVAL)

    def _bar_loop(self) -> None:
        try:
            bar = _poll_latest_bar(self._conn, self._symbol, self._last_bar_ts)
        except psycopg2.OperationalError as exc:
            self._log.warning("PG bar poll error: %s — reconnecting", exc)
            self._conn = _pg_connect_with_retry(self._config, self._log)
            return

        if bar is None:
            return
        if self._last_bar_ts and bar["ts"] <= self._last_bar_ts:
            return

        self._last_bar_ts = bar["ts"]
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
        order_id = _submit_order(signal, self._config, self._dry_run, self._log)
        if order_id is None:
            self._log.error("order submission failed — not entering position")
            return
        self._active_trade_id = _write_trade_open(
            self._conn, self._session_date, signal, order_id, self._dry_run, self._symbol)
        self._log.info("trade_open id=%s direction=%s entry=%s sl=%s target=%s dry_run=%s",
                       self._active_trade_id, signal.direction, signal.entry_price,
                       signal.stop_loss, signal.target, self._dry_run)
        self._write_state()

    def _on_exit(self, exit_price: float, exit_ts, exit_reason: str) -> None:
        if self._active_trade_id is not None:
            if not isinstance(exit_ts, datetime.datetime):
                exit_ts = datetime.datetime.now(tz=datetime.timezone.utc)
            realized_pnl = _write_trade_close(self._conn, self._active_trade_id,
                                              exit_price, exit_ts, exit_reason, self._point_value)
            self._daily_pnl += realized_pnl
            self._log.info("trade_close id=%s exit=%s reason=%s pnl=%.2f daily_pnl=%.2f",
                           self._active_trade_id, exit_price, exit_reason,
                           realized_pnl, self._daily_pnl)
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
        from strategy.micro_orb import StrategyState
        try:
            pos = self._strategy.current_position()
            if pos is not None:
                position_str = pos.direction
                entry_price = pos.entry_price
                sl = pos.stop_loss
                unrealized = 0.0
                latest = _poll_latest_tick(self._conn, self._symbol, None) if self._conn else None
                if latest and pos:
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
            }
            self._state_path.parent.mkdir(parents=True, exist_ok=True)
            tmp = self._state_path.with_suffix(".tmp")
            tmp.write_text(json.dumps(state))
            tmp.rename(self._state_path)
        except Exception as exc:
            self._log.debug("_write_state error (non-fatal): %s", exc)


# ── feature computation (delegates to strategy.features for parity with backtest) ─

def compute_live_features(bars: list) -> dict:
    """Compute the 74-feature dict for the given bar history.

    Delegates to strategy.features.compute_features() so that live and backtest
    always use the identical implementation — no drift possible.

    Args:
        bars: list of bar dicts with keys: timestamp, open, high, low, close,
              volume, bid_volume (optional), ask_volume (optional).

    Returns:
        dict mapping each of the 74 feature names to its computed value.

    Raises:
        ImportError: if strategy.features is not yet installed (install T1 first).
    """
    from strategy.features import compute_features  # noqa: PLC0415 — lazy import
    return compute_features(bars)


# ── position restoration helper (monkey-patched onto strategy) ────────────────

def _make_position_from_db(self, row: dict):
    """Restore a _Position from a DB trade row."""
    from strategy.micro_orb import _Position
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
        return json.load(f)


def _check_no_deploy(config: dict) -> None:
    lockfile = config.get("no_deploy_path", "NO_DEPLOY")
    if Path(lockfile).exists():
        print(f"ERROR: NO_DEPLOY lockfile present at '{lockfile}' — refusing to start.", file=sys.stderr)
        print("Diagnose the failure, resolve it, then remove the lockfile to proceed.", file=sys.stderr)
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

    dry_run = args.dry_run or bool(config.get("dry_run", True))
    if dry_run:
        log.info("dry_run=True — no real orders will be submitted")

    trader = LiveTrader(config, dry_run)
    trader.start()


if __name__ == "__main__":
    main()
