#!/usr/bin/env python3
"""
scripts/test_live_cycle.py — 5-minute ORB live-cycle integration test.

What it does:
  1. Kills any running live_trader
  2. Backs up live_config.json and patches RTH window to now
  3. Injects synthetic MNQ ticks: 5 ORB-building bars + 1 breakout bar
  4. Starts live_trader in background
  5. Tails the log for up to 60s watching for trade_open signal
  6. Reports result (PASS / FAIL) and restores original config

Usage:
    python3 scripts/test_live_cycle.py [--price PRICE] [--orb-pts PTS] [--direction {LONG,SHORT}]

Options:
    --price      Base MNQ price to use for synthetic bars  [default: 19800]
    --orb-pts    ORB range half-width in points            [default: 10]
    --direction  Force breakout direction                  [default: LONG]
    --no-restore Do not restore live_config.json at end
    --dry-run    Force dry_run=true in the patched config
"""
from __future__ import annotations

import argparse
import datetime
import json
import os
import signal
import subprocess
import sys
import tempfile
import time
import zoneinfo
from pathlib import Path

REPO = Path(__file__).parent.parent
CONFIG_PATH = REPO / "config" / "live_config.json"
LOG_PATH = REPO / "data" / "logs" / "live_trader_stdout.log"
PID_FILE = REPO / "data" / "live_trader.pid"
ET = zoneinfo.ZoneInfo("America/New_York")

GREEN = "\033[32m"
RED = "\033[31m"
YELLOW = "\033[33m"
CYAN = "\033[36m"
RESET = "\033[0m"


def log(msg: str, color: str = "") -> None:
    ts = datetime.datetime.now().strftime("%H:%M:%S")
    print(f"{color}[{ts}] {msg}{RESET}")


def load_env() -> dict:
    env = {}
    env_path = REPO / ".env"
    if env_path.exists():
        for line in env_path.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, _, v = line.partition("=")
                env[k.strip()] = v.strip()
    return env


def get_pg_conn(env: dict):
    import psycopg2
    return psycopg2.connect(
        host=env.get("PG_HOST", "localhost"),
        port=int(env.get("PG_PORT", "5432")),
        dbname=env.get("PG_DB", "rithmic"),
        user=env.get("PG_USER", "rithmic_user"),
        password=env.get("PG_PASSWORD", ""),
        connect_timeout=5,
    )


def kill_live_trader() -> None:
    """Kill any running live_trader.py process."""
    result = subprocess.run(["pgrep", "-f", "live_trader.py"], capture_output=True, text=True)
    pids = result.stdout.strip().split()
    if pids:
        for pid in pids:
            try:
                os.kill(int(pid), signal.SIGTERM)
                log(f"Sent SIGTERM to live_trader PID {pid}", YELLOW)
            except ProcessLookupError:
                pass
        time.sleep(2)
    else:
        log("No live_trader running")


def patch_config(base_price: float, orb_mins: int, dry_run_flag: bool) -> dict:
    """
    Patch live_config.json so RTH opens now (covering the synthetic bars)
    and returns the original config for later restore.
    """
    cfg = json.loads(CONFIG_PATH.read_text())
    original = json.loads(CONFIG_PATH.read_text())  # deep copy

    now_et = datetime.datetime.now(tz=ET)
    # Open session far enough back to include all injected bars + a margin
    rth_open = (now_et - datetime.timedelta(minutes=orb_mins + 2)).replace(second=0, microsecond=0)
    rth_open_str = rth_open.strftime("%H:%M:%S")

    cfg["orb"]["rth_open"] = rth_open_str
    cfg["orb"]["rth_close"] = "23:59:00"
    cfg["orb"]["eod_exit_minutes_before_close"] = 0
    if dry_run_flag:
        cfg["dry_run"] = True

    tmp_fd, tmp_path = tempfile.mkstemp(dir=CONFIG_PATH.parent, suffix=".tmp")
    with os.fdopen(tmp_fd, "w") as f:
        json.dump(cfg, f, indent=2)
    os.replace(tmp_path, CONFIG_PATH)

    log(f"Config patched: rth_open={rth_open_str}, rth_close=23:59, dry_run={cfg.get('dry_run')}", CYAN)
    return original


def restore_config(original: dict) -> None:
    tmp_fd, tmp_path = tempfile.mkstemp(dir=CONFIG_PATH.parent, suffix=".tmp")
    with os.fdopen(tmp_fd, "w") as f:
        json.dump(original, f, indent=2)
    os.replace(tmp_path, CONFIG_PATH)
    log("live_config.json restored to original", CYAN)


def _cleanup_test_artifacts(env: dict) -> None:
    """Remove synthetic ticks and ORB_PY live_position so real data is unaffected."""
    try:
        conn = get_pg_conn(env)
        cur = conn.cursor()
        cur.execute("DELETE FROM ticks WHERE source = %s", ("test",))
        cur.execute("DELETE FROM live_position WHERE strategy = %s", ("ORB_PY",))
        conn.commit()
        conn.close()
        log("Test artifacts cleaned: synthetic ticks + ORB_PY row removed", CYAN)
    except Exception as exc:
        log(f"Cleanup warning: {exc}", YELLOW)


def reset_db_state(env: dict) -> None:
    """Cancel any open test trades and clear live_position for today's session."""
    conn = get_pg_conn(env)
    cur = conn.cursor()
    today = datetime.date.today()
    # Close any open dry_run trades from today so reconciliation doesn't restore them
    cur.execute(
        "UPDATE trades SET exit_time = NOW(), exit_reason = 'TEST_RESET' "
        "WHERE session_date = %s AND dry_run = TRUE AND exit_time IS NULL",
        (today,),
    )
    # Clear live_position so the dashboard starts fresh
    cur.execute(
        "DELETE FROM live_position WHERE session_date = %s AND instrument = 'MNQ'",
        (today,),
    )
    conn.commit()
    conn.close()
    log("DB state reset: open test trades cancelled, live_position cleared", CYAN)


def _orb_base_dt(orb_mins: int) -> datetime.datetime:
    now_et = datetime.datetime.now(tz=ET)
    base_dt = now_et - datetime.timedelta(minutes=orb_mins + 1)
    return base_dt.replace(second=0, microsecond=0)


def inject_synthetic_bars(
    env: dict,
    base_price: float,
    orb_pts: float,
    orb_mins: int,
    direction: str,
    skip_breakout: bool = False,
) -> tuple[float, float]:
    """
    Insert synthetic ticks into the ticks table:
      - orb_mins bars that build the ORB range
      - 1 breakout bar above (LONG) or below (SHORT) the ORB (unless skip_breakout)

    Returns (orb_high, orb_low).
    """
    conn = get_pg_conn(env)
    cur = conn.cursor()

    base_dt = _orb_base_dt(orb_mins)
    orb_high = base_price + orb_pts
    orb_low = base_price - orb_pts

    rows = []

    # ORB-building bars — price oscillates inside the range
    for i in range(orb_mins):
        bar_start = base_dt + datetime.timedelta(minutes=i)
        prices = [base_price + orb_pts * 0.5, base_price - orb_pts * 0.5,
                  base_price + orb_pts * 0.3, base_price]
        for j, p in enumerate(prices):
            ts = bar_start + datetime.timedelta(seconds=j * 15)
            rows.append((ts, "MNQ", "CME", p, 1, "B", True, "test"))

    if not skip_breakout:
        # Breakout bar (1 minute after ORB complete)
        brk_start = base_dt + datetime.timedelta(minutes=orb_mins)
        if direction == "LONG":
            brk_prices = [orb_high + 3.0, orb_high + 5.0, orb_high + 4.0, orb_high + 5.25]
        else:
            brk_prices = [orb_low - 3.0, orb_low - 5.0, orb_low - 4.0, orb_low - 5.25]
        for j, p in enumerate(brk_prices):
            ts = brk_start + datetime.timedelta(seconds=j * 15)
            rows.append((ts, "MNQ", "CME", p, 1, "B" if direction == "LONG" else "S", direction == "LONG", "test"))

    # Delete any existing ticks in our test time window (regardless of source)
    window_end = base_dt + datetime.timedelta(minutes=orb_mins + 2)
    cur.execute(
        "DELETE FROM ticks WHERE symbol = 'MNQ' AND exchange = 'CME' "
        "AND ts_event >= %s AND ts_event < %s",
        (base_dt, window_end),
    )

    cur.executemany(
        "INSERT INTO ticks (ts_event, symbol, exchange, price, size, side, is_buy, source) "
        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
        rows,
    )
    conn.commit()
    conn.close()

    label = "ORB bars only (no breakout)" if skip_breakout else f"ORB + breakout {direction}"
    log(f"Injected {len(rows)} synthetic ticks: ORB {orb_low}–{orb_high}, {label}", CYAN)
    log(f"Bars: {base_dt.strftime('%H:%M')}–{(base_dt + datetime.timedelta(minutes=orb_mins + 1)).strftime('%H:%M')} ET")
    return orb_high, orb_low


def inject_breakout_bar(
    env: dict,
    base_price: float,
    orb_pts: float,
    orb_mins: int,
    direction: str,
) -> None:
    """Inject only the breakout bar into an already-running test session."""
    conn = get_pg_conn(env)
    cur = conn.cursor()

    base_dt = _orb_base_dt(orb_mins)
    orb_high = base_price + orb_pts
    orb_low = base_price - orb_pts
    brk_start = base_dt + datetime.timedelta(minutes=orb_mins)

    if direction == "LONG":
        brk_prices = [orb_high + 3.0, orb_high + 5.0, orb_high + 4.0, orb_high + 5.25]
    else:
        brk_prices = [orb_low - 3.0, orb_low - 5.0, orb_low - 4.0, orb_low - 5.25]

    rows = [
        (brk_start + datetime.timedelta(seconds=j * 15),
         "MNQ", "CME", p, 1,
         "B" if direction == "LONG" else "S", direction == "LONG", "test")
        for j, p in enumerate(brk_prices)
    ]

    # Remove any prior breakout ticks in this slot
    brk_end = brk_start + datetime.timedelta(minutes=1)
    cur.execute(
        "DELETE FROM ticks WHERE symbol = 'MNQ' AND exchange = 'CME' "
        "AND ts_event >= %s AND ts_event < %s",
        (brk_start, brk_end),
    )
    cur.executemany(
        "INSERT INTO ticks (ts_event, symbol, exchange, price, size, side, is_buy, source) "
        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
        rows,
    )
    conn.commit()
    conn.close()
    log(f"Injected breakout bar {direction}: prices {brk_prices}", CYAN)
    log("live_trader will pick it up within 5 seconds…", YELLOW)


def start_live_trader() -> subprocess.Popen:
    """Start live_trader.py in background, log to data/logs/live_trader_stdout.log."""
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    log_file = open(LOG_PATH, "a")
    proc = subprocess.Popen(
        [sys.executable, str(REPO / "live_trader.py"), "--config", str(CONFIG_PATH)],
        cwd=str(REPO),
        stdout=log_file,
        stderr=log_file,
        start_new_session=True,
    )
    log(f"live_trader started (PID {proc.pid})", GREEN)
    return proc


def tail_for_signal(timeout: int = 90, start_pos: int = 0) -> str | None:
    """
    Read live_trader_stdout.log looking for trade_open or error events.
    Returns the matching log line or None on timeout.
    start_pos should be the file size recorded BEFORE starting live_trader so
    lines written during startup replay are not missed.
    """
    log(f"Watching log for trade_open signal (timeout {timeout}s)…", YELLOW)
    deadline = time.monotonic() + timeout
    seen_pos = start_pos

    while time.monotonic() < deadline:
        time.sleep(1)
        if not LOG_PATH.exists():
            continue
        with open(LOG_PATH) as f:
            f.seek(seen_pos)
            chunk = f.read()
            seen_pos = f.tell()
        for line in chunk.splitlines():
            print(f"  {line}")
            if '"trade_open"' in line or "trade_open" in line:
                return line
            if "startup complete" in line:
                log("Trader started — waiting for bar processing…", CYAN)
    return None


def main() -> int:
    parser = argparse.ArgumentParser(description="5m ORB live cycle integration test")
    parser.add_argument("--price", type=float, default=19800.0, help="Base MNQ price")
    parser.add_argument("--orb-pts", type=float, default=10.0, help="ORB half-width in points")
    parser.add_argument("--direction", choices=["LONG", "SHORT"], default="LONG")
    parser.add_argument("--no-restore", action="store_true", help="Keep patched config after test")
    parser.add_argument("--dry-run", action="store_true", help="Force dry_run=true")
    parser.add_argument("--timeout", type=int, default=90, help="Seconds to wait for signal")
    parser.add_argument("--demo", action="store_true",
                        help="Demo mode: reach WATCHING state and hold — use --fire to trigger")
    parser.add_argument("--fire", action="store_true",
                        help="Inject breakout bar into a running --demo session and exit")
    args = parser.parse_args()

    orb_mins = 5  # Always test with 5-min ORB

    log("=" * 60)
    log("5m ORB Live Cycle Test", CYAN)
    log(f"  Price: {args.price}  ORB ±{args.orb_pts}pts  Dir: {args.direction}")
    log("=" * 60)

    env = load_env()
    original_cfg = None

    # ── --fire mode: just inject the breakout bar into a running --demo session ──
    if args.fire:
        inject_breakout_bar(env, args.price, args.orb_pts, orb_mins, args.direction)
        return 0

    try:
        # 1. Kill any running live_trader
        kill_live_trader()

        # 2. Reset DB state (cancel stale test trades, clear live_position)
        reset_db_state(env)

        # 3. Patch config
        original_cfg = patch_config(args.price, orb_mins, args.dry_run)

        # 4. Inject synthetic bars (ORB-building only in demo mode)
        orb_high, orb_low = inject_synthetic_bars(
            env, args.price, args.orb_pts, orb_mins, args.direction,
            skip_breakout=args.demo,
        )

        # 5. Start live_trader — record log position BEFORE launch so startup
        #    lines (including replay trade_open) are not missed by the watcher
        log_start_pos = LOG_PATH.stat().st_size if LOG_PATH.exists() else 0
        _proc = start_live_trader()
        time.sleep(2)  # let it initialize

        # ── demo mode: reach WATCHING and hold for --fire ─────────────────────
        if args.demo:
            log("=" * 60, CYAN)
            log("DEMO — ORB bars injected. Strategy should be in WATCHING state.", CYAN)
            log(f"  ORB {orb_low}–{orb_high} | Check http://localhost:3000/live", CYAN)
            log("  Run with --fire to inject the breakout bar and trigger the trade.", CYAN)
            log("=" * 60, CYAN)
            log("live_trader running. Press Ctrl+C when done.", YELLOW)
            try:
                while True:
                    time.sleep(5)
            except KeyboardInterrupt:
                pass
            return 0

        # 6. Watch for signal (normal test mode)
        hit = tail_for_signal(timeout=args.timeout, start_pos=log_start_pos)

        if hit:
            log("=" * 60, GREEN)
            log("PASS — trade_open signal detected", GREEN)
            log(f"  {hit}", GREEN)
            log("=" * 60, GREEN)
            return 0
        else:
            log("=" * 60, RED)
            log("FAIL — no trade_open within timeout", RED)
            log(f"  ORB {orb_low}–{orb_high}, breakout bar {args.direction} injected but strategy did not fire", RED)
            log("  Check data/logs/live_trader_stdout.log for details", RED)
            log("=" * 60, RED)
            return 1

    finally:
        # Always kill live_trader and clean up test artifacts
        kill_live_trader()
        _cleanup_test_artifacts(env)
        if original_cfg and not args.no_restore:
            restore_config(original_cfg)


if __name__ == "__main__":
    sys.exit(main())
