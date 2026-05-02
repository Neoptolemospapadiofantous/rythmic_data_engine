#!/usr/bin/env python3
"""
eod_summary.py — Crash-safe end-of-day session summary writer.

Intended to run at ~16:15 ET daily (after RTH close) via systemd timer or cron,
independent of whether live_trader.py completed cleanly.

Workflow:
  1. Sync C++ live_trades → trades (idempotent, no-op if live_trades absent)
  2. Compute SessionSummary from all trades for today
  3. Write session_summary row (upsert — idempotent if live_trader already wrote it)

This script is idempotent: running it multiple times for the same session_date
will update the existing row, not create duplicates.

Usage:
    python scripts/eod_summary.py                        # process today
    python scripts/eod_summary.py --date 2026-04-22     # backfill a specific date
    python scripts/eod_summary.py --dry-run              # show without writing
    python scripts/eod_summary.py --no-cpp-sync          # skip live_trades sync step
"""
from __future__ import annotations

import argparse
import logging
import subprocess
import sys
from datetime import date
from pathlib import Path
from typing import Optional

_SCRIPT_DIR = Path(__file__).parent
_ENGINE_DIR = _SCRIPT_DIR.parent
sys.path.insert(0, str(_ENGINE_DIR))

from models import Trade, SessionSummary, get_conn

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    level=logging.INFO,
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("eod_summary")


def _run_cpp_sync(session_date: date, dry_run: bool) -> bool:
    """Invoke sync_cpp_trades.py as a subprocess; returns True on success."""
    sync_script = _SCRIPT_DIR / "sync_cpp_trades.py"
    if not sync_script.exists():
        log.warning("sync_cpp_trades.py not found at %s — skipping C++ sync", sync_script)
        return False

    cmd = [sys.executable, str(sync_script), "--date", str(session_date)]
    if dry_run:
        cmd.append("--dry-run")

    log.info("Running C++ sync: %s", " ".join(cmd))
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.stdout:
        for line in result.stdout.splitlines():
            log.info("[cpp-sync] %s", line)
    if result.stderr:
        for line in result.stderr.splitlines():
            log.info("[cpp-sync] %s", line)

    if result.returncode != 0:
        log.warning("sync_cpp_trades.py exited with code %d", result.returncode)
        return False
    return True


def _compute_max_drawdown(trades: list[Trade], start_equity: float = 0.0) -> float:
    """Calculate maximum intraday drawdown from peak equity over the trade sequence."""
    equity = start_equity
    peak = equity
    max_dd = 0.0
    for t in sorted(trades, key=lambda x: x.entry_time):
        equity += t.pnl or 0.0
        if equity > peak:
            peak = equity
        dd = peak - equity
        if dd > max_dd:
            max_dd = dd
    return max_dd


def write_eod_summary(
    session_date: date,
    dry_run: bool = False,
    cpp_sync: bool = True,
    start_equity: Optional[float] = None,
) -> dict:
    """Compute and write (or log) the EOD session summary for session_date.

    Returns a summary dict with keys: date, trade_count, gross_pnl, win_count,
    max_drawdown, wrote_db.
    """
    conn = get_conn()
    try:
        Trade.ensure_schema(conn)
        SessionSummary.ensure_schema(conn)

        if cpp_sync:
            _run_cpp_sync(session_date, dry_run=dry_run)

        trades = Trade.for_date(conn, session_date)
        completed = [t for t in trades if t.exit_time is not None]

        gross_pnl = sum(t.pnl or 0.0 for t in completed)
        win_count = sum(1 for t in completed if (t.pnl or 0.0) > 0)
        max_dd = _compute_max_drawdown(completed, start_equity or 0.0)
        end_equity = (start_equity or 0.0) + gross_pnl

        session_id = f"{session_date}_python"
        summary = SessionSummary(
            session_id=session_id,
            date=session_date,
            source="python",
            gross_pnl=gross_pnl,
            trade_count=len(completed),
            win_count=win_count,
            max_drawdown=max_dd,
            start_equity=start_equity,
            end_equity=end_equity,
            notes=f"eod_summary.py — {len(trades)} total trades ({len(completed)} completed)",
        )

        result = {
            "date": str(session_date),
            "trade_count": len(completed),
            "gross_pnl": round(gross_pnl, 2),
            "win_count": win_count,
            "max_drawdown": round(max_dd, 2),
            "wrote_db": False,
        }

        if dry_run:
            log.info(
                "[DRY-RUN] Would write session_summary: %s  trades=%d  pnl=%.2f  wins=%d  dd=%.2f",
                session_id, len(completed), gross_pnl, win_count, max_dd,
            )
        else:
            summary.save(conn)
            result["wrote_db"] = True
            log.info(
                "Wrote session_summary: %s  trades=%d  pnl=%.2f  wins=%d  dd=%.2f",
                session_id, len(completed), gross_pnl, win_count, max_dd,
            )

        return result

    finally:
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Write EOD session summary from trades table."
    )
    parser.add_argument(
        "--date", metavar="YYYY-MM-DD",
        help="Session date to summarise (default: today)",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Compute and log the summary without writing to DB",
    )
    parser.add_argument(
        "--no-cpp-sync", action="store_true",
        help="Skip the live_trades → trades sync step",
    )
    parser.add_argument(
        "--start-equity", type=float, default=None,
        help="Starting account equity for drawdown computation",
    )
    args = parser.parse_args()

    session_date = date.fromisoformat(args.date) if args.date else date.today()

    result = write_eod_summary(
        session_date=session_date,
        dry_run=args.dry_run,
        cpp_sync=not args.no_cpp_sync,
        start_equity=args.start_equity,
    )

    log.info("EOD summary result: %s", result)

    if not result["wrote_db"] and not args.dry_run:
        sys.exit(1)


if __name__ == "__main__":
    main()
