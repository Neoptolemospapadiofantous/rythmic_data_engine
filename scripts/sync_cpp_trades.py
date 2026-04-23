#!/usr/bin/env python3
"""
sync_cpp_trades.py — Sync C++ ORB trades from nq_trades into unified trades table.

The C++ engine (orb_db.hpp) writes fills to the nq_trades table with its own
schema. This script reads those rows and upserts them into the trades table
defined by migrations/001_trades.sql and models.py.

Idempotent: uses ON CONFLICT (symbol, entry_time, direction) DO UPDATE so it
can be run multiple times without creating duplicates.

Intended use: called automatically as part of the EOD process (via --eod flag
or scripts/eod_summary.py), and also manually when investigating discrepancies.

Usage:
    python scripts/sync_cpp_trades.py               # sync today only
    python scripts/sync_cpp_trades.py --date 2026-04-22
    python scripts/sync_cpp_trades.py --all          # all available history
    python scripts/sync_cpp_trades.py --dry-run      # show without writing
    python scripts/sync_cpp_trades.py --eod          # sync today + write session_summary
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Optional

# Allow running from scripts/ or from project root
_SCRIPT_DIR = Path(__file__).parent
_ENGINE_DIR = _SCRIPT_DIR.parent
sys.path.insert(0, str(_ENGINE_DIR))

import psycopg2
import psycopg2.extras

from models import Trade, SessionSummary, get_conn

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    level=logging.INFO,
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("sync_cpp_trades")


# ── nq_trades schema (C++ side, from orb_db.hpp) ──────────────────
# Columns expected in nq_trades.  The C++ schema may evolve; we read
# only the columns we need and skip rows missing required fields.
_NQ_TRADES_COLUMNS = {
    "id", "symbol", "direction", "entry_price", "exit_price",
    "entry_time", "exit_time", "quantity", "pnl", "pnl_points",
    "stop_loss", "session_date",
}

_NQ_TRADES_REQUIRED = {"symbol", "direction", "entry_price", "entry_time"}


# ── helpers ────────────────────────────────────────────────────────

def _nq_trades_exists(conn: psycopg2.extensions.connection) -> bool:
    """Return True if the nq_trades table exists in the DB."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = 'public' AND table_name = 'nq_trades'
            )
        """)
        return cur.fetchone()[0]


def _get_nq_trades_columns(conn: psycopg2.extensions.connection) -> set[str]:
    """Return the actual column names present in nq_trades."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT column_name FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = 'nq_trades'
        """)
        return {row[0] for row in cur.fetchall()}


def _fetch_cpp_trades(conn: psycopg2.extensions.connection,
                      session_date: Optional[date],
                      available_cols: set[str]) -> list[dict]:
    """Read rows from nq_trades for the given date (or all if date is None)."""
    # Build SELECT only for columns that exist in both schemas
    wanted = _NQ_TRADES_COLUMNS & available_cols
    if not (_NQ_TRADES_REQUIRED <= wanted):
        missing = _NQ_TRADES_REQUIRED - wanted
        log.warning("nq_trades missing required columns: %s — skipping sync", missing)
        return []

    cols = ", ".join(sorted(wanted))
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        if session_date is not None:
            # Filter by session_date if the column exists, else by entry_time date
            if "session_date" in wanted:
                cur.execute(
                    f"SELECT {cols} FROM nq_trades WHERE session_date = %s",
                    (session_date,)
                )
            else:
                cur.execute(
                    f"SELECT {cols} FROM nq_trades WHERE entry_time::date = %s",
                    (session_date,)
                )
        else:
            cur.execute(f"SELECT {cols} FROM nq_trades ORDER BY entry_time")
        return [dict(r) for r in cur.fetchall()]


def _cpp_row_to_trade(row: dict) -> Optional[Trade]:
    """Convert a nq_trades row dict to a Trade dataclass.  Returns None on error."""
    try:
        entry_time = row["entry_time"]
        if isinstance(entry_time, str):
            entry_time = datetime.fromisoformat(entry_time)
        if entry_time.tzinfo is None:
            entry_time = entry_time.replace(tzinfo=timezone.utc)

        exit_time = row.get("exit_time")
        if isinstance(exit_time, str) and exit_time:
            exit_time = datetime.fromisoformat(exit_time)
            if exit_time.tzinfo is None:
                exit_time = exit_time.replace(tzinfo=timezone.utc)

        s_date = row.get("session_date")
        if s_date is None:
            s_date = entry_time.date()
        elif isinstance(s_date, str):
            s_date = date.fromisoformat(s_date)

        direction = str(row["direction"]).upper()
        if direction not in ("LONG", "SHORT"):
            log.warning("Skipping row with unknown direction %r", direction)
            return None

        return Trade(
            session_date=s_date,
            symbol=str(row.get("symbol", "NQ")),
            direction=direction,
            entry_price=float(row["entry_price"]),
            entry_time=entry_time,
            exit_price=float(row["exit_price"]) if row.get("exit_price") is not None else None,
            exit_time=exit_time,
            quantity=int(row.get("quantity", 1)),
            pnl=float(row["pnl"]) if row.get("pnl") is not None else None,
            pnl_points=float(row["pnl_points"]) if row.get("pnl_points") is not None else None,
            stop_loss=float(row["stop_loss"]) if row.get("stop_loss") is not None else None,
            source="cpp",
        )
    except (KeyError, ValueError, TypeError) as exc:
        log.warning("Failed to convert nq_trades row %r: %s", row, exc)
        return None


# ── EOD convenience wrapper ────────────────────────────────────────

def run_eod_sync(conn: psycopg2.extensions.connection,
                 session_date: Optional[date] = None) -> dict:
    """Sync C++ nq_trades → unified trades for session_date (default: today).

    Callable without side effects when nq_trades doesn't exist or is empty.
    Returns the same result dict as sync(): {found, synced, skipped, errors}.

    This function is the intended entry point for pipeline/EOD automation.
    External callers should ensure Trade.ensure_schema(conn) has already run.
    """
    if session_date is None:
        session_date = date.today()
    return sync(conn, session_date, dry_run=False)


# ── main sync logic ────────────────────────────────────────────────

def sync(conn: psycopg2.extensions.connection,
         session_date: Optional[date],
         dry_run: bool = False) -> dict:
    """Sync C++ trades for the given date into the unified trades table.

    Returns a summary dict: {found, synced, skipped, errors}.
    """
    if not _nq_trades_exists(conn):
        log.info("nq_trades table does not exist — nothing to sync")
        return {"found": 0, "synced": 0, "skipped": 0, "errors": 0,
                "reason": "nq_trades_missing"}

    available_cols = _get_nq_trades_columns(conn)
    log.debug("nq_trades columns: %s", available_cols)

    cpp_rows = _fetch_cpp_trades(conn, session_date, available_cols)
    log.info("Found %d rows in nq_trades%s",
             len(cpp_rows),
             f" for {session_date}" if session_date else " (all history)")

    synced = skipped = errors = 0
    for row in cpp_rows:
        trade = _cpp_row_to_trade(row)
        if trade is None:
            errors += 1
            continue

        if dry_run:
            log.info("[DRY-RUN] Would upsert: %s %s entry=%.2f @ %s",
                     trade.direction, trade.symbol,
                     trade.entry_price, trade.entry_time)
            synced += 1
            continue

        try:
            trade.save(conn)
            synced += 1
            log.debug("Synced trade id=%s %s %s @ %.2f",
                      trade.id, trade.direction, trade.symbol, trade.entry_price)
        except Exception as exc:
            log.error("Failed to save trade %r: %s", trade, exc)
            conn.rollback()
            errors += 1

    result = {
        "found": len(cpp_rows),
        "synced": synced,
        "skipped": skipped,
        "errors": errors,
    }
    log.info("Sync complete: %s", result)
    return result


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Sync C++ nq_trades into the unified trades table."
    )
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--date", metavar="YYYY-MM-DD",
                       help="Sync trades for this session date only (default: today)")
    group.add_argument("--all", action="store_true",
                       help="Sync all available history from nq_trades")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show what would be synced without writing")
    parser.add_argument("--eod", action="store_true",
                        help="After syncing, write EOD session_summary for the synced date")
    args = parser.parse_args()

    if args.all:
        session_date = None
    elif args.date:
        session_date = date.fromisoformat(args.date)
    else:
        session_date = date.today()

    conn = get_conn()
    try:
        # Ensure destination tables exist before syncing
        Trade.ensure_schema(conn)
        SessionSummary.ensure_schema(conn)

        result = sync(conn, session_date, dry_run=args.dry_run)

        if result.get("errors", 0) > 0:
            log.warning("%d errors during sync — check logs above", result["errors"])
            sys.exit(1)

        if args.eod and session_date is not None:
            # Import here to avoid circular dependency if eod_summary imports this module.
            import importlib.util
            _eod_path = _ENGINE_DIR / "scripts" / "eod_summary.py"
            spec = importlib.util.spec_from_file_location("eod_summary", _eod_path)
            eod_mod = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(eod_mod)
            eod_result = eod_mod.write_eod_summary(
                session_date=session_date,
                dry_run=args.dry_run,
                cpp_sync=False,  # already synced above
            )
            log.info("EOD summary: %s", eod_result)
        elif args.eod and session_date is None:
            log.warning("--eod with --all is not supported; run eod_summary.py separately per date")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
