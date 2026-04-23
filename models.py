#!/usr/bin/env python3
"""
models.py — Database models for the live NQ ORB trading system.

Provides Trade and SessionSummary dataclasses backed by raw psycopg2 queries
(no ORM dependency — matches the project's existing psycopg2 + .env style).

Usage:
    from models import Trade, SessionSummary, get_conn

    conn = get_conn()
    Trade.ensure_schema(conn)

    trade = Trade(session_date="2026-04-23", symbol="NQ", direction="LONG",
                  entry_price=17850.0, entry_time=datetime.now(timezone.utc))
    trade.save(conn)
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field, asdict
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Optional

import psycopg2
import psycopg2.extras

# ── env loading (same pattern as audit_data.py / migrate_parquet.py) ──

_ENGINE_DIR = Path(__file__).parent


def _load_env() -> None:
    env = _ENGINE_DIR / ".env"
    if env.exists():
        for line in env.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                if k.strip() not in os.environ:
                    os.environ[k.strip()] = v.strip()


def get_conn() -> psycopg2.extensions.connection:
    """Return a psycopg2 connection using PG_* environment variables."""
    _load_env()
    return psycopg2.connect(
        host=os.environ.get("PG_HOST", "localhost"),
        port=int(os.environ.get("PG_PORT", "5432")),
        dbname=os.environ.get("PG_DB", "rithmic"),
        user=os.environ.get("PG_USER", "rithmic_user"),
        password=os.environ.get("PG_PASSWORD", ""),
        connect_timeout=10,
        options="-c statement_timeout=5000",
        cursor_factory=psycopg2.extras.RealDictCursor,
    )


# ── Trade ──────────────────────────────────────────────────────────

@dataclass
class Trade:
    """One completed trade (entry + exit pair).

    source is 'python' for trades recorded by live_trader.py and 'cpp'
    for trades synced from the C++ engine via sync_cpp_trades.py.
    """
    session_date: date
    symbol: str
    direction: str                       # 'LONG' or 'SHORT'
    entry_price: float
    entry_time: datetime

    exit_price: Optional[float] = None
    exit_time: Optional[datetime] = None
    quantity: int = 1
    pnl: Optional[float] = None          # dollars
    pnl_points: Optional[float] = None   # NQ points
    stop_loss: Optional[float] = None
    source: str = "python"               # 'python' or 'cpp'
    session_id: Optional[str] = None
    ml_prediction: Optional[float] = None
    ml_confidence: Optional[float] = None

    # Set by DB on insert
    id: Optional[int] = field(default=None, repr=False)

    # ── schema ────────────────────────────────────────────────────

    @staticmethod
    def ensure_schema(conn: psycopg2.extensions.connection) -> None:
        """Create trades table if it doesn't exist (idempotent)."""
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS trades (
                    id             BIGSERIAL        PRIMARY KEY,
                    session_date   DATE             NOT NULL,
                    symbol         VARCHAR(16)      NOT NULL DEFAULT 'NQ',
                    direction      CHAR(5)          NOT NULL
                                   CHECK (direction IN ('LONG', 'SHORT')),
                    entry_price    DOUBLE PRECISION NOT NULL,
                    exit_price     DOUBLE PRECISION,
                    entry_time     TIMESTAMPTZ      NOT NULL,
                    exit_time      TIMESTAMPTZ,
                    quantity       INTEGER          NOT NULL DEFAULT 1
                                   CHECK (quantity > 0),
                    pnl            DOUBLE PRECISION,
                    pnl_points     DOUBLE PRECISION,
                    stop_loss      DOUBLE PRECISION,
                    source         VARCHAR(8)       NOT NULL DEFAULT 'python'
                                   CHECK (source IN ('python', 'cpp')),
                    session_id     VARCHAR(64),
                    ml_prediction  DOUBLE PRECISION,
                    ml_confidence  DOUBLE PRECISION,
                    created_at     TIMESTAMPTZ      NOT NULL DEFAULT NOW(),
                    updated_at     TIMESTAMPTZ      NOT NULL DEFAULT NOW()
                );
                CREATE UNIQUE INDEX IF NOT EXISTS idx_trades_upsert_key
                    ON trades (symbol, entry_time, direction);
                CREATE INDEX IF NOT EXISTS idx_trades_session_date
                    ON trades (session_date);
            """)
        conn.commit()

    # ── write ─────────────────────────────────────────────────────

    def save(self, conn: psycopg2.extensions.connection) -> int:
        """Insert or update this trade; returns the assigned id."""
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO trades (
                    session_date, symbol, direction, entry_price, exit_price,
                    entry_time, exit_time, quantity, pnl, pnl_points, stop_loss,
                    source, session_id, ml_prediction, ml_confidence
                ) VALUES (
                    %(session_date)s, %(symbol)s, %(direction)s, %(entry_price)s,
                    %(exit_price)s, %(entry_time)s, %(exit_time)s, %(quantity)s,
                    %(pnl)s, %(pnl_points)s, %(stop_loss)s, %(source)s,
                    %(session_id)s, %(ml_prediction)s, %(ml_confidence)s
                )
                ON CONFLICT (symbol, entry_time, direction) DO UPDATE SET
                    exit_price    = EXCLUDED.exit_price,
                    exit_time     = EXCLUDED.exit_time,
                    pnl           = EXCLUDED.pnl,
                    pnl_points    = EXCLUDED.pnl_points,
                    stop_loss     = EXCLUDED.stop_loss,
                    session_id    = EXCLUDED.session_id,
                    ml_prediction = EXCLUDED.ml_prediction,
                    ml_confidence = EXCLUDED.ml_confidence,
                    updated_at    = NOW()
                RETURNING id
            """, {
                "session_date": self.session_date,
                "symbol": self.symbol,
                "direction": self.direction,
                "entry_price": self.entry_price,
                "exit_price": self.exit_price,
                "entry_time": self.entry_time,
                "exit_time": self.exit_time,
                "quantity": self.quantity,
                "pnl": self.pnl,
                "pnl_points": self.pnl_points,
                "stop_loss": self.stop_loss,
                "source": self.source,
                "session_id": self.session_id,
                "ml_prediction": self.ml_prediction,
                "ml_confidence": self.ml_confidence,
            })
            row = cur.fetchone()
            self.id = row["id"]
        conn.commit()
        return self.id

    # ── read ──────────────────────────────────────────────────────

    @classmethod
    def for_date(cls, conn: psycopg2.extensions.connection,
                 session_date: date, symbol: str = "NQ") -> list[Trade]:
        """Return all trades for a given session date."""
        with conn.cursor() as cur:
            cur.execute("""
                SELECT * FROM trades
                WHERE session_date = %s AND symbol = %s
                ORDER BY entry_time
            """, (session_date, symbol))
            return [cls(**_row_to_trade_kwargs(r)) for r in cur.fetchall()]

    @classmethod
    def get(cls, conn: psycopg2.extensions.connection, trade_id: int) -> Optional[Trade]:
        """Fetch a single trade by primary key."""
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM trades WHERE id = %s", (trade_id,))
            row = cur.fetchone()
            return cls(**_row_to_trade_kwargs(row)) if row else None

    @classmethod
    def open_position(cls, conn: psycopg2.extensions.connection,
                      symbol: str = "NQ") -> Optional[Trade]:
        """Return the most recent trade with no exit (open position), or None."""
        with conn.cursor() as cur:
            cur.execute("""
                SELECT * FROM trades
                WHERE symbol = %s AND exit_time IS NULL AND source = 'python'
                ORDER BY entry_time DESC
                LIMIT 1
            """, (symbol,))
            row = cur.fetchone()
            return cls(**_row_to_trade_kwargs(row)) if row else None


def _row_to_trade_kwargs(row: dict) -> dict:
    """Map a DB row dict to Trade constructor kwargs (excludes created_at/updated_at)."""
    return {k: v for k, v in dict(row).items()
            if k in Trade.__dataclass_fields__}


# ── SessionSummary ─────────────────────────────────────────────────

@dataclass
class SessionSummary:
    """End-of-day summary for one trading session.

    session_id format: "{date}_{source}", e.g. "2026-04-23_python".
    """
    session_id: str
    date: date
    gross_pnl: float = 0.0
    trade_count: int = 0
    win_count: int = 0
    max_drawdown: float = 0.0
    start_equity: Optional[float] = None
    end_equity: Optional[float] = None
    notes: Optional[str] = None
    source: str = "python"

    # ── schema ────────────────────────────────────────────────────

    @staticmethod
    def ensure_schema(conn: psycopg2.extensions.connection) -> None:
        """Create session_summary table if it doesn't exist (idempotent)."""
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS session_summary (
                    session_id    VARCHAR(64)      PRIMARY KEY,
                    date          DATE             NOT NULL,
                    source        VARCHAR(8)       NOT NULL DEFAULT 'python'
                                  CHECK (source IN ('python', 'cpp', 'mixed')),
                    gross_pnl     DOUBLE PRECISION NOT NULL DEFAULT 0.0,
                    trade_count   INTEGER          NOT NULL DEFAULT 0,
                    win_count     INTEGER          NOT NULL DEFAULT 0,
                    max_drawdown  DOUBLE PRECISION NOT NULL DEFAULT 0.0,
                    start_equity  DOUBLE PRECISION,
                    end_equity    DOUBLE PRECISION,
                    notes         TEXT,
                    created_at    TIMESTAMPTZ      NOT NULL DEFAULT NOW(),
                    updated_at    TIMESTAMPTZ      NOT NULL DEFAULT NOW()
                );
                CREATE INDEX IF NOT EXISTS idx_session_summary_date
                    ON session_summary (date);
            """)
        conn.commit()

    # ── write ─────────────────────────────────────────────────────

    def save(self, conn: psycopg2.extensions.connection) -> None:
        """Upsert this session summary row."""
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO session_summary (
                    session_id, date, source, gross_pnl, trade_count, win_count,
                    max_drawdown, start_equity, end_equity, notes
                ) VALUES (
                    %(session_id)s, %(date)s, %(source)s, %(gross_pnl)s,
                    %(trade_count)s, %(win_count)s, %(max_drawdown)s,
                    %(start_equity)s, %(end_equity)s, %(notes)s
                )
                ON CONFLICT (session_id) DO UPDATE SET
                    gross_pnl    = EXCLUDED.gross_pnl,
                    trade_count  = EXCLUDED.trade_count,
                    win_count    = EXCLUDED.win_count,
                    max_drawdown = EXCLUDED.max_drawdown,
                    end_equity   = EXCLUDED.end_equity,
                    notes        = EXCLUDED.notes,
                    updated_at   = NOW()
            """, {
                "session_id": self.session_id,
                "date": self.date,
                "source": self.source,
                "gross_pnl": self.gross_pnl,
                "trade_count": self.trade_count,
                "win_count": self.win_count,
                "max_drawdown": self.max_drawdown,
                "start_equity": self.start_equity,
                "end_equity": self.end_equity,
                "notes": self.notes,
            })
        conn.commit()

    # ── read ──────────────────────────────────────────────────────

    @classmethod
    def for_date(cls, conn: psycopg2.extensions.connection,
                 session_date: date) -> list[SessionSummary]:
        """Return all session summaries for a given date (may be >1 if cpp+python)."""
        with conn.cursor() as cur:
            cur.execute("""
                SELECT session_id, date, source, gross_pnl, trade_count,
                       win_count, max_drawdown, start_equity, end_equity, notes
                FROM session_summary
                WHERE date = %s
                ORDER BY source
            """, (session_date,))
            return [cls(**dict(r)) for r in cur.fetchall()]

    @classmethod
    def build_from_trades(cls, trades: list[Trade], source: str = "python",
                          start_equity: Optional[float] = None) -> SessionSummary:
        """Compute a SessionSummary from a list of completed trades."""
        if not trades:
            session_date = date.today()
        else:
            session_date = (trades[0].session_date
                            if isinstance(trades[0].session_date, date)
                            else trades[0].session_date)

        gross_pnl = sum(t.pnl or 0.0 for t in trades)
        wins = sum(1 for t in trades if (t.pnl or 0.0) > 0)

        # Max intraday drawdown from peak equity
        equity = start_equity or 0.0
        peak = equity
        max_dd = 0.0
        for t in trades:
            equity += (t.pnl or 0.0)
            if equity > peak:
                peak = equity
            dd = peak - equity
            if dd > max_dd:
                max_dd = dd

        return cls(
            session_id=f"{session_date}_{source}",
            date=session_date,
            source=source,
            gross_pnl=gross_pnl,
            trade_count=len(trades),
            win_count=wins,
            max_drawdown=max_dd,
            start_equity=start_equity,
            end_equity=(start_equity or 0.0) + gross_pnl,
        )
