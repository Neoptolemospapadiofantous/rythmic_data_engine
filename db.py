"""
db.py — DuckDB tick database.

Single writer (collector), multiple read-only readers (bot, analysis).

Schema:
    ticks  — raw tick data (ts_event, price, size, side, is_buy, source)
    bars   — pre-aggregated OHLCV bars (computed from ticks on flush)

Usage:
    # Writer (collector process)
    db = TickDB()
    db.write(rows)

    # Reader (bot / analysis)
    db = TickDB(read_only=True)
    df = db.ticks(start="2026-04-01", end="2026-04-07")
    df = db.bars("1min", start="2026-04-01", end="2026-04-07", rth_only=True)
    df = db.latest_ticks(n=1000)
"""
from __future__ import annotations

from datetime import datetime, timezone, timedelta
from pathlib import Path

import duckdb
import pandas as pd

DB_PATH = Path(__file__).parent / "data" / "rithmic.duckdb"

RTH_START_MIN = 570   # 9:30 ET
RTH_END_MIN   = 960   # 16:00 ET

SCHEMA = """
CREATE TABLE IF NOT EXISTS ticks (
    ts_event  TIMESTAMPTZ NOT NULL,
    price     DOUBLE      NOT NULL,
    size      BIGINT      NOT NULL,
    side      VARCHAR,
    is_buy    BOOLEAN,
    source    VARCHAR
);

CREATE INDEX IF NOT EXISTS idx_ticks_ts ON ticks(ts_event);

CREATE TABLE IF NOT EXISTS bars (
    ts        TIMESTAMPTZ NOT NULL,
    timeframe VARCHAR     NOT NULL,  -- '1min', '5min', '15min'
    open      DOUBLE,
    high      DOUBLE,
    low       DOUBLE,
    close     DOUBLE,
    volume    BIGINT,
    PRIMARY KEY (ts, timeframe)
);
"""


class TickDB:
    def __init__(self, path: Path = DB_PATH, read_only: bool = False):
        path.parent.mkdir(parents=True, exist_ok=True)
        self._con = duckdb.connect(str(path), read_only=read_only)
        if not read_only:
            self._con.execute(SCHEMA)

    def close(self):
        self._con.close()

    # ── Write ──────────────────────────────────────────────────────
    def write(self, rows: list[dict]) -> int:
        """Insert tick rows. Returns count inserted."""
        if not rows:
            return 0
        df = pd.DataFrame(rows)
        df["ts_event"] = pd.to_datetime(df["ts_event"], utc=True)
        # Deduplicate against existing data in last 5 seconds
        self._con.execute("""
            INSERT INTO ticks
            SELECT * FROM df
            WHERE ts_event NOT IN (
                SELECT ts_event FROM ticks
                WHERE ts_event >= (SELECT MIN(ts_event) FROM df)
            )
        """)
        return len(rows)

    def compute_bars(self, timeframe: str = "1min"):
        """Compute OHLCV bars from ticks and upsert into bars table."""
        tf_map = {"1min": "1 minute", "5min": "5 minutes", "15min": "15 minutes"}
        interval = tf_map.get(timeframe, "1 minute")
        self._con.execute(f"""
            INSERT OR REPLACE INTO bars
            SELECT
                time_bucket(INTERVAL '{interval}', ts_event) AS ts,
                '{timeframe}' AS timeframe,
                FIRST(price ORDER BY ts_event) AS open,
                MAX(price)                     AS high,
                MIN(price)                     AS low,
                LAST(price  ORDER BY ts_event) AS close,
                SUM(size)                      AS volume
            FROM ticks
            GROUP BY 1, 2
        """)

    # ── Read ───────────────────────────────────────────────────────
    def ticks(
        self,
        start: str | None = None,
        end:   str | None = None,
        rth_only: bool = False,
    ) -> pd.DataFrame:
        """Return tick DataFrame filtered by date range."""
        where = self._date_filter("ts_event", start, end)
        if rth_only:
            where += self._rth_filter("ts_event")
        q = f"SELECT * FROM ticks{where} ORDER BY ts_event"
        return self._con.execute(q).df()

    def bars(
        self,
        timeframe: str = "1min",
        start: str | None = None,
        end:   str | None = None,
        rth_only: bool = False,
    ) -> pd.DataFrame:
        """Return OHLCV bars. Falls back to computing from ticks if bars table empty."""
        where = f" WHERE timeframe = '{timeframe}'"
        if start:
            where += f" AND ts >= '{start}'"
        if end:
            where += f" AND ts < '{end}'"
        if rth_only:
            where += self._rth_filter("ts")

        df = self._con.execute(f"SELECT * FROM bars{where} ORDER BY ts").df()

        if df.empty:
            # Compute on the fly from ticks
            df = self._bars_from_ticks(timeframe, start, end, rth_only)

        return df

    def latest_ticks(self, n: int = 1000) -> pd.DataFrame:
        """Return the N most recent ticks."""
        return self._con.execute(
            f"SELECT * FROM ticks ORDER BY ts_event DESC LIMIT {n}"
        ).df()

    def latest_price(self) -> float | None:
        """Return the most recent price."""
        row = self._con.execute(
            "SELECT price FROM ticks ORDER BY ts_event DESC LIMIT 1"
        ).fetchone()
        return float(row[0]) if row else None

    def date_range(self) -> tuple[str, str]:
        """Return (earliest, latest) date strings in the DB."""
        row = self._con.execute(
            "SELECT MIN(ts_event)::VARCHAR, MAX(ts_event)::VARCHAR FROM ticks"
        ).fetchone()
        return (row[0] or "", row[1] or "")

    def row_count(self) -> int:
        return self._con.execute("SELECT COUNT(*) FROM ticks").fetchone()[0]

    def summary(self) -> dict:
        """Summary stats for the database."""
        count = self.row_count()
        earliest, latest = self.date_range()
        price = self.latest_price()
        return {
            "ticks":    count,
            "earliest": earliest,
            "latest":   latest,
            "price":    price,
            "db_path":  str(DB_PATH),
        }

    # ── Helpers ────────────────────────────────────────────────────
    def _date_filter(self, col: str, start: str | None, end: str | None) -> str:
        parts = []
        if start:
            parts.append(f"{col} >= '{start}'")
        if end:
            parts.append(f"{col} < '{end}'")
        return (" WHERE " + " AND ".join(parts)) if parts else ""

    def _rth_filter(self, col: str) -> str:
        # RTH = 9:30-16:00 ET (minutes 570-960)
        return (
            f" AND (EXTRACT(HOUR FROM {col} AT TIME ZONE 'America/New_York') * 60"
            f"    + EXTRACT(MINUTE FROM {col} AT TIME ZONE 'America/New_York'))"
            f"    BETWEEN {RTH_START_MIN} AND {RTH_END_MIN}"
        )

    def _bars_from_ticks(
        self, timeframe: str, start: str | None, end: str | None, rth_only: bool
    ) -> pd.DataFrame:
        tf_map = {"1min": "1 minute", "5min": "5 minutes", "15min": "15 minutes"}
        interval = tf_map.get(timeframe, "1 minute")
        where = self._date_filter("ts_event", start, end)
        if rth_only:
            where += self._rth_filter("ts_event")
        q = f"""
            SELECT
                time_bucket(INTERVAL '{interval}', ts_event) AS ts,
                FIRST(price ORDER BY ts_event) AS open,
                MAX(price)                     AS high,
                MIN(price)                     AS low,
                LAST(price  ORDER BY ts_event) AS close,
                SUM(size)                      AS volume
            FROM ticks{where}
            GROUP BY 1
            ORDER BY 1
        """
        return self._con.execute(q).df()

    def __enter__(self): return self
    def __exit__(self, *_): self.close()
