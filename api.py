"""
api.py — Python API for pulling data from the Rithmic Engine DB.

This is what the main bot imports. Drop-in compatible with DataLoader
for tick and bar data, but reads from the live DuckDB instead of parquet.

Usage (from the bot):
    from rithmic_engine.api import RithmicDB

    db = RithmicDB()

    # Ticks (same schema as DataLoader.trades())
    ticks = db.ticks(start="2026-04-01", end="2026-04-07", rth_only=True)

    # OHLCV bars (same schema as DataLoader.bars())
    bars  = db.bars("1min", start="2026-04-01", end="2026-04-07", rth_only=True)

    # Latest price (for live trading reference)
    price = db.latest_price()

    # Recent ticks (for live inference features)
    recent = db.latest_ticks(n=500)

    # Summary
    print(db.summary())
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd

from db import TickDB, DB_PATH


class RithmicDB:
    """Read-only interface to the Rithmic Engine DuckDB.

    Designed to be used from the main bot process — always opens read-only
    so the collector can keep writing without conflicts.
    """

    def __init__(self, path: Path = DB_PATH):
        self._db = TickDB(path=path, read_only=True)

    def ticks(
        self,
        start: str | None = None,
        end:   str | None = None,
        rth_only: bool = False,
    ) -> pd.DataFrame:
        """Return tick data. Schema: ts_event, price, size, side, is_buy, source."""
        return self._db.ticks(start=start, end=end, rth_only=rth_only)

    def bars(
        self,
        timeframe: str = "1min",
        start: str | None = None,
        end:   str | None = None,
        rth_only: bool = False,
    ) -> pd.DataFrame:
        """Return OHLCV bars. Schema: ts, open, high, low, close, volume."""
        return self._db.bars(timeframe=timeframe, start=start, end=end, rth_only=rth_only)

    def latest_ticks(self, n: int = 1000) -> pd.DataFrame:
        """Return N most recent ticks."""
        return self._db.latest_ticks(n=n)

    def latest_price(self) -> float | None:
        """Return the most recent NQ price in the DB."""
        return self._db.latest_price()

    def date_range(self) -> tuple[str, str]:
        """Return (earliest_date, latest_date) available in the DB."""
        return self._db.date_range()

    def summary(self) -> dict:
        return self._db.summary()

    def close(self):
        self._db.close()

    def __enter__(self): return self
    def __exit__(self, *_): self.close()
