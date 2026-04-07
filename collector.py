"""
collector.py — 24/7 Rithmic AMP tick collector → DuckDB.

Connects to Rithmic AMP, subscribes to live NQ ticks, writes every tick
directly to the DuckDB database. Reconnects automatically on disconnect.

Usage:
    python collector.py            # run until Ctrl-C
    python collector.py --status   # show DB stats
    python collector.py --replay   # replay today's amp parquet into DB
"""
from __future__ import annotations

import asyncio
import gc
import os
import sys
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

# ── Path setup ────────────────────────────────────────────────────
os.chdir(Path(__file__).parent)
sys.path.insert(0, str(Path(__file__).parent))

from db import TickDB, DB_PATH

LOG_FILE = Path("data/logs/collector.log")

FLUSH_EVERY_N   = 200       # write to DB every N ticks
FLUSH_EVERY_SEC = 30        # or every N seconds
BACKOFF_INITIAL = 30
BACKOFF_MAX     = 300


# ── Logging ───────────────────────────────────────────────────────
def log(msg: str):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    try:
        LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
        with open(LOG_FILE, "a") as f:
            f.write(line + "\n")
    except Exception:
        pass


def _load_env():
    env = Path(__file__).parent / ".env"
    if env.exists():
        for line in env.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                if k.strip() not in os.environ:
                    os.environ[k.strip()] = v.strip()


def _parse_tick(tick) -> dict | None:
    try:
        if isinstance(tick, dict):
            price     = float(tick.get("trade_price", tick.get("price", 0)) or 0)
            size      = int(tick.get("trade_size",   tick.get("size",  0))  or 0)
            aggressor = tick.get("aggressor", tick.get("aggressor_side", 0))
            ts        = tick.get("datetime", tick.get("timestamp"))
        else:
            price     = float(getattr(tick, "trade_price", getattr(tick, "price", 0)) or 0)
            size      = int(getattr(tick, "trade_size",   getattr(tick, "size",  0))  or 0)
            aggressor = getattr(tick, "aggressor", getattr(tick, "aggressor_side", 0))
            ts        = getattr(tick, "datetime", getattr(tick, "timestamp", None))

        if price <= 0 or size <= 0:
            return None

        if isinstance(aggressor, str):
            is_buy = aggressor.upper() in ("BUY", "B", "1")
        else:
            is_buy = int(aggressor or 0) == 1

        if ts is None:
            ts = datetime.now(timezone.utc)
        elif not hasattr(ts, "tzinfo") or ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)

        return {
            "ts_event": ts,
            "price":    price,
            "size":     size,
            "side":     "B" if is_buy else "A",
            "is_buy":   is_buy,
            "source":   "amp_rithmic",
        }
    except Exception:
        return None


class Collector:
    def __init__(self):
        self._buf:        list[dict] = []
        self._last_flush: float = time.monotonic()
        self._total:      int = 0
        self._db = TickDB()
        log(f"DB opened: {DB_PATH}  ({self._db.row_count():,} existing ticks)")

    async def run(self):
        attempt = 0
        while True:
            try:
                log(f"Connecting to Rithmic AMP (attempt {attempt + 1})...")
                await self._session()
                attempt = 0
            except KeyboardInterrupt:
                self._flush()
                log("Stopped.")
                break
            except Exception as e:
                delay = min(BACKOFF_INITIAL * (2 ** attempt), BACKOFF_MAX)
                log(f"Disconnected: {e} — reconnecting in {delay}s")
                self._flush()
                attempt += 1
                await asyncio.sleep(delay)

    async def _session(self):
        from client import RithmicConfig, get_client
        from async_rithmic import DataType

        cfg    = RithmicConfig.from_env()
        client = get_client(cfg)

        async def on_tick(tick):
            row = _parse_tick(tick)
            if row is None:
                return
            self._buf.append(row)
            self._total += 1

            now = time.monotonic()
            if len(self._buf) >= FLUSH_EVERY_N or (now - self._last_flush) >= FLUSH_EVERY_SEC:
                self._flush()

        client.on_tick += on_tick

        try:
            await client.connect()
            front    = await client.get_front_month_contract(symbol=cfg.symbol, exchange=cfg.exchange)
            contract = front or cfg.symbol
            log(f"Connected — streaming {contract} on {cfg.exchange}")

            await client.subscribe_to_market_data(
                symbol=contract, exchange=cfg.exchange, data_type=DataType.LAST_TRADE
            )

            while True:
                await asyncio.sleep(60)
                stats = self._db.summary()
                log(f"  ticks total={stats['ticks']:,}  session={self._total:,}  "
                    f"latest={stats['latest']}  price={stats['price']}")

        finally:
            try:
                await client.unsubscribe_from_market_data(
                    symbol=contract or cfg.symbol, exchange=cfg.exchange,
                    data_type=DataType.LAST_TRADE,
                )
            except Exception:
                pass
            try:
                await client.disconnect()
            except Exception:
                pass

    def _flush(self):
        if not self._buf:
            return
        try:
            n = self._db.write(self._buf)
            log(f"  Wrote {n} ticks to DB (session total: {self._total:,})")
            self._buf.clear()
            self._last_flush = time.monotonic()
        except Exception as e:
            log(f"  DB write error: {e}")


def _replay_parquet():
    """Import existing amp parquet files into the DB (catch-up after outage)."""
    amp_dir = Path("data/amp_trades")
    if not amp_dir.exists():
        print("No amp_trades directory found")
        return

    import pandas as pd
    files = sorted(amp_dir.glob("amp_*.parquet"))
    if not files:
        print("No parquet files found")
        return

    db = TickDB()
    for f in files:
        df = pd.read_parquet(f)
        rows = df.to_dict("records")
        n = db.write(rows)
        print(f"  {f.name}: imported {n} ticks")
    db.close()
    print(f"Done. DB now has {TickDB(read_only=True).row_count():,} ticks")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Rithmic AMP → DuckDB collector")
    parser.add_argument("--status", action="store_true", help="Show DB stats")
    parser.add_argument("--replay", action="store_true", help="Import parquet files into DB")
    args = parser.parse_args()

    _load_env()

    if args.status:
        with TickDB(read_only=True) as db:
            s = db.summary()
            print(f"Ticks:    {s['ticks']:,}")
            print(f"Range:    {s['earliest']}  →  {s['latest']}")
            print(f"Price:    {s['price']}")
            print(f"DB path:  {s['db_path']}")
    elif args.replay:
        _replay_parquet()
    else:
        gc.freeze()
        _load_env()
        asyncio.run(Collector().run())
