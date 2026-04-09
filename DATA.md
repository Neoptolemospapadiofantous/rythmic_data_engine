# Rithmic Engine — Data Storage Reference

## Storage Backend

**PostgreSQL 16 + TimescaleDB** running on Oracle ARM VM (or localhost for dev).

Connection configured via `.env`:
```
PG_HOST=localhost
PG_PORT=5432
PG_DB=rithmic
PG_USER=rithmic_user
PG_PASSWORD=...
```

---

## Schema

### `ticks` — raw trade prints (hypertable, partitioned by day)

| Column     | Type            | Notes                          |
|------------|-----------------|--------------------------------|
| `ts_event` | TIMESTAMPTZ     | Exchange timestamp (UTC, µs precision) |
| `symbol`   | VARCHAR(32)     | e.g. `NQ`                      |
| `exchange` | VARCHAR(32)     | e.g. `CME`                     |
| `price`    | DOUBLE PRECISION| Trade price                    |
| `size`     | BIGINT          | Contracts traded               |
| `side`     | CHAR(1)         | `B` = buy aggressor, `A` = ask |
| `is_buy`   | BOOLEAN         | True if buy aggressor          |
| `source`   | VARCHAR(32)     | Always `amp_rithmic`           |

**Primary key / dedup:** `UNIQUE (symbol, exchange, ts_event)`

**Compression:** TimescaleDB compresses chunks older than 7 days (~10:1 ratio).

### `bars_1min`, `bars_5min`, `bars_15min` — OHLCV (continuous aggregates)

| Column   | Type        | Notes                    |
|----------|-------------|--------------------------|
| `ts`     | TIMESTAMPTZ | Bucket open time (UTC)   |
| `open`   | DOUBLE      | First price in bucket    |
| `high`   | DOUBLE      |                          |
| `low`    | DOUBLE      |                          |
| `close`  | DOUBLE      | Last price in bucket     |
| `volume` | BIGINT      | Total contracts          |

Auto-refreshed by TimescaleDB background worker. Read-only (materialized views).

### `audit_log` — collector event trail

| Column     | Type        | Notes                              |
|------------|-------------|------------------------------------|
| `ts`       | TIMESTAMPTZ | Event time                         |
| `severity` | VARCHAR(8)  | `INFO`, `WARN`, `ERROR`            |
| `event`    | VARCHAR(64) | e.g. `collector.start`, `db.write` |
| `details`  | TEXT        | Free-form detail string            |

---

## File Layout

```
rithmic_engine/
├── data/
│   ├── logs/
│   │   ├── collector.log   ← structured collector log (rotated daily)
│   │   └── sync.log        ← R2 upload log (if sync enabled)
│   └── rithmic.duckdb      ← LEGACY: old Python-era DB, no longer written to
├── .env                    ← credentials (never commit)
└── build/
    └── rithmic_engine      ← compiled binary
```

> **Note:** `data/rithmic.duckdb` is a leftover from the old Python collector.
> It is not written to by the C++ engine. Safe to delete once the Python bot
> has been updated to read from PostgreSQL instead.

---

## Data Volumes (NQ, normal RTH)

| Period  | Ticks      | Compressed (PG) |
|---------|-----------|-----------------|
| 1 day   | ~585,000  | ~6 MB           |
| 1 week  | ~2.9 M    | ~30 MB          |
| 1 month | ~12.5 M   | ~125 MB         |
| 1 year  | ~150 M    | ~1.5 GB         |
| 5 years | ~750 M    | ~7.5 GB         |

---

## Reading from Python (bot integration)

```python
import psycopg2
import pandas as pd

conn = psycopg2.connect(
    host=os.environ["PG_HOST"],
    port=os.environ["PG_PORT"],
    dbname=os.environ["PG_DB"],
    user=os.environ["PG_USER"],
    password=os.environ["PG_PASSWORD"],
)

# Raw ticks (RTH only)
df = pd.read_sql("""
    SELECT ts_event, price, size, side, is_buy
    FROM ticks
    WHERE symbol = 'NQ'
      AND ts_event BETWEEN %s AND %s
      AND EXTRACT(HOUR FROM ts_event AT TIME ZONE 'America/New_York') * 60
        + EXTRACT(MINUTE FROM ts_event AT TIME ZONE 'America/New_York')
        BETWEEN 570 AND 960
    ORDER BY ts_event
""", conn, params=("2026-04-01", "2026-04-08"))

# 1-minute OHLCV bars
df = pd.read_sql("""
    SELECT ts, open, high, low, close, volume
    FROM bars_1min
    WHERE ts BETWEEN %s AND %s
    ORDER BY ts
""", conn, params=("2026-04-01", "2026-04-08"))

# Latest price
cur = conn.cursor()
cur.execute("SELECT price FROM ticks ORDER BY ts_event DESC LIMIT 1")
price = cur.fetchone()[0]
```

---

## Useful Queries

```sql
-- Tick rate last 5 minutes
SELECT time_bucket('1 minute', ts_event) AS min,
       COUNT(*) AS ticks, MIN(price) AS low, MAX(price) AS high
FROM ticks
WHERE ts_event > NOW() - INTERVAL '5 minutes'
GROUP BY 1 ORDER BY 1;

-- Latest price
SELECT price, ts_event FROM ticks ORDER BY ts_event DESC LIMIT 1;

-- DB size
SELECT pg_size_pretty(pg_database_size('rithmic'));

-- Compression savings
SELECT hypertable_name,
       pg_size_pretty(before_compression_total_bytes) AS before,
       pg_size_pretty(after_compression_total_bytes)  AS after
FROM chunk_compression_stats('ticks');

-- Recent audit events
SELECT ts, severity, event, details FROM audit_log ORDER BY ts DESC LIMIT 20;
```

---

## Collector Write Path

```
Rithmic LastTrade callback
  → push to std::vector buffer (< 1 µs)
  → flush every 200 ticks OR 30 seconds
  → TickDB::write() — UNNEST batch INSERT with ON CONFLICT DO NOTHING
  → TimescaleDB background worker refreshes bar aggregates
```

Flush triggers: `FLUSH_EVERY_N = 200` ticks or `FLUSH_EVERY_SEC = 30` seconds.
