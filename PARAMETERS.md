# Rithmic Engine — System Parameters & Data Flow

Complete reference for latency budgets, data rates, hardware sizing,
and the full tick-to-database pipeline.

---

## 1. Network & Latency

### Rithmic AMP Gateway
| Parameter | Value | Notes |
|---|---|---|
| Protocol | WebSocket over TLS 1.3 | `wss://ritpz01001.01.rithmic.com:443` |
| Gateway location | Chicago (Equinix CH4) | Co-located with CME |
| SSL handshake (one-time) | 50 – 200 ms | Per reconnect, not per tick |
| Tick delivery latency | 1 – 10 ms | Exchange → your client (depends on your location) |
| Tick delivery from Oracle VM (EU/US) | 15 – 80 ms | Typical Oracle ARM VM latency to Chicago |
| Tick delivery from co-located server | < 1 ms | If you co-locate at Equinix CH4/NY4 |
| Heartbeat interval | 30 s (server-set) | Sent in `ResponseLogin.heartbeat_interval` |
| Max reconnect backoff | 300 s | Exponential: 30 → 60 → 120 → 300 |

### Wire Protocol Overhead
| Layer | Size |
|---|---|
| WebSocket frame header | 2 – 10 bytes |
| Rithmic length prefix | 4 bytes |
| LastTrade protobuf (typical) | 60 – 120 bytes |
| **Total per tick on wire** | **~80 – 140 bytes** |

---

## 2. Data Rates — NQ Futures (CME)

| Market condition | Ticks / minute | Ticks / second | Bytes / second |
|---|---|---|---|
| Pre/post market | 10 – 100 | < 2 | < 280 B |
| Normal trading hours (RTH) | 500 – 3,000 | 8 – 50 | 1 – 7 KB |
| Active session (FOMC, CPI) | 3,000 – 10,000 | 50 – 167 | 7 – 23 KB |
| Extreme volatility spike | up to 30,000 | up to 500 | up to 70 KB |

RTH = Regular Trading Hours: 09:30 – 16:00 ET (14:30 – 21:00 UTC)
NQ futures trade nearly 24/5 (Sunday 18:00 – Friday 17:00 ET)

---

## 3. Processing Pipeline — Latency Budget

```
 Rithmic server
      │  ~1–80 ms  (network)
      ▼
 WebSocket recv (Boost.Beast)
      │  < 1 µs   (in-process callback)
      ▼
 Collector::on_tick()
      │  < 1 µs   (push to std::vector buffer)
      ▼
 [buffer: 200 ticks OR 30 seconds]
      │
      ▼
 TickDB::write()  — PostgreSQL UNNEST batch INSERT
      │  5 – 20 ms  (200 rows, local socket)
      ▼
 ticks hypertable  (TimescaleDB)
      │
      ├── bars_1min  continuous aggregate  (refreshes every 1 min)
      ├── bars_5min  continuous aggregate  (refreshes every 5 min)
      └── bars_15min continuous aggregate  (refreshes every 15 min)
```

| Step | Latency | CPU |
|---|---|---|
| WebSocket frame decode | < 1 µs | Boost.Beast, single-threaded |
| Protobuf parse (LastTrade) | < 5 µs | ~60 byte message |
| Buffer append | < 1 µs | std::vector push_back |
| PostgreSQL batch write (200 rows) | 5 – 20 ms | libpq over Unix socket |
| TimescaleDB aggregate refresh | 50 – 500 ms | Background worker, does not block writes |
| Audit log flush (60s interval) | 1 – 5 ms | Batched, non-blocking |

---

## 4. Data Volume Estimates

All figures for NQ futures at normal RTH activity (~1,500 ticks/min average):

| Period | Raw ticks | PostgreSQL compressed | Uncompressed |
|---|---|---|---|
| 1 day (RTH only, 6.5 h) | ~585,000 | ~6 MB | ~58 MB |
| 1 week | ~2.9 M | ~30 MB | ~290 MB |
| 1 month | ~12.5 M | ~125 MB | ~1.2 GB |
| 1 year | ~150 M | ~1.5 GB | ~15 GB |
| 5 years | ~750 M | ~7.5 GB | ~75 GB |

TimescaleDB compression ratio: ~10:1 for time-series tick data.
Continuous aggregate views add ~5% overhead.

---

## 5. Hardware Requirements

### Minimum (single collector, local bots)
| Resource | Minimum | Recommended |
|---|---|---|
| CPU | 1 core | 2+ cores |
| RAM | 2 GB | 4 GB |
| Disk | 20 GB SSD | 100 GB SSD |
| Network | 10 Mbps | 100 Mbps |

### Oracle Cloud Free Tier ARM VM (what we use)
| Resource | Provided | Usage |
|---|---|---|
| CPU | 4 OCPUs (Ampere A1) | 1 for collector, 3 for PostgreSQL |
| RAM | 24 GB | 18 GB to PostgreSQL, 4 GB OS, 2 GB collector |
| Disk | 100 GB boot volume | ~7 GB for 5 years of compressed ticks |
| Network | 1 Gbps | ~70 KB/s peak tick ingestion |
| Egress | 10 TB/month free | Bot queries from outside Oracle network |

---

## 6. PostgreSQL Tuning (Oracle 24 GB ARM VM)

These settings go in `/etc/postgresql/16/main/postgresql.conf`.
`timescaledb-tune` sets most of these automatically.

```ini
# Memory
shared_buffers          = 6GB        # 25% of RAM
effective_cache_size    = 18GB       # 75% of RAM
work_mem                = 256MB      # per sort/hash operation
maintenance_work_mem    = 2GB        # for VACUUM, index builds

# WAL / Durability
wal_compression         = on         # reduces WAL size ~50%
checkpoint_completion_target = 0.9
max_wal_size            = 4GB

# Connections
max_connections         = 50         # collector + bots + admin
shared_preload_libraries = 'timescaledb'

# TimescaleDB
timescaledb.max_background_workers = 4
```

Apply: `sudo systemctl reload postgresql`

---

## 7. Full Data Flow Diagram

```
┌─────────────────────────────────────────────────────────────────────┐
│                         RITHMIC ENGINE                              │
└─────────────────────────────────────────────────────────────────────┘

  ┌──────────────┐     SSL/WS      ┌──────────────────────────────┐
  │  CME Exchange│ ──── ~1ms ────► │  Rithmic AMP Gateway         │
  └──────────────┘                 │  ritpz01001.01.rithmic.com   │
                                   └──────────────┬───────────────┘
                                                  │ SSL WebSocket
                                           15-80ms│ (Oracle ARM VM)
                                                  │
                          ┌───────────────────────▼───────────────────┐
                          │           Oracle ARM VM                   │
                          │  ┌────────────────────────────────────┐   │
                          │  │  rithmic_engine (C++ binary)       │   │
                          │  │                                    │   │
                          │  │  RithmicClient                     │   │
                          │  │  ├─ TLS handshake (one-time)       │   │
                          │  │  ├─ RequestRithmicSystemInfo (16)  │   │
                          │  │  ├─ RequestLogin (10)              │   │
                          │  │  ├─ RequestHeartbeat (18) / 30s   │   │
                          │  │  └─ subscribe LastTrade (100)      │   │
                          │  │                                    │   │
                          │  │  Collector                         │   │
                          │  │  ├─ on_tick() callback  < 1µs     │   │
                          │  │  ├─ buffer (200 ticks / 30s)      │   │
                          │  │  └─ flush → TickDB::write()       │   │
                          │  │                                    │   │
                          │  │  AuditLog                          │   │
                          │  │  └─ flush every 60s → audit_log   │   │
                          │  └──────────────┬─────────────────────┘   │
                          │                 │ libpq (Unix socket)      │
                          │                 │ 5-20ms per 200-row batch │
                          │  ┌──────────────▼─────────────────────┐   │
                          │  │  PostgreSQL 16 + TimescaleDB       │   │
                          │  │                                    │   │
                          │  │  ticks          (hypertable)       │   │
                          │  │  ├─ partitioned by day             │   │
                          │  │  ├─ compressed after 7 days        │   │
                          │  │  └─ unique index on ts_event       │   │
                          │  │                                    │   │
                          │  │  bars_1min  ┐                      │   │
                          │  │  bars_5min  ├─ continuous aggs     │   │
                          │  │  bars_15min ┘  auto-refresh        │   │
                          │  │                                    │   │
                          │  │  audit_log  (event trail)          │   │
                          │  └──────────────┬─────────────────────┘   │
                          └─────────────────┼─────────────────────────┘
                                            │
                              TCP port 5432 │
                    ┌─────────────────────── ┼────────────────────────┐
                    │                        │                        │
             ┌──────▼──────┐         ┌───────▼──────┐         ┌──────▼──────┐
             │  Python Bot │         │   C++ Bot    │         │  Analytics  │
             │  psycopg2   │         │   libpq      │         │  DBeaver /  │
             │             │         │              │         │  pgAdmin    │
             └─────────────┘         └──────────────┘         └─────────────┘
```

---

## 8. Rithmic Protocol Reference

| Template ID | Message | Direction |
|---|---|---|
| 10 | RequestLogin | Client → Server |
| 11 | ResponseLogin | Server → Client |
| 12 | RequestLogout | Client → Server |
| 13 | ResponseLogout | Server → Client |
| 16 | RequestRithmicSystemInfo | Client → Server |
| 17 | ResponseRithmicSystemInfo | Server → Client |
| 18 | RequestHeartbeat | Client → Server |
| 19 | ResponseHeartbeat | Server → Client |
| 100 | RequestMarketDataUpdate | Client → Server |
| 101 | ResponseMarketDataUpdate | Server → Client |
| 150 | LastTrade | Server → Client (streaming) |

**Wire framing:** `[4-byte big-endian length][protobuf payload]`

**Key field numbers** (all messages share field 154467 = `template_id`):

| Field | Number | Type |
|---|---|---|
| `template_id` | 154467 | int32 |
| `user` | 131003 | string |
| `password` | 130004 | string |
| `system_name` | 153628 | string |
| `heartbeat_interval` | 153633 | double |
| `symbol` | 110100 | string |
| `exchange` | 110101 | string |
| `trade_price` | 100006 | double |
| `trade_size` | 100178 | int32 |
| `aggressor` | 112003 | enum (BUY=1, SELL=2) |
| `ssboe` | 150100 | int32 (seconds since epoch) |
| `usecs` | 150101 | int32 (microseconds) |

**Timestamp reconstruction** (C++):
```cpp
int64_t ts_micros = (int64_t)lt.ssboe() * 1'000'000LL + lt.usecs();
```

---

## 9. Monitoring Queries

```sql
-- Live tick rate (last 5 minutes)
SELECT
    time_bucket('1 minute', ts_event) AS minute,
    COUNT(*)                           AS ticks,
    MIN(price)                         AS low,
    MAX(price)                         AS high
FROM ticks
WHERE ts_event > NOW() - INTERVAL '5 minutes'
GROUP BY 1 ORDER BY 1;

-- Latest price
SELECT price, ts_event FROM ticks ORDER BY ts_event DESC LIMIT 1;

-- DB size breakdown
SELECT
    hypertable_name,
    pg_size_pretty(hypertable_size(format('%I', hypertable_name)::regclass)) AS total,
    pg_size_pretty(hypertable_detailed_size(format('%I', hypertable_name)::regclass) -> 'compressed_size') AS compressed
FROM timescaledb_information.hypertables;

-- Recent audit events
SELECT ts, severity, event, details
FROM audit_log
ORDER BY ts DESC LIMIT 50;

-- Collector uptime (time since last restart event)
SELECT ts, details
FROM audit_log
WHERE event = 'collector.start'
ORDER BY ts DESC LIMIT 1;
```

---

## 10. Quick Reference

```bash
# On Oracle VM — check service
sudo systemctl status rithmic-engine
sudo journalctl -u rithmic-engine -f

# Binary commands
./build/rithmic_engine              # start collector
./build/rithmic_engine --status     # tick count + latest price
./build/rithmic_engine --audit      # last 20 audit events

# Tests
./build/test_db                     # integration tests (needs .env)

# Build
mkdir -p build && cd build && cmake .. -DCMAKE_BUILD_TYPE=Release && make -j4
```
