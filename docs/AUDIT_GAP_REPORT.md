# Audit Gap Report — rithmic_engine
**Date:** 2026-04-30  
**Compiled by:** Reviewer 5 + Coordinator 1 (BridgeSwarm T4)  
**Sources:** T1 (Builder 2 — C++ core), T2 (Builder 3 — execution layer), T3 (Scout 4 — Python/tests/config), Coordinator 1 (cross-cutting)  
**Status:** COMPLETE

---

## 1. Executive Summary

The seven most operationally dangerous gaps, in priority order:

- **`trade_route = "simulator"` is still set in live_config.json.** All live-mode orders are silently routed to Rithmic's paper system — no real fills occur. cross_system.yaml claims this is FIXED but the config file still has the old value. A live session today would paper-trade without any indication.
- **The execution layer has no audit trail.** Orders, fills, risk halts, EOD flattens, and session starts write to LOG() only — zero entries reach the `audit_log` PostgreSQL table. A full trading session is invisible to any compliance or post-mortem audit.
- **News blackout is a stub.** `is_news_blackout()` always returns `false`. The system trades through FOMC, CPI, and other scheduled releases with no restriction. A single news-spike fill can breach the $2,500 trailing drawdown cap.
- **Exit order rejection creates an infinite retry loop.** `on_order_rejected()` re-submits the exit immediately with no counter, no backoff, and no circuit breaker. A rejected-every-time scenario floods ORDER_PLANT indefinitely while the position stays open.
- **`daily_loss_limit` is 10x too tight on C++ side.** `"daily_loss_limit": -200.0` (C++ executor) vs `"prop_firm.daily_loss_limit": 2000.0` (Python/Legends). C++ halts at $200 daily loss while the actual Legends limit is $2000.
- **SQL injection in the audit system itself.** `AuditLog::flush()` concatenates the `event` field raw into a multi-row INSERT. Any caller that allows external data to reach `event` can execute arbitrary SQL against the audit database.
- **Zero unit tests for any execution layer component.** RiskManager, OrderManager, OrbStrategy, LatencyLogger, and OrbDB have no test coverage. There is no way to validate risk math, breakout logic, or stop-loss mechanics without running live.

---

## 2. CRITICAL Findings

| ID | File | Line | Description |
|----|------|------|-------------|
| C1 | `src/execution/orb_strategy.hpp` | 209–214 | News blackout stub — always returns false |
| C2 | `src/execution/order_manager.hpp` | 401–408 | Exit rejection infinite retry loop |
| C3 | All execution layer files | — | Zero unit tests for execution layer |
| C4 | `config/live_config.json` | 11 | `trade_route = "simulator"` — live mode routes to paper despite FIXED claim |
| C5 | `live_trader.py` | `_load_config()` | Config never validated via Pydantic schema at startup |
| C6 | `go_live.py` | promotion gates | No gate blocking `trade_route == "simulator"` from promotion |
| C7 | `live_trader.py` | `_submit_order()` | dry_run guard is runtime-only NotImplementedError — not architecturally enforced |

### C1 — `is_news_blackout()` permanently stubbed
`is_news_blackout()` always returns `false`. The `news_blackout_min` config field exists but is never read. The system trades through CPI (8:30 ET), FOMC (14:00 ET), and other scheduled releases without restriction.  
**Fix:** Implement a hardcoded schedule for common news times. Block entries for `news_blackout_min` minutes either side. Wire up the existing `news_blackout_min` config field.

### C2 — Exit order rejection has no retry limit
When an exit order is rejected, `on_order_rejected()` immediately calls `initiate_exit_locked("rejected_exit_retry", 0.0)`. No retry counter, no backoff, no circuit breaker. If the exchange rejects every exit (account suspended, market closed), this floods ORDER_PLANT indefinitely while the position stays open.  
**Fix:** Add `int rejected_exit_retries_` counter. After 3 rejections stop retrying, emit CRITICAL log, halt new entries. Require manual intervention.

### C3 — Zero unit tests for execution layer
No test coverage exists for: `RiskManager`, `OrderManager`, `OrbStrategy`, `LatencyLogger`, `OrbDB`. ~30+ critical scenarios have never been tested.  
**Fix:** Create `tests/execution/`. Minimum coverage: trailing DD cap, consistency cap edge cases, daily reset (RiskManager); state machine transitions, stop placement (OrderManager); breakout detection, EOD flatten, news blackout (OrbStrategy).

### C4 — `trade_route = "simulator"` in live_config.json (T3-C1)
`"trade_route": "simulator"` routes all orders to Rithmic's paper system even when `dry_run=false`. `cross_system.yaml` BUG-14 claims this is FIXED on 2026-04-28 but the config still has the old value. A live session today would silently paper-trade with no indication.  
**Fix:** Change to `"Rithmic Order Routing"`. Add go_live.py gate K: `if cfg.get("trade_route") == "simulator": fail`.

### C5 — live_trader.py never validates config with Pydantic (T3-C2)
`_load_config()` uses `json.load()` only. `config/live_config_schema.py` (Pydantic v2) exists but is never imported or called in `live_trader.py`. Invalid config (wrong `point_value`, missing `prop_firm`, `trade_route='simulator'`) passes undetected at startup — validation only happens in `go_live.py` at promotion time.  
**Fix:** Add `from config.live_config_schema import LiveConfig; cfg = LiveConfig.model_validate(json.load(f)).model_dump()` in `_load_config()`.

### C6 — go_live.py has no gate for `trade_route` (T3-C3)
No promotion gate checks `trade_route != "simulator"`. Gate B validates JSON loads but does not invoke Pydantic validators. A config with `trade_route='simulator'` passes all 10 gates.  
**Fix:** Add gate K to `go_live.py`: block promotion when `trade_route == "simulator"`.

### C7 — dry_run bypass via runtime NotImplementedError (T3-C4)
The dry_run guard raises `NotImplementedError` in the live path — a single line deletion removes the protection. There is no architectural separation between paper and live order paths, and no test verifies `SystemExit` is raised with `dry_run=False`.  
**Fix:** Replace `raise NotImplementedError` with `sys.exit(1)` + CRITICAL log. Add `test_dry_run_false_exits_before_order()` asserting SystemExit.

---

## 3. HIGH Findings

### 3a — Security

| ID | File | Line | Description |
|----|------|------|-------------|
| H-SEC-1 | `src/audit.cpp` | 80–83 | SQL injection — `event` and `severity` fields not escaped in `flush()` |
| H-SEC-2 | `src/config.hpp`, `src/main.cpp` | 35–43, 44 | PostgreSQL password printed to stdout via `--status` |
| H-SEC-3 | `src/config.hpp` | 35–43 | libpq keyword=value connection string injection via password field |
| H-SEC-4 | `src/execution/executor_main.cpp` | 527, 633 | Rithmic username logged in plaintext |
| H-SEC-5 | `src/execution/executor_main.cpp` | 1411–1436 | `NQ_FIRE_TEST_ORDER` env var fires live market orders in production |

**H-SEC-1:** `AuditLog::flush()` builds a multi-row INSERT by concatenating `batch[i].event` and `batch[i].severity` raw. Only `details` has an escape lambda. Any caller passing `O''Brien'); DROP TABLE audit_log;--` as the event name executes arbitrary SQL.  
**Fix:** Use `PQexecParams` with `$1..$N` placeholders for all columns. Eliminates the need for the manual `esc_details` lambda too.

**H-SEC-2:** `pg_connstr()` stores the plaintext password in `DBSummary::connstr`, which is printed by `std::printf("DB: %s\n", s.connstr.c_str())` in `--status` mode.  
**Fix:** Strip or redact the password from `DBSummary::connstr` before storing.

**H-SEC-3:** `pg_connstr()` uses unquoted keyword=value format. A password containing a space followed by a keyword (e.g., `secret dbname=postgres`) allows connecting to an unintended database.  
**Fix:** Use `PQconnectdbParams()` with separate keyword/value arrays — values are never parsed for special characters.

**H-SEC-4:** MD plant login logs include the Rithmic username in plaintext (`executor_main.cpp:527, 633`). Log files are typically world-readable on Linux.  
**Fix:** Redact usernames from log lines, or use a PII-safe logging level.

**H-SEC-5:** If `NQ_FIRE_TEST_ORDER` is set, the executor fires real market orders regardless of config, calling `send_new_order(..., false)` hardcoded. This test hook can accidentally fire in production if the env var leaks from a dev shell.  
**Fix:** Guard with an explicit `dry_run == false` assertion and a startup CRITICAL banner. Better: remove the hook and use a proper integration test harness.

---

### 3b — Audit Trail Gaps

| ID | File | Line | Description |
|----|------|------|-------------|
| H-AUD-1 | All execution layer files | — | Execution layer has zero `AuditLog` calls — orders/fills/halts invisible in audit_log |
| H-AUD-2 | `src/execution/risk_manager.hpp` | 145–149 | Risk halt events not written to audit_log (only LOG) |
| H-AUD-3 | (no file) | — | No integration test verifying end-to-end audit trail |

**H-AUD-1:** `grep AuditLog src/execution/executor_main.cpp` returns nothing. All execution events (order submissions, fills, risk halts, EOD flattens, session starts) write to LOG() only. A compliance audit of `audit_log` shows zero trading activity.  
**Fix:** Pass an `AuditLog*` into `OrbStrategy`/`Executor`. Log at minimum: `order.submitted`, `order.filled`, `order.cancelled`, `order.rejected`, `risk.halted`, `risk.reset`, `strategy.signal`, `session.started`.

**H-AUD-2:** `halt()` writes to LOG() only. `live_sessions.halt_reason` is updated by the eod_loop ~1 second later — a process crash in that window loses the halt permanently. No `halt_time` timestamp is recorded.  
**Fix:** Add `db->write_halt_event(reason, now_ns())` immediately inside `halt()`. Add `halt_timestamp` column to `live_sessions`.

**H-AUD-3:** No test drives a full trade cycle through the stack and asserts `audit_log` entries are present in PostgreSQL. `TEST(audit_log)` in `test_db.cpp` tests `AuditLog` in isolation. `test_live_trader.py` mocks the DB entirely.  
**Fix:** Add an integration test (requires test DB) that drives `OrbStrategy + Executor` in dry-run and asserts `audit_log` contains `order.submitted`, `order.filled`.

---

### 3b-2 — Python / Config (T3 Findings)

| ID | File | Line | Description |
|----|------|------|-------------|
| H-PY-1 | `live_trader.py` | ~143–174 | `_reconcile_position()` never queries `live_trades` (C++ executor table) |
| H-PY-2 | `live_trader.py` | ~202–214 | Order submitted before DB write — DB failure leaves orphaned Rithmic position |
| H-PY-3 | `live_trader.py` | ~228 | Commission $4.00 hardcoded — not enforced to match `formula_audit.yaml` |
| H-PY-4 | `live_trader.py` | `_write_trade_close()` | No PnL sanity check — 10x PnL error passes silently on misconfigured point_value |
| H-PY-5 | `config/live_config.json` | — | `daily_loss_limit` 10x magnitude mismatch: C++ uses -200.0, Legends limit is $2000 |

**H-PY-1:** On restart, `_reconcile_position()` queries only `trades WHERE source='python'`. If C++ executor crashed with an open position, Python starts fresh and enters a second position while Rithmic still holds the C++ fill — two positions against a one-contract Legends limit.  
**Fix:** Query both `trades` and `live_trades`; merge results before setting startup state.

**H-PY-2:** `_submit_order()` (Rithmic submit) is called before `_write_trade_open()` (DB record). If the DB write fails, the position is open in Rithmic but absent from the DB. On restart, reconciliation finds no open trade and Python enters a second position.  
**Fix:** Write DB record first (status=`pending`), submit to Rithmic, then update to `open`.

**H-PY-3:** `commission_rt = 4.0` is hardcoded in `_write_trade_close()`. No CI test verifies this matches `formula_audit.yaml`. If commission changes (Legends rebate adjustment), PnL calculations drift silently.  
**Fix:** Add `"commission_rt": 4.0` to `live_config.json`; read from config. Add test asserting `commission_rt` matches `formula_audit.yaml` constant.

**H-PY-4:** PnL is calculated and written to DB without any bounds check. `escalation.yaml` defines a `pnl_sanity_check` rule but it is not enforced in code. A 10x PnL error (misconfigured `point_value`) passes silently.  
**Fix:** Before DB write: `if abs(pnl_usd) > 500: log.critical(...); sys.exit(1)`.

**H-PY-5:** `"daily_loss_limit": -200.0` (flat key read by C++ executor) vs `"prop_firm.daily_loss_limit": 2000.0` (Python). C++ executor halts 10x too early. `cross_system.yaml` BUG-7 documents sign convention but not this magnitude discrepancy.  
**Fix:** Align both to the same magnitude. Add CI test: `assert abs(live_config["daily_loss_limit"]) == live_config["prop_firm"]["daily_loss_limit"]`.

---

### 3c — Reliability / Correctness

| ID | File | Line | Description |
|----|------|------|-------------|
| H-REL-1 | `src/execution/executor_main.cpp` | 540–566 | Infinite hang on MD plant login — no timeout |
| H-REL-2 | `src/execution/executor_main.cpp` | 998–1003 | No cancel-on-shutdown for pending entry orders |
| H-REL-3 | `src/execution/executor_main.cpp` | 397–406 | Process crash mid-order: no position recovery mechanism |
| H-REL-4 | `src/execution/orb_strategy_main.cpp` | — | Standalone binary diverges from live execution strategy |
| H-REL-5 | `src/execution/latency_logger.hpp` | 63–68 | Single pending record corrupts stop-order latency |
| H-REL-6 | `src/execution/sdk_md_feed.hpp` | 271–289 | `SdkMdFeed` leaks `RApi::REngine` on exception |

**H-REL-1:** MD plant login is a bare `for(;;)` around `async_read` with no `expires_after()` timer. If the server connects but never sends template_id=11, the coroutine hangs forever. Same issue in ORDER_PLANT login (lines 640–657).  
**Fix:** Apply `beast::get_lowest_layer(*md_ws).expires_after(std::chrono::seconds(15))` before the login read loop.

**H-REL-2:** Signal handler sets `g_flatten_requested=true`. The eod_loop calls `flatten_now()` on the next 1-second tick. If the process exits before the next tick (e.g., double SIGINT), the cancel is never sent. An unmanaged PENDING_ENTRY can fill after session end on Legends.  
**Fix:** Call `order_mgr.flatten_now("shutdown")` synchronously via `asio::post()` in the signal handling path rather than deferring to the eod_loop flag.

**H-REL-3:** If the process is SIGKILL'd mid-trade, `carried_pos` is never written. On next restart, `carried_pos = FLAT` — the snapshot reconciliation (lines 845–863) only catches WORKING orders still on exchange, not already-filled positions.  
**Fix:** On startup, read `live_position` for today; if state != FLAT, halt and warn before accepting signals.

**H-REL-4:** `orb_strategy_main.cpp` uses bar-close prices for breakout detection vs. tick prices in live; different SL formula; no session time filtering; no max_daily_trades; different config structure. Results from this binary cannot validate live execution behavior.  
**Fix:** Rewrite to use the same `OrbStrategy` class, or explicitly document as a legacy tool and exclude from parity comparisons.

**H-REL-5:** `LatencyLogger::pending_` holds exactly one record. Calling `on_signal(stop_basket, ...)` then `on_submit(stop_basket, ...)` immediately after entry fill overwrites the entry's pending record before `on_fill` is called. Entry fill latency is permanently lost.  
**Fix:** Change `pending_` to `std::unordered_map<std::string, TradeLatency>` keyed by basket_id.

**H-REL-6:** `engine_ = new RApi::REngine(...)` then `engine_->login(...)` may fail; `start()` returns false but does not `delete engine_`. If `SdkMdFeed` is destroyed before `stop()`, the engine leaks.  
**Fix:** Wrap `engine_` in `std::unique_ptr<RApi::REngine, REngineDeleter>` with a custom deleter.

---

## 4. MEDIUM Findings

### 4a — Input Validation & Safety

| ID | File | Line | Description |
|----|------|------|-------------|
| M-VAL-1 | `src/execution/risk_manager.hpp` | 56 | `on_trade_pnl` does not validate NaN/Inf — silently disables all risk checks |
| M-VAL-2 | `src/execution/sdk_md_feed.hpp` | 102–123 | SDK tick data: NaN price passes `> 0.0` guard and corrupts ORB state |
| M-VAL-3 | `src/log.hpp` | 16–25 | Log injection — control characters not sanitized from wire data |
| M-VAL-4 | `src/wal.hpp` | 47–62 | WAL symbol/exchange unquoted — comma in symbol corrupts CSV replay |

**M-VAL-1:** If `pnl_usd = NaN`, all risk state variables become NaN. All risk comparisons (`equity_ > peak_equity_`) evaluate to false, effectively disabling all risk checks.  
**Fix:** `if (!std::isfinite(pnl_usd)) { halt("non_finite_pnl: " + std::to_string(pnl_usd)); return; }` at top of `on_trade_pnl`.

**M-VAL-2:** `TradePrint()` checks `dPrice <= 0.0` — this evaluates to false for NaN, so NaN ticks pass the filter. NaN propagates to `orb_high`/`orb_low` and P&L calculations.  
**Fix:** Add `|| !std::isfinite(pInfo->dPrice)` to the filter condition.

**M-VAL-3:** `Logger::write()` writes message strings to stdout and file without stripping `\n`/`\r`. A compromised upstream server can inject fake log lines via wire messages (e.g., error codes containing newlines).  
**Fix:** Sanitize control characters in `Logger::write()` — replace `c < 0x20 || c == 0x7f` with `'?'`.

**M-VAL-4:** `TickValidator::valid()` allows commas (0x2C) in symbol/exchange. A symbol like `NQ,CME` writes a malformed WAL line that corrupts replay parsing.  
**Fix:** Tighten character allowlist to `[A-Z0-9]` for symbol, `[A-Z0-9_]` for exchange. Covered by F-13/LOW too, but WAL corruption risk elevates this.

---

### 4b — Data Integrity & Consistency

| ID | File | Line | Description |
|----|------|------|-------------|
| M-INT-1 | `src/execution/orb_db.hpp` | 52–187 | `write_trade` and `upsert_session` not transactional |
| M-INT-2 | `src/execution/executor_main.cpp` | 1043–1065 | DB failure during order execution: trade record permanently lost |
| M-INT-3 | `src/execution/order_manager.hpp` | 118–135 | No order deduplication — duplicate fill delivery inserts two trade rows |
| M-INT-4 | `src/wal.hpp` | 40–83 | WAL has no integrity check (no CRC/hash) — silent corruption undetected |
| M-INT-5 | `src/collector.cpp` | 182–238 | BBO and depth data have no WAL protection — lost on crash |

**M-INT-1:** After a trade closes, `write_trade` and `upsert_session` are called independently ~1 second apart. A crash between the two leaves a trade row without a session stats update.  
**Fix:** Wrap both calls in a `BEGIN`/`COMMIT` transaction block.

**M-INT-2:** When `write_trade()` fails, the error is logged and reconnect attempted, but the trade is not retried — the P&L record is permanently lost. In-memory risk state is already updated, so accounting and risk diverge.  
**Fix:** Implement a write-ahead buffer: store failed Position snapshots locally and retry on reconnect.

**M-INT-3:** No UNIQUE constraint on `live_trades(basket_id_entry)`. If exchange delivers a fill notification twice (duplicate delivery), two rows are inserted and total P&L is doubled.  
**Fix:** Add UNIQUE constraint on `basket_id_entry`. Record processed basket IDs in-memory and ignore duplicates.

**M-INT-4:** WAL is plain CSV with no per-entry checksum. Partial disk writes or bit flips produce valid-looking rows with wrong values that are silently replayed to DB.  
**Fix:** Append CRC-32 to each WAL line; verify on replay and discard failing lines.

**M-INT-5:** Tick data is WAL-protected. BBO (`flush_bbo()`) and depth (`flush_depth()`) are written directly to DB with no pre-write WAL. A crash during flush loses BBO/depth data permanently.  
**Fix:** Extend WAL to support BBO/depth rows, or document the loss risk explicitly.

---

### 4c — Audit Trail Gaps (Medium)

| ID | File | Line | Description |
|----|------|------|-------------|
| M-AUD-1 | `src/collector.cpp` | 242–254 | `DataSentinel` alerts go to `sentinel_alerts` table, NOT `audit_log` |
| M-AUD-2 | `src/audit.cpp` | 76–93 | `AuditLog` buffer unbounded on DB failure — OOM risk under prolonged outage |
| M-AUD-3 | `src/collector.cpp`, `src/client.cpp` | — | 8 operationally significant events not written to `audit_log` |

**M-AUD-1:** `flush_sentinel()` drains `DataSentinel` alerts to `sentinel_alerts` only — invisible to `audit_data.py` and `audit_engine.py`. An operator reviewing `audit_log` cannot see that 47 price-jump alerts fired during a session.  
**Fix:** Cross-write CRITICAL/ERROR severity sentinel alerts to `audit_log`, or update `audit_engine.py` to include sentinel alert counts in its audit report.

**M-AUD-2:** On flush failure, events are re-queued via `buf_.insert(buf_.begin(), batch.begin(), batch.end())`. Under a prolonged DB outage, this buffer grows without bound and can OOM the process.  
**Fix:** Add `max_buf_size_` limit (~10,000 events). When exceeded, drop oldest events and emit a WARN log.

**M-AUD-3:** Missing audit events in collector/client layer (see Section 7 for full list).

---

### 4d — Configuration & Portability

| ID | File | Line | Description |
|----|------|------|-------------|
| M-CFG-1 | `src/execution/orb_config.hpp` | 241–275 | Hand-rolled JSON parser: can't handle duplicate keys, substring key collisions |
| M-CFG-2 | `src/execution/orb_config.hpp` | 217–225 | `dry_run` parsing fragile (covered by M-CFG-1) |
| M-CFG-3 | `src/execution/executor_main.cpp` | 470 | SSL certificate path is relative — fails if launched from different directory |
| M-CFG-4 | `src/execution/executor_main.cpp` | 169 | `__builtin_bswap32` is GCC/Clang-only — not portable |
| M-CFG-5 | `src/execution/sdk_md_feed.hpp` | 262–281 | `const_cast` of `cfg_` string members — SDK may corrupt live config strings |

**M-CFG-1:** `json_str`/`json_int`/`json_dbl` do substring search for `"key"`. Cannot handle duplicate keys, keys that are substrings of other keys, multi-line values. `nlohmann/json` is already used in `orb_strategy_main.cpp`.  
**Fix:** Replace hand-rolled parser with `nlohmann/json`. Covers M-CFG-2 as well.

**M-CFG-3:** `ssl_ctx.load_verify_file("certs/rithmic_ssl_cert_auth_params")` requires the process CWD to contain `certs/`. Systemd/supervisor launches from `/` by default.  
**Fix:** Resolve cert path relative to config file directory, or add `ssl_cert_path` field to `OrbConfig`.

---

### 4e — Tests with Wrong Assumptions

| ID | File | Line | Description |
|----|------|------|-------------|
| M-TEST-1 | `tests/test_live_trader.py` | 47–50 | `_make_config()` uses `point_value=20.0` (NQ) — should be 2.0 (MNQ) |
| M-TEST-2 | `tests/test_db.cpp` | 159–180 | `TEST(audit_log)` does not test SQL metacharacters or concurrent access |
| M-TEST-3 | `tests/audit_engine.py` | 178–179 | Source invariant check does not verify executor/risk_manager have audit calls |
| M-TEST-4 | `src/execution/orb_db.hpp` | 360–378 | `get_total_pnl()` includes test/dry-run trades — corrupts consistency cap baseline |

**M-TEST-1:** All tests using `_make_config()` exercise NQ parameters (point_value=20.0), not MNQ (2.0). PnL assertions are off by 10x versus production behavior.  
**Fix:** Change defaults to `point_value: 2.0, symbol: "MNQ"`. Add a test verifying the helper uses correct MNQ values.

**M-TEST-2:** `TEST(audit_log)` writes three clean events and verifies row count. It does not test event strings with SQL metacharacters, concurrent log()/flush() calls, or DB mid-flush disconnection.  
**Fix:** Add `TEST(audit_log_sql_metacharacters)` and `TEST(audit_log_concurrent)`.

**M-TEST-3:** `audit_engine.py` source invariant check only verifies `audit_log` appears in `dashboard.cpp`. Does not check `executor_main.cpp`, `orb_strategy.cpp`, or `risk_manager.hpp`.  
**Fix:** Add source invariant checks for execution layer files in Section 2 of `audit_engine.py`.

**M-TEST-4:** `get_total_pnl()` sums all `live_trades` rows. Test/dry-run trades written to the table permanently distort the P&L baseline used to seed `RiskManager.total_profit_` and the consistency cap.  
**Fix:** Add `is_test` boolean column to `live_trades`; exclude `is_test=true` from P&L sum.

---

### 4f — Startup / Schema

| ID | File | Line | Description |
|----|------|------|-------------|
| M-OPS-1 | `src/execution/orb_db.hpp` | 423–515 | `ensure_schema()` runs DDL on every startup — acquires schema locks on production DB |
| M-OPS-2 | `src/main.cpp` | 60–61 | `cmd_audit()` `read_only=true` flag is misleading — connection is actually read-write |

**M-OPS-1:** `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` runs synchronously at startup and can delay production startup by seconds on a loaded DB.  
**Fix:** Run migrations out-of-band; add a schema version check to skip DDL if version matches.

**M-OPS-2:** `TickDB db(cfg.pg_connstr(), /*read_only=*/true)` only prevents `ensure_schema()` from running. The underlying libpq connection is full read-write.  
**Fix:** Connect with a PostgreSQL role that has only SELECT privileges, or append `options=-c default_transaction_read_only=on` to the connection string.

---

## 5. LOW Findings

| ID | File | Line | Description |
|----|------|------|-------------|
| L-01 | `src/validator.hpp` | 215–216 | `DataSentinel::emit()` uses `operator[]` — unintended map insertion |
| L-02 | `src/config.hpp` | 65–71 | Whitespace-only password passes `Config::validate()` |
| L-03 | `src/validator.hpp` | 41–49 | `TickValidator` allows comma/space in symbol — too permissive |
| L-04 | `src/dashboard.cpp` | 444 | Dashboard pipeline does not subscribe BBO/depth callbacks — silently discards |
| L-05 | `src/log.hpp` | 38 | `std::localtime()` not thread-safe — use `localtime_r()` |
| L-06 | `src/execution/risk_manager.hpp` | 142 | `halted()` public accessor reads atomic without mutex — inconsistent with `halt_reason_` |
| L-07 | `src/execution/order_manager.hpp` | 180–185 | Partial fill logs WARN but stores `pos_.qty` not `fill_qty` — wrong if qty > 1 |
| L-08 | `src/execution/orb_config.hpp` | — | No bounds validation on config params (point_value, qty, DD cap, etc.) |
| L-09 | `src/execution/orb_config.hpp` | 127–136 | `pg_connstr()` includes password in plain string — risk if ever logged |
| L-10 | `src/execution/orb_db.hpp` | 414–419 | `notify_tick()` uses `PQexec` + format string — only non-parameterized runtime query |
| L-11 | `src/execution/orb_strategy.hpp` | 58–59 | `long_taken`/`short_taken` never cleared intra-day — intentional but undocumented |
| L-12 | `src/execution/executor_main.cpp` | 1543 | Signal handler omits `ioc_ref.stop()` deliberately — absence of stop() is undocumented |
| L-13 | `src/execution/orb_db.hpp` | 579 | `strategy_` hardcoded as `"ORB"` — not forward-compatible with multiple strategies |
| L-14 | `src/execution/risk_manager.hpp` | 87–97 | Consistency cap first-day edge case (prior_profit=0) correct but untested |
| L-15 | `quality_rules/` | — | Quality rules may not cover execution audit event presence |

---

## 6. Missing Tests — Top 10 Priority List

| # | Test | Category | Urgency |
|---|------|----------|---------|
| 1 | `RiskManager`: trailing drawdown cap fires at correct threshold | Risk correctness | IMMEDIATE |
| 2 | `RiskManager`: `on_trade_pnl(NaN)` triggers halt | Safety guard | IMMEDIATE |
| 3 | `OrderManager`: exit rejection retry counter stops at 3, halts entries | Reliability | IMMEDIATE |
| 4 | Integration: full dry-run trade cycle produces `audit_log` entries in DB | Audit trail | HIGH |
| 5 | `OrbStrategy`: `is_news_blackout()` blocks entry during configured windows | News risk | HIGH |
| 6 | `OrbStrategy`: breakout detection and EOD flatten timing | Strategy correctness | HIGH |
| 7 | `AuditLog`: event string with SQL metacharacters (`'; DROP TABLE...`) survives flush | SQL injection | HIGH |
| 8 | `RiskManager`: consistency cap first-day edge case (prior_profit=0 → no halt) | Risk edge case | MEDIUM |
| 9 | `test_live_trader.py`: fix MNQ constants (`point_value=2.0`, `symbol="MNQ"`) | Test correctness | MEDIUM |
| 10 | `OrbDB`: `get_total_pnl()` excludes `is_test=true` rows | Data integrity | MEDIUM |

---

## 7. Missing Audit Events

Events that should flow to the `audit_log` PostgreSQL table but currently do not:

### Execution Layer (currently LOG() only)

| Missing Event | Source Location | Severity |
|---|---|---|
| `order.submitted` | `order_manager.hpp:send_market_order()` | INFO |
| `order.filled` | `order_manager.hpp:on_fill_notification_locked()` | INFO |
| `order.cancelled` | `order_manager.hpp:cancel_stop_locked()` | INFO |
| `order.rejected` | `order_manager.hpp:on_order_rejected()` | WARN |
| `risk.halted` | `risk_manager.hpp:halt()` | CRITICAL |
| `risk.daily_reset` | `risk_manager.hpp:reset_daily()` | INFO |
| `session.started` | `executor_main.cpp:951–957` | INFO |
| `executor.reconnect` | `executor_main.cpp:1242–1318` | WARN |
| `executor.open_pos_at_startup` | `executor_main.cpp:845–863` | CRITICAL |

### Collector / Client Layer (currently LOG() only)

| Missing Event | Source Location | Severity |
|---|---|---|
| `connection.established` | `client.cpp:162` | INFO |
| `auth.failed` | `client.cpp:157` | ERROR |
| `connection.reconnect_attempt` | `client.cpp:573` | WARN |
| `tick.rejected` | `collector.cpp:63–68` | WARN |
| `db.reconnect.success` | `collector.cpp:89–93` | INFO |
| `db.reconnect.failed` | `collector.cpp:89–93` | ERROR |
| `wal.recovery_started` | `collector.cpp:31` | WARN |
| `wal.size_threshold_exceeded` | *(not implemented)* | CRITICAL |

### Events Currently Present in `audit_log`

`collector.start`, `collector.stop`, `wal.replay`, `ticks.written`, `ticks.write_error`, `bbo.write_error`, `depth.write_error`, `connection.lost`

---

## 8. Recommended Next Steps

### Immediate (before next live session)

1. **Fix `trade_route` in live_config.json** — C4. Change `"simulator"` → `"Rithmic Order Routing"`.
2. **Add go_live.py gate K for trade_route** — C6. Block promotion when `trade_route == "simulator"`.
3. **Add exit rejection retry limit** — C2. Max 3 retries, then halt entries.
4. **Fix `daily_loss_limit` magnitude** — H-PY-5. Align C++ flat key to `abs(prop_firm.daily_loss_limit) = 2000`.
5. **Implement `is_news_blackout()`** — C1. Hardcode 8:30/14:00 ET windows. Wire `news_blackout_min` config field.
6. **Fix SQL injection in `AuditLog::flush()`** — H-SEC-1. Migrate to `PQexecParams` with `$1..$N` for all columns.
7. **Redact PostgreSQL password from `--status` output** — H-SEC-2. Strip or mask password in `DBSummary::connstr`.
8. **Add Pydantic validation to `live_trader.py` startup** — C5. Call `LiveConfig.model_validate()` in `_load_config()`.
9. **Add NaN guard in `on_trade_pnl()`** — M-VAL-1. Single line at top of function.
10. **Add login timeout on MD/ORDER plants** — H-REL-1. `expires_after(15s)` before login read loop.

### Short Term (within 1 week)

7. **Integrate `AuditLog` into execution layer** — H-AUD-1. Pass `AuditLog*` to Executor/OrbStrategy. Log `order.submitted`, `order.filled`, `risk.halted` at minimum.
8. **Fix `test_live_trader.py` MNQ constants** — M-TEST-1. One-line fix; all PnL assertions are currently 10x wrong.
9. **Remove or gate `NQ_FIRE_TEST_ORDER` hook** — H-SEC-5. Add startup CRITICAL banner or remove entirely.
10. **Fix `LatencyLogger` to track multiple baskets** — H-REL-5. Change `pending_` to `unordered_map`.
11. **Fix process crash mid-order recovery** — H-REL-3. Read `live_position` on startup; halt if non-FLAT.
12. **Write unit tests for `RiskManager` (items 1–3 from Section 6)** — C3 partial.

### Medium Term (within 1 month)

13. **Write integration test for end-to-end audit trail** — H-AUD-3. Requires test DB.
14. **Migrate `OrbConfig` JSON parser to `nlohmann/json`** — M-CFG-1. Already a transitive dependency.
15. **Transactional `write_trade` + `upsert_session`** — M-INT-1. Wrap in `BEGIN`/`COMMIT`.
16. **Fix `orb_strategy_main.cpp` parity** — H-REL-4. Rewrite to use live `OrbStrategy` class.
17. **Add `AuditLog` SQL metacharacter test** — M-TEST-2.
18. **Cross-write `DataSentinel` CRITICAL alerts to `audit_log`** — M-AUD-1.

### Long Term

19. **Add WAL integrity checksums** — M-INT-4.
20. **WAL protection for BBO/depth data** — M-INT-5.
21. **Review `quality_rules/` for execution audit coverage** — L-15.

---

## Appendix — Finding Count Summary

| Source | CRITICAL | HIGH | MEDIUM | LOW | Total |
|--------|---------|------|--------|-----|-------|
| T1 — C++ Core (Builder 2) | 0 | 3 | 7 | 5 | 15 |
| T2 — Execution Layer (Builder 3) | 3 | 12\* | 12\* | 12\* | 39 |
| T3 — Python/Tests/Config (Scout 4) | 4 | 10 | 9 | 4 | 27 |
| Coordinator 1 — Cross-cutting | 0 | 3 | 5 | 1 | 9 |
| **Total (deduplicated)** | **7** | **~18** | **~24** | **~15** | **~64** |

\* T2 count includes both `execution_layer_audit.md` and `audit_gap_T2.md` structured findings, with deduplication applied to overlapping entries.

> **Note:** Findings from T1 (H-SEC-1) and Coordinator 1 identifying the same SQL injection vulnerability in `src/audit.cpp` have been merged into a single finding (H-SEC-1). Similarly, the missing execution audit trail finding appears in both T2 and Coordinator — merged into H-AUD-1/H-AUD-2. T3 CRITICAL findings C4-C7 (trade_route, Pydantic, go_live gate, dry_run bypass) are incorporated in Section 2 above.
