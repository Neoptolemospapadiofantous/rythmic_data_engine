# Execution Layer Audit — T2 Findings
**Date:** 2026-04-30  
**Auditor:** Builder 3 (BridgeSwarm)  
**Scope:** `src/execution/` + `src/orb_strategy_main.cpp`  
**Files reviewed:** risk_manager.hpp/.cpp, order_manager.hpp, executor_main.cpp, orb_strategy.hpp, orb_config.hpp, orb_db.hpp, latency_logger.hpp/.cpp, sdk_md_feed.hpp, orb_strategy_main.cpp

---

## Summary

| Severity | Count |
|----------|-------|
| CRITICAL | 3 |
| HIGH     | 7 |
| MEDIUM   | 9 |
| LOW/INFO | 8 |

---

## CRITICAL Issues

### C1 — News blackout is a stub (orb_strategy.hpp:209-214)
`is_news_blackout()` always returns `false`. The function body says _"stub — real impl would check a schedule"_. The system trades through CPI, FOMC, and other high-impact news releases without any restriction. This is the most operationally dangerous gap: a violent news spike can breach the trailing drawdown cap in a single fill.

**File:** `src/execution/orb_strategy.hpp`, line 209  
**Fix:** Implement a news schedule check. At minimum, block entries 5 minutes either side of 8:30 ET, 10:00 ET, and 14:00 ET on FOMC days. Wire up the `news_blackout_min` config field which already exists but is unused.

---

### C2 — Exit order rejection retry has no backoff or limit (order_manager.hpp:401-408)
When an exit order is rejected by the exchange, `on_order_rejected()` immediately calls `initiate_exit_locked("rejected_exit_retry", 0.0)` which re-submits the exit. There is no retry counter, no backoff, and no circuit breaker. If the exchange rejects every exit attempt (e.g., session halted, invalid account state), this creates a tight rejection→retry→rejection loop, flooding the ORDER_PLANT with orders. The position stays open and cannot be flattened programmatically.

**File:** `src/execution/order_manager.hpp`, line 401-408  
**Fix:** Add a `rejected_exit_count_` counter; after 3 rejections, stop retrying and emit a CRITICAL log + halt new entries. Require manual intervention.

---

### C3 — No unit tests for any execution layer component
Zero test coverage exists for: `RiskManager`, `OrderManager`, `OrbStrategy`, `LatencyLogger`, `OrbDB`. These components execute financial calculations and order management logic with real money. There is no way to validate correctness of risk math (trailing drawdown, consistency cap), breakout signal generation, or stop-loss mechanics without running live or in dry-run mode.

**Affected files:** All execution layer files  
**Fix:** Create `tests/execution/` with at minimum:
- RiskManager: trailing DD cap, consistency cap edge cases, daily reset
- OrbStrategy: breakout detection, EOD flatten timing, ORB range locking
- OrderManager: state machine transitions, stop placement, EOD cancel race

---

## HIGH Issues

### H1 — Credentials logged in plaintext (executor_main.cpp:527, 633)
The login logs include the Rithmic username in plaintext. Log files are typically world-readable on Linux. While passwords are not logged, usernames expose account identifiers to anyone with log file access.

**File:** `src/execution/executor_main.cpp`, lines 527, 633  
**Fix:** Redact usernames from log lines or use a PII-safe logging level.

---

### H2 — Infinite hang on MD plant login with no timeout (executor_main.cpp:540-566)
The MD plant login loop is a bare `for (;;)` around `async_read`. If the server is connected but never sends template_id=11, the coroutine hangs forever. No `expires_after()` timer is set on the stream during login.

**File:** `src/execution/executor_main.cpp`, lines 540-566  
**Fix:** Apply `beast::get_lowest_layer(*md_ws).expires_after(std::chrono::seconds(15))` before the login read loop. Same fix needed for ORDER_PLANT login (lines 640-657).

---

### H3 — `NQ_FIRE_TEST_ORDER` env var fires live orders in production (executor_main.cpp:1411-1436)
If `NQ_FIRE_TEST_ORDER` is set in the environment, the executor fires real market orders regardless of config. This is a hidden test hook that can accidentally execute in production if the env var leaks from a dev shell. The orders bypass the normal dry_run=false path and call `order_plant->send_new_order(..., false)` hardcoded.

**File:** `src/execution/executor_main.cpp`, lines 1411-1436  
**Fix:** Guard with an explicit `dry_run == false` assertion. Log a CRITICAL warning visible in the startup banner. Better: remove the hook and use a proper integration test harness.

---

### H4 — Risk halt has no persistent DB record (risk_manager.hpp:145-149)
When `halt()` is called, it only logs to stdout/file. The halt reason is eventually written to `live_sessions.halt_reason` via the next `upsert_session` call in the eod_loop (1-second cadence). However, if the process is killed between the halt and the next upsert, the halt reason is permanently lost from the DB. There is also no `halt_time` timestamp recorded — only a boolean + text.

**File:** `src/execution/risk_manager.hpp`, line 145-149  
**Fix:** Add an immediate `db->write_halt_event(reason, timestamp)` call path. At minimum, add a `halt_timestamp` field to `live_sessions`.

---

### H5 — `orb_strategy_main.cpp` diverges from live execution strategy
The standalone `orb_strategy` binary implements a different ORB logic:
- Uses bar **close** prices for breakout detection vs. live strategy uses **tick** prices
- SL formula: `orb_high - sl_ticks * tick_size` vs. live: `fill_price - cfg_.sl_points`  
- Takes config from `cfg["orb"]` subkey vs. live `OrbConfig` flat structure
- No session time filtering, no max_daily_trades, no consistency cap

Any Python/C++ parity tests using this binary are testing the wrong thing. Results from `orb_strategy_main` cannot be used to validate live execution behavior.

**File:** `src/orb_strategy_main.cpp`  
**Fix:** Either rewrite to use the same `OrbStrategy` class as the executor, or clearly document it as a legacy tool and exclude its output from parity comparisons.

---

### H6 — `LatencyLogger` single pending record corrupts stop-order latency (latency_logger.hpp:63-68)
`LatencyLogger::pending_` holds exactly one record. The executor calls `lat_.on_signal(stop_basket, sl_price, false)` then `lat_.on_submit(stop_basket, sl_price)` immediately after entry fill — overwriting the entry's pending record before `on_fill` has been called for it. The entry fill latency is lost; all subsequent fills see the stop order's submit timestamp.

In practice the entry fill typically arrives via `on_fill_notification_locked` which is called from within the same lock scope as `submit_stop_order_locked`, so timing is tight. The latency recorded for entries after the first stop submission will be incorrect.

**File:** `src/execution/latency_logger.hpp`, line 63  
**Fix:** Maintain a `std::unordered_map<std::string, TradeLatency> pending_` keyed by basket_id.

---

### H7 — `SdkMdFeed` leaks `RApi::REngine` on exception between construction and login (sdk_md_feed.hpp:271-289)
`engine_ = new RApi::REngine(...)` is called, then `engine_->login(...)` may return false or throw. The `start()` method returns `false` but does not `delete engine_`. The destructor only calls `stop()` which guards `if (engine_)`, so the engine is cleaned up if `stop()` is called explicitly — but if the `SdkMdFeed` is destroyed before `stop()`, or if exceptions propagate unexpectedly, the engine leaks.

**File:** `src/execution/sdk_md_feed.hpp`, lines 271-289  
**Fix:** Wrap `engine_` in `std::unique_ptr<RApi::REngine, REngineDeleter>` with a custom deleter that calls `logout`+`delete`.

---

## MEDIUM Issues

### M1 — `on_trade_pnl` does not validate NaN/Inf inputs (risk_manager.hpp:56-101)
If `pnl_usd = NaN` is passed (e.g., from a corrupted fill price), all risk state variables silently become NaN. Subsequent comparisons (`equity_ > peak_equity_`, `drawdown >= cap`) all evaluate to `false`, effectively disabling all risk checks. The system would continue trading with infinite apparent equity.

**File:** `src/execution/risk_manager.hpp`, line 56  
**Fix:** Add `if (!std::isfinite(pnl_usd)) { halt("non_finite_pnl: " + std::to_string(pnl_usd)); return; }` at the top of `on_trade_pnl`.

---

### M2 — `write_trade` and `upsert_session` are not transactional (orb_db.hpp:52-187)
After a trade closes, `write_trade` (INSERT to `live_trades`) and `upsert_session` (UPDATE to `live_sessions`) are called independently with ~1 second between them (eod_loop cadence). A crash between the two leaves the DB in an inconsistent state: a trade exists without the corresponding session stats update.

**File:** `src/execution/orb_db.hpp`, lines 52-187  
**Fix:** Wrap both calls in a `BEGIN`/`COMMIT` transaction block.

---

### M3 — `__builtin_bswap32` is GCC/Clang-specific (executor_main.cpp:169)
The `proto_frame` helper uses `__builtin_bswap32` for big-endian framing. This is non-portable.

**File:** `src/execution/executor_main.cpp`, line 169  
**Fix:** Use `htonl()` from `<arpa/inet.h>`.

---

### M4 — SSL certificate path is relative (executor_main.cpp:470)
`ssl_ctx.load_verify_file("certs/rithmic_ssl_cert_auth_params")` requires the process working directory to contain `certs/`. If launched from a different directory (systemd service, cron, supervisor), SSL peer verification silently fails or throws at connection time.

**File:** `src/execution/executor_main.cpp`, line 470  
**Fix:** Resolve the cert path relative to the config file's directory, or add a `ssl_cert_path` field to `OrbConfig`.

---

### M5 — `get_total_pnl()` includes test/dry-run trades if recorded (orb_db.hpp:360-378)
The query `SELECT COALESCE(SUM(pnl_usd), 0.0) FROM live_trades WHERE instrument=$1 AND strategy=$2` sums all historical trades. If test trades were executed in non-dry-run mode and wrote rows to `live_trades`, they permanently inflate or deflate the P&L baseline used to seed `RiskManager.total_profit_`. This affects the consistency cap calculation going forward.

**File:** `src/execution/orb_db.hpp`, line 360  
**Fix:** Add a `is_test` boolean column to `live_trades` and exclude `is_test=true` rows from the P&L sum.

---

### M6 — `orb_config.hpp` hand-rolled JSON parser is brittle (orb_config.hpp:241-275)
The `json_str`/`json_int`/`json_dbl` extractors do substring searches for `"key"`. They cannot handle:
- Duplicate keys (first match wins, silently ignoring overrides)
- Keys that are substrings of other keys (e.g., searching for `"sl"` matches `"trail_be_offset"` at wrong position)
- JSON with comments
- Values that span multiple lines

**File:** `src/execution/orb_config.hpp`, lines 241-275  
**Fix:** Replace with `nlohmann/json` which is already available (used in `orb_strategy_main.cpp`).

---

### M7 — `dry_run` config parsing is fragile (orb_config.hpp:217-225)
The `dry_run` value is detected by checking if the 4 characters after the colon are `"true"`. A JSON value of `true` (boolean) would match, but `"true"` (string) would fail. A trailing comment like `true // dry` also works by accident. More critically: a key like `"not_dry_run": true` at a position before `"dry_run"` would not confuse the parser (it searches for `"dry_run"` literally), but the same-named prefix issue in `json_str` could apply to similar key names.

**File:** `src/execution/orb_config.hpp`, lines 217-225  
**Fix:** Covered by M6 (migrate to nlohmann/json).

---

### M8 — `const_cast` of `cfg_` string members in SDK feed (sdk_md_feed.hpp:262-281)
`SdkMdFeed::start()` casts `const char*` return values of `std::string::c_str()` to `char*` for the SDK API. If the SDK modifies the pointed-to memory (which some C APIs do), it would corrupt live config strings (`cfg_.app_name`, `cfg_.app_version`, etc.).

**File:** `src/execution/sdk_md_feed.hpp`, lines 262-281  
**Fix:** Copy strings into local `char` buffers before passing to the SDK.

---

### M9 — `ensure_schema()` runs on every startup (orb_db.hpp:423-515)
DDL migrations run synchronously during executor startup, including `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` statements. On a loaded production DB, these acquire schema locks and can delay startup by seconds.

**File:** `src/execution/orb_db.hpp`, line 423  
**Fix:** Run migrations out-of-band via a separate migration tool; add a schema version check to `ensure_schema()` that skips DDL if version matches.

---

## LOW / INFO Issues

### L1 — `halted_` is `std::atomic<bool>` but `halt_reason_` is plain `std::string` (risk_manager.hpp:158)
The `halted()` accessor returns the atomic without holding the mutex, while `halt_reason_` is only protected by the mutex. External callers that read `halted()` without the mutex get a potentially stale `halt_reason_`. In practice all callers that need the reason call `can_trade(reason)` which correctly acquires the mutex.

**File:** `src/execution/risk_manager.hpp`, line 142  
**Recommendation:** Remove the `halted()` public accessor (it's only used in one place for a check that duplicates `can_trade()`), or document the split atomicity explicitly.

---

### L2 — Partial fill handling silently treats partial as full (order_manager.hpp:180-185, 247-251)
Both entry and exit partial fill paths log `WARN: treating as full fill`. At qty=1 this is correct (physically impossible to partially fill 1 contract). But the code stores `pos_.qty` (the original quantity) not `fill_qty`, so if qty is ever changed to >1, slippage and PnL calculations would be wrong.

**File:** `src/execution/order_manager.hpp`, lines 180-185  
**Recommendation:** Assert `pos_.qty == 1` or fully implement partial fill handling if qty>1 is ever planned.

---

### L3 — No bounds validation on config params (orb_config.hpp)
`OrbConfig::from_file()` applies no validation on: `point_value > 0`, `qty > 0`, `trailing_drawdown_cap > 0`, `consistency_cap_pct` in (0, 1], `daily_loss_limit < 0`. The executor validates a small subset (lines 1516-1523) but misses most.

**File:** `src/execution/orb_config.hpp`  
**Recommendation:** Add a `validate()` method to `OrbConfig` called from the executor after load.

---

### L4 — `pg_connstr()` includes password in plain string (orb_config.hpp:127-136)
The connection string returned by `pg_connstr()` contains the password in plaintext. It's passed directly to `PQconnectdb` — acceptable. But if this string ever gets logged (e.g., in a PQ error message that includes the connection string), the password is exposed.

**File:** `src/execution/orb_config.hpp`, line 127  
**Recommendation:** Override the connection string format or use PQ's service file / `.pgpass` file instead.

---

### L5 — `notify_tick()` uses `PQexec` + format string (orb_db.hpp:414-419)
`snprintf(sql, ..., "NOTIFY live_tick, '%.2f'", price)` then `PQexec(conn_, sql)`. The `price` is a `double` formatted with `%.2f` — no injection risk from this specific value. However, using `PQexec` for anything other than static DDL is a pattern that invites future injection vulnerabilities if the format string is changed. All non-DDL queries should use `PQexecParams`.

**File:** `src/execution/orb_db.hpp`, line 414  
**Recommendation:** Use `SELECT pg_notify('live_tick', $1)` with `PQexecParams` and a string-formatted price parameter.

---

### L6 — `OrbStrategy` single-direction lock: `long_taken`/`short_taken` never cleared intra-day (orb_strategy.hpp:58-59)
Once `long_taken=true`, no further LONG signals are emitted that day regardless of re-tests or strategy reloads. This is intentional (one signal per direction per day) but is not documented as a deliberate design choice — a reader might think it's a bug.

**File:** `src/execution/orb_strategy.hpp`, lines 58-59  
**Recommendation:** Add a comment clarifying this is deliberate strategy behavior.

---

### L7 — `g_running = false` from exception handler races signal handler (executor_main.cpp:1543)
Both `handle_signal()` and the coroutine exception handler set `g_running = false`. The signal handler sets it atomically; the exception handler does too (`std::atomic<bool>`). No race on the atomic, but both paths then call `ioc_ref.stop()` — and `ioc_ref.stop()` is not safe to call from a signal handler (ASIO docs require it to be called from within the `io_context` thread or via `post`). The signal handler currently does NOT call `ioc_ref.stop()` — it only sets the atomic. The eod_loop checks and stops gracefully. This is correct but delicate.

**File:** `src/execution/executor_main.cpp`, lines 157-162  
**Recommendation:** Add a comment explaining why `ioc_ref.stop()` is intentionally absent from the signal handler.

---

### L8 — `strategy_` hardcoded as `"ORB"` in OrbDB (orb_db.hpp:579)
All DB queries filter on `strategy = 'ORB'`. If a second strategy is ever added to this executor, it would collide with existing rows or require a new `OrbDB` instance with a different strategy string. Currently fine but not forward-compatible.

**File:** `src/execution/orb_db.hpp`, line 579  
**Recommendation:** Pass strategy string through constructor (already accepted but defaults to `"ORB"`).

---

## Test Coverage Gap Summary

| Component | Current Tests | Required Tests |
|-----------|--------------|----------------|
| RiskManager | 0 | trailing_dd_cap, consistency_cap, daily_loss, reset_daily, NaN input |
| OrderManager | 0 | state machine, stop placement, EOD cancel race, rejection retry |
| OrbStrategy | 0 | ORB range building, breakout detection, EOD flatten, news blackout |
| LatencyLogger | 0 | concurrent baskets, slippage calc, missing submit |
| OrbDB | 0 | write_trade, upsert_session, get_total_pnl, schema migration |
| executor_main | 0 | integration test: dry-run round trip |

**Total uncovered critical paths: ~30+ scenarios**

---

## Recommendations Priority Order

1. **Immediate:** Implement `is_news_blackout()` (C1)
2. **Immediate:** Add rejection retry limit to exit handling (C2)
3. **Before next live session:** NaN guard in `on_trade_pnl` (M1)
4. **Before next live session:** Login timeout on MD/OP plants (H2)
5. **Short term:** Write unit tests for RiskManager and OrbStrategy (C3)
6. **Short term:** Remove or gate `NQ_FIRE_TEST_ORDER` hook (H3)
7. **Medium term:** Fix `LatencyLogger` to track multiple baskets (H6)
8. **Medium term:** Migrate config parser to nlohmann/json (M6)
9. **Medium term:** Transactional write_trade + upsert_session (M2)
10. **Long term:** `orb_strategy_main.cpp` parity fix (H5)
