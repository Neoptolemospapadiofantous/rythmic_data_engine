# Execution Layer Audit Gap Report — T2
**Date:** 2026-04-30  
**Agent:** Builder 3  
**Format:** file:line | severity | description | recommended fix  
**See also:** docs/execution_layer_audit.md (full detailed analysis)

---

## Findings

---

### T2-01 — Risk halt events not written to audit_log table
**File:** `src/execution/risk_manager.hpp:145-149`  
**Severity:** HIGH  
**Description:** `halt()` writes to `LOG()` (stdout/file only). Halt events are never persisted to PostgreSQL. The `live_sessions.halt_reason` field is updated by the eod_loop ~1 second later (in executor_main.cpp:1033-1038), but: (a) a process crash before that 1-second window loses the halt permanently, (b) there is no `halt_time` timestamp, (c) the main `audit_log` table (if present in src/audit.hpp) receives no `risk.halted` event. This means the compliance audit trail has no record of which risk limit fired, when, and why.  
**Recommended fix:** After setting `halted_ = true`, immediately call `db->write_halt_event(reason, now_ns())` (add method to OrbDB). Also write an `audit_log` entry with event_type='risk.halted', payload=reason, timestamp.

---

### T2-02 — No parameterized query for `notify_tick()` — pattern risk
**File:** `src/execution/orb_db.hpp:414-419`  
**Severity:** LOW  
**Description:** `notify_tick()` builds a SQL string with `snprintf(sql, 80, "NOTIFY live_tick, '%.2f'", price)` then calls `PQexec(conn_, sql)`. The price is a `double` formatted with `%.2f` — not an injection risk from this specific value. However, this is the only non-parameterized query in OrbDB that uses runtime data. All other INSERT/UPDATE/SELECT use `PQexecParams` with bound parameters. The inconsistency is a maintenance hazard: future modifications to this query (e.g., adding channel name from user input) could introduce injection.  
**Recommended fix:** Replace with `SELECT pg_notify('live_tick', $1)` using `PQexecParams` with a `char*` price string.

---

### T2-03 — LatencyLogger logs raw prices and basket IDs — no masking
**File:** `src/execution/latency_logger.hpp:92-98`  
**Severity:** LOW  
**Description:** `on_fill()` logs: basket_id, entry/exit flag, signal→submit latency, submit→fill latency, slippage ticks, slippage USD. No sensitive data masking. Basket IDs include the symbol and epoch milliseconds (`NQ-<ms>-<seq>`). Fill prices and slippage are logged verbatim. For a solo trader this is acceptable — the logs are operational data needed for debugging. No PII is involved. Not a material security issue in this context, but the logs should be access-controlled (chmod 640, log rotation with retention policy).  
**Recommended fix:** Ensure log files are owner-read-only. No code change required unless regulatory compliance demands masked prices.

---

### T2-04 — DB failure during order execution is logged but not audited
**File:** `src/execution/executor_main.cpp:1043-1065`  
**Severity:** MEDIUM  
**Description:** When `db->write_trade()` fails (line 1052-1055), the error is logged and `db->reconnect()` is called. The trade is NOT retried — the P&L record is permanently lost from the DB. The risk manager's in-memory state is already updated (risk_.on_trade_pnl was called at line 275 of order_manager.hpp before this DB write), so risk continues correctly, but the accounting record is gone. Similarly, `flush_position()` failures (lines 1057-1064) silently swallow the error. No audit_log event is written for "DB write failure for trade X".  
**Recommended fix:** (1) Implement a write-ahead buffer: if `write_trade` fails, store the Position snapshot in a local queue and retry on reconnect. (2) Write an `audit_log` entry with event_type='db.write_failed', payload=trade summary, even if DB is down (use WAL file as fallback).

---

### T2-05 — No order deduplication or replay protection
**File:** `src/execution/order_manager.hpp:118-135`  
**Severity:** MEDIUM  
**Description:** `on_signal()` checks `pos_.state != PosState::FLAT` and rejects duplicate signals. This is a basic guard. However: (a) basket IDs are monotonically increasing but never checked against a sent-order set, so a reconnect that replays fills cannot be distinguished from new fills; (b) there is no idempotency key on `write_trade()` in OrbDB — if `on_fill_notification` fires twice for the same basket_id (exchange duplicate delivery), two trade rows will be inserted; (c) on reconnect, the executor does not query open orders from the exchange before accepting new signals (beyond the snapshot reconciliation in executor_main.cpp:845-863).  
**Recommended fix:** Add UNIQUE constraint on `live_trades(basket_id_entry)`. In `on_fill_notification`, record processed basket IDs in a set and ignore duplicates. The basket_id is currently stored in `user_tag` — add it as a column to `live_trades`.

---

### T2-06 — No cancel-on-shutdown for pending entry orders
**File:** `src/execution/executor_main.cpp:998-1003`  
**Severity:** HIGH  
**Description:** When SIGINT/SIGTERM fires, `handle_signal()` sets `g_running=false` and `g_flatten_requested=true`. The eod_loop calls `order_mgr.flatten_now("kill_signal")` on the next 1-second tick. `flatten_now()` handles PENDING_ENTRY by calling `cancel_cb_` for the entry basket. BUT: the 1-second delay window means the signal handler fires → process may exit before the next eod_loop tick → cancel never sent → exchange has an open pending order. On Legends, an unmanaged PENDING_ENTRY can fill after the session ends.  
**Recommended fix:** Call `order_mgr.flatten_now("shutdown")` synchronously in the signal handling path, not via the deferred `g_flatten_requested` flag. Use `asio::post(ex, [&]{ order_mgr.flatten_now("kill_signal"); })` to keep thread safety while executing immediately.

---

### T2-07 — SDK market data NaN/Inf not validated before strategy feed
**File:** `src/execution/sdk_md_feed.hpp:102-123`  
**Severity:** MEDIUM  
**Description:** `TradePrint()` checks `!pInfo->bPriceFlag || pInfo->dPrice <= 0.0 || pInfo->llSize <= 0` and rejects zero/negative prices. However, it does NOT check for `std::isnan(pInfo->dPrice)` or `std::isinf(pInfo->dPrice)`. A NaN tick price would: (a) pass the `> 0.0` guard (NaN comparisons always return false, so `NaN > 0.0` is false, but `NaN <= 0.0` is also false — the guard `dPrice <= 0.0` evaluates to false for NaN, so the tick PASSES the filter), (b) corrupt ORB high/low (`if (tick.price > session_.orb_high)` evaluates to false for NaN, so orb_high is never updated but last_price_ becomes NaN), (c) corrupt P&L calculations if NaN reaches fill_price.  
**Recommended fix:** Add `|| !std::isfinite(pInfo->dPrice)` to the filter condition. Same fix needed in executor_main.cpp:1347 for WebSocket path.

---

### T2-08 — Missing AuditLog events (no calls to audit_log table from execution layer)
**File:** All execution layer files  
**Severity:** HIGH  
**Description:** The execution layer produces zero entries to any `audit_log` table. The following events occur with no persistent audit record beyond the operational log file:

| Event | Current | Should be in audit_log |
|-------|---------|----------------------|
| order.submitted | LOG() only | YES — basket_id, symbol, qty, type, price |
| order.filled | LOG() only | YES — basket_id, fill_price, fill_qty, slippage |
| order.cancelled | LOG() only | YES — basket_id, reason |
| order.rejected | LOG() only | YES — basket_id, reject_reason, rp_code |
| risk.halted | LOG() only | YES — halt_reason, equity, peak_equity |
| risk.daily_reset | LOG() only | YES — date, starting_equity |
| position.opened | DB live_position | Partial — live_position is transient |
| position.closed | DB live_trades | YES — already recorded ✓ |
| session.started | LOG() only | YES — date, config params |
| executor.reconnect | LOG() only | YES — reconnect count, reason |
| executor.startup_open_position | LOG() only | YES — CRITICAL event needs audit record |

**Recommended fix:** Create or reuse the `audit_log` table from `src/audit.hpp`. Add `OrbDB::write_audit_event(type, payload, severity)` method. Call from: `OrderManager::send_market_order()`, `on_fill_notification_locked()`, `on_order_rejected()`, `RiskManager::halt()`, `RiskManager::reset_daily()`, executor reconnect logic.

---

### T2-09 — Consistency cap first-day edge case: cap never fires (correct but untested)
**File:** `src/execution/risk_manager.hpp:87-97`  
**Severity:** LOW  
**Description:** On the first profitable trading day (prior_profit = 0, since total_profit_ = 0 at start), `prior_profit = total_profit_ - daily_pnl_ = 0 - daily_pnl_`. Since `daily_pnl_ > 0`, `prior_profit < 0`, and the condition `prior_profit > 0.0 && daily_pnl_ > 0.0` evaluates to false — cap does not fire. This is the correct Legends 50K rule behavior (can't be inconsistent if you have no prior profit). However, it's an important edge case with no test coverage. A regression could accidentally fire the cap on day 1 and prevent all trading.  
**Recommended fix:** Add a unit test: seed_total_profit(0), on_trade_pnl(+500), assert !halted(). Also test: seed_total_profit(100), on_trade_pnl(+31) — should halt (31% > 30%). And: seed_total_profit(-500), on_trade_pnl(+100) — should NOT halt (prior_profit negative).

---

### T2-10 — `halted_` atomic vs `halt_reason_` mutex: inconsistent read path
**File:** `src/execution/risk_manager.hpp:125-128, 142`  
**Severity:** LOW  
**Description:** `halted()` (line 142) reads `halted_` atomically without the mutex. `can_trade(reason)` (line 105-123) acquires the mutex and reads both `halted_` and `halt_reason_`. The `halted()` accessor is only used in executor_main.cpp:1033 as a parameter to `upsert_session()` — where a false-positive "true" would cause `risk_halted=true` in the DB even if `halt_reason_` hasn't been written yet. Because both `halted_` and `halt_reason_` are set inside the same mutex-held `halt()` call, and the eod_loop always reads `halted()` from the same thread as the io_context (after all fill callbacks have completed), there is no actual race in practice. However, the code allows callers to call `halted()` from any thread (it's `const` public), and a multi-threaded caller could see `halted_=true` before `halt_reason_` is set.  
**Recommended fix:** Remove the standalone `halted()` accessor; replace with `can_trade()` everywhere. Or document the invariant that `halted()` is only safe to call from the io_context thread.

---

### T2-11 — Process crash mid-order: no recovery mechanism
**File:** `src/execution/executor_main.cpp:397-406`  
**Severity:** HIGH  
**Description:** The reconnect reconciliation (lines 397-406) halts new entries if `carried_pos.state != FLAT`. `carried_pos` is an in-memory variable populated only at clean session exit (line 1464). If the process is SIGKILL'd mid-trade, `carried_pos` is never written — the next restart has `carried_pos = Position{}` (FLAT), and the reconciliation does not fire. The snapshot reconciliation (lines 845-863) checks for WORKING orders on the exchange and halts if found. This is the only recovery path, and it only works if the order is still on the exchange (not if it was filled before the crash).  
**Recommended fix:** Persist current position state to DB on every fill via `live_position` (already done via `flush_position`). On startup, read `live_position` for today and if state != 'FLAT', halt and warn. This closes the crash gap where `carried_pos` is empty but the position was filled.

---

### T2-12 — `is_news_blackout()` permanently stubbed — trades fire through news events  
**File:** `src/execution/orb_strategy.hpp:209-214`  
**Severity:** CRITICAL  
**Description:** Always returns `false`. The `news_blackout_min` config field exists but is never read. The system trades through CPI (8:30 ET), FOMC (14:00 ET), and other scheduled releases without restriction. A violent price spike during these events can hit the trailing drawdown cap ($2500 from peak) in a single fill, halting trading for the day.  
**Recommended fix:** Implement a hardcoded schedule for common news times (8:30, 10:00, 14:00 ET). Block entries for `news_blackout_min` minutes either side. For FOMC days, use an external calendar or require manual override flag.

---

### T2-13 — Exit order rejection loop has no limit — can spam orders
**File:** `src/execution/order_manager.hpp:401-408`  
**Severity:** CRITICAL  
**Description:** When an exit order is rejected, `on_order_rejected()` immediately retries via `initiate_exit_locked("rejected_exit_retry", 0.0)`. No counter, no backoff, no circuit breaker. If the exchange rejects every exit (e.g., account suspended, market closed), this creates a tight rejection→retry loop that floods ORDER_PLANT with orders while the position remains open.  
**Recommended fix:** Add `int rejected_exit_retries_ = 0;` counter. After 3 rejections, stop retrying, emit CRITICAL log, halt new entries. Require manual intervention to resume.

---

## Summary Table

| ID | File | Line | Severity | Category |
|----|------|------|----------|----------|
| T2-01 | risk_manager.hpp | 145-149 | HIGH | Audit gap |
| T2-02 | orb_db.hpp | 414-419 | LOW | SQL pattern |
| T2-03 | latency_logger.hpp | 92-98 | LOW | Data masking |
| T2-04 | executor_main.cpp | 1043-1065 | MEDIUM | Error handling |
| T2-05 | order_manager.hpp | 118-135 | MEDIUM | Replay protection |
| T2-06 | executor_main.cpp | 998-1003 | HIGH | Cancel-on-shutdown |
| T2-07 | sdk_md_feed.hpp | 102-123 | MEDIUM | Input validation |
| T2-08 | All execution files | — | HIGH | Missing audit events |
| T2-09 | risk_manager.hpp | 87-97 | LOW | Logic edge case |
| T2-10 | risk_manager.hpp | 125-142 | LOW | Thread safety |
| T2-11 | executor_main.cpp | 397-406 | HIGH | Crash recovery |
| T2-12 | orb_strategy.hpp | 209-214 | CRITICAL | News blackout stub |
| T2-13 | order_manager.hpp | 401-408 | CRITICAL | Retry loop |

## Missing Audit Events (T2-08 Detail)

Events that should flow to the `audit_log` table but currently do not:

```
risk.halted          — risk_manager.hpp:halt() → LOG only
risk.daily_reset     — risk_manager.hpp:reset_daily() → LOG only
order.submitted      — order_manager.hpp:send_market_order() → LOG only
order.filled         — order_manager.hpp:on_fill_notification_locked() → LOG only
order.cancelled      — order_manager.hpp:cancel_stop_locked() → LOG only
order.rejected       — order_manager.hpp:on_order_rejected() → LOG only
session.started      — executor_main.cpp:951-957 → LOG only
executor.reconnect   — executor_main.cpp:1242-1318 → LOG only
executor.open_pos_at_startup — executor_main.cpp:845-863 → LOG only, CRITICAL
```
