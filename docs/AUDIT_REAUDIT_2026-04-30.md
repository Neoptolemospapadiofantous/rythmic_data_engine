# Re-Audit Report — rithmic_engine
**Date:** 2026-04-30  
**Scope:** Verification of 10 stated fixes from the post-fix round + residual gap analysis  
**Reference:** `docs/AUDIT_GAP_REPORT.md`

---

## 1. Verified Fixed

### Fix 1 — config/live_config.json: trade_route + daily_loss_limit
**Status: PASS**

`config/live_config.json` line 11: `"trade_route": "Rithmic Order Routing"` — confirmed changed from `"simulator"`.  
`config/live_config.json` line 16: `"daily_loss_limit": -2000.0` — confirmed changed from `-200.0`.  
The nested `prop_firm.daily_loss_limit` is `2000.0` (positive magnitude), consistent with the flat key's absolute value.  
Both originally-critical issues (C4 and H-PY-5) are resolved in the config file.

---

### Fix 2 — src/audit.cpp: PQexecParams + UNNEST
**Status: PASS (with one minor residual noted below)**

`AuditLog::flush()` now builds four PostgreSQL array literals (`{…}`) via `make_pg_array()` — a lambda that quotes and backslash-escapes double-quotes and backslashes in every value — then calls `PQexecParams` with `$1::timestamptz[]` … `$4::text[]`. No string concatenation of event/severity into the SQL body. SQL injection via `event` or `details` is eliminated (H-SEC-1 resolved).

**Residual (new issue, see Section 3):** `make_pg_array` is a local lambda that manually implements PostgreSQL array quoting. The escaping handles `"` and `\` but not the zero byte (`\0`), which libpq would truncate in a C-string anyway. In practice not exploitable, but worth noting.

---

### Fix 3 — src/audit.hpp: MAX_BUF constant + drop-oldest
**Status: PASS**

`static constexpr size_t MAX_BUF = 10000;` at line 51.  
`AuditLog::log()` (audit.cpp line 35): `if (buf_.size() >= MAX_BUF) buf_.erase(buf_.begin());` before push_back.  
OOM risk under prolonged DB outage is bounded (M-AUD-2 resolved).

---

### Fix 4 — src/execution/risk_manager.hpp: NaN/Inf guard in on_trade_pnl()
**Status: PASS**

Lines 58-61: `if (!std::isfinite(pnl_usd)) { halt("non_finite_pnl: " + std::to_string(pnl_usd)); return; }` — present and correct at the top of the function before any arithmetic. Covers NaN, +Inf, -Inf (M-VAL-1 resolved).

---

### Fix 5 — src/db.cpp: redact_pg_password() applied to DBSummary::connstr
**Status: PASS**

`redact_pg_password()` defined at lines 13-24. `TickDB::summary()` (line 983) calls `s.connstr = redact_pg_password(connstr_)`. Replaces the password value with `***` before the string is stored in `DBSummary`. H-SEC-2 resolved for the `--status` output path.

**Note:** H-SEC-3 (keyword=value connection string injection via password containing spaces) is still open — `pg_connstr()` still builds an unquoted keyword=value string. `redact_pg_password()` only redacts for display; the underlying connection uses the raw connstr.

---

### Fix 6 — src/execution/order_manager.hpp: rejected_exit_count_ + 3-retry cap
**Status: PASS**

`rejected_exit_count_` (int, line 541) and `entry_halted_` (bool, line 542) are present.  
`on_order_rejected()` (lines 409-428): increments `rejected_exit_count_`, retries exit via `initiate_exit_locked` if `<= 3`, sets `entry_halted_ = true` after the third rejection, and logs CRITICAL.  
On successful exit fill (line 282): resets both counters.  
`on_signal()` checks `entry_halted_` at the top (lines 104-107).  
C2 is resolved.

**One quirk:** The retry fires up to 3 times (values 1, 2, 3) and halts on value 4+ (the condition is `<= 3` before the `else`). On the 4th rejection `entry_halted_` is set but no additional exit is attempted — correct behavior per design intent.

---

### Fix 7 — src/execution/orb_strategy.hpp: is_news_blackout() implementation
**Status: PASS**

`is_news_blackout()` (lines 209-227): checks four hardcoded ET news times — `{8,30}`, `{10,0}`, `{14,0}`, `{14,30}` — by comparing `now_min` against `news_min - buf` … `news_min + buf` where `buf = cfg_.news_blackout_min`. `cfg_.news_blackout_min` defaults to 2 in `orb_config.hpp`. C1 is resolved.

**Note:** The default `news_blackout_min = 2` is a 2-minute window (±2 min). This is narrower than typical algo-firm policies (5–10 min). Consider increasing the default.

---

### Fix 8 — go_live.py: Gate K (trade_route != "simulator")
**Status: PASS**

`_gate_trade_route()` (lines 355-367): checks `route.lower() == "simulator"` and returns FAIL. Gate K is included in `_ALL_GATES` (line 382) and listed in the help text (line 535). `run_preflight()` loops all gates (line 457). C6 is resolved.

---

### Fix 9 — live_trader.py: Pydantic validation at config load
**Status: PASS**

`_load_config()` (lines 583-591): imports `LiveConfig` from `config.live_config_schema` and calls `LiveConfig.model_validate(cfg)`, raising `SystemExit` on failure. C5 is resolved.

The schema (`config/live_config_schema.py`) enforces: `trade_route != "simulator"`, `daily_loss_limit < 0`, `symbol == "MNQ"`, `point_value == 2.0`, `orb.tick_size == 0.25`, cross-field consistency between flat keys and `prop_firm`, and `sl_points` / `orb.stop_loss_ticks` agreement.

---

### Fix 10 — tests/test_live_trader.py: point_value=2.0, symbol="MNQ"
**Status: PASS**

`_make_config()` (lines 38-59): `"point_value": 2.0` in the `orb` section, `"symbol": "MNQ"` at root. PnL assertions in `TestDailyPnl` use correct MNQ math:
- `test_daily_pnl_accumulates_after_exit`: +10 pts × $2.00 − $4 = **$16.00** (line 516-519).
- `test_daily_pnl_negative_on_loss`: −4 pts × $2.00 − $4 = **−$12.00** (line 541-544).
- `test_write_trade_close_returns_pnl` uses `point_value=20.0` explicitly in the call (NQ test case — intentional, not a regression).

M-TEST-1 is resolved.

---

## 2. Still Open — CRITICAL / HIGH

The following items from `AUDIT_GAP_REPORT.md` remain unaddressed.

### CRITICAL Still Open

**C3 — Zero unit tests for execution layer**  
`tests/` contains no C++ tests for `RiskManager`, `OrderManager`, `OrbStrategy`, or `LatencyLogger`. The `tests/test_live_trader.py` additions are Python-side only. Priority items 1–5 from Section 6 of the original report are unimplemented.

**C7 — dry_run guard is NotImplementedError (architectural weakness)**  
`_submit_order()` raises `NotImplementedError` when `dry_run=False`. The test `test_dry_run_false_raises_not_implemented` exists and correctly asserts this. However the original report flagged that this is not architecturally enforced — the guard is in user-space Python, one deletion away from live orders. The fix recommendation (gate via `sys.exit(1)` + CRITICAL log, or separate module) has not been applied. The Pydantic validation at startup partially mitigates this (Schema rejects `trade_route='simulator'`), but `dry_run=False` with a valid route would still reach the NotImplementedError path.

---

### HIGH Still Open

**H-SEC-3 — libpq keyword=value connection string injection**  
`pg_connstr()` in `orb_config.hpp` still assembles a raw keyword=value connection string. A password containing a space + keyword (e.g., `host= evil.db`) can override connection targets. `redact_pg_password()` only redacts display output; the underlying connect call uses the raw string.

**H-SEC-4 — Rithmic username logged in plaintext**  
`executor_main.cpp` line 527: `LOG("[EXECUTOR] MD Login: user=%s system=%s", orb_cfg.md_user.c_str(), ...)`. MD login user is logged. Line 1265 also sets `req.set_user(orb_cfg.md_user)` in a reconnect context. Log files remain world-readable on Linux.

**H-SEC-5 — NQ_FIRE_TEST_ORDER hook still present**  
`executor_main.cpp` lines 1410-1412: `if (std::getenv("NQ_FIRE_TEST_ORDER") != nullptr)` — fires live market orders. No `dry_run` guard around it, no CRITICAL startup banner. An env var leaking from a dev shell fires live orders.

**H-AUD-1 — Execution layer has zero AuditLog calls**  
`grep AuditLog src/execution/executor_main.cpp` returns empty. Orders, fills, risk halts, EOD flattens, and session starts write to `LOG()` only. The `audit_log` PostgreSQL table has zero execution-layer entries per session.

**H-AUD-2 — Risk halt events not in audit_log**  
`halt()` in `risk_manager.hpp` writes to `LOG()` only. No `AuditLog*` is passed into `RiskManager`.

**H-REL-1 — MD plant login loop has no timeout**  
MD plant login at `executor_main.cpp` lines 542-558: a bare `for(;;)` with `async_read` and no `expires_after()` call before the loop. If the server connects but never sends `template_id=11`, the coroutine hangs forever. The ORDER_PLANT login at lines 640-657 has similar structure. (Note: a 5-second `expires_after` is set at line 692 during *trade route* discovery, but not during the initial login read.)

**H-REL-2 — No cancel-on-shutdown for pending entry orders**  
SIGTERM handler sets a flag; `flatten_now()` is called on the next 1-second eod_loop tick. A double SIGINT or process kill before that tick leaves a PENDING_ENTRY that can fill after session end.

**H-REL-5 — LatencyLogger single pending_ record corrupts stop-order latency**  
`LatencyLogger::pending_` is a single `TradeLatency` struct. `submit_stop_order_locked()` calls `lat_.on_signal(stop_basket, ...)` immediately after entry fill, overwriting the pending entry record before `on_fill` is called. Entry fill latency is permanently lost.

**H-PY-1 — _reconcile_position() does not query live_trades (C++ table)**  
On restart, Python only queries `trades WHERE source='python'`. A C++ executor crash with an open position results in two simultaneous positions on Rithmic.

**H-PY-2 — Order submitted before DB write**  
`_on_signal()` calls `_submit_order()` before `_write_trade_open()`. DB failure after Rithmic fill leaves the position absent from the DB; reconciliation re-enters on restart.

**H-PY-3 — Commission $4.00 hardcoded**  
`commission_rt = 4.0` in `_write_trade_close()`. Not read from config; no test verifying it matches `formula_audit.yaml`.

**H-PY-4 — No PnL sanity check**  
`_write_trade_close()` writes `pnl_usd` to DB without bounds check. A 10x misconfigured `point_value` passes silently (partially mitigated by Pydantic schema enforcing `point_value=2.0`, but the value flows through Python arithmetic without runtime clamping).

---

## 3. New Issues Introduced by the Fixes

### N-1 — audit.cpp make_pg_array: NULL byte not handled (LOW)
The `make_pg_array` lambda escapes `"` and `\` but does not handle embedded null bytes (`\0`). Since C++ `std::string` can hold nulls but `c_str()` truncates at the first one, an `event` or `details` string containing `\0` would silently truncate in the PostgreSQL array literal. In practice, none of the callers produce null-containing strings, but the function is not defensively documented.

### N-2 — audit.cpp: failed flush re-queues at head, bypasses MAX_BUF cap (LOW-MEDIUM)
On flush failure, `buf_.insert(buf_.begin(), batch.begin(), batch.end())` re-inserts the batch before the current tail. This can temporarily exceed `MAX_BUF` by up to `MAX_BUF` entries (one full failed batch re-queued on top of a full buffer). Under a sustained DB outage where flush is called every minute, the buffer can hold up to ~20,000 entries between two consecutive flush attempts (one fill-to-cap cycle + one re-queue). Not OOM-critical at 10,000 string entries but the MAX_BUF invariant is not strictly maintained after a failure.

### N-3 — live_trader.py: ImportError in _load_config() silently promoted to SystemExit (LOW)
The `try/except Exception` around `LiveConfig.model_validate(cfg)` also catches `ImportError` if `config/live_config_schema.py` or `pydantic` is missing. The error message `"Config validation failed: No module named 'pydantic'"` (or similar) could mislead an operator into thinking the config is malformed rather than that a dependency is missing. A `ModuleNotFoundError` should be re-raised with a clearer message or allowed to propagate.

### N-4 — is_news_blackout() default window too narrow (LOW)
`news_blackout_min = 2` (default in `orb_config.hpp`) blocks only ±2 minutes around news events. CPI/NFP releases regularly cause 30-60 second price spikes that extend beyond 2 minutes. Standard prop firm guidance is 5-10 minutes. The field exists and is configurable, but the default is below industry practice. Not a regression but introduced as a documented default by the fix.

### N-5 — Schema validator enforces symbol == "MNQ" unconditionally (LOW)
`LiveConfig.symbol_must_be_mnq()` in `live_config_schema.py` raises `ValueError` if `symbol != "MNQ"`. This makes the schema non-reusable for any other contract (MES, MYM, etc.) even though `config/MES_config.json`, `config/MNQ_config.json`, and `config/MYM_config.json` exist. Not a live-trading risk, but the schema will prevent validating sibling configs.

---

## 4. Net Status

### Risk Reduction Summary

| Fix | Original Finding | Severity Before | Status |
|-----|-----------------|-----------------|--------|
| trade_route changed | C4 | CRITICAL | RESOLVED |
| daily_loss_limit aligned | H-PY-5 | HIGH | RESOLVED |
| SQL injection in AuditLog | H-SEC-1 | HIGH | RESOLVED |
| MAX_BUF cap | M-AUD-2 | MEDIUM | RESOLVED |
| NaN guard in on_trade_pnl | M-VAL-1 | MEDIUM | RESOLVED |
| PG password redacted in --status | H-SEC-2 | HIGH | RESOLVED |
| Exit rejection retry cap | C2 | CRITICAL | RESOLVED |
| is_news_blackout() implemented | C1 | CRITICAL | RESOLVED |
| Gate K in go_live.py | C6 | CRITICAL | RESOLVED |
| Pydantic validation at startup | C5 | CRITICAL | RESOLVED |
| MNQ constants in tests | M-TEST-1 | MEDIUM | RESOLVED |

**5 of 7 original CRITICALs resolved.** Remaining: C3 (no execution layer tests) and C7 (dry_run NotImplementedError architectural weakness).

### Remaining CRITICAL Exposure

- **C3**: No C++ unit tests for risk/order/strategy — risk math and breakout logic untested without live execution.
- **C7**: Live order path guarded only by Python `NotImplementedError` — easily bypassed by code change.

### Remaining HIGH Exposure (operationally significant)

- H-SEC-5: `NQ_FIRE_TEST_ORDER` hook fires live market orders on env var — immediate removal recommended.
- H-AUD-1/2: Execution layer invisible in audit_log — compliance risk.
- H-REL-1: MD plant login hangs indefinitely if server does not respond — can block the trading session from starting.
- H-PY-1/2: Position reconciliation gaps and order/DB ordering — can result in doubled positions on restart.

### New Issues

4 LOW issues and 1 LOW-MEDIUM issue introduced by the fixes. None are blocking for production.

### Overall Assessment

The fix round eliminated the most immediate production risks (wrong route, wrong loss limit, infinite exit loop, news blackout bypass, SQL injection, config validation). The system is materially safer than the original audit state. The two remaining CRITICALs and the `NQ_FIRE_TEST_ORDER` hook (H-SEC-5) are the highest-priority items before the next live session.
