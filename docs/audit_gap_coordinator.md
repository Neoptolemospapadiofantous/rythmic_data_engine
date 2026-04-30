# Coordinator Audit Findings — Cross-Cutting Gaps

> Compiled by Coordinator 1 during swarm execution.
> These are findings from direct code analysis that cross task boundaries.

---

## 1. SQL Injection in AuditLog::flush() — HIGH

**File:** `src/audit.cpp:71-86`

`flush()` builds a multi-row INSERT via string concatenation. Only the `details` field has a manual escape lambda (`esc_details`). The `event` and `severity` fields are concatenated raw:

```cpp
sql += "('" + batch[i].ts + "',"
       "'" + batch[i].event + "',"      // ← NOT escaped
       "'" + batch[i].severity + "',"   // ← NOT escaped
       + esc_details(batch[i].details) + ")";
```

`event` originates from internal callers and is currently controlled strings, but any future caller that allows user/external data to reach `event` opens a SQL injection path. `severity` is enum-derived (safe), but `event` is a free string parameter.

**Recommended fix:** Replace with `PQexecParams` using `$1..$N` placeholders for all columns. This also eliminates the need for the manual `esc_details` lambda.

---

## 2. Execution Layer Has No AuditLog Integration — HIGH

**File:** `src/execution/executor_main.cpp`

```
grep -n "AuditLog|audit_" src/execution/executor_main.cpp → (no results)
```

The execution layer (order submissions, fills, risk halts, strategy signals) writes ONLY to LOG() (stdout/file). None of these events reach the `audit_log` PostgreSQL table. This means:

- Order submissions are not audited
- Fill confirmations are not audited
- Risk halts (trailing drawdown trips, daily loss trips, consistency cap trips) are not audited
- EOD flattens are not audited

The `live_sessions` table captures `risk_halted` flag and `halt_reason`, but there is no timestamped event-level audit trail.

**Recommended fix:** Pass an `AuditLog*` into `OrbStrategy` or `Executor`. Log at minimum: `order.submitted`, `order.filled`, `order.cancelled`, `risk.halted`, `risk.reset`, `strategy.signal`.

---

## 3. DataSentinel Alerts Not in audit_log — MEDIUM

**Files:** `src/collector.cpp:242-254`, `src/validator.hpp`

`flush_sentinel()` drains `DataSentinel` alerts to the `sentinel_alerts` table — a **separate table**, not `audit_log`. The audit trail (queried by `audit_data.py` and `audit_engine.py`) does NOT include market data anomalies (price jumps, crossed markets, timestamp gaps, volume spikes).

**Gap:** An operator reviewing `audit_log` would not see that 47 price-jump alerts fired during a session.

**Recommended fix:** Either cross-write sentinel CRITICAL/ERROR severity alerts to `audit_log`, or update `audit_engine.py` to include sentinel alert counts in its audit report.

---

## 4. AuditLog Buffer Unbounded on DB Failure — MEDIUM

**File:** `src/audit.cpp:76-93`

On flush failure, events are re-queued:
```cpp
buf_.insert(buf_.begin(), batch.begin(), batch.end());
```

If the DB is down for an extended period, this buffer grows without bound. Under a prolonged outage, this could cause OOM.

**Recommended fix:** Add a `max_buf_size_` limit (e.g., 10,000 events). When exceeded, drop oldest events and emit a WARN log. The alternative (dropping newest) risks losing the failure event itself.

---

## 5. test_live_trader.py Uses Wrong Contract Constants — MEDIUM

**File:** `tests/test_live_trader.py:47-50`

The shared test config helper `_make_config()` uses:
```python
"point_value": 20.0,   # NQ value — NOT MNQ
"symbol": "NQ",        # wrong symbol
```

Tests using this helper exercise the strategy with NQ parameters, not MNQ. PnL assertions in those tests would be off by 10x versus production behavior. The strategy under test is for MNQ (point_value=2.0).

**Recommended fix:** Change `_make_config()` defaults to `point_value: 2.0` and `symbol: "MNQ"`. Add a test that explicitly verifies the default helper uses correct MNQ values.

---

## 6. No Tests for AuditLog SQL Injection Resilience — MEDIUM

**File:** `tests/test_db.cpp:159-180`

`TEST(audit_log)` writes three clean events and verifies row count. It does NOT test:

- Logging an event string containing SQL metacharacters: `"event'; DROP TABLE audit_log; --"`
- Logging details with quotes: `"detail='value'"`
- Concurrent log() calls from multiple threads while flush() runs
- Behavior when DB connection drops mid-flush (re-queue + retry test)

**Recommended additions to test_db.cpp:**
```cpp
TEST(audit_log_sql_metacharacters) {
    AuditLog audit(db.conn());
    audit.info("test.event'; DROP TABLE audit_log; --", "details='injected'");
    audit.flush();
    // DB must still exist and have the row
    // verify COUNT(*) FROM audit_log WHERE event LIKE 'test.event%' == 1
}
```

---

## 7. audit_engine.py Does Not Verify Execution Audit Coverage — MEDIUM

**File:** `tests/audit_engine.py:178-179`

The source invariant check only verifies `audit_log` appears in `dashboard.cpp`. It does NOT check whether `executor_main.cpp`, `orb_strategy.cpp`, or `risk_manager.hpp` contain audit calls.

**Recommended addition to Section 2 (source invariants):**
```python
("audit_log or AuditLog in executor_main.cpp", SRC_DIR / "execution/executor_main.cpp", "audit", True),
("audit_log or AuditLog in risk_manager.hpp",  SRC_DIR / "execution/risk_manager.hpp",  "audit", True),
```

---

## 8. No Integration Test Verifying End-to-End Audit Trail — HIGH

There is no test that:
1. Executes a simulated trade through the full stack (strategy → executor → risk manager)
2. Verifies the resulting `audit_log` entries are present in PostgreSQL

The existing `TEST(audit_log)` in `test_db.cpp` tests the AuditLog class in isolation. `test_live_trader.py` mocks the DB entirely. No test closes the loop: "given a trade, are the audit events actually in the DB?"

**Recommended fix:** Add an integration test (requires test DB) that drives a full trade cycle via `OrbStrategy` + `Executor` and then asserts `audit_log` contains `order.submitted`, `order.filled`.

---

## 9. Missing Quality Rules Coverage — LOW

**Directory:** `quality_rules/`

Check whether quality_rules YAML files cover:
- Execution layer audit event presence (currently not covered by any rule based on source invariant checks)
- Sentinel alert rate thresholds
- AuditLog flush interval (currently 60s — no rule enforcing this doesn't drift)

---

## Summary Table

| # | File(s) | Severity | Category |
|---|---------|----------|----------|
| 1 | src/audit.cpp:71 | HIGH | SQL Injection |
| 2 | src/execution/executor_main.cpp | HIGH | Missing Audit Events |
| 3 | src/collector.cpp:242, validator.hpp | MEDIUM | Audit Trail Gap |
| 4 | src/audit.cpp:89 | MEDIUM | Resource Exhaustion |
| 5 | tests/test_live_trader.py:47 | MEDIUM | Wrong Test Constants |
| 6 | tests/test_db.cpp:159 | MEDIUM | Missing Test Coverage |
| 7 | tests/audit_engine.py:178 | MEDIUM | Incomplete Source Audit |
| 8 | (no file) | HIGH | Missing Integration Test |
| 9 | quality_rules/ | LOW | Rule Coverage Gap |
