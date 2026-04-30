# T3 Audit — Python + Test Coverage Gap Analysis

**Agent:** Scout 4  
**Date:** 2026-04-30  
**Scope:** `live_trader.py`, `go_live.py`, `models.py`, `backtest.py`, `audit_data.py`, `migrate_parquet.py`, `tests/*`, `quality_rules/*`, `config/*`, `migrations/*`

---

## Summary

| Severity | Count |
|----------|-------|
| CRITICAL | 4 |
| HIGH     | 10 |
| MEDIUM   | 9 |
| LOW      | 4 |
| **Total**| **27** |

---

## CRITICAL Findings

### T3-C1: `trade_route = "simulator"` in live_config.json
- **File:** `config/live_config.json` (line 11)
- **Issue:** `"trade_route": "simulator"` routes all orders to Rithmic's paper system even when `dry_run=false`.
- **Impact:** Live mode silently executes as paper — no real fills. Operator believes system is trading but it is not.
- **cross_system.yaml BUG-14** documents this as FIXED on 2026-04-28, but the file still shows the old value.
- **Fix:** Change to `"Rithmic Order Routing"` and add go_live.py gate to block promotion if `trade_route == "simulator"`.

### T3-C2: `live_trader.py` Does Not Validate Config With Pydantic Schema at Startup
- **File:** `live_trader.py` `_load_config()` / `config/live_config_schema.py`
- **Issue:** Config is loaded with `json.load()` only. `config/live_config_schema.py` (Pydantic v2) is never imported or called in `live_trader.py`.
- **Impact:** Any invalid config (wrong `point_value`, missing `prop_firm` block, `trade_route='simulator'`) passes undetected at startup. Validation only happens in `go_live.py`, which is only run once at promotion time.
- **Fix:**
  ```python
  from config.live_config_schema import LiveConfig
  cfg = LiveConfig.model_validate(json.load(f)).model_dump()
  ```

### T3-C3: go_live.py Missing Gate for `trade_route`
- **File:** `go_live.py` (promotion gates A–J)
- **Issue:** No gate checks `trade_route != "simulator"`. Gate B validates JSON loads but does not call the Pydantic schema validators.
- **Impact:** A config with `trade_route='simulator'` passes all 10 promotion gates — live promotion completes but real orders are never sent.
- **Fix:** Add gate K:
  ```python
  route = cfg.get("trade_route", "")
  if route == "simulator":
      return GateResult("trade_route must not be 'simulator'", False, route)
  ```

### T3-C4: dry_run Bypass via `NotImplementedError` — Not Compile-Time Enforced
- **File:** `live_trader.py` `_submit_order()`
- **Issue:** The dry_run guard raises `NotImplementedError` in the live path, but this is runtime-only. A single line deletion removes the guard. There is no architectural separation between paper and live order paths.
- **Impact:** Accidental or malicious deletion of the `raise NotImplementedError` line causes silent live trading with no indication.
- **Fix:** Replace with `sys.exit(1)` + critical log on the live path, and add a test `test_dry_run_false_exits_before_order()` asserting `SystemExit` is raised.

---

## HIGH Findings

### T3-H1: `_reconcile_position()` Does Not Query C++ `live_trades` Table
- **File:** `live_trader.py` lines ~143–174
- **Issue:** On restart, `_reconcile_position()` queries `trades WHERE source='python' AND exit_time IS NULL`. It never queries `live_trades` (written by the C++ executor).
- **Impact:** If C++ executor crashed with an open position, Python starts fresh and enters a second position while Rithmic still holds the first → two open positions against one-contract Legends limit.
- **Fix:** Query both `trades` AND `live_trades`, merge results before setting state.

### T3-H2: `_write_trade_open()` Not Wrapped — DB Failure Leaves Orphaned Live Position
- **File:** `live_trader.py` lines ~202–214
- **Issue:** `_submit_order()` (Rithmic submit) is called before `_write_trade_open()` (DB record). If the DB write fails, the position is open in Rithmic but absent from the database.
- **Impact:** On restart, `_reconcile_position()` does not find the open trade. Python enters a second position.
- **Fix:** Reverse order: write DB record first (with `status='pending'`), then submit order, then update status to `'open'`. Use a two-phase commit pattern.

### T3-H3: Hardcoded Commission `$4.00` in `_write_trade_close()`
- **File:** `live_trader.py` line ~228
- **Issue:** `commission_rt = 4.0` is hardcoded. `formula_audit.yaml` defines `COMMISSION_RT=4.0` as a constant but there is no enforcement that the code and the config agree.
- **Impact:** If commission changes (e.g., Legends adjusts rebate), PnL calculations drift silently. No test catches this divergence.
- **Fix:**
  1. Add `"commission_rt": 4.0` to `live_config.json`
  2. Read it in `_write_trade_close()` from config
  3. Add CI test asserting config `commission_rt` matches `formula_audit.yaml` constant

### T3-H4: No PnL Sanity Check Before DB Write
- **File:** `live_trader.py` `_write_trade_close()`
- **Issue:** PnL is calculated and written to DB without any bounds check. `escalation.yaml` documents a `pnl_sanity_check` rule but it is not enforced in code.
- **Impact:** If `point_value` is misconfigured as `20.0` (NQ) instead of `2.0` (MNQ), a 10-point trade reports `$200` PnL instead of `$20` — a 10× error that is not caught until manual reconciliation.
- **Fix:** Add before DB write:
  ```python
  if abs(pnl_usd) > 500:
      log.critical(f"PnL sanity check failed: ${pnl_usd:.2f} exceeds $500 limit. Check point_value.")
      sys.exit(1)
  ```

### T3-H5: `daily_loss_limit` Magnitude Mismatch Between Flat Config and `prop_firm` Block
- **File:** `config/live_config.json`
- **Issue:** `"daily_loss_limit": -200.0` (flat key used by C++) vs `"prop_firm.daily_loss_limit": 2000.0` (used by Python). Different sign convention AND different magnitude (200 vs 2000).
- **Impact:** C++ executor halts at $200 daily loss; Legends limit is $2000. Risk management is 10× too tight on C++ side — or 10× too loose depending on which value is authoritative.
- **Fix:** Align both to the same value with documented sign convention. Cross-system.yaml BUG-7 mentions sign, but not the 10× magnitude discrepancy.

### T3-H6: No Test for Network Disconnect Mid-Trade
- **File:** `tests/test_live_trader.py`
- **Issue:** No test simulates PostgreSQL connection loss after order submission but before DB write confirmation.
- **Impact:** Orphaned position scenario (T3-H2) is not caught by any test.
- **Fix:** Add test using `mock_pg_conn.execute.side_effect = psycopg2.OperationalError`. Assert position state rolls back or is marked for reconciliation.

### T3-H7: No Test for SIGTERM During `_write_session_summary()`
- **File:** `tests/test_live_trader.py`
- **Issue:** No test simulates SIGTERM arriving during end-of-day session summary commit.
- **Impact:** Session summary could be partially written (trade count inconsistent), only caught on next-day reconciliation.
- **Fix:** Add test: raise `signal.SIGTERM` during `_write_session_summary()`, assert `session_summary.crash_exit=True` and trade count is either complete or absent (not partial).

### T3-H8: DOWN Migrations Are Commented Out — No Executable Rollback
- **File:** `migrations/*.sql`
- **Issue:** All migration files have DOWN steps in comments (`-- To roll back, copy and run these manually`). No migration runner tracks applied state.
- **Impact:** Database recovery after failed deployment requires manual intervention with copy-paste SQL. In a live trading emergency, this is unacceptably slow.
- **Fix:** Introduce Alembic or Flyway. At minimum, create paired `*_down.sql` files that are tested in CI.

### T3-H9: `go_live.py` Gate B Does Not Call Pydantic Validation
- **File:** `go_live.py` gate B
- **Issue:** Gate B verifies the config JSON is loadable, but does not call `LiveConfig.model_validate()`. Validator constraints (e.g., `trade_route_not_simulator`, `point_value_must_be_2`) are never exercised during promotion.
- **Impact:** Configs with invalid values pass gate B and proceed through remaining 9 gates.
- **Fix:** Change gate B to call `LiveConfig.model_validate(cfg_dict)` and return FAIL if `ValidationError` raised.

### T3-H10: `conftest.py` Provides No DB or Rithmic Mock Fixtures
- **File:** `tests/conftest.py`
- **Issue:** `conftest.py` only provides timeout markers and C++ binary skip logic. No shared `mock_pg_conn` or `mock_rithmic_client` fixtures are defined.
- **Impact:** Each test file defines its own mocks inconsistently. Tests may attempt real connections if a mock is missed.
- **Fix:** Add shared fixtures:
  ```python
  @pytest.fixture
  def mock_pg_conn():
      return MagicMock(spec=psycopg2.extensions.connection)

  @pytest.fixture
  def mock_rithmic_client():
      return MagicMock()
  ```

---

## MEDIUM Findings

### T3-M1: `_reconcile_position()` Uses `ORDER BY entry_time DESC` (Returns Newest, Not Oldest)
- **File:** `live_trader.py` line ~157
- **Issue:** If two open positions exist (shouldn't, but defensive), the newest is selected. The oldest (higher risk, further from stop) should be prioritized.
- **Fix:** Change to `ORDER BY entry_time ASC LIMIT 1` with comment explaining intent.

### T3-M2: `backtest.py` Does Not Share ORB Signal Logic With Live Trader
- **File:** `backtest.py`
- **Issue:** `backtest.py` imports from `strategy.micro_orb` but `live_trader.py` uses its own internal signal generation. No test confirms both produce identical signals on the same input.
- **Impact:** Backtest results are not representative of live execution.
- **Fix:** Add `test_backtest_live_signal_parity()` feeding identical bars to both paths and asserting identical entry/exit signals.

### T3-M3: `migrate_parquet.py` Has No Idempotency Guard
- **File:** `migrate_parquet.py`
- **Issue:** Migration progress is tracked in `data/migrate_progress.json`, but if the file is deleted or corrupted, the migration reruns and inserts duplicate rows.
- **Impact:** Duplicate trade records inflate PnL history and audit logs.
- **Fix:** Use `INSERT ... ON CONFLICT DO NOTHING` (already used in some places) and add a `migrated_at` timestamp to parquet metadata.

### T3-M4: `test_orb_parity.py` Only Tests C++ Binary Output — Not Source Parity
- **File:** `tests/test_orb_parity.py`
- **Issue:** The test runs `build/orb_strategy` binary and compares output to Python. If the binary is stale (not rebuilt after source change), the test passes with outdated C++ logic.
- **Impact:** C++ logic drift is undetected until next build.
- **Fix:** Add a pre-test step verifying binary mtime > source mtime. Or run `cmake --build build` in test setup.

### T3-M5: `models.py` `ensure_schema()` and `migrations/001_trades.sql` Are Dual Sources of Truth
- **File:** `models.py`, `migrations/001_trades.sql`
- **Issue:** Both define the `trades` table. If they drift, production DB may differ from what `models.py` expects.
- **Impact:** Silent schema mismatch causes runtime errors or data corruption.
- **Fix:** Document canonical source (migrations) and add a CI test comparing `\d trades` against `models.py` column list.

### T3-M6: `quality_rules/cross_system.yaml` Contracts Not Covered by `test_audit_system.py`
- **File:** `tests/test_audit_system.py`, `quality_rules/cross_system.yaml`
- **Issue:** `cross_system.yaml` defines 11 contracts (XSYS-001 to XSYS-011), but `test_audit_system.py` only covers formula test vectors (TV-001 to TV-004). No test exercises XSYS contracts.
- **Fix:** Add parametrized test:
  ```python
  @pytest.mark.parametrize("contract_id", ["XSYS-001", ..., "XSYS-011"])
  def test_cross_system_contract(contract_id): ...
  ```

### T3-M7: `escalation.yaml` Rules Are Advisory — Not Code-Enforced
- **File:** `quality_rules/escalation.yaml`
- **Issue:** Escalation rules (e.g., `pnl_sanity_check`, `point_value_mismatch_critical`) exist in YAML but are read only by `scripts/audit_daemon.py` as a background process. They are not enforced synchronously in `live_trader.py`.
- **Impact:** A point-value error produces a log warning from `audit_daemon.py` minutes later, not an immediate halt.
- **Fix:** Add synchronous checks for CRITICAL escalation rules inside `live_trader.py` event loop.

### T3-M8: `config/live_config_schema.py` Pydantic Errors Not Human-Readable on Failure
- **File:** `config/live_config_schema.py`
- **Issue:** If `model_validate()` fails, the raw `ValidationError` is raised. Operators see cryptic Pydantic error messages, not actionable guidance.
- **Fix:** Add error handler:
  ```python
  except ValidationError as e:
      for err in e.errors():
          print(f"Config error at '{'.'.join(str(x) for x in err['loc'])}': {err['msg']}")
      sys.exit(1)
  ```

### T3-M9: `ui/app.py` Flask Dashboard Has No Authentication
- **File:** `ui/app.py`, `ui/routers/live.py`
- **Issue:** Live position, PnL, and equity endpoints are exposed with no auth. Deployment on Oracle VM binds to all interfaces by default (`0.0.0.0`).
- **Impact:** Any host on the same network can read live trading state.
- **Fix:** Add HTTP Basic Auth or API key header check. Bind to `127.0.0.1` only in systemd service.

---

## LOW Findings

### T3-L1: Win Rate Counts Breakeven Trades as Losses
- **File:** `models.py` `SessionSummary`
- **Issue:** `wins = sum(1 for t in completed if (t.pnl or 0.0) > 0)` — trades with `pnl=0.0` are losses.
- **Fix:** Use `>= 0` or add a separate `breakeven_count` field.

### T3-L2: `backtest.py` Max Drawdown Is Trade-Level, Not Intraday-Level
- **File:** `backtest.py`, `models.py` `SessionSummary`
- **Issue:** Max drawdown is computed across trade PnLs, not tick-by-tick equity. Intraday excursions are not captured.
- **Fix:** Track running equity in backtest loop; update max drawdown at each tick.

### T3-L3: `go_live.py` Missing Gate for C++ Executor Binary Availability
- **File:** `go_live.py`
- **Issue:** Promotion completes without verifying that `build/nq_executor` exists and is executable. If the binary is missing, systemd fails to start after promotion.
- **Fix:** Add gate: `assert os.access("build/nq_executor", os.X_OK), "nq_executor binary not found"`.

### T3-L4: `migrate_parquet.py` Logs Contain Plaintext Parquet File Paths
- **File:** `migrate_parquet.py`
- **Issue:** Log output includes full file paths that may contain account identifiers or dates that reveal trading activity.
- **Impact:** Low (local logs, not remote), but worth sanitizing for compliance.
- **Fix:** Log only filename, not full path.

---

## Cross-Cutting Observations

### Config Dual-Key Pattern (Flat vs Nested)
`live_config.json` has both flat keys (used by C++, e.g., `"orb_minutes"`, `"daily_loss_limit"`) and nested keys (used by Python, e.g., `"orb.orb_period_minutes"`, `"prop_firm.daily_loss_limit"`). These can drift independently. T3-H5 is one example where they already have different values.

**Recommendation:** Document the dual-key contract explicitly in `config/live_config_schema.py` and add a CI test that flat and nested representations agree for all shared parameters.

### Quality Rules Without Tests
`quality_rules/*.yaml` defines 30+ rules but the corresponding test coverage (via `scripts/formula_audit.py`, `scripts/cross_system_audit.py`) is invoked only in `make quality-gate`, not in `make test`. Rules can be violated without any CI signal on `make test`.

**Recommendation:** Either fold audit scripts into pytest (so `pytest` runs them) or ensure `make test` calls `make quality-gate` as a dependency.

---

## Prioritized Fix List

| Priority | ID | Fix |
|----------|----|-----|
| P0 | T3-C1 | Change `trade_route` from `"simulator"` to `"Rithmic Order Routing"` in live_config.json |
| P0 | T3-C3 | Add go_live.py gate K: block promotion if `trade_route == "simulator"` |
| P0 | T3-C2 | Use Pydantic in `live_trader.py` `_load_config()` |
| P0 | T3-H5 | Resolve `daily_loss_limit` mismatch: flat `−200` vs nested `+2000` |
| P1 | T3-H4 | Add PnL sanity check (halt if `abs(pnl) > 500`) |
| P1 | T3-H3 | Move `commission_rt` from hardcode to config |
| P1 | T3-H1 | Query both `trades` and `live_trades` in `_reconcile_position()` |
| P1 | T3-H2 | Write DB record before submitting order (two-phase commit) |
| P1 | T3-H9 | Call Pydantic in go_live.py gate B |
| P2 | T3-H6 | Add test: network disconnect mid-trade |
| P2 | T3-H7 | Add test: SIGTERM during session summary write |
| P2 | T3-H8 | Create executable DOWN migration files |
| P2 | T3-H10 | Add shared mock fixtures to conftest.py |
| P3 | T3-M6 | Add parametrized XSYS contract tests |
| P3 | T3-M7 | Make CRITICAL escalation rules synchronous in live_trader.py |
| P3 | T3-M9 | Add Flask auth and bind to 127.0.0.1 |
