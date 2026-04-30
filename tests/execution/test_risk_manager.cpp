/*  ═══════════════════════════════════════════════════════════════════════════
    test_risk_manager.cpp — Unit tests for RiskManager (C3 audit finding)

    Standalone: no external test framework required.
    Build (from repo root):
        g++ -std=c++20 -Isrc/execution -Isrc \
            tests/execution/test_risk_manager.cpp \
            -o build/test_risk_manager && ./build/test_risk_manager

    CMake target suggestion (add to CMakeLists.txt):
        add_executable(test_risk_manager tests/execution/test_risk_manager.cpp)
        target_include_directories(test_risk_manager PRIVATE src/execution src/)
        target_compile_features(test_risk_manager PRIVATE cxx_std_20)
        add_test(NAME risk_manager_tests COMMAND test_risk_manager)
    ═══════════════════════════════════════════════════════════════════════════ */
#include <iostream>
#include <cassert>
#include <stdexcept>
#include <cmath>
#include <limits>
#include <string>

// RiskManager is header-only (risk_manager.hpp inlines all logic).
// orb_config.hpp and log.hpp are in src/execution/ — no libpq needed.
#include "../../src/execution/risk_manager.hpp"

// ─── Minimal test harness ─────────────────────────────────────────────────────
static int tests_run = 0, tests_failed = 0;

#define TEST(name) void test_##name()
#define RUN(name) do { \
    ++tests_run; \
    try { test_##name(); std::cout << "PASS " #name "\n"; } \
    catch (std::exception& e) { ++tests_failed; std::cout << "FAIL " #name ": " << e.what() << "\n"; } \
} while(0)
#define ASSERT(cond) do { if (!(cond)) throw std::runtime_error("Assert failed: " #cond); } while(0)
#define ASSERT_EQ(a, b) do { if ((a) != (b)) throw std::runtime_error("ASSERT_EQ failed: " #a " != " #b); } while(0)
#define ASSERT_NEAR(a, b, eps) do { if (std::abs((a)-(b)) > (eps)) throw std::runtime_error("ASSERT_NEAR failed: " #a " vs " #b); } while(0)

// ─── Helper: build a minimal OrbConfig for testing ───────────────────────────
static OrbConfig make_cfg(double daily_loss_limit  = -1000.0,
                          double trailing_dd_cap   = 2500.0,
                          double consistency_cap   = 0.30) {
    OrbConfig c;
    c.daily_loss_limit      = daily_loss_limit;
    c.trailing_drawdown_cap = trailing_dd_cap;
    c.consistency_cap_pct   = consistency_cap;
    return c;
}

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

// 1. No halt when P&L is within all limits
TEST(no_halt_within_limits) {
    OrbConfig cfg = make_cfg(-2000.0, 2500.0);
    RiskManager rm(cfg, 50000.0);
    rm.on_trade_pnl(-500.0);
    ASSERT(!rm.halted());
    ASSERT(rm.can_trade());
    ASSERT_NEAR(rm.daily_pnl(), -500.0, 0.001);
}

// 2. Halt when daily loss limit is breached (daily_pnl <= limit)
TEST(halt_on_daily_loss_limit) {
    OrbConfig cfg = make_cfg(-1000.0, 2500.0);
    RiskManager rm(cfg, 50000.0);
    rm.on_trade_pnl(-1001.0);  // strictly below the -1000 limit
    ASSERT(rm.halted());
    std::string reason;
    ASSERT(!rm.can_trade(reason));
    ASSERT(!reason.empty());
}

// 3. No halt when loss exactly equals the limit boundary (boundary inclusive check)
//    API: halts when daily_pnl_ <= daily_loss_limit, so exactly at limit IS a halt
TEST(halt_at_daily_loss_limit_boundary) {
    OrbConfig cfg = make_cfg(-1000.0, 2500.0);
    RiskManager rm(cfg, 50000.0);
    rm.on_trade_pnl(-1000.0);  // exactly at limit — should halt (<=)
    ASSERT(rm.halted());
}

// 4. NaN guard — non-finite PnL triggers immediate halt
TEST(halt_on_nan_pnl) {
    OrbConfig cfg = make_cfg(-1000.0, 2500.0);
    RiskManager rm(cfg, 50000.0);
    rm.on_trade_pnl(std::numeric_limits<double>::quiet_NaN());
    ASSERT(rm.halted());
}

// 5. Inf guard — positive infinity also triggers halt
TEST(halt_on_inf_pnl) {
    OrbConfig cfg = make_cfg(-1000.0, 2500.0);
    RiskManager rm(cfg, 50000.0);
    rm.on_trade_pnl(std::numeric_limits<double>::infinity());
    ASSERT(rm.halted());
}

// 6. Negative infinity also triggers halt
TEST(halt_on_neg_inf_pnl) {
    OrbConfig cfg = make_cfg(-1000.0, 2500.0);
    RiskManager rm(cfg, 50000.0);
    rm.on_trade_pnl(-std::numeric_limits<double>::infinity());
    ASSERT(rm.halted());
}

// 7. Trailing drawdown: equity drop from peak >= cap triggers halt
TEST(halt_on_trailing_drawdown) {
    OrbConfig cfg = make_cfg(-999999.0, 2500.0);  // large daily limit so only dd fires
    RiskManager rm(cfg, 50000.0);
    // Grow peak to 52000, then drop 2500 points -> drawdown exactly 2500 -> halt
    rm.on_trade_pnl(2000.0);   // equity = 52000, peak = 52000
    ASSERT(!rm.halted());
    rm.on_trade_pnl(-2500.0);  // equity = 49500, drawdown from peak = 2500 >= 2500
    ASSERT(rm.halted());
}

// 8. Trailing drawdown does NOT halt when drawdown is just below cap
TEST(no_halt_below_trailing_drawdown_cap) {
    OrbConfig cfg = make_cfg(-999999.0, 2500.0);
    RiskManager rm(cfg, 50000.0);
    rm.on_trade_pnl(2000.0);   // equity = 52000, peak = 52000
    rm.on_trade_pnl(-2499.0);  // equity = 49501, drawdown = 2499 < 2500
    ASSERT(!rm.halted());
}

// 9. Peak equity tracking — profitable trade raises the peak, subsequent loss
//    is measured from that higher peak
TEST(peak_equity_rises_after_profit) {
    OrbConfig cfg = make_cfg(-999999.0, 2500.0);
    RiskManager rm(cfg, 50000.0);
    ASSERT_NEAR(rm.peak_equity(), 50000.0, 0.001);

    rm.on_trade_pnl(3000.0);
    ASSERT_NEAR(rm.peak_equity(), 53000.0, 0.001);
    ASSERT_NEAR(rm.equity(),      53000.0, 0.001);

    // Lose 1000 — peak stays at 53000, drawdown = 1000, no halt
    rm.on_trade_pnl(-1000.0);
    ASSERT_NEAR(rm.peak_equity(), 53000.0, 0.001);
    ASSERT_NEAR(rm.equity(),      52000.0, 0.001);
    ASSERT(!rm.halted());
}

// 10. Peak does not fall after a loss
TEST(peak_does_not_fall_on_loss) {
    OrbConfig cfg = make_cfg(-999999.0, 2500.0);
    RiskManager rm(cfg, 50000.0);
    rm.on_trade_pnl(1000.0);  // peak = 51000
    rm.on_trade_pnl(-500.0);  // equity = 50500, peak still 51000
    ASSERT_NEAR(rm.peak_equity(), 51000.0, 0.001);
}

// 11. Daily reset clears daily_pnl and halted flag; peak and equity persist
TEST(reset_daily_clears_halt) {
    OrbConfig cfg = make_cfg(-1000.0, 2500.0);
    RiskManager rm(cfg, 50000.0);
    rm.on_trade_pnl(-1001.0);
    ASSERT(rm.halted());

    rm.reset_daily();
    ASSERT(!rm.halted());
    ASSERT(rm.can_trade());
    ASSERT_NEAR(rm.daily_pnl(), 0.0, 0.001);
    // Equity and peak are NOT reset — they persist across days
    ASSERT_NEAR(rm.equity(), 48999.0, 0.001);
}

// 12. Consistency cap: today's profit > 30% of prior cumulative profit triggers halt
//     Example: prior_profit = 1000, daily_pnl = 301 → 30.1% > 30% → halt
TEST(halt_on_consistency_cap) {
    OrbConfig cfg = make_cfg(-999999.0, 9999999.0, 0.30);
    RiskManager rm(cfg, 50000.0);
    // Seed 1000 of historical profit across prior days
    rm.seed_total_profit(1000.0);
    // Now trade today — 301 profit → daily_pnl/prior_profit = 301/1000 = 30.1% > 30%
    rm.on_trade_pnl(301.0);
    ASSERT(rm.halted());
}

// 13. Consistency cap not triggered when profit is within 30%
TEST(no_halt_under_consistency_cap) {
    OrbConfig cfg = make_cfg(-999999.0, 9999999.0, 0.30);
    RiskManager rm(cfg, 50000.0);
    rm.seed_total_profit(1000.0);
    rm.on_trade_pnl(299.0);  // 299/1000 = 29.9% < 30%
    ASSERT(!rm.halted());
}

// 14. Consistency cap not applied when prior_profit <= 0 (first trading day)
TEST(no_consistency_cap_on_first_day) {
    OrbConfig cfg = make_cfg(-999999.0, 9999999.0, 0.30);
    RiskManager rm(cfg, 50000.0);
    // total_profit_ starts at 0; after a +500 trade: prior_profit = 0 - 500 = -500 (no halt)
    rm.on_trade_pnl(500.0);
    ASSERT(!rm.halted());
}

// 15. set_equity updates equity and peak but does NOT trigger halt
TEST(set_equity_updates_peak_no_halt) {
    OrbConfig cfg = make_cfg(-999999.0, 2500.0);
    RiskManager rm(cfg, 50000.0);
    rm.set_equity(55000.0);
    ASSERT_NEAR(rm.equity(),      55000.0, 0.001);
    ASSERT_NEAR(rm.peak_equity(), 55000.0, 0.001);
    ASSERT(!rm.halted());
}

// 16. Multiple small losses accumulate: each within limit, but total exceeds it
TEST(cumulative_loss_breaches_daily_limit) {
    OrbConfig cfg = make_cfg(-1000.0, 2500.0);
    RiskManager rm(cfg, 50000.0);
    rm.on_trade_pnl(-400.0);
    ASSERT(!rm.halted());
    rm.on_trade_pnl(-400.0);
    ASSERT(!rm.halted());
    rm.on_trade_pnl(-201.0);  // daily_pnl = -1001 -> halt
    ASSERT(rm.halted());
}

// ═══════════════════════════════════════════════════════════════════════════════
int main() {
    RUN(no_halt_within_limits);
    RUN(halt_on_daily_loss_limit);
    RUN(halt_at_daily_loss_limit_boundary);
    RUN(halt_on_nan_pnl);
    RUN(halt_on_inf_pnl);
    RUN(halt_on_neg_inf_pnl);
    RUN(halt_on_trailing_drawdown);
    RUN(no_halt_below_trailing_drawdown_cap);
    RUN(peak_equity_rises_after_profit);
    RUN(peak_does_not_fall_on_loss);
    RUN(reset_daily_clears_halt);
    RUN(halt_on_consistency_cap);
    RUN(no_halt_under_consistency_cap);
    RUN(no_consistency_cap_on_first_day);
    RUN(set_equity_updates_peak_no_halt);
    RUN(cumulative_loss_breaches_daily_limit);

    std::cout << "\n" << (tests_run - tests_failed) << "/" << tests_run << " passed\n";
    return tests_failed > 0 ? 1 : 0;
}
