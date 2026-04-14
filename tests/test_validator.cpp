// Unit tests for TickValidator + DataSentinel — no DB or network needed.
//
// Run: ./build/test_validator

#include <cassert>
#include <chrono>
#include <cstdio>
#include <stdexcept>
#include <string>
#include <thread>

#include "../src/db.hpp"
#include "../src/validator.hpp"

// ── helpers ────────────────────────────────────────────────────────

static int g_passed = 0;
static int g_failed = 0;

#define TEST(name) \
    static void test_##name(); \
    struct _reg_##name { _reg_##name() { \
        std::printf("  %-50s ", #name); \
        try { test_##name(); std::printf("PASS\n"); ++g_passed; } \
        catch (std::exception& e) { std::printf("FAIL: %s\n", e.what()); ++g_failed; } \
    }} _inst_##name; \
    static void test_##name()

#define ASSERT(expr) \
    if (!(expr)) throw std::runtime_error("Assertion failed: " #expr)

#define ASSERT_EQ(a, b) \
    if ((a) != (b)) throw std::runtime_error( \
        std::string("Expected ") + std::to_string(b) + " got " + std::to_string(a))

// Returns a TickRow with all fields set to valid values.
static TickRow make_valid_tick() {
    int64_t now_us = std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    TickRow r;
    r.ts_micros = now_us;
    r.price     = 21000.0;
    r.size      = 5;
    r.is_buy    = true;
    r.symbol    = "NQ";
    r.exchange  = "CME";
    return r;
}

// ═══════════════════════════════════════════════════════════════════
// TickValidator tests
// ═══════════════════════════════════════════════════════════════════

TEST(valid_tick_passes) {
    TickRow r = make_valid_tick();
    std::string reason;
    ASSERT(TickValidator::valid(r, &reason));
    ASSERT(reason.empty());
}

TEST(price_zero_rejected) {
    TickRow r = make_valid_tick();
    r.price = 0.0;
    std::string reason;
    bool ok = TickValidator::valid(r, &reason);
    ASSERT(!ok);
    ASSERT(reason.find("price") != std::string::npos);
}

TEST(price_negative_rejected) {
    TickRow r = make_valid_tick();
    r.price = -1.0;
    std::string reason;
    ASSERT(!TickValidator::valid(r, &reason));
}

TEST(price_too_high_rejected) {
    TickRow r = make_valid_tick();
    r.price = 2'000'000.0;
    std::string reason;
    ASSERT(!TickValidator::valid(r, &reason));
}

TEST(size_zero_rejected) {
    TickRow r = make_valid_tick();
    r.size = 0;
    std::string reason;
    bool ok = TickValidator::valid(r, &reason);
    ASSERT(!ok);
    ASSERT(reason.find("size") != std::string::npos);
}

TEST(size_negative_rejected) {
    TickRow r = make_valid_tick();
    r.size = -1;
    std::string reason;
    ASSERT(!TickValidator::valid(r, &reason));
}

TEST(size_too_large_rejected) {
    TickRow r = make_valid_tick();
    r.size = 100'000;
    std::string reason;
    ASSERT(!TickValidator::valid(r, &reason));
}

TEST(timestamp_zero_rejected) {
    TickRow r = make_valid_tick();
    r.ts_micros = 0;
    std::string reason;
    ASSERT(!TickValidator::valid(r, &reason));
}

TEST(timestamp_future_drift_rejected) {
    TickRow r = make_valid_tick();
    // 49 hours in the future — beyond the 48-hour drift limit
    r.ts_micros += 49LL * 3600LL * 1'000'000LL;
    std::string reason;
    ASSERT(!TickValidator::valid(r, &reason));
}

TEST(timestamp_past_drift_rejected) {
    TickRow r = make_valid_tick();
    // 49 hours in the past — beyond the 48-hour drift limit
    r.ts_micros -= 49LL * 3600LL * 1'000'000LL;
    std::string reason;
    ASSERT(!TickValidator::valid(r, &reason));
}

TEST(symbol_empty_rejected) {
    TickRow r = make_valid_tick();
    r.symbol = "";
    std::string reason;
    ASSERT(!TickValidator::valid(r, &reason));
}

TEST(exchange_empty_rejected) {
    TickRow r = make_valid_tick();
    r.exchange = "";
    std::string reason;
    ASSERT(!TickValidator::valid(r, &reason));
}

TEST(symbol_nonprintable_rejected) {
    TickRow r = make_valid_tick();
    r.symbol = "NQ\x01";
    std::string reason;
    ASSERT(!TickValidator::valid(r, &reason));
}

TEST(reason_string_populated) {
    TickRow r = make_valid_tick();
    r.price = 0.0;
    std::string reason;
    bool ok = TickValidator::valid(r, &reason);
    ASSERT(!ok);
    ASSERT(!reason.empty());
}

TEST(reason_null_ok) {
    TickRow valid = make_valid_tick();
    ASSERT(TickValidator::valid(valid, nullptr));

    TickRow bad = make_valid_tick();
    bad.price = -5.0;
    ASSERT(!TickValidator::valid(bad, nullptr));
}

// ═══════════════════════════════════════════════════════════════════
// DataSentinel tests
// ═══════════════════════════════════════════════════════════════════

TEST(sentinel_no_alerts_on_normal_ticks) {
    DataSentinel::Config cfg;
    cfg.alert_cooldown_sec = 0.0;  // disable rate limiting for tests
    DataSentinel s(cfg);

    int64_t ts = 1700000000000000LL;
    s.observe_tick(21000.0, 5, ts);
    s.observe_tick(21000.25, 3, ts + 100000);  // +0.1s, +0.25 pts = 0.001%
    s.observe_tick(21000.50, 2, ts + 200000);

    auto alerts = s.drain_alerts();
    ASSERT(alerts.empty());
    ASSERT_EQ(s.tick_count(), 3);
}

TEST(sentinel_price_jump_detected) {
    DataSentinel::Config cfg;
    cfg.price_jump_pct = 1.0;
    cfg.alert_cooldown_sec = 0.0;
    DataSentinel s(cfg);

    int64_t ts = 1700000000000000LL;
    s.observe_tick(20000.0, 5, ts);
    s.observe_tick(20500.0, 5, ts + 100000);  // +2.5% jump

    auto alerts = s.drain_alerts();
    ASSERT(!alerts.empty());
    ASSERT(alerts[0].check == "price_jump");
    ASSERT(alerts[0].severity == "WARN");
}

TEST(sentinel_timestamp_backward_detected) {
    DataSentinel::Config cfg;
    cfg.alert_cooldown_sec = 0.0;
    DataSentinel s(cfg);

    int64_t ts = 1700000000000000LL;
    s.observe_tick(21000.0, 5, ts);
    s.observe_tick(21000.25, 5, ts - 1000000);  // 1 second backward

    auto alerts = s.drain_alerts();
    ASSERT(!alerts.empty());
    bool found = false;
    for (auto& a : alerts) {
        if (a.check == "timestamp_backward") found = true;
    }
    ASSERT(found);
}

TEST(sentinel_timestamp_gap_detected) {
    DataSentinel::Config cfg;
    cfg.max_timestamp_gap_sec = 60.0;
    cfg.alert_cooldown_sec = 0.0;
    DataSentinel s(cfg);

    int64_t ts = 1700000000000000LL;
    s.observe_tick(21000.0, 5, ts);
    s.observe_tick(21000.25, 5, ts + 120'000'000LL);  // +120s gap

    auto alerts = s.drain_alerts();
    ASSERT(!alerts.empty());
    bool found = false;
    for (auto& a : alerts) {
        if (a.check == "timestamp_gap") found = true;
    }
    ASSERT(found);
    ASSERT_EQ(s.gap_count(), 1);
}

TEST(sentinel_volume_spike_detected) {
    DataSentinel::Config cfg;
    cfg.volume_spike_multiplier = 3.0;
    cfg.alert_cooldown_sec = 0.0;
    DataSentinel s(cfg);

    int64_t ts = 1700000000000000LL;
    // Feed 15 normal-sized ticks
    for (int i = 0; i < 15; ++i) {
        s.observe_tick(21000.0, 5, ts + i * 100000);
    }
    // Then a huge volume tick
    s.observe_tick(21000.0, 100, ts + 15 * 100000);  // 100 vs avg ~5 = 20x

    auto alerts = s.drain_alerts();
    bool found = false;
    for (auto& a : alerts) {
        if (a.check == "volume_spike") found = true;
    }
    ASSERT(found);
}

TEST(sentinel_crossed_market_detected) {
    DataSentinel::Config cfg;
    cfg.alert_cooldown_sec = 0.0;
    DataSentinel s(cfg);

    s.observe_bbo(21001.0, 21000.0);  // bid > ask = crossed

    auto alerts = s.drain_alerts();
    ASSERT(!alerts.empty());
    ASSERT(alerts[0].check == "crossed_market");
}

TEST(sentinel_wide_spread_detected) {
    DataSentinel::Config cfg;
    cfg.wide_spread_points = 3.0;
    cfg.alert_cooldown_sec = 0.0;
    DataSentinel s(cfg);

    s.observe_bbo(21000.0, 21005.0);  // 5 pt spread > 3 pt threshold

    auto alerts = s.drain_alerts();
    ASSERT(!alerts.empty());
    bool found = false;
    for (auto& a : alerts) {
        if (a.check == "wide_spread") found = true;
    }
    ASSERT(found);
}

TEST(sentinel_normal_bbo_no_alert) {
    DataSentinel::Config cfg;
    cfg.alert_cooldown_sec = 0.0;
    DataSentinel s(cfg);

    s.observe_bbo(21000.0, 21000.25);  // normal 0.25 spread

    auto alerts = s.drain_alerts();
    ASSERT(alerts.empty());
}

TEST(sentinel_rate_limiting) {
    DataSentinel::Config cfg;
    cfg.price_jump_pct = 0.01;  // very sensitive
    cfg.alert_cooldown_sec = 100.0;  // long cooldown
    DataSentinel s(cfg);

    int64_t ts = 1700000000000000LL;
    s.observe_tick(20000.0, 5, ts);
    s.observe_tick(21000.0, 5, ts + 100000);  // big jump — should alert
    s.observe_tick(22000.0, 5, ts + 200000);  // another big jump — rate limited

    auto alerts = s.drain_alerts();
    // Only 1 price_jump alert despite 2 jumps (rate limited)
    int jump_count = 0;
    for (auto& a : alerts) {
        if (a.check == "price_jump") ++jump_count;
    }
    ASSERT_EQ(jump_count, 1);
}

TEST(sentinel_drain_clears_alerts) {
    DataSentinel::Config cfg;
    cfg.alert_cooldown_sec = 0.0;
    DataSentinel s(cfg);

    s.observe_bbo(21001.0, 21000.0);  // crossed
    auto a1 = s.drain_alerts();
    ASSERT(!a1.empty());

    auto a2 = s.drain_alerts();
    ASSERT(a2.empty());  // already drained
}

TEST(sentinel_invalid_price_logged) {
    DataSentinel::Config cfg;
    cfg.alert_cooldown_sec = 0.0;
    DataSentinel s(cfg);

    s.observe_tick(-5.0, 5, 1700000000000000LL);

    auto alerts = s.drain_alerts();
    ASSERT(!alerts.empty());
    ASSERT(alerts[0].check == "invalid_price");
    ASSERT(alerts[0].severity == "ERROR");
}

// ── main ───────────────────────────────────────────────────────────

int main() {
    std::printf("\n=== TickValidator + DataSentinel unit tests ===\n\n");

    // Tests execute via static initializers (TEST macro)

    std::printf("\n=== Results: %d passed, %d failed ===\n\n",
                g_passed, g_failed);

    return g_failed > 0 ? 1 : 0;
}
