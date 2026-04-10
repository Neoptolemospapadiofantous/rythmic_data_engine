// Unit tests for TickValidator — no DB or network needed.
//
// Run: ./build/test_validator

#include <cassert>
#include <chrono>
#include <cstdio>
#include <stdexcept>
#include <string>

#include "../src/db.hpp"
#include "../src/validator.hpp"

// ── helpers ────────────────────────────────────────────────────────

static int g_passed = 0;
static int g_failed = 0;

#define TEST(name) \
    static void test_##name(); \
    struct _reg_##name { _reg_##name() { \
        std::printf("  %-40s ", #name); \
        try { test_##name(); std::printf("PASS\n"); ++g_passed; } \
        catch (std::exception& e) { std::printf("FAIL: %s\n", e.what()); ++g_failed; } \
    }} _inst_##name; \
    static void test_##name()

#define ASSERT(expr) \
    if (!(expr)) throw std::runtime_error("Assertion failed: " #expr)

#define ASSERT_EQ(a, b) \
    if ((a) != (b)) throw std::runtime_error( \
        std::string("Expected ") + std::to_string(b) + " got " + std::to_string(a))

// ── helpers ────────────────────────────────────────────────────────

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

// ── tests ──────────────────────────────────────────────────────────

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
    r.price = 2'000'000.0;  // well above MAX_PRICE (1 000 000)
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
    r.size = 100'000;  // above MAX_SIZE (50 000)
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
    // 2 hours in the future — beyond the 1-hour drift limit
    r.ts_micros += 2LL * 3600LL * 1'000'000LL;
    std::string reason;
    ASSERT(!TickValidator::valid(r, &reason));
}

TEST(timestamp_past_drift_rejected) {
    TickRow r = make_valid_tick();
    // 2 hours in the past — beyond the 1-hour drift limit
    r.ts_micros -= 2LL * 3600LL * 1'000'000LL;
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
    r.symbol = "NQ\x01";  // contains non-printable byte 0x01
    std::string reason;
    ASSERT(!TickValidator::valid(r, &reason));
}

TEST(reason_string_populated) {
    // Any invalid tick must fill reason with a non-empty string
    TickRow r = make_valid_tick();
    r.price = 0.0;
    std::string reason;
    bool ok = TickValidator::valid(r, &reason);
    ASSERT(!ok);
    ASSERT(!reason.empty());
}

TEST(reason_null_ok) {
    // Passing nullptr for reason must not crash on either valid or invalid tick
    TickRow valid = make_valid_tick();
    ASSERT(TickValidator::valid(valid, nullptr));  // valid tick, null reason

    TickRow bad = make_valid_tick();
    bad.price = -5.0;
    ASSERT(!TickValidator::valid(bad, nullptr));  // invalid tick, null reason — no crash
}

// ── main ───────────────────────────────────────────────────────────

int main() {
    std::printf("\n=== TickValidator unit tests ===\n\n");

    // Tests execute via static initializers (TEST macro) — nothing to call here.

    std::printf("\n=== Results: %d passed, %d failed ===\n\n",
                g_passed, g_failed);

    return g_failed > 0 ? 1 : 0;
}
