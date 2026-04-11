#pragma once
// validator.hpp — Tick data validity checks.
//
// Called before every tick enters the buffer.  Rejects data that is
// structurally wrong or physically implausible so no garbage ever reaches
// the WAL or the database.
//
// Rules (all must pass):
//   price  — finite, > 0, < 1 000 000  (NQ all-time high ~22 000)
//   size   — integer, 1 … 50 000       (50k contracts is implausibly large)
//   ts     — µs since epoch, > 0,
//             within ±36 hours of system clock (Rithmic replays the last known
//             tick on reconnect; NQ has a 23h trading day so after a full day
//             offline the replayed tick can be ~24h old; the unique index on
//             (symbol,exchange,ts_event,price,size) prevents true duplicates)
//   symbol — non-empty, ≤ 32 chars, printable ASCII
//   exchange — non-empty, ≤ 32 chars, printable ASCII

#include <chrono>
#include <cmath>
#include <cstdint>
#include <string>

#include "db.hpp"   // TickRow

struct TickValidator {

    static constexpr int64_t MAX_DRIFT_US   = 129'600'000'000LL; // 36 hours
    static constexpr double  MAX_PRICE      = 1'000'000.0;
    static constexpr int64_t MAX_SIZE       = 50'000;

    // Returns true if the tick should be accepted.
    // If reason is non-null it is filled with a short rejection cause.
    [[nodiscard]]
    static bool valid(const TickRow& r, std::string* reason = nullptr) {
        auto fail = [&](const char* msg) -> bool {
            if (reason) *reason = msg;
            return false;
        };

        // ── symbol / exchange ─────────────────────────────────────────
        if (r.symbol.empty() || r.symbol.size() > 32)
            return fail("symbol empty or too long");
        if (r.exchange.empty() || r.exchange.size() > 32)
            return fail("exchange empty or too long");

        for (char c : r.symbol)
            if (c < 0x20 || c > 0x7e) return fail("symbol non-printable");
        for (char c : r.exchange)
            if (c < 0x20 || c > 0x7e) return fail("exchange non-printable");

        // ── price ─────────────────────────────────────────────────────
        if (!std::isfinite(r.price) || r.price <= 0.0 || r.price > MAX_PRICE)
            return fail("price out of range");

        // ── size ──────────────────────────────────────────────────────
        if (r.size <= 0 || r.size > MAX_SIZE)
            return fail("size out of range");

        // ── timestamp ─────────────────────────────────────────────────
        if (r.ts_micros <= 0)
            return fail("timestamp non-positive");

        int64_t now_us = std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
        int64_t drift = r.ts_micros - now_us;
        if (drift > MAX_DRIFT_US || drift < -MAX_DRIFT_US)
            return fail("timestamp drift > 36 hours");

        return true;
    }
};
