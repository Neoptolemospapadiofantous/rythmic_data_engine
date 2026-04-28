#pragma once
/*  ═══════════════════════════════════════════════════════════════════════════
    latency_logger.hpp — Per-trade nanosecond timestamp and slippage recording

    Captures:
      - signal_ts_ns  : when OrbStrategy emitted the signal
      - submit_ts_ns  : when RequestNewOrder was sent on the wire
      - fill_ts_ns    : when OrderNotification (FILL) arrived
      - entry_price   : actual fill price
      - signal_price  : price at signal time (for slippage calc)

    Slippage in ticks = |fill_price - signal_price| / NQ_TICK_SIZE
    Slippage in USD   = ticks × tick_value_  (instrument-specific, passed at construction)
    ═══════════════════════════════════════════════════════════════════════════ */
#include "orb_config.hpp"
#include "log.hpp"
#include <chrono>
#include <cmath>
#include <cstdint>
#include <mutex>
#include <string>

// ─── Per-trade latency record ─────────────────────────────────────────────────
struct TradeLatency {
    std::string basket_id;

    // Timestamps (nanoseconds since Unix epoch)
    int64_t signal_ts_ns  = 0;  // OrbStrategy fired signal
    int64_t submit_ts_ns  = 0;  // RequestNewOrder sent to wire
    int64_t fill_ts_ns    = 0;  // fill notification received

    // Prices
    double signal_price = 0.0;  // last trade price when signal fired
    double submit_price = 0.0;  // price in the order (market → 0, limit → limit)
    double fill_price   = 0.0;  // actual fill price from exchange

    bool   is_entry     = true; // true=entry, false=exit

    // Computed fields (populated by LatencyLogger::finalize)
    int64_t signal_to_submit_us = 0;  // signal → wire (microseconds)
    int64_t submit_to_fill_ms   = 0;  // wire → fill  (milliseconds)
    int     slippage_ticks      = 0;  // |fill - signal| in ticks
    double  slippage_usd        = 0.0;
};

// ─── LatencyLogger ────────────────────────────────────────────────────────────
// Threadsafe — callers may be on different threads (signal thread vs fill CB).
// Uses a simple mutex for the pending record.
class LatencyLogger {
public:
    // tick_value: dollars per tick for this instrument (NQ=$5.00, MNQ=$0.50)
    // Derived from orb_cfg.point_value × NQ_TICK_SIZE (0.25 pts/tick).
    explicit LatencyLogger(double tick_value = MNQ_TICK_VALUE)
        : tick_value_(tick_value) {}
    // Record signal emission — returns ns timestamp stored
    int64_t on_signal(const std::string& basket_id, double signal_price, bool is_entry) {
        int64_t ts = now_ns();
        std::lock_guard<std::mutex> lk(mu_);
        pending_.basket_id    = basket_id;
        pending_.signal_ts_ns = ts;
        pending_.signal_price = signal_price;
        pending_.is_entry     = is_entry;
        pending_.submit_ts_ns = 0;
        pending_.fill_ts_ns   = 0;
        return ts;
    }

    // Record order submission
    void on_submit(const std::string& basket_id, double order_price = 0.0) {
        int64_t ts = now_ns();
        std::lock_guard<std::mutex> lk(mu_);
        if (pending_.basket_id != basket_id) return;
        pending_.submit_ts_ns = ts;
        pending_.submit_price = order_price;
    }

    // Record fill — returns finalized record
    TradeLatency on_fill(const std::string& basket_id, double fill_price) {
        int64_t ts = now_ns();
        TradeLatency rec;
        {
            std::lock_guard<std::mutex> lk(mu_);
            if (pending_.basket_id != basket_id) return rec;
            rec = pending_;
        }
        rec.fill_ts_ns = ts;
        rec.fill_price = fill_price;
        finalize(rec, tick_value_);
        LOG("[LAT] %s %s signal→submit=%lldus submit→fill=%lldms slippage=%dticks ($%.2f)",
            rec.basket_id.c_str(),
            rec.is_entry ? "ENTRY" : "EXIT",
            (long long)rec.signal_to_submit_us,
            (long long)rec.submit_to_fill_ms,
            rec.slippage_ticks,
            rec.slippage_usd);
        last_ = rec;
        return rec;
    }

    const TradeLatency& last() const { return last_; }

private:
    static void finalize(TradeLatency& r, double tick_value) {
        if (r.submit_ts_ns > r.signal_ts_ns)
            r.signal_to_submit_us = (r.submit_ts_ns - r.signal_ts_ns) / 1000;
        if (r.fill_ts_ns > r.submit_ts_ns)
            r.submit_to_fill_ms = (r.fill_ts_ns - r.submit_ts_ns) / 1'000'000;

        double diff = std::abs(r.fill_price - r.signal_price);
        r.slippage_ticks = static_cast<int>(std::round(diff / NQ_TICK_SIZE));
        r.slippage_usd   = r.slippage_ticks * tick_value;
    }

    static int64_t now_ns() {
        return std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
    }

    double       tick_value_;
    std::mutex   mu_;
    TradeLatency pending_;
    TradeLatency last_;
};
