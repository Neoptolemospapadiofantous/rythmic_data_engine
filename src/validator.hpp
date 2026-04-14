#pragma once
// validator.hpp — Tick + BBO data validity checks.
//
// TickValidator: stateless structural checks — called before every tick enters
// the buffer.  Rejects data that is structurally wrong or physically implausible
// so no garbage ever reaches the WAL or the database.
//
// DataSentinel: stateful economic plausibility checks — tracks rolling state
// to detect price jumps, timestamp gaps, bid-ask inversions, and volume spikes.
// Modelled after the bot's python/data/sentinel.py.

#include <chrono>
#include <cmath>
#include <cstdint>
#include <deque>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "db.hpp"   // TickRow, BBORow

// ═══════════════════════════════════════════════════════════════════
// TickValidator — stateless, per-tick structural gate
// ═══════════════════════════════════════════════════════════════════

struct TickValidator {

    static constexpr int64_t MAX_DRIFT_US   = 172'800'000'000LL; // 48 hours
    static constexpr double  MAX_PRICE      = 1'000'000.0;
    static constexpr int64_t MAX_SIZE       = 50'000;

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
            return fail("timestamp drift > 48 hours");

        return true;
    }
};


// ═══════════════════════════════════════════════════════════════════
// SentinelAlert — structured anomaly event
// ═══════════════════════════════════════════════════════════════════

struct SentinelAlert {
    std::string ts;          // ISO 8601
    std::string check;       // e.g. "price_jump", "crossed_market"
    std::string severity;    // INFO, WARN, ERROR, CRITICAL
    std::string message;
    double      value = 0.0; // optional numeric payload
};


// ═══════════════════════════════════════════════════════════════════
// DataSentinel — stateful economic plausibility checks
// ═══════════════════════════════════════════════════════════════════
//
// Thread-safe. Call observe_tick / observe_bbo from the collector
// callbacks; call drain_alerts() periodically to harvest events.

class DataSentinel {
public:
    struct Config {
        double   price_jump_pct          = 2.0;    // % move triggers alert
        double   volume_spike_multiplier = 5.0;    // vs 30-tick rolling avg
        double   max_timestamp_gap_sec   = 120.0;  // gap threshold (seconds)
        double   wide_spread_points      = 5.0;    // NQ: >5 pts is extreme
        double   alert_cooldown_sec      = 10.0;   // rate limit per check type
    };

    DataSentinel() : cfg_() {}
    explicit DataSentinel(Config cfg) : cfg_(cfg) {}

    // ── Tick observation ───────────────────────────────────────────
    void observe_tick(double price, int64_t size, int64_t ts_micros) {
        std::lock_guard lock(mu_);
        ++tick_count_;

        if (price <= 0.0) {
            emit("invalid_price", "ERROR",
                 "Non-positive price: " + std::to_string(price), price);
            return;
        }

        // Price jump detection
        if (last_price_ > 0.0) {
            double pct = std::abs(price - last_price_) / last_price_ * 100.0;
            if (pct > cfg_.price_jump_pct) {
                char buf[128];
                std::snprintf(buf, sizeof(buf),
                    "Price jumped %.2f%%: %.2f -> %.2f", pct, last_price_, price);
                emit("price_jump", "WARN", buf, pct);
            }
        }

        // Timestamp monotonicity
        if (last_ts_us_ > 0 && ts_micros < last_ts_us_) {
            double gap_sec = static_cast<double>(last_ts_us_ - ts_micros) / 1e6;
            char buf[96];
            std::snprintf(buf, sizeof(buf),
                "Timestamp went backward by %.3fs", gap_sec);
            emit("timestamp_backward", "WARN", buf, gap_sec);
        }

        // Timestamp gap (stale data detection)
        if (last_ts_us_ > 0 && ts_micros > last_ts_us_) {
            double gap_sec = static_cast<double>(ts_micros - last_ts_us_) / 1e6;
            if (gap_sec > cfg_.max_timestamp_gap_sec) {
                char buf[96];
                std::snprintf(buf, sizeof(buf),
                    "Tick gap: %.1fs (threshold: %.1fs)",
                    gap_sec, cfg_.max_timestamp_gap_sec);
                emit("timestamp_gap", "WARN", buf, gap_sec);
                ++gap_count_;
            }
        }

        // Volume spike detection (rolling 30-tick average)
        recent_sizes_.push_back(size);
        if (recent_sizes_.size() > 30) recent_sizes_.pop_front();
        if (recent_sizes_.size() >= 10) {
            double avg = 0.0;
            for (auto s : recent_sizes_) avg += static_cast<double>(s);
            avg /= static_cast<double>(recent_sizes_.size());
            if (avg > 0.0 && static_cast<double>(size) > avg * cfg_.volume_spike_multiplier) {
                double ratio = static_cast<double>(size) / avg;
                char buf[128];
                std::snprintf(buf, sizeof(buf),
                    "Volume spike: %lld vs avg %.0f (%.1fx)",
                    (long long)size, avg, ratio);
                emit("volume_spike", "WARN", buf, ratio);
            }
        }

        last_price_ = price;
        last_ts_us_ = ts_micros;
    }

    // ── BBO observation ────────────────────────────────────────────
    void observe_bbo(double bid, double ask) {
        std::lock_guard lock(mu_);

        // Bid-ask inversion (crossed market)
        if (bid > 0.0 && ask > 0.0 && bid > ask) {
            char buf[96];
            std::snprintf(buf, sizeof(buf),
                "Crossed market: bid=%.2f > ask=%.2f", bid, ask);
            emit("crossed_market", "WARN", buf, bid - ask);
        }

        // Wide spread detection
        if (bid > 0.0 && ask > 0.0) {
            double spread = ask - bid;
            if (spread > cfg_.wide_spread_points) {
                char buf[96];
                std::snprintf(buf, sizeof(buf),
                    "Wide spread: %.2f pts (bid=%.2f, ask=%.2f)",
                    spread, bid, ask);
                emit("wide_spread", "WARN", buf, spread);
            }
        }
    }

    // ── Alert harvest ──────────────────────────────────────────────
    std::vector<SentinelAlert> drain_alerts() {
        std::lock_guard lock(mu_);
        std::vector<SentinelAlert> out;
        out.swap(alerts_);
        return out;
    }

    // ── Stats ──────────────────────────────────────────────────────
    int64_t tick_count()  const { std::lock_guard lock(mu_); return tick_count_; }
    int64_t alert_count() const { std::lock_guard lock(mu_); return alert_count_; }
    int64_t gap_count()   const { std::lock_guard lock(mu_); return gap_count_; }

private:
    void emit(const std::string& check, const std::string& severity,
              const std::string& message, double value = 0.0) {
        auto now = std::chrono::steady_clock::now();
        double elapsed = std::chrono::duration<double>(
            now - last_alert_time_[check]).count();
        if (elapsed < cfg_.alert_cooldown_sec && last_alert_time_.count(check))
            return; // rate limited

        last_alert_time_[check] = now;
        ++alert_count_;

        // ISO 8601 timestamp
        auto sys_now = std::chrono::system_clock::now();
        auto t = std::chrono::system_clock::to_time_t(sys_now);
        struct tm tm_utc;
        gmtime_r(&t, &tm_utc);
        char ts_buf[32];
        std::strftime(ts_buf, sizeof(ts_buf), "%Y-%m-%dT%H:%M:%SZ", &tm_utc);

        alerts_.push_back({ts_buf, check, severity, message, value});
    }

    Config cfg_;
    mutable std::mutex mu_;

    double  last_price_ = 0.0;
    int64_t last_ts_us_ = 0;
    int64_t tick_count_  = 0;
    int64_t alert_count_ = 0;
    int64_t gap_count_   = 0;

    std::deque<int64_t> recent_sizes_;  // rolling 30-tick window
    std::vector<SentinelAlert> alerts_;
    std::unordered_map<std::string, std::chrono::steady_clock::time_point> last_alert_time_;
};
