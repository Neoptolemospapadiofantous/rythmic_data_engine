#pragma once
/*  ═══════════════════════════════════════════════════════════════════════════
    orb_strategy.hpp — NQ Opening Range Breakout strategy core

    Tick ingestion, 1-minute bar building, ORB high/low tracking, breakout
    signal generation.

    Call order (all from the same io_context thread — no extra locking needed
    for strategy state, only position state is mutex-protected in OrderManager):
        on_tick()       ← every incoming LastTrade from MD plant
        check_eod()     ← called periodically (e.g. every second) to flatten
        reset_session() ← called at start of each RTH session
    ═══════════════════════════════════════════════════════════════════════════ */
#include "orb_config.hpp"
#include "log.hpp"
#include <cmath>
#include <ctime>
#include <functional>
#include <mutex>
#include <string>
#include <limits>

// ─── Signal type emitted by the strategy ─────────────────────────────────────
enum class OrbSignal { NONE, BUY, SELL, FLATTEN_EOD };

struct OrbTick {
    int64_t ts_micros;   // microseconds since Unix epoch (UTC)
    double  price;
    int64_t size;
    bool    is_buy;
};

// ─── 1-minute bar accumulator ─────────────────────────────────────────────────
struct MinuteBar {
    int    minute_utc = -1;  // minutes since epoch for this bar
    double open  = 0.0;
    double high  = std::numeric_limits<double>::lowest();
    double low   = std::numeric_limits<double>::max();
    double close = 0.0;
    int64_t volume = 0;
    bool   complete = false;

    void update(double price, int64_t sz) {
        if (volume == 0) open = price;
        if (price > high) high = price;
        if (price < low)  low  = price;
        close  = price;
        volume += sz;
    }
};

// ─── ORB session state ────────────────────────────────────────────────────────
struct OrbSession {
    double orb_high = std::numeric_limits<double>::lowest();
    double orb_low  = std::numeric_limits<double>::max();
    bool   orb_set  = false;    // true when opening range is complete
    int    trades_today = 0;
    bool   long_taken   = false; // only one direction per session (first break wins)
    bool   short_taken  = false;
    bool   risk_halted  = false;
    std::string halt_reason;

    void reset() {
        orb_high   = std::numeric_limits<double>::lowest();
        orb_low    = std::numeric_limits<double>::max();
        orb_set    = false;
        trades_today = 0;
        long_taken   = false;
        short_taken  = false;
        risk_halted  = false;
        halt_reason.clear();
    }
};

// ─── OrbStrategy ─────────────────────────────────────────────────────────────
// Pure signal generator — has no I/O, no DB, no sockets.
// Caller wires the signal_cb to the OrderManager.
class OrbStrategy {
public:
    using SignalCallback = std::function<void(OrbSignal, double price, const std::string& reason)>;

    explicit OrbStrategy(const OrbConfig& cfg)
        : cfg_(cfg) {
        session_.reset();
    }

    void set_signal_callback(SignalCallback cb) { signal_cb_ = std::move(cb); }

    // ── Called once per trading day before the session opens ──────────────────
    void reset_session() {
        session_.reset();
        current_bar_ = MinuteBar{};
        eod_emitted_ = false;
        LOG("[ORB] Session reset — ORB window %d min, SL=%.1f pts, trail_step=%.1f pts",
            cfg_.orb_minutes, cfg_.sl_points, cfg_.trail_step);
    }

    // ── Ingest one tick ───────────────────────────────────────────────────────
    // ts_micros: microseconds since Unix epoch (UTC).
    // ET = UTC - 4h (EDT) or UTC - 5h (EST); we use ET offset from config or detect.
    void on_tick(const OrbTick& tick) {
        if (session_.risk_halted) return;

        int et_hour, et_min, et_sec;
        utc_micros_to_et(tick.ts_micros, et_hour, et_min, et_sec);

        // Build 1-minute bars (UTC epoch minutes drive bar boundaries)
        int cur_epoch_min = static_cast<int>(tick.ts_micros / 1'000'000 / 60);

        if (current_bar_.minute_utc < 0) {
            current_bar_.minute_utc = cur_epoch_min;
        }

        if (cur_epoch_min != current_bar_.minute_utc) {
            // Bar closed — handle ORB window
            handle_completed_bar(current_bar_, et_hour, et_min);
            current_bar_ = MinuteBar{};
            current_bar_.minute_utc = cur_epoch_min;
        }
        current_bar_.update(tick.price, tick.size);
        last_price_ = tick.price;
        last_et_hour_ = et_hour;
        last_et_min_  = et_min;

        // ORB not yet set — accumulate high/low during opening range
        if (!session_.orb_set) {
            bool in_orb_window = is_in_orb_window(et_hour, et_min);
            if (in_orb_window) {
                if (tick.price > session_.orb_high) session_.orb_high = tick.price;
                if (tick.price < session_.orb_low)  session_.orb_low  = tick.price;
            }
            // We set the ORB as soon as the window closes (handled in handle_completed_bar)
            return;
        }

        // ORB set — check for breakout signal
        if (session_.trades_today >= cfg_.max_daily_trades) return;
        if (et_hour >= cfg_.last_entry_hour) return;
        if (is_news_blackout(et_hour, et_min)) return;

        check_breakout(tick.price, et_hour, et_min);
    }

    // ── Periodic EOD check (call once per second) ─────────────────────────────
    void check_eod(int et_hour, int et_min) {
        if (et_hour > cfg_.eod_flatten_hour) { emit_eod_flatten(); return; }
        if (et_hour == cfg_.eod_flatten_hour && et_min >= cfg_.eod_flatten_min) {
            emit_eod_flatten();
        }
    }

    // ── Risk halt (called by RiskManager when limit breached) ─────────────────
    void halt_trading(const std::string& reason) {
        session_.risk_halted = true;
        session_.halt_reason = reason;
        LOG("[ORB] Trading halted: %s", reason.c_str());
    }

    // ── Notify strategy that a trade closed ──────────────────────────────────
    // trades_today is already incremented at signal time in check_breakout;
    // directional flags are also set there, but re-affirmed here for safety.
    void notify_trade_filled(OrbSignal dir) {
        if (dir == OrbSignal::BUY)  session_.long_taken  = true;
        if (dir == OrbSignal::SELL) session_.short_taken = true;
    }

    void seed_orb_range(double high, double low) {
        session_.orb_high = high;
        session_.orb_low  = low;
        session_.orb_set  = true;
        LOG("[ORB] Range seeded externally: high=%.2f low=%.2f", high, low);
    }

    const OrbSession& session() const { return session_; }
    double last_price() const { return last_price_; }
    double orb_high()   const { return session_.orb_high; }
    double orb_low()    const { return session_.orb_low; }
    bool   orb_set()    const { return session_.orb_set; }

private:
    OrbConfig    cfg_;
    OrbSession   session_;
    MinuteBar    current_bar_;
    SignalCallback signal_cb_;
    double       last_price_   = 0.0;
    int          last_et_hour_ = 0;
    int          last_et_min_  = 0;
    bool         eod_emitted_  = false;

    static void utc_micros_to_et(int64_t ts_us, int& h, int& m, int& s) {
        int64_t ts_sec = ts_us / 1'000'000;
        time_t tt = static_cast<time_t>(ts_sec);
        struct tm utc_tm;
        gmtime_r(&tt, &utc_tm);
        int64_t et_sec = ts_sec - us_et_offset(utc_tm) * 3600LL;
        h = static_cast<int>((et_sec / 3600) % 24);
        m = static_cast<int>((et_sec % 3600) / 60);
        s = static_cast<int>(et_sec % 60);
        if (h < 0) h += 24;
    }

    bool is_in_orb_window(int et_hour, int et_min) const {
        int total_min = et_hour * 60 + et_min;
        int open_min  = cfg_.session_open_hour * 60 + cfg_.session_open_min;
        int close_min = open_min + cfg_.orb_minutes;
        return (total_min >= open_min && total_min < close_min);
    }

    bool is_news_blackout(int et_hour, int et_min) const {
        // CPI/FOMC typically at 8:30, 10:00, 14:00 ET.
        // Simple approach: block for news_blackout_min minutes each side of common times.
        // In production, wire up a news schedule; this is the safety net.
        (void)et_hour; (void)et_min;
        return false;  // stub — real impl would check a schedule
    }

    void handle_completed_bar(const MinuteBar& bar, int et_hour, int et_min) {
        if (bar.volume == 0) return;
        int total_min = et_hour * 60 + et_min;
        int orb_end   = cfg_.session_open_hour * 60 + cfg_.session_open_min + cfg_.orb_minutes;

        // Set ORB when we pass the opening range close minute
        if (!session_.orb_set && total_min >= orb_end) {
            // Sanity check — we should have seen ticks during the window
            if (session_.orb_high > session_.orb_low) {
                session_.orb_set = true;
                LOG("[ORB] Opening range set: high=%.2f low=%.2f range=%.2f pts",
                    session_.orb_high, session_.orb_low,
                    session_.orb_high - session_.orb_low);
            }
        }
    }

    void check_breakout(double price, int /*et_hour*/, int /*et_min*/) {
        double buy_level  = session_.orb_high + cfg_.breakout_buffer;
        double sell_level = session_.orb_low  - cfg_.breakout_buffer;

        if (!session_.long_taken && price > buy_level) {
            LOG("[ORB] LONG breakout signal: price=%.2f orb_high=%.2f trades_today=%d",
                price, session_.orb_high, session_.trades_today);
            if (signal_cb_) signal_cb_(OrbSignal::BUY, price, "orb_breakout_long");
            session_.long_taken   = true;
            session_.trades_today++;   // count at entry, not at close
        } else if (!session_.short_taken && price < sell_level) {
            LOG("[ORB] SHORT breakout signal: price=%.2f orb_low=%.2f trades_today=%d",
                price, session_.orb_low, session_.trades_today);
            if (signal_cb_) signal_cb_(OrbSignal::SELL, price, "orb_breakout_short");
            session_.short_taken  = true;
            session_.trades_today++;
        }
    }

    void emit_eod_flatten() {
        if (!eod_emitted_) {
            eod_emitted_ = true;
            LOG("[ORB] EOD flatten triggered at ET %02d:%02d",
                last_et_hour_, last_et_min_);
            if (signal_cb_) signal_cb_(OrbSignal::FLATTEN_EOD, last_price_, "eod_flatten");
        }
    }
};
