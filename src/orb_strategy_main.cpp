// orb_strategy_main.cpp — Standalone ORB signal binary for C++/Python parity tests.
//
// Usage:  ./build/orb_strategy --config path/to/live_config.json
// Input:  JSON array of OHLCV bars on stdin (ts as ISO 8601 string, ignored)
// Output: one JSON line to stdout:
//           {"signal":"LONG","stop_loss":17506.0,"entry":17520.0,"target":17532.0}
//         or, if no signal fires:
//           {"signal":null,"stop_loss":null,"entry":null,"target":null}
//
// ORB logic matches Python MicroORBStrategy._check_breakout:
//   - First orb_period_minutes bars build the opening range (max high, min low)
//   - After range locks: close > orb_high → LONG; close < orb_low → SHORT
//   - SL for LONG  = orb_high - stop_loss_ticks * tick_size
//   - SL for SHORT = orb_low  + stop_loss_ticks * tick_size
//   - Entry = breakout bar close
//   - Target for LONG  = entry + target_ticks * tick_size
//   - Target for SHORT = entry - target_ticks * tick_size
//
// Exit codes: 0 = success, 1 = error (message on stderr)

#include <cstdio>
#include <cstring>
#include <fstream>
#include <iostream>
#include <limits>
#include <stdexcept>
#include <string>

#include <nlohmann/json.hpp>

using json = nlohmann::json;

static void usage(const char* prog) {
    std::fprintf(stderr, "Usage: %s --config <path/to/live_config.json>\n", prog);
}

int main(int argc, char* argv[]) {
    // ── Parse arguments ──────────────────────────────────────────────
    const char* config_path = nullptr;
    for (int i = 1; i < argc; ++i) {
        if (std::strcmp(argv[i], "--config") == 0 && i + 1 < argc)
            config_path = argv[++i];
    }
    if (!config_path) {
        usage(argv[0]);
        return 1;
    }

    // ── Load and parse config ────────────────────────────────────────
    json cfg;
    try {
        std::ifstream f(config_path);
        if (!f.is_open()) {
            std::fprintf(stderr, "Cannot open config: %s\n", config_path);
            return 1;
        }
        f >> cfg;
    } catch (const std::exception& e) {
        std::fprintf(stderr, "Config parse error: %s\n", e.what());
        return 1;
    }

    int    orb_period = 5;
    int    sl_ticks   = 16;
    int    tgt_ticks  = 48;
    double tick_size  = 0.25;
    try {
        const auto& orb = cfg.at("orb");
        orb_period = orb.at("orb_period_minutes").get<int>();
        sl_ticks   = orb.at("stop_loss_ticks").get<int>();
        tgt_ticks  = orb.at("target_ticks").get<int>();
        tick_size  = orb.at("tick_size").get<double>();
    } catch (const std::exception& e) {
        std::fprintf(stderr, "Config field error: %s\n", e.what());
        return 1;
    }

    // ── Read bars array from stdin ───────────────────────────────────
    json bars;
    try {
        std::cin >> bars;
    } catch (const std::exception& e) {
        std::fprintf(stderr, "stdin parse error: %s\n", e.what());
        return 1;
    }
    if (!bars.is_array()) {
        std::fprintf(stderr, "stdin must be a JSON array of bar objects\n");
        return 1;
    }

    // ── ORB state machine ────────────────────────────────────────────
    double orb_high     = std::numeric_limits<double>::lowest();
    double orb_low      = std::numeric_limits<double>::max();
    int    bar_count    = 0;
    bool   range_locked = false;
    bool   signalled    = false;

    std::string signal_dir;
    double      signal_entry = 0.0;
    double      signal_sl    = 0.0;
    double      signal_tgt   = 0.0;

    for (const auto& bar : bars) {
        double high, low, close;
        try {
            high  = bar.at("high").get<double>();
            low   = bar.at("low").get<double>();
            close = bar.at("close").get<double>();
        } catch (const std::exception& e) {
            std::fprintf(stderr, "Bar field error: %s\n", e.what());
            return 1;
        }

        ++bar_count;

        if (!range_locked) {
            if (high > orb_high) orb_high = high;
            if (low  < orb_low)  orb_low  = low;
            if (bar_count >= orb_period)
                range_locked = true;
        } else if (!signalled) {
            if (close > orb_high) {
                signal_dir   = "LONG";
                signal_entry = close;
                signal_sl    = orb_high - sl_ticks * tick_size;
                signal_tgt   = signal_entry + tgt_ticks * tick_size;
                signalled    = true;
            } else if (close < orb_low) {
                signal_dir   = "SHORT";
                signal_entry = close;
                signal_sl    = orb_low + sl_ticks * tick_size;
                signal_tgt   = signal_entry - tgt_ticks * tick_size;
                signalled    = true;
            }
        }
    }

    // ── Emit result ──────────────────────────────────────────────────
    json result;
    if (signalled) {
        result["signal"]    = signal_dir;
        result["stop_loss"] = signal_sl;
        result["entry"]     = signal_entry;
        result["target"]    = signal_tgt;
    } else {
        result["signal"]    = nullptr;
        result["stop_loss"] = nullptr;
        result["entry"]     = nullptr;
        result["target"]    = nullptr;
    }

    std::cout << result.dump() << "\n";
    return 0;
}
