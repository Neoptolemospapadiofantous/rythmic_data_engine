#pragma once
/*  ═══════════════════════════════════════════════════════════════════════════
    orb_config.hpp — NQ Micro ORB execution engine configuration

    Loaded from config/orb_config.json at startup.
    All numeric defaults reflect the best-known Legends 50K params.
    ═══════════════════════════════════════════════════════════════════════════ */
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <stdexcept>
#include <string>

namespace fs = std::filesystem;

// ─── Contract constants (tick geometry is same for NQ and MNQ) ───────────────
inline constexpr double NQ_TICK_SIZE     = 0.25;   // minimum price increment (NQ and MNQ)
inline constexpr double NQ_COMMISSION    = 2.0;    // $ per side (NQ only)
inline constexpr double MNQ_COMMISSION   = 0.50;   // $ per side (MNQ: exchange+NFA+brokerage)
// Per-tick dollar values differ by contract: NQ=$5.00, MNQ=$0.50
inline constexpr double NQ_TICK_VALUE    = 5.00;   // NQ only — do NOT use for MNQ slippage
inline constexpr double MNQ_TICK_VALUE   = 0.50;   // MNQ = 2.0 $/pt × 0.25 tick

// ─── US Eastern Time offset (EDT=4, EST=5) ───────────────────────────────────
// Proper DST rule: second Sunday of March at 07:00 UTC (2:00 AM EST)
//                  first Sunday of November at 06:00 UTC (2:00 AM EDT)
#include <ctime>
inline int us_et_offset(const struct tm& utc_tm) {
    int year  = utc_tm.tm_year + 1900;
    int month = utc_tm.tm_mon + 1;
    int mday  = utc_tm.tm_mday;
    int hour  = utc_tm.tm_hour;

    if (month < 3 || month > 11) return 5;  // EST
    if (month > 3 && month < 11) return 4;  // EDT

    // Day-of-week for 1st of month (Sakamoto's algorithm, 0=Sunday)
    auto first_dow = [](int y, int m) -> int {
        static const int t[] = {0,3,2,5,0,3,5,1,4,6,2,4};
        y -= (m < 3);
        return (y + y/4 - y/100 + y/400 + t[m-1] + 1) % 7;
    };

    if (month == 3) {
        int fd        = first_dow(year, 3);
        int first_sun = (fd == 0) ? 1 : (8 - fd);
        int second_sun = first_sun + 7;
        if (mday < second_sun) return 5;
        if (mday > second_sun) return 4;
        return (hour >= 7) ? 4 : 5;   // spring forward at 07:00 UTC
    }
    // month == 11
    int fd        = first_dow(year, 11);
    int first_sun = (fd == 0) ? 1 : (8 - fd);
    if (mday < first_sun) return 4;
    if (mday > first_sun) return 5;
    return (hour < 6) ? 4 : 5;        // fall back at 06:00 UTC
}

struct OrbConfig {
    // ── Strategy params ────────────────────────────────────────────
    int    orb_minutes         = 15;   // opening range duration (9:30–9:45 ET)
    double sl_points           = 15.0; // stop-loss distance in points
    double trail_be_trigger    = 3.0;  // MFE required before trailing activates
    double trail_step          = 10.0; // trailing stop distance in points
    int    trail_delay_secs    = 300;  // seconds after fill before trailing starts
    double trail_be_offset     = 1.0;  // SL move to entry + this offset at BE trigger
    double breakout_buffer     = 0.0;  // extra points beyond ORB high/low to confirm break
    int    max_daily_trades    = 3;    // max entries per session
    int    last_entry_hour     = 13;   // no new entries at or after this ET hour
    int    eod_flatten_hour    = 15;   // EOD flatten hour (ET)
    int    eod_flatten_min     = 55;   // EOD flatten minute (ET)
    int    news_blackout_min   = 5;    // minutes before/after news event to block entry
    int    qty                 = 1;    // contract quantity per trade

    // ── Session open (defaults: RTH 9:30 ET) ──────────────────────
    int    session_open_hour   = 9;    // ET hour of session open (ORB window start)
    int    session_open_min    = 30;   // ET minute of session open

    // ── Risk rules (Legends 50K Master) ───────────────────────────
    double trailing_drawdown_cap = 2500.0; // max $ drawdown from equity peak
    double consistency_cap_pct   = 0.30;   // no single day > 30% of total profit
    double daily_loss_limit      = -1000.0; // halt if daily_pnl <= this value

    // ── Rithmic instrument ─────────────────────────────────────────
    std::string symbol         = "MNQ";
    std::string trade_contract = "";    // specific contract e.g. MNQM6 (leave empty = use symbol)
    std::string exchange    = "CME";
    double      point_value = 2.0;    // $/point: MNQ=2.0, NQ=20.0 — read from config
    std::string environment = "legends"; // "legends" or "paper"

    // ── Rithmic MD connection (AMP — TICKER_PLANT) ───────────────────
    // MD feed uses AMP credentials. Legends allows only one concurrent
    // session — using Legends for both plants causes FORCED LOGOUT on MD.
    std::string md_user;
    std::string md_password;
    std::string md_system_name = "Rithmic 01";
    std::string md_url         = "wss://ritpz01001.01.rithmic.com:443";

    // ── Rithmic ORDER connection (Legends — ORDER_PLANT) ───────────
    std::string rithmic_user;
    std::string rithmic_password;
    std::string rithmic_system_name = "LegendsTrading";
    std::string rithmic_url         = "wss://ritpz01001.01.rithmic.com:443";
    std::string app_name            = "nepa:OentexNQBot";
    std::string app_version         = "1.0";

    // ── Rithmic account (ORDER_PLANT) ──────────────────────────────
    std::string account_id   = "";   // e.g. LTARAPAPA502114908626
    std::string fcm_id       = "";   // Rithmic FCM identifier (usually empty)
    std::string ib_id        = "";   // Rithmic IB identifier (usually empty)
    std::string trade_route  = "Rithmic Order Routing";

    // ── Account ───────────────────────────────────────────────────
    double starting_balance = 50000.0; // actual Rithmic account balance at last sync

    // ── Safety ────────────────────────────────────────────────────
    bool dry_run = true;   // true = log signals only, no real orders

    // ── Database ──────────────────────────────────────────────────
    std::string pg_host     = "localhost";
    std::string pg_port     = "5432";
    std::string pg_db       = "rithmic";
    std::string pg_user     = "rithmic_user";
    std::string pg_password;

    std::string pg_connstr() const {
        return "host="      + pg_escape_kv(pg_host)     +
               " port="     + pg_escape_kv(pg_port)     +
               " dbname="   + pg_escape_kv(pg_db)       +
               " user="     + pg_escape_kv(pg_user)     +
               " password=" + pg_escape_kv(pg_password) +
               " connect_timeout=10"
               " application_name=nq_executor";
    }

    // ── Parse from JSON file ───────────────────────────────────────
    // Minimal hand-rolled parser — avoids pulling in nlohmann/json
    // when not already present, but uses it when available via cmake.
    // We use a simple key-value grep approach for robustness.
    static OrbConfig from_file(const fs::path& path) {
        if (!fs::exists(path))
            throw std::runtime_error("Config file not found: " + path.string());

        std::ifstream f(path);
        if (!f) throw std::runtime_error("Cannot open config: " + path.string());

        std::string text((std::istreambuf_iterator<char>(f)),
                          std::istreambuf_iterator<char>());

        OrbConfig c;

        // Load env overrides first (same as existing Config pattern)
        load_dotenv(".env");
        c.pg_password = env("PG_PASSWORD", "");
        if (c.pg_password.empty())
            c.pg_password = env("RITHMIC_PG_PASSWORD", "");

        // AMP credentials for TICKER_PLANT (market data feed only)
        // AMP and Legends each get their own session — no session conflict.
        c.md_user        = env("RITHMIC_AMP_USER",     "");
        c.md_password    = env("RITHMIC_AMP_PASSWORD",  "");
        c.md_system_name = env("RITHMIC_AMP_SYSTEM",   "Rithmic 01");
        c.md_url         = env("RITHMIC_AMP_URL",       c.md_url.c_str());

        // Legends credentials for ORDER_PLANT (execution) — prop firm account
        // Username: JSON field takes effect; env var overrides (allows runtime swap without rebuild)
        c.rithmic_user        = env("RITHMIC_LEGENDS_USER",     "");
        if (c.rithmic_user.empty())
            c.rithmic_user    = json_str(text, "rithmic_legends_user", "");
        c.rithmic_password    = env("RITHMIC_LEGENDS_PASSWORD",  "");
        c.rithmic_system_name = env("RITHMIC_LEGENDS_SYSTEM",   "LegendsTrading");
        c.rithmic_url         = env("RITHMIC_LEGENDS_URL",       c.rithmic_url.c_str());
        c.app_name            = env("RITHMIC_APP_NAME",          "nepa:OentexNQBot");
        c.app_version         = env("RITHMIC_APP_VERSION",       "1.0");

        // Pull strategy fields from JSON text with simple extractor
        c.orb_minutes          = json_int(text,  "orb_minutes",          c.orb_minutes);
        c.sl_points            = json_dbl(text,  "sl_points",            c.sl_points);
        c.trail_be_trigger     = json_dbl(text,  "trail_be_trigger",     c.trail_be_trigger);
        c.trail_step           = json_dbl(text,  "trail_step",           c.trail_step);
        c.trail_delay_secs     = json_int(text,  "trail_delay_secs",     c.trail_delay_secs);
        c.trail_be_offset      = json_dbl(text,  "trail_be_offset",      c.trail_be_offset);
        c.breakout_buffer      = json_dbl(text,  "breakout_buffer",      c.breakout_buffer);
        c.max_daily_trades     = json_int(text,  "max_daily_trades",     c.max_daily_trades);
        c.last_entry_hour      = json_int(text,  "last_entry_hour",      c.last_entry_hour);
        c.eod_flatten_hour     = json_int(text,  "eod_flatten_hour",     c.eod_flatten_hour);
        c.eod_flatten_min      = json_int(text,  "eod_flatten_min",      c.eod_flatten_min);
        c.news_blackout_min    = json_int(text,  "news_blackout_min",    c.news_blackout_min);
        c.qty                  = json_int(text,  "qty",                  c.qty);

        c.trailing_drawdown_cap = json_dbl(text, "trailing_drawdown_cap", c.trailing_drawdown_cap);
        c.consistency_cap_pct   = json_dbl(text, "consistency_cap_pct",   c.consistency_cap_pct);
        c.daily_loss_limit      = json_dbl(text, "daily_loss_limit",      c.daily_loss_limit);

        c.symbol         = json_str(text, "symbol",         c.symbol);
        c.trade_contract = json_str(text, "trade_contract", c.trade_contract);
        c.exchange       = json_str(text, "exchange",       c.exchange);
        c.point_value    = json_dbl(text, "point_value",    c.point_value);
        c.environment       = json_str(text, "environment",       c.environment);
        c.starting_balance  = json_dbl(text, "starting_balance",  c.starting_balance);
        // Env var takes precedence so account_id can be managed in .env without touching JSON
        {
            const char* acct_env = std::getenv("RITHMIC_LEGENDS_ACCOUNT");
            c.account_id = acct_env && acct_env[0]
                         ? std::string(acct_env)
                         : json_str(text, "account_id", c.account_id);
        }
        c.fcm_id           = json_str(text, "fcm_id",           c.fcm_id);
        c.ib_id            = json_str(text, "ib_id",            c.ib_id);
        c.trade_route      = json_str(text, "trade_route",      c.trade_route);
        c.session_open_hour = (int)json_dbl(text, "session_open_hour", c.session_open_hour);
        c.session_open_min  = (int)json_dbl(text, "session_open_min",  c.session_open_min);

        // dry_run: look for "dry_run": true/false
        {
            auto pos = text.find("\"dry_run\"");
            if (pos != std::string::npos) {
                auto colon = text.find(':', pos);
                if (colon != std::string::npos) {
                    auto vp = text.find_first_not_of(" \t\r\n", colon + 1);
                    if (vp != std::string::npos)
                        c.dry_run = (text.substr(vp, 4) == "true");
                }
            }
        }

        // DB overrides from JSON
        c.pg_host = json_str(text, "pg_host", c.pg_host);
        c.pg_port = json_str(text, "pg_port", c.pg_port);
        c.pg_db   = json_str(text, "pg_db",   c.pg_db);
        c.pg_user = json_str(text, "pg_user", c.pg_user);
        if (c.pg_password.empty())
            c.pg_password = json_str(text, "pg_password", "");

        return c;
    }

private:
    // Wrap a libpq keyword=value value in single quotes, escaping ' and \.
    // Prevents injection via crafted host/password values (H-SEC-3).
    static std::string pg_escape_kv(const std::string& v) {
        std::string out = "'";
        for (char c : v) {
            if (c == '\'') out += "\\'";
            else if (c == '\\') out += "\\\\";
            else out += c;
        }
        out += "'";
        return out;
    }

    // Simple JSON field extractors (no deps)
    static int json_int(const std::string& s, const std::string& key, int def) {
        auto pos = s.find("\"" + key + "\"");
        if (pos == std::string::npos) return def;
        auto colon = s.find(':', pos);
        if (colon == std::string::npos) return def;
        auto vp = s.find_first_not_of(" \t\r\n", colon + 1);
        if (vp == std::string::npos) return def;
        try { return std::stoi(s.substr(vp)); }
        catch (...) { return def; }
    }

    static double json_dbl(const std::string& s, const std::string& key, double def) {
        auto pos = s.find("\"" + key + "\"");
        if (pos == std::string::npos) return def;
        auto colon = s.find(':', pos);
        if (colon == std::string::npos) return def;
        auto vp = s.find_first_not_of(" \t\r\n", colon + 1);
        if (vp == std::string::npos) return def;
        try { return std::stod(s.substr(vp)); }
        catch (...) { return def; }
    }

    static std::string json_str(const std::string& s,
                                const std::string& key,
                                const std::string& def) {
        auto pos = s.find("\"" + key + "\"");
        if (pos == std::string::npos) return def;
        auto colon = s.find(':', pos);
        if (colon == std::string::npos) return def;
        auto q1 = s.find('"', colon + 1);
        if (q1 == std::string::npos) return def;
        auto q2 = s.find('"', q1 + 1);
        if (q2 == std::string::npos) return def;
        return s.substr(q1 + 1, q2 - q1 - 1);
    }

    static void load_dotenv(const fs::path& path) {
        if (!fs::exists(path)) return;
        std::ifstream f(path);
        std::string line;
        while (std::getline(f, line)) {
            auto s = trim(line);
            if (s.empty() || s[0] == '#') continue;
            auto eq = s.find('=');
            if (eq == std::string::npos) continue;
            auto k = trim(s.substr(0, eq));
            auto v = trim(s.substr(eq + 1));
            if (!k.empty() && !std::getenv(k.c_str()))
                setenv(k.c_str(), v.c_str(), 0);
        }
    }

    static std::string env(const char* k, const char* d) {
        const char* v = std::getenv(k);
        return v ? v : d;
    }

    static std::string trim(const std::string& s) {
        const std::string_view ws = " \t\r\n\"";
        auto b = s.find_first_not_of(ws);
        if (b == std::string::npos) return {};
        return s.substr(b, s.find_last_not_of(ws) - b + 1);
    }
};
