#pragma once
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

namespace fs = std::filesystem;

struct Config {
    // ── Rithmic connection ─────────────────────────────────────────
    std::string user;
    std::string password;
    std::string system_name = "Rithmic 01";
    std::string url         = "wss://ritpz01001.01.rithmic.com:443";
    std::string app_name    = "nepa:OentexNQBot";
    std::string app_version = "1.0";
    std::string symbol      = "NQ";
    std::string exchange    = "CME";

    // ── PostgreSQL connection ──────────────────────────────────────
    std::string pg_host     = "localhost";
    std::string pg_port     = "5432";
    std::string pg_db       = "rithmic";
    std::string pg_user     = "rithmic_user";
    std::string pg_password;

    // ── Paths ──────────────────────────────────────────────────────
    std::string cert_path   = "certs/rithmic_ssl_cert_auth_params";

    // Build a libpq connection string
    std::string pg_connstr() const {
        return "host="     + pg_host     +
               " port="    + pg_port     +
               " dbname="  + pg_db       +
               " user="    + pg_user     +
               " password=" + pg_password +
               " connect_timeout=10"
               " application_name=rithmic_engine";
    }

    static Config from_env(const fs::path& env_file = ".env") {
        load_dotenv(env_file);
        Config c;
        c.user         = env("RITHMIC_AMP_USER",     "");
        c.password     = env("RITHMIC_AMP_PASSWORD",  "");
        c.system_name  = env("RITHMIC_AMP_SYSTEM",   "Rithmic 01");
        c.url          = env("RITHMIC_AMP_URL",
                             "wss://ritpz01001.01.rithmic.com:443");
        c.app_name     = env("RITHMIC_APP_NAME",     "nepa:OentexNQBot");
        c.app_version  = env("RITHMIC_APP_VERSION",   "1.0");
        c.symbol       = env("RITHMIC_SYMBOL",        "NQ");
        c.exchange     = env("RITHMIC_EXCHANGE",      "CME");
        c.pg_host      = env("PG_HOST",               "localhost");
        c.pg_port      = env("PG_PORT",               "5432");
        c.pg_db        = env("PG_DB",                 "rithmic");
        c.pg_user      = env("PG_USER",               "rithmic_user");
        c.pg_password  = env("PG_PASSWORD",           "");
        return c;
    }

    std::vector<std::string> validate() const {
        std::vector<std::string> errs;
        if (user.empty())        errs.push_back("RITHMIC_AMP_USER not set");
        if (password.empty())    errs.push_back("RITHMIC_AMP_PASSWORD not set");
        if (pg_password.empty()) errs.push_back("PG_PASSWORD not set");
        return errs;
    }

private:
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
        const char* v = std::getenv(k); return v ? v : d;
    }
    static std::string trim(const std::string& s) {
        const std::string_view ws = " \t\r\n";
        auto b = s.find_first_not_of(ws);
        if (b == std::string::npos) return {};
        return s.substr(b, s.find_last_not_of(ws) - b + 1);
    }
};
