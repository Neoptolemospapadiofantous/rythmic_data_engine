// test_connection — step-by-step Rithmic + PostgreSQL pipeline test.
//
// Runs every stage of the data flow, times each one, and prints a report:
//   PostgreSQL connect → schema
//   TCP + SSL + WebSocket connect
//   RequestRithmicSystemInfo (validate system name)
//   Login
//   Subscribe
//   Receive N live ticks (wire latency)
//   DB write (UNNEST batch)
//   DB read-back (COUNT)
//
// Usage:  ./build/test_connection  [path/to/.env]

#include <chrono>
#include <cstdio>
#include <exception>
#include <string>

#include <boost/asio.hpp>

#include "client.hpp"
#include "config.hpp"
#include "db.hpp"

namespace asio = boost::asio;

static void banner(const char* text) {
    const int W = 58;
    std::string line(W, '=');
    std::printf("\n%s\n  %s\n%s\n", line.c_str(), text, line.c_str());
}

int main(int argc, char* argv[]) {
    const char* env_path = argc > 1 ? argv[1] : ".env";

    banner("Rithmic Engine — Connection & Data-Flow Test");

    // ── Load config ────────────────────────────────────────────────
    Config cfg;
    try {
        cfg = Config::from_env(env_path);
    } catch (std::exception& e) {
        std::fprintf(stderr, "Config error: %s\n", e.what());
        return 1;
    }
    auto errs = cfg.validate();
    if (!errs.empty()) {
        for (auto& e : errs) std::fprintf(stderr, "  %s\n", e.c_str());
        std::fprintf(stderr, "  → Copy .env.example to .env and fill in credentials.\n");
        return 1;
    }

    // ── PostgreSQL ─────────────────────────────────────────────────
    std::printf("\n[1] PostgreSQL  (%s:%s/%s)\n",
                cfg.pg_host.c_str(), cfg.pg_port.c_str(), cfg.pg_db.c_str());

    std::unique_ptr<TickDB> db;
    {
        auto t0 = std::chrono::steady_clock::now();
        try {
            db = std::make_unique<TickDB>(cfg.pg_connstr());
            auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                          std::chrono::steady_clock::now() - t0)
                          .count();
            std::printf("  ✓  Connected + schema ready         %5lld ms\n",
                        (long long)ms);
        } catch (std::exception& e) {
            auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                          std::chrono::steady_clock::now() - t0)
                          .count();
            std::printf("  ✗  Failed (%lld ms): %s\n", (long long)ms, e.what());
            return 1;
        }
    }

    // ── Rithmic pipeline ───────────────────────────────────────────
    std::printf("\n[2] Rithmic AMP  (%s)\n", cfg.url.c_str());
    std::printf("    Waiting for %s/%s ticks (60 s timeout)...\n\n",
                cfg.symbol.c_str(), cfg.exchange.c_str());

    ConnectionTestResult result;
    asio::io_context ioc;
    RithmicClient    client(ioc, cfg);

    asio::co_spawn(
        ioc,
        [&]() -> asio::awaitable<void> {
            result = co_await client.run_connection_test(*db, 5);
        }(),
        [&](std::exception_ptr ep) {
            if (ep) {
                try { std::rethrow_exception(ep); }
                catch (std::exception& e) {
                    std::fprintf(stderr, "  Fatal: %s\n", e.what());
                }
            }
            ioc.stop();
        });

    ioc.run();

    // ── Print results ──────────────────────────────────────────────
    result.print();

    // ── Summary ────────────────────────────────────────────────────
    std::printf("\n");
    if (result.all_ok()) {
        std::printf("  Result: PASS ✓  full data-flow verified\n");
    } else {
        std::printf("  Result: FAIL ✗  see ✗ steps above\n");
    }
    std::printf("%s\n\n", std::string(58, '=').c_str());

    return result.all_ok() ? 0 : 1;
}
