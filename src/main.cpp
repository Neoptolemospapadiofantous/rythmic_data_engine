#include <csignal>
#include <cstdio>
#include <cstring>
#include <string>

#include <sched.h>
#include <sys/resource.h>

#include "audit.hpp"
#include "collector.hpp"
#include "config.hpp"
#include "db.hpp"
#include "log.hpp"

// ── Signal handling ────────────────────────────────────────────────

static Collector* g_collector = nullptr;

static void handle_signal(int) {
    if (g_collector) g_collector->stop();
}

// ── CLI ────────────────────────────────────────────────────────────

static void print_usage(const char* argv0) {
    std::printf(
        "Usage: %s [MODE]\n"
        "\n"
        "Modes:\n"
        "  (none)    Collect live ticks → PostgreSQL  [default]\n"
        "  --status  Print database statistics\n"
        "  --audit   Show last 20 audit events\n"
        "\n",
        argv0);
}

// ── --status ───────────────────────────────────────────────────────

static int cmd_status(const Config& cfg) {
    try {
        TickDB db(cfg.pg_connstr(), /*read_only=*/true);
        auto s = db.summary();
        std::printf("Ticks:    %lld\n",   (long long)s.tick_count);
        std::printf("Earliest: %s\n",     s.earliest.empty() ? "n/a" : s.earliest.c_str());
        std::printf("Latest:   %s\n",     s.latest.empty()   ? "n/a" : s.latest.c_str());
        std::printf("Price:    %s\n",     s.price ? std::to_string(*s.price).c_str() : "n/a");
        std::printf("DB:       %s\n",     s.connstr.c_str());
    } catch (std::exception& e) {
        std::fprintf(stderr, "Error: %s\n", e.what());
        return 1;
    }
    return 0;
}

// ── --audit ────────────────────────────────────────────────────────

static int cmd_audit(const Config& cfg) {
    try {
        TickDB db(cfg.pg_connstr(), /*read_only=*/true);
        PGresult* res = PQexec(db.conn(),
            "SELECT ts, severity, event, details "
            "FROM audit_log ORDER BY ts DESC LIMIT 20");
        if (!res || PQresultStatus(res) != PGRES_TUPLES_OK) {
            std::fprintf(stderr, "Audit query failed\n");
            if (res) PQclear(res);
            return 1;
        }
        int rows = PQntuples(res);
        std::printf("%-26s %-6s %-30s %s\n", "Timestamp", "Sev", "Event", "Details");
        std::printf("%s\n", std::string(90, '-').c_str());
        for (int i = 0; i < rows; ++i) {
            std::printf("%-26s %-6s %-30s %s\n",
                        PQgetvalue(res, i, 0),
                        PQgetvalue(res, i, 1),
                        PQgetvalue(res, i, 2),
                        PQgetvalue(res, i, 3));
        }
        PQclear(res);
    } catch (std::exception& e) {
        std::fprintf(stderr, "Error: %s\n", e.what());
        return 1;
    }
    return 0;
}

// ── main ───────────────────────────────────────────────────────────

int main(int argc, char* argv[]) {
    Config cfg = Config::from_env(".env");

    bool mode_status = false;
    bool mode_audit  = false;

    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--status") { mode_status = true; continue; }
        if (arg == "--audit")  { mode_audit  = true; continue; }
        if (arg == "--help" || arg == "-h") { print_usage(argv[0]); return 0; }
        std::fprintf(stderr, "Unknown argument: %s\n", arg.c_str());
        print_usage(argv[0]);
        return 1;
    }

    if (mode_status) return cmd_status(cfg);
    if (mode_audit)  return cmd_audit(cfg);

    // ── Default: run collector ─────────────────────────────────────
    auto errs = cfg.validate();
    if (!errs.empty()) {
        for (auto& e : errs)
            std::fprintf(stderr, "Config error: %s\n", e.c_str());
        std::fprintf(stderr, "Copy .env.example to .env and fill in credentials.\n");
        return 1;
    }

    // ── Latency tuning ─────────────────────────────────────────────
    // Raise process priority so the OS scheduler preempts us less.
    // setpriority(-20) = highest nice level; fails silently if unprivileged.
    if (setpriority(PRIO_PROCESS, 0, -10) != 0)
        std::fprintf(stderr, "Note: could not set process priority (run as root for -20)\n");

    Collector collector(cfg);
    g_collector = &collector;
    std::signal(SIGINT,  handle_signal);
    std::signal(SIGTERM, handle_signal);
    collector.run();
    g_collector = nullptr;
    return 0;
}
