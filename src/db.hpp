#pragma once
#include <chrono>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include <libpq-fe.h>

struct TickRow {
    int64_t     ts_micros;   // microseconds since Unix epoch (UTC)
    double      price;
    int64_t     size;
    bool        is_buy;
    std::string symbol;      // e.g. "NQ", "ES", "CL"
    std::string exchange;    // e.g. "CME", "NYMEX"
};

struct DBSummary {
    int64_t               tick_count = 0;
    std::string           earliest;
    std::string           latest;
    std::optional<double> price;
    std::string           connstr;
};

// PostgreSQL + TimescaleDB tick database.
//
// Schema:
//   ticks       — raw tick data; hypertable partitioned by day
//   bars_1min   — continuous aggregate (auto-refreshed every minute)
//   bars_5min   — continuous aggregate
//   bars_15min  — continuous aggregate
//   audit_log   — event audit trail (written by AuditLog)
//
// Single writer (collector) + unlimited concurrent readers (bots).
// Uses libpq; not thread-safe — create one TickDB per thread/process.
class TickDB {
public:
    explicit TickDB(const std::string& connstr, bool read_only = false);
    ~TickDB();

    TickDB(const TickDB&)            = delete;
    TickDB& operator=(const TickDB&) = delete;

    // Write a batch of ticks; returns the number actually inserted (after dedup)
    int write(const std::vector<TickRow>& rows);

    // Read helpers
    int64_t               row_count();
    std::optional<double> latest_price();
    DBSummary             summary();

    // Raw connection for audit logger
    PGconn* conn() const { return conn_; }

    void close();

private:
    void ensure_schema();
    void exec(const char* sql);

    // Format int64 microseconds as "YYYY-MM-DD HH:MM:SS.ffffff+00"
    static std::string format_ts(int64_t ts_micros);

    PGconn*     conn_     = nullptr;
    std::string connstr_;
    bool        read_only_;
};
