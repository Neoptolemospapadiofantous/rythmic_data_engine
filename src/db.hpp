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

struct BBORow {
    int64_t     ts_micros;
    double      bid_price;
    int32_t     bid_size;
    int32_t     bid_orders;
    double      ask_price;
    int32_t     ask_size;
    int32_t     ask_orders;
    std::string symbol;
    std::string exchange;
};

struct DepthRow {
    int64_t     ts_micros;         // from ssboe+usecs
    int64_t     source_ns;         // from source_ssboe+source_nsecs (nanosecond precision)
    int64_t     sequence_number;
    int8_t      update_type;       // 1=NEW, 2=CHANGE, 3=DELETE
    int8_t      transaction_type;  // 1=BUY, 2=SELL
    double      depth_price;
    double      prev_depth_price;
    int32_t     depth_size;
    std::string exchange_order_id;
    std::string symbol;
    std::string exchange;
};

struct DBSummary {
    int64_t               tick_count = 0;
    std::string           earliest;
    std::string           latest;
    std::optional<double> price;
    std::string           connstr;
};

struct SessionRow {
    int64_t id = 0;
    std::string mode = "collect";
};

struct QualityMetric {
    std::string metric;
    double      value;
    std::string labels_json;  // optional
};

struct SentinelAlertRow {
    int64_t     session_id = 0;
    std::string check_name;
    std::string severity;
    std::string message;
    double      value = 0.0;
};

struct GateResult {
    std::string gate_name;
    std::string status;     // "pass", "fail", "skip"
    double      threshold  = 0.0;
    double      actual     = 0.0;
    std::string details_json;
    int64_t     session_id = 0;
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

    // Write a batch of BBO snapshots; returns the number actually inserted
    int write_bbo(const std::vector<BBORow>& rows);

    // Write a batch of depth-by-order events; returns the number actually inserted
    int write_depth(const std::vector<DepthRow>& rows);

    // Reconnect after a DB failure (PQreset + schema check)
    void reconnect();

    // Returns true if the connection is currently alive
    bool is_connected() const { return conn_ && PQstatus(conn_) == CONNECTION_OK; }

    // Session lifecycle
    int64_t start_session(const std::string& mode = "collect",
                          const std::string& params_json = "");
    void    end_session(int64_t session_id, int64_t ticks, int64_t bbo,
                        int64_t depth, int64_t rejected, int64_t gaps,
                        int64_t alerts, const std::string& summary_json = "");

    // Quality metrics
    void write_metric(const QualityMetric& m);
    void write_metrics(const std::vector<QualityMetric>& ms);

    // Sentinel alerts
    void write_sentinel_alerts(const std::vector<SentinelAlertRow>& alerts);

    // Gate results
    void write_gate_result(const GateResult& g);

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
