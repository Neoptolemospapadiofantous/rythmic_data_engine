#pragma once
#include <atomic>
#include <chrono>
#include <memory>
#include <mutex>
#include <vector>

#include <boost/asio.hpp>

#include "audit.hpp"
#include "client.hpp"
#include "config.hpp"
#include "db.hpp"
#include "wal.hpp"

namespace asio = boost::asio;

class Collector {
public:
    // Flush thresholds — tuned for sub-100ms tick-to-PG latency.
    // At RTH volume (~25 ticks/s): 5-tick batch fills in ~200ms, avg ~100ms.
    // 0.1s time fallback caps latency during off-hours quiet periods.
    static constexpr int    FLUSH_EVERY_N         = 5;
    static constexpr double FLUSH_EVERY_SEC       = 0.1;
    static constexpr int    BBO_FLUSH_EVERY_N     = 5;
    static constexpr double BBO_FLUSH_EVERY_SEC   = 0.1;
    static constexpr int    DEPTH_FLUSH_EVERY_N   = 10;
    static constexpr double DEPTH_FLUSH_EVERY_SEC = 0.1;

    explicit Collector(const Config& cfg);
    ~Collector();

    void run();
    void stop();

private:
    void on_tick(TickRow row);
    void on_bbo(BBORow row);
    void on_depth(DepthRow row);
    int  flush();
    int  flush_bbo();
    int  flush_depth();
    void status_log();

    Config                         cfg_;
    std::unique_ptr<TickDB>        db_;
    std::unique_ptr<AuditLog>      audit_;
    std::unique_ptr<Wal>           wal_;
    asio::io_context               ioc_;
    std::unique_ptr<RithmicClient> client_;

    std::mutex           buf_mu_;
    std::vector<TickRow> buf_;
    std::chrono::steady_clock::time_point last_flush_;
    std::chrono::steady_clock::time_point last_audit_flush_;

    std::mutex            bbo_mu_;
    std::vector<BBORow>   bbo_buf_;
    std::chrono::steady_clock::time_point last_bbo_flush_;

    std::mutex              depth_mu_;
    std::vector<DepthRow>   depth_buf_;
    std::chrono::steady_clock::time_point last_depth_flush_;

    std::atomic<int64_t> session_total_{0};
    std::atomic<int64_t> rejected_total_{0};
    std::atomic<bool>    running_{true};
};
