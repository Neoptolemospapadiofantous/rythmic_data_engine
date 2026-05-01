#pragma once
#include <atomic>
#include <chrono>
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include <boost/asio.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/beast.hpp>
#include <boost/beast/ssl.hpp>
#include <boost/beast/websocket.hpp>
#include <boost/beast/websocket/ssl.hpp>

#include "config.hpp"
#include "db.hpp"

// ── ConnectionTestResult ───────────────────────────────────────────
// Returned by RithmicClient::run_connection_test().
// Each Step records one stage of the handshake/data-flow pipeline.
struct ConnectionTestResult {
    struct Step {
        std::string name;
        int64_t     ms   {-1};   // wall-clock duration
        bool        ok   {false};
        std::string detail;
    };
    std::vector<Step> steps;

    double  first_price     = 0;
    int64_t first_size      = 0;
    bool    first_is_buy    = false;
    int64_t wire_latency_us = 0;   // now − tick.ssboe+usecs
    int64_t db_total_ticks  = 0;

    bool all_ok() const {
        if (steps.empty()) return false;
        for (auto& s : steps) if (!s.ok) return false;
        return true;
    }

    void print() const;   // defined in client.cpp
};

namespace asio      = boost::asio;
namespace beast     = boost::beast;
namespace http      = beast::http;
namespace websocket = beast::websocket;
namespace ssl       = asio::ssl;
using tcp           = asio::ip::tcp;
using use_awaitable_t = asio::use_awaitable_t<>;
inline constexpr use_awaitable_t use_awaitable{};

// Callback invoked for every valid tick received
using TickCallback  = std::function<void(TickRow)>;
using BBOCallback   = std::function<void(BBORow)>;
using DepthCallback = std::function<void(DepthRow)>;

// Thrown by login() on authentication rejection — not a transient error,
// so the reconnect loop propagates it rather than retrying.
struct LoginError : std::runtime_error {
    using std::runtime_error::runtime_error;
};

// Rithmic WebSocket client (ticker plant only — data collection).
//
// Wire protocol: every WebSocket message carries a 4-byte big-endian signed
// int (payload length) followed by a protobuf-serialised message.  The length
// prefix is redundant with WS framing but is required by the Rithmic protocol.
//
// Connection sequence (mirrors async_rithmic TickerPlant):
//   1. Connect + RequestRithmicSystemInfo(16) → validate system_name → close
//   2. Reconnect + RequestLogin(10) → RequestHeartbeat(18) → subscribe(100)
//   3. Receive loop — dispatch LastTrade(150), send heartbeats on schedule
class RithmicClient {
public:
    using WsStream = websocket::stream<beast::ssl_stream<beast::tcp_stream>>;

    explicit RithmicClient(asio::io_context& ioc, const Config& cfg);

    // Set the callback invoked for each tick / BBO / depth event
    void set_on_tick(TickCallback cb)   { on_tick_  = std::move(cb); }
    void set_on_bbo(BBOCallback cb)     { on_bbo_   = std::move(cb); }
    void set_on_depth(DepthCallback cb) { on_depth_ = std::move(cb); }

    // Run the connection + reconnection loop (runs until stop() is called)
    asio::awaitable<void> run();

    // Step-by-step timed connection + data-flow test.
    // Connects, logs in, receives n_ticks ticks, writes them to db, reads back.
    // Returns a ConnectionTestResult with per-step timing and pass/fail.
    asio::awaitable<ConnectionTestResult>
    run_connection_test(TickDB& db, int n_ticks = 5);

    // Request a clean shutdown
    void stop() { running_ = false; }

private:
    // ── helpers ────────────────────────────────────────────────────
    asio::awaitable<std::unique_ptr<WsStream>> connect_ws();

    asio::awaitable<void> get_system_info(WsStream& ws);
    asio::awaitable<void> login(WsStream& ws);
    asio::awaitable<void> subscribe(WsStream& ws,
                                    const std::string& symbol,
                                    const std::string& exchange);
    asio::awaitable<void> subscribe_depth(WsStream& ws,
                                          const std::string& symbol,
                                          const std::string& exchange);
    asio::awaitable<void> unsubscribe(WsStream& ws,
                                      const std::string& symbol,
                                      const std::string& exchange);
    asio::awaitable<void> send_heartbeat(WsStream& ws);
    asio::awaitable<void> send_logout(WsStream& ws);

    asio::awaitable<void> receive_loop(WsStream& ws);
    void dispatch_message(const std::string& payload);

    // Serialize proto message with Rithmic 4-byte big-endian length prefix
    template <class Msg>
    static std::string frame(const Msg& msg) {
        std::string payload = msg.SerializeAsString();
        int32_t len = 0;
        // big-endian encode
        auto sz = static_cast<uint32_t>(payload.size());
        len = static_cast<int32_t>(__builtin_bswap32(sz));
        std::string wire(reinterpret_cast<char*>(&len), 4);
        wire += payload;
        return wire;
    }

    // Strip 4-byte length prefix, return raw protobuf payload
    static std::string strip_header(const std::string& wire) {
        if (wire.size() < 4)
            throw std::runtime_error("Rithmic: message too short");
        return wire.substr(4);
    }

    asio::io_context&  ioc_;
    ssl::context       ssl_ctx_;
    Config             cfg_;
    TickCallback       on_tick_;
    BBOCallback        on_bbo_;
    DepthCallback      on_depth_;
    double             heartbeat_interval_ = 30.0;
    std::atomic<bool>  running_{true};
    std::atomic<bool>  hb_response_pending_{false}; // set by dispatch_message on template 18
};
