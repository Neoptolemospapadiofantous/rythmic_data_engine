#pragma once
#include <atomic>
#include <chrono>
#include <functional>
#include <memory>
#include <string>

#include <boost/asio.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/beast.hpp>
#include <boost/beast/ssl.hpp>
#include <boost/beast/websocket.hpp>
#include <boost/beast/websocket/ssl.hpp>

#include "config.hpp"
#include "db.hpp"

namespace asio      = boost::asio;
namespace beast     = boost::beast;
namespace http      = beast::http;
namespace websocket = beast::websocket;
namespace ssl       = asio::ssl;
using tcp           = asio::ip::tcp;
using use_awaitable_t = asio::use_awaitable_t<>;
inline constexpr use_awaitable_t use_awaitable{};

// Callback invoked for every valid tick received
using TickCallback = std::function<void(TickRow)>;

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
    explicit RithmicClient(asio::io_context& ioc, const Config& cfg);

    // Set the callback invoked for each tick
    void set_on_tick(TickCallback cb) { on_tick_ = std::move(cb); }

    // Run the connection + reconnection loop (runs until stop() is called)
    asio::awaitable<void> run();

    // Request a clean shutdown
    void stop() { running_ = false; }

private:
    // ── WebSocket type ─────────────────────────────────────────────
    using WsStream = websocket::stream<beast::ssl_stream<beast::tcp_stream>>;

    // ── helpers ────────────────────────────────────────────────────
    asio::awaitable<std::unique_ptr<WsStream>> connect_ws();

    asio::awaitable<void> get_system_info(WsStream& ws);
    asio::awaitable<void> login(WsStream& ws);
    asio::awaitable<void> subscribe(WsStream& ws,
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
    double             heartbeat_interval_ = 30.0;
    std::atomic<bool>  running_{true};
};
