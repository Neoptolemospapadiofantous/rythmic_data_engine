#include "client.hpp"
#include "log.hpp"

// Generated protobuf headers (built into cmake binary dir)
#include "proto/rithmic.pb.h"

#include <boost/asio/experimental/awaitable_operators.hpp>
#include <boost/asio/experimental/parallel_group.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/endian/conversion.hpp>

#include <chrono>
#include <cstring>
#include <stdexcept>
#include <string>

namespace asio_exp = boost::asio::experimental;
using namespace asio_exp::awaitable_operators;

// ── Framing helpers ────────────────────────────────────────────────

template <class Msg>
std::string RithmicClient::frame(const Msg& msg) {
    std::string payload = msg.SerializeAsString();
    int32_t len = boost::endian::native_to_big(static_cast<int32_t>(payload.size()));
    std::string wire(reinterpret_cast<char*>(&len), 4);
    wire += payload;
    return wire;
}

std::string RithmicClient::strip_header(const std::string& wire) {
    if (wire.size() < 4)
        throw std::runtime_error("Rithmic: message too short");
    return wire.substr(4);
}

// ── Constructor ────────────────────────────────────────────────────

RithmicClient::RithmicClient(asio::io_context& ioc, const Config& cfg)
    : ioc_(ioc), ssl_ctx_(ssl::context::tls_client), cfg_(cfg)
{
    // Load Rithmic's custom CA certificate
    ssl_ctx_.load_verify_locations(cfg_.cert_path);
    ssl_ctx_.set_verify_mode(ssl::verify_peer);
}

// ── connect_ws ─────────────────────────────────────────────────────

asio::awaitable<std::unique_ptr<RithmicClient::WsStream>>
RithmicClient::connect_ws() {
    // Parse "wss://host:port" from cfg_.url
    std::string url = cfg_.url;
    if (url.substr(0, 6) == "wss://") url = url.substr(6);
    std::string host, port;
    auto colon = url.find(':');
    if (colon != std::string::npos) {
        host = url.substr(0, colon);
        port = url.substr(colon + 1);
    } else {
        host = url;
        port = "443";
    }

    auto ex = co_await asio::this_coro::executor;

    // Resolve
    tcp::resolver resolver(ex);
    auto results = co_await resolver.async_resolve(host, port, use_awaitable);

    // Create stream
    auto ws = std::make_unique<WsStream>(ex, ssl_ctx_);

    // TCP connect
    co_await beast::get_lowest_layer(*ws).async_connect(results, use_awaitable);

    // SNI
    if (!SSL_set_tlsext_host_name(ws->next_layer().native_handle(), host.c_str()))
        throw std::runtime_error("SSL_set_tlsext_host_name failed");

    // SSL handshake
    co_await ws->next_layer().async_handshake(ssl::stream_base::client, use_awaitable);

    // WS handshake
    ws->set_option(websocket::stream_base::decorator([&](websocket::request_type& req) {
        req.set(http::field::user_agent, "rithmic_engine/1.0");
    }));
    co_await ws->async_handshake(host + ":" + port, "/", use_awaitable);

    co_return ws;
}

// ── send helpers ───────────────────────────────────────────────────

static asio::awaitable<void> ws_write(RithmicClient::WsStream& ws,
                                      const std::string& data) {
    ws.binary(true);
    co_await ws.async_write(asio::buffer(data), use_awaitable);
}

// ── get_system_info ────────────────────────────────────────────────

asio::awaitable<void> RithmicClient::get_system_info(WsStream& ws) {
    rti::RequestRithmicSystemInfo req;
    req.set_template_id(16);
    co_await ws_write(ws, frame(req));

    // Wait for response with template_id == 17
    beast::flat_buffer buf;
    for (;;) {
        buf.clear();
        co_await ws.async_read(buf, use_awaitable);
        auto payload = strip_header(beast::buffers_to_string(buf.data()));

        rti::Base base;
        base.ParseFromString(payload);
        if (base.template_id() != 17) continue;

        rti::ResponseRithmicSystemInfo resp;
        resp.ParseFromString(payload);

        bool found = false;
        for (auto& sn : resp.system_name())
            if (sn == cfg_.system_name) { found = true; break; }

        if (!found) {
            std::string avail;
            for (auto& sn : resp.system_name()) avail += sn + " ";
            throw std::runtime_error(
                "System name '" + cfg_.system_name +
                "' not found. Available: " + avail);
        }
        co_return;
    }
}

// ── login ──────────────────────────────────────────────────────────

asio::awaitable<void> RithmicClient::login(WsStream& ws) {
    rti::RequestLogin req;
    req.set_template_id(10);
    req.set_template_version("3.9");
    req.set_user(cfg_.user);
    req.set_password(cfg_.password);
    req.set_system_name(cfg_.system_name);
    req.set_app_name(cfg_.app_name);
    req.set_app_version(cfg_.app_version);
    req.set_infra_type(rti::RequestLogin::TICKER_PLANT);
    co_await ws_write(ws, frame(req));

    // Wait for response with template_id == 11
    beast::flat_buffer buf;
    for (;;) {
        buf.clear();
        co_await ws.async_read(buf, use_awaitable);
        auto payload = strip_header(beast::buffers_to_string(buf.data()));

        rti::Base base;
        base.ParseFromString(payload);
        if (base.template_id() != 11) continue;

        rti::ResponseLogin resp;
        resp.ParseFromString(payload);

        if (!resp.rp_code().empty() && resp.rp_code(0) != "0")
            throw std::runtime_error("Login failed: " + resp.rp_code(0));

        if (resp.heartbeat_interval() > 0)
            heartbeat_interval_ = resp.heartbeat_interval();

        LOG("Login OK — heartbeat interval: %.0fs", heartbeat_interval_);
        co_return;
    }
}

// ── send_heartbeat ─────────────────────────────────────────────────

asio::awaitable<void> RithmicClient::send_heartbeat(WsStream& ws) {
    using namespace std::chrono;
    auto now  = system_clock::now().time_since_epoch();
    auto secs = duration_cast<seconds>(now).count();
    auto usec = duration_cast<microseconds>(now).count() % 1'000'000;

    rti::RequestHeartbeat req;
    req.set_template_id(18);
    req.set_ssboe(static_cast<int32_t>(secs));
    req.set_usecs(static_cast<int32_t>(usec));
    co_await ws_write(ws, frame(req));
}

// ── subscribe / unsubscribe ────────────────────────────────────────

asio::awaitable<void> RithmicClient::subscribe(WsStream& ws,
                                                const std::string& symbol,
                                                const std::string& exchange) {
    rti::RequestMarketDataUpdate req;
    req.set_template_id(100);
    req.set_symbol(symbol);
    req.set_exchange(exchange);
    req.set_request(rti::RequestMarketDataUpdate::SUBSCRIBE);
    req.set_update_bits(1);  // LAST_TRADE
    co_await ws_write(ws, frame(req));
    LOG("Subscribed to %s/%s", symbol.c_str(), exchange.c_str());
}

asio::awaitable<void> RithmicClient::unsubscribe(WsStream& ws,
                                                   const std::string& symbol,
                                                   const std::string& exchange) {
    rti::RequestMarketDataUpdate req;
    req.set_template_id(100);
    req.set_symbol(symbol);
    req.set_exchange(exchange);
    req.set_request(rti::RequestMarketDataUpdate::UNSUBSCRIBE);
    req.set_update_bits(1);
    co_await ws_write(ws, frame(req));
}

asio::awaitable<void> RithmicClient::send_logout(WsStream& ws) {
    rti::RequestLogout req;
    req.set_template_id(12);
    co_await ws_write(ws, frame(req));
}

// ── dispatch_message ───────────────────────────────────────────────

void RithmicClient::dispatch_message(const std::string& payload) {
    rti::Base base;
    base.ParseFromString(payload);

    if (base.template_id() == 150) {
        rti::LastTrade lt;
        lt.ParseFromString(payload);

        if (lt.trade_price() <= 0 || lt.trade_size() <= 0) return;

        using namespace std::chrono;
        int64_t ts_micros =
            static_cast<int64_t>(lt.ssboe()) * 1'000'000LL +
            static_cast<int64_t>(lt.usecs());

        bool is_buy = (lt.aggressor() == rti::LastTrade::BUY);

        if (on_tick_)
            on_tick_(TickRow{ts_micros, lt.trade_price(),
                             lt.trade_size(), is_buy});
    }
    // template_id 19 = ResponseHeartbeat — silently ignored
    // template_id 101 = ResponseMarketDataUpdate — silently ignored
}

// ── receive_loop ───────────────────────────────────────────────────

asio::awaitable<void> RithmicClient::receive_loop(WsStream& ws) {
    auto ex = co_await asio::this_coro::executor;
    asio::steady_timer hb_timer(ex);
    auto schedule_hb = [&] {
        hb_timer.expires_after(
            std::chrono::duration_cast<std::chrono::steady_clock::duration>(
                std::chrono::duration<double>(heartbeat_interval_)));
    };
    schedule_hb();

    while (running_) {
        beast::flat_buffer buf;

        // Wait for either a WS message or the heartbeat deadline
        auto [which, read_ec, read_n, timer_ec] =
            co_await asio_exp::make_parallel_group(
                ws.async_read(buf, asio::as_tuple(use_awaitable)),
                hb_timer.async_wait(asio::as_tuple(use_awaitable))
            ).async_wait(asio_exp::wait_for_one(), use_awaitable);

        if (which[0] == 0) {
            // A WS message arrived first
            if (read_ec) throw beast::system_error(read_ec);
            dispatch_message(strip_header(beast::buffers_to_string(buf.data())));
        } else {
            // Heartbeat timer fired first
            if (timer_ec == asio::error::operation_aborted) break;
            co_await send_heartbeat(ws);
            schedule_hb();
        }
    }
}

// ── run (main loop) ────────────────────────────────────────────────

asio::awaitable<void> RithmicClient::run() {
    int attempt = 0;

    while (running_) {
        try {
            // ── Step 1: system info probe ──────────────────────────
            {
                auto ws = co_await connect_ws();
                co_await get_system_info(*ws);
                ws->async_close(websocket::close_code::normal,
                                asio::detached);
            }

            // ── Step 2: real session ───────────────────────────────
            {
                auto ws = co_await connect_ws();
                co_await login(*ws);
                co_await send_heartbeat(*ws);

                // Get front-month contract (best-effort: use symbol directly)
                std::string contract = cfg_.symbol;
                LOG("Connected — streaming %s on %s",
                    contract.c_str(), cfg_.exchange.c_str());

                co_await subscribe(*ws, contract, cfg_.exchange);
                co_await receive_loop(*ws);

                // Clean shutdown
                co_await unsubscribe(*ws, contract, cfg_.exchange);
                co_await send_logout(*ws);
                ws->async_close(websocket::close_code::normal, asio::detached);
            }

            attempt = 0;

        } catch (std::exception& e) {
            if (!running_) break;
            auto delay = std::min(30 * (1 << std::min(attempt, 4)), 300);
            LOG("Disconnected: %s — reconnecting in %ds", e.what(), delay);
            ++attempt;

            asio::steady_timer t(co_await asio::this_coro::executor);
            t.expires_after(std::chrono::seconds(delay));
            co_await t.async_wait(use_awaitable);
        }
    }
}
