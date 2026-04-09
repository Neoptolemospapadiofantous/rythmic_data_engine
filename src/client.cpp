#include "client.hpp"
#include "log.hpp"

// Generated protobuf headers (built into cmake binary dir)
#include "rithmic.pb.h"

#include <boost/asio/experimental/awaitable_operators.hpp>
#include <boost/asio/experimental/parallel_group.hpp>
#include <boost/asio/steady_timer.hpp>

#include <chrono>
#include <cstdio>
#include <stdexcept>
#include <string>

namespace asio_exp = boost::asio::experimental;
using namespace asio_exp::awaitable_operators;

// ── Constructor ────────────────────────────────────────────────────

RithmicClient::RithmicClient(asio::io_context& ioc, const Config& cfg)
    : ioc_(ioc), ssl_ctx_(ssl::context::tls_client), cfg_(cfg)
{
    // Load Rithmic's custom CA certificate
    ssl_ctx_.load_verify_file(cfg_.cert_path);
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

// ── ConnectionTestResult::print ────────────────────────────────────

void ConnectionTestResult::print() const {
    for (auto& s : steps) {
        if (s.ms >= 0)
            std::printf("  %s  %-44s  %5lld ms  %s\n",
                s.ok ? "✓" : "✗", s.name.c_str(),
                (long long)s.ms, s.detail.c_str());
        else
            std::printf("  %s  %-44s  (no time)  %s\n",
                s.ok ? "✓" : "✗", s.name.c_str(), s.detail.c_str());
    }
    if (first_price > 0) {
        std::printf("\n");
        std::printf("  First tick  price=%.2f  qty=%lld  %s\n",
            first_price, (long long)first_size, first_is_buy ? "BUY" : "SELL");
        if (wire_latency_us > 0)
            std::printf("  Wire latency  %lld µs  (%.1f ms)\n",
                (long long)wire_latency_us, wire_latency_us / 1000.0);
        if (db_total_ticks > 0)
            std::printf("  DB total rows  %lld\n", (long long)db_total_ticks);
    }
}

// ── run_connection_test ────────────────────────────────────────────

asio::awaitable<ConnectionTestResult>
RithmicClient::run_connection_test(TickDB& db, int n_ticks) {
    ConnectionTestResult result;
    using clock = std::chrono::steady_clock;

    auto ms_since = [](clock::time_point t0) -> int64_t {
        return std::chrono::duration_cast<std::chrono::milliseconds>(
            clock::now() - t0).count();
    };
    auto push = [&](std::string name, int64_t ms, bool ok, std::string detail = "") {
        result.steps.push_back({std::move(name), ms, ok, std::move(detail)});
    };

    // ── Step 1: TCP + SSL + WS + system info probe ─────────────────
    clock::time_point t = clock::now();
    try {
        auto probe = co_await connect_ws();
        push("TCP + SSL + WebSocket connect", ms_since(t), true, cfg_.url);
        t = clock::now();
        co_await get_system_info(*probe);
        push("RequestRithmicSystemInfo (16→17)", ms_since(t), true,
             "system=" + cfg_.system_name);
        probe->async_close(websocket::close_code::normal, asio::detached);
    } catch (std::exception& e) {
        push("Connect / SystemInfo", ms_since(t), false, e.what());
        co_return result;
    }

    // ── Step 2: Login ──────────────────────────────────────────────
    std::unique_ptr<WsStream> ws;
    t = clock::now();
    try {
        ws = co_await connect_ws();
        co_await login(*ws);
        push("RequestLogin (10→11)", ms_since(t), true,
             "hb=" + std::to_string(static_cast<int>(heartbeat_interval_)) + "s");
    } catch (std::exception& e) {
        push("RequestLogin (10→11)", ms_since(t), false, e.what());
        co_return result;
    }

    // ── Step 3: Subscribe ──────────────────────────────────────────
    t = clock::now();
    try {
        co_await subscribe(*ws, cfg_.symbol, cfg_.exchange);
        push("RequestMarketDataUpdate (100→101)", ms_since(t), true,
             cfg_.symbol + "/" + cfg_.exchange);
    } catch (std::exception& e) {
        push("Subscribe", ms_since(t), false, e.what());
        co_return result;
    }

    // ── Step 4: Receive N ticks (60s timeout) ─────────────────────
    std::vector<TickRow> ticks;
    t = clock::now();
    {
        auto ex = co_await asio::this_coro::executor;
        asio::steady_timer deadline(ex);
        deadline.expires_after(std::chrono::seconds(60));
        bool timed_out  = false;
        bool read_error = false;
        std::string read_errmsg;

        while (static_cast<int>(ticks.size()) < n_ticks && !timed_out && !read_error) {
            beast::flat_buffer buf;
            auto [order, rd_ec, rd_n, tm_ec] =
                co_await asio_exp::make_parallel_group(
                    ws->async_read(buf, asio::deferred),
                    deadline.async_wait(asio::deferred)
                ).async_wait(asio_exp::wait_for_one(), use_awaitable);

            if (order[0] == 1) { timed_out = true; break; }
            if (rd_ec)         { read_error = true; read_errmsg = rd_ec.message(); break; }

            auto payload = strip_header(beast::buffers_to_string(buf.data()));
            rti::Base base; base.ParseFromString(payload);
            if (base.template_id() != 150) continue;

            rti::LastTrade lt; lt.ParseFromString(payload);
            if (lt.trade_price() <= 0 || lt.trade_size() <= 0) continue;

            int64_t ts_us  = static_cast<int64_t>(lt.ssboe()) * 1'000'000LL + lt.usecs();
            bool    is_buy = (lt.aggressor() == rti::LastTrade::BUY);

            if (ticks.empty()) {
                result.first_price   = lt.trade_price();
                result.first_size    = lt.trade_size();
                result.first_is_buy  = is_buy;
                auto now_us = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::system_clock::now().time_since_epoch()).count();
                result.wire_latency_us = now_us - ts_us;
            }
            ticks.push_back({ts_us, lt.trade_price(), lt.trade_size(), is_buy});
        }

        int got = static_cast<int>(ticks.size());
        if (read_error) {
            push("Receive ticks", ms_since(t), false, read_errmsg);
        } else if (timed_out && got == 0) {
            push("Receive ticks (60s timeout)", ms_since(t), false,
                 "no ticks received — market may be closed");
        } else {
            std::string det = "first price=" + std::to_string(result.first_price);
            if (result.wire_latency_us > 0)
                det += " wire=" + std::to_string(result.wire_latency_us / 1000) + "ms";
            push("Receive " + std::to_string(got) + "/" + std::to_string(n_ticks) + " ticks",
                 ms_since(t), true, det);
        }
    }

    // ── Step 5: DB write ───────────────────────────────────────────
    if (!ticks.empty()) {
        t = clock::now();
        try {
            int written = db.write(ticks);
            push("DB write — UNNEST batch INSERT", ms_since(t), true,
                 std::to_string(written) + "/" + std::to_string(ticks.size()) + " new rows");
        } catch (std::exception& e) {
            push("DB write", ms_since(t), false, e.what());
        }
    }

    // ── Step 6: DB read-back ───────────────────────────────────────
    t = clock::now();
    try {
        result.db_total_ticks = db.row_count();
        push("DB read — COUNT(*) ticks", ms_since(t), true,
             std::to_string(result.db_total_ticks) + " total rows");
    } catch (std::exception& e) {
        push("DB read", ms_since(t), false, e.what());
    }

    // ── Clean shutdown ─────────────────────────────────────────────
    try {
        co_await unsubscribe(*ws, cfg_.symbol, cfg_.exchange);
        co_await send_logout(*ws);
        ws->async_close(websocket::close_code::normal, asio::detached);
    } catch (...) {}

    co_return result;
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
        auto [order, read_ec, read_n, timer_ec] =
            co_await asio_exp::make_parallel_group(
                ws.async_read(buf, asio::deferred),
                hb_timer.async_wait(asio::deferred)
            ).async_wait(asio_exp::wait_for_one(), use_awaitable);

        if (order[0] == 0) {
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
    auto ex = co_await asio::this_coro::executor;

    while (running_) {
        int  delay_s   = 0;
        bool had_error = false;

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
            // co_await is not allowed inside catch — record and act after
            if (!running_) break;
            delay_s = std::min(30 * (1 << std::min(attempt, 4)), 300);
            LOG("Disconnected: %s — reconnecting in %ds", e.what(), delay_s);
            ++attempt;
            had_error = true;
        }

        if (had_error && running_) {
            asio::steady_timer t(ex);
            t.expires_after(std::chrono::seconds(delay_s));
            co_await t.async_wait(use_awaitable);
        }
    }
}
