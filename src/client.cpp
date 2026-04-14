#include "client.hpp"
#include "log.hpp"

// Generated protobuf headers (built into cmake binary dir)
#include "rithmic.pb.h"

#include <boost/asio/experimental/awaitable_operators.hpp>
#include <boost/asio/experimental/parallel_group.hpp>  // used by connection test
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

    // TCP_NODELAY — disable Nagle's algorithm so small frames go out immediately
    // instead of being buffered for up to 40ms waiting for more data.
    // SO_RCVBUF/SO_SNDBUF — bump kernel socket buffers to absorb tick bursts.
    {
        auto& sock = beast::get_lowest_layer(*ws).socket();
        sock.set_option(asio::ip::tcp::no_delay(true));
        sock.set_option(asio::socket_base::receive_buffer_size(1 << 20));  // 1 MB
        sock.set_option(asio::socket_base::send_buffer_size(256 << 10));   // 256 KB
    }

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
    req.set_update_bits(1 | 2);  // LAST_TRADE | BBO
    co_await ws_write(ws, frame(req));
    LOG("Subscribed to %s/%s (LAST_TRADE|BBO)", symbol.c_str(), exchange.c_str());
}

asio::awaitable<void> RithmicClient::subscribe_depth(WsStream& ws,
                                                      const std::string& symbol,
                                                      const std::string& exchange) {
    rti::RequestMarketDataUpdate req;
    req.set_template_id(100);
    req.set_symbol(symbol);
    req.set_exchange(exchange);
    req.set_request(rti::RequestMarketDataUpdate::SUBSCRIBE);
    req.set_update_bits(64);  // DEPTH_BY_ORDER
    co_await ws_write(ws, frame(req));
    LOG("Subscribed depth-by-order for %s/%s", symbol.c_str(), exchange.c_str());
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

            std::string sym  = lt.symbol().empty()  ? cfg_.symbol   : lt.symbol();
            std::string exch = lt.exchange().empty() ? cfg_.exchange : lt.exchange();

            if (ticks.empty()) {
                result.first_price   = lt.trade_price();
                result.first_size    = lt.trade_size();
                result.first_is_buy  = is_buy;
                auto now_us = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::system_clock::now().time_since_epoch()).count();
                result.wire_latency_us = now_us - ts_us;
            }
            ticks.push_back({ts_us, lt.trade_price(), lt.trade_size(),
                             is_buy, sym, exch});
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

        // Use symbol/exchange from message if present, fall back to config
        std::string sym  = lt.symbol().empty()   ? cfg_.symbol   : lt.symbol();
        std::string exch = lt.exchange().empty()  ? cfg_.exchange : lt.exchange();

        if (on_tick_)
            on_tick_(TickRow{ts_micros, lt.trade_price(),
                             lt.trade_size(), is_buy,
                             std::move(sym), std::move(exch)});

    } else if (base.template_id() == 151) {
        rti::BestBidOffer bbo;
        bbo.ParseFromString(payload);

        // Accept one-sided updates (Rithmic sends bid-only or ask-only on partial fills)
        if (bbo.bid_price() <= 0 && bbo.ask_price() <= 0) return;

        int64_t ts_us = static_cast<int64_t>(bbo.ssboe()) * 1'000'000LL +
                        static_cast<int64_t>(bbo.usecs());

        std::string sym  = bbo.symbol().empty()   ? cfg_.symbol   : bbo.symbol();
        std::string exch = bbo.exchange().empty()  ? cfg_.exchange : bbo.exchange();

        if (on_bbo_)
            on_bbo_(BBORow{ts_us,
                           bbo.bid_price(), bbo.bid_size(), bbo.bid_orders(),
                           bbo.ask_price(), bbo.ask_size(), bbo.ask_orders(),
                           std::move(sym), std::move(exch)});

    } else if (base.template_id() == 160) {
        rti::DepthByOrder dbo;
        dbo.ParseFromString(payload);

        if (dbo.depth_price() <= 0) return;

        int64_t ts_us  = static_cast<int64_t>(dbo.ssboe()) * 1'000'000LL +
                         static_cast<int64_t>(dbo.usecs());
        int64_t src_ns = static_cast<int64_t>(dbo.source_ssboe()) * 1'000'000'000LL +
                         static_cast<int64_t>(dbo.source_nsecs());

        if (on_depth_)
            on_depth_(DepthRow{ts_us, src_ns, dbo.sequence_number(),
                               static_cast<int8_t>(dbo.update_type()),
                               static_cast<int8_t>(dbo.transaction_type()),
                               dbo.depth_price(), dbo.prev_depth_price(),
                               dbo.depth_size(), dbo.exchange_order_id(),
                               dbo.symbol().empty()   ? cfg_.symbol   : dbo.symbol(),
                               dbo.exchange().empty()  ? cfg_.exchange : dbo.exchange()});
    } else if (base.template_id() == 18) {
        // RequestHeartbeat from Rithmic — we must respond with ResponseHeartbeat (19)
        hb_response_pending_.store(true);
    }
    // template_id 19 = ResponseHeartbeat (our own replies echoed back) — ignored
    // template_id 101 = ResponseMarketDataUpdate — silently ignored
}

// ── receive_loop ───────────────────────────────────────────────────
//
// Uses Beast's built-in tcp_stream timeout instead of parallel_group.
// parallel_group cancels the in-progress async_read when the heartbeat
// timer wins, which corrupts Beast's WebSocket frame parser and causes
// "Operation canceled [system:125]" on the next read — disconnecting
// every ~60 seconds and preventing BBO/depth data from ever arriving.
//
// With Beast timeout, the tcp_stream returns beast::error::timeout on
// expiry without corrupting the frame parser, so we can safely send
// the heartbeat and continue reading.

asio::awaitable<void> RithmicClient::receive_loop(WsStream& ws) {
    auto last_hb = std::chrono::steady_clock::now();
    int hb_secs = std::max(1, static_cast<int>(heartbeat_interval_));

    // Set per-operation timeout on the underlying tcp_stream
    beast::get_lowest_layer(ws).expires_after(std::chrono::seconds(hb_secs));

    while (running_) {
        beast::flat_buffer buf;
        boost::system::error_code ec;

        co_await ws.async_read(buf, asio::redirect_error(use_awaitable, ec));

        if (ec == beast::error::timeout) {
            // Read timed out — send heartbeat and keep going
            co_await send_heartbeat(ws);
            last_hb = std::chrono::steady_clock::now();
            beast::get_lowest_layer(ws).expires_after(std::chrono::seconds(hb_secs));
            continue;
        }

        if (ec) throw beast::system_error(ec);

        // Reset timeout after each successful read
        beast::get_lowest_layer(ws).expires_after(std::chrono::seconds(hb_secs));

        dispatch_message(strip_header(beast::buffers_to_string(buf.data())));

        // Respond to Rithmic's heartbeat requests (template 18)
        if (hb_response_pending_.exchange(false))
            co_await send_heartbeat(ws);

        // Proactively send our own heartbeat if interval nearly elapsed
        auto now = std::chrono::steady_clock::now();
        double elapsed = std::chrono::duration<double>(now - last_hb).count();
        if (elapsed >= heartbeat_interval_ * 0.9) {
            co_await send_heartbeat(ws);
            last_hb = now;
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
                co_await subscribe_depth(*ws, contract, cfg_.exchange);
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
