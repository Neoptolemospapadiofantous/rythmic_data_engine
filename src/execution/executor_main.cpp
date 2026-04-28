/*  ═══════════════════════════════════════════════════════════════════════════
    executor_main.cpp — NQ Micro ORB Execution Engine entry point

    Architecture:
        Rithmic MD plant (WebSocket/protobuf)
            → LastTrade ticks → OrbStrategy → signal
            → OrderManager  → ORDER_PLANT send (or dry_run log)
            → on fill       → OrderManager state machine
            → RiskManager   → halt if limits breached
            → OrbDB         → live_trades / live_sessions rows written

    Connection sequence (mirrors existing rithmic_engine/src/client.cpp):
        1. Connect MD plant WS → system_info → login (TICKER_PLANT) → subscribe NQ
        2. Receive LastTrade loop → on_tick → OrbStrategy
        3. In live mode: connect ORDER_PLANT WS → login (ORDER_PLANT)
        4. Heartbeat timer (30s) on both plants

    The order plant connection is a second WS stream using the same protobuf
    protocol.  RequestNewOrder (314) / RithmicOrderNotification (351, internal acks) /
    ExchangeOrderNotification (352, fills with fill_price) / AccountPnLPositionUpdate (451)
    messages are all defined in rithmic.proto.  Template 308 subscription is sent after
    login to activate fill/reject delivery.

    Build:
        cd ~/rithmic_engine/build
        cmake .. -DCMAKE_BUILD_TYPE=Release
        make nq_executor -j4

    Usage:
        ./nq_executor --config config/orb_config.json [--dry-run]
    ═══════════════════════════════════════════════════════════════════════════ */

#include "orb_config.hpp"
#include "orb_strategy.hpp"
#include "order_manager.hpp"
#include "risk_manager.hpp"
#include "latency_logger.hpp"
#include "orb_db.hpp"

// Reuse existing client/db infrastructure
#include "client.hpp"
#include "config.hpp"
#include "db.hpp"
#include "log.hpp"

#include "rithmic.pb.h"

#include <boost/asio.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/beast.hpp>
#include <boost/beast/ssl.hpp>
#include <boost/beast/websocket.hpp>
#include <boost/beast/websocket/ssl.hpp>

#include <atomic>
#include <chrono>
#include <csignal>
#include <cstring>
#include <ctime>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iostream>
#include <memory>
#include <mutex>
#include <string>
#include <thread>

namespace asio      = boost::asio;
namespace beast     = boost::beast;
namespace websocket = beast::websocket;
namespace ssl       = asio::ssl;
using tcp           = asio::ip::tcp;
namespace fs        = std::filesystem;

// ─── Globals ──────────────────────────────────────────────────────────────────
static std::atomic<bool> g_running{true};
static std::atomic<bool> g_flatten_requested{false}; // set by signal handler; acted on in eod_loop

// ─── Position DB write helper ─────────────────────────────────────────────────
// Reads current state from order_mgr + strategy and issues an UPSERT to
// live_position. Safe to call at any frequency — OrbDB::write_position never throws.
static void flush_position(OrbDB* db,
                            const std::string& today,
                            const OrderManager& order_mgr,
                            const OrbStrategy& strategy,
                            bool op_connected,
                            double point_value = 2.0) {
    if (!db || !db->is_connected()) return;

    Position snap = order_mgr.position_snapshot();
    double last_px = strategy.last_price();

    // Build state string
    std::string state_str;
    switch (snap.state) {
        case PosState::FLAT:          state_str = "FLAT";          break;
        case PosState::PENDING_ENTRY: state_str = "PENDING_ENTRY"; break;
        case PosState::LONG:          state_str = "LONG";          break;
        case PosState::SHORT:         state_str = "SHORT";         break;
        case PosState::PENDING_EXIT:  state_str = "PENDING_EXIT";  break;
    }

    std::string dir_str;
    if (snap.direction == OrbSignal::BUY)  dir_str = "LONG";
    if (snap.direction == OrbSignal::SELL) dir_str = "SHORT";

    double unreal_pts = 0.0;
    double unreal_usd = 0.0;
    if ((snap.state == PosState::LONG || snap.state == PosState::SHORT) &&
        snap.entry_price > 0.0 && last_px > 0.0) {
        unreal_pts = (snap.state == PosState::LONG)
            ? (last_px - snap.entry_price)
            : (snap.entry_price - last_px);
        // MNQ: $2/point, 1 contract, round-trip commission ($4) deducted at close
        unreal_usd = unreal_pts * point_value;
    }

    // entry_time: format fill_time as UTC string (empty if FLAT/PENDING)
    std::string entry_time_str;
    if (snap.entry_price > 0.0 &&
        (snap.state == PosState::LONG || snap.state == PosState::SHORT ||
         snap.state == PosState::PENDING_EXIT)) {
        // fill_time is a steady_clock point; we approximate wall time as now - elapsed
        auto elapsed = std::chrono::steady_clock::now() - snap.fill_time;
        auto fill_wall = std::chrono::system_clock::now() - elapsed;
        time_t fill_tt = std::chrono::system_clock::to_time_t(fill_wall);
        struct tm utc_tm;
        gmtime_r(&fill_tt, &utc_tm);
        char tbuf[32];
        std::strftime(tbuf, sizeof(tbuf), "%Y-%m-%d %H:%M:%S", &utc_tm);
        entry_time_str = tbuf;
    }

    const auto& sess = strategy.session();

    db->write_position(today,
                       state_str,
                       dir_str,
                       snap.entry_price,
                       entry_time_str,
                       last_px,
                       unreal_pts,
                       unreal_usd,
                       snap.sl_price,
                       strategy.orb_set() ? strategy.orb_high() : 0.0,
                       strategy.orb_set() ? strategy.orb_low()  : 0.0,
                       strategy.orb_set(),
                       sess.trades_today,
                       /*md_connected=*/true,
                       op_connected);
}

static void handle_signal(int /*sig*/) {
    // Only async-signal-safe operations here — no mutexes, no LOG.
    // Flatten is deferred to eod_loop which checks g_flatten_requested each second.
    g_running          = false;
    g_flatten_requested = true;
}

// ─── Framing helpers (mirrors RithmicClient::frame / strip_header) ────────────
template <class Msg>
static std::string proto_frame(const Msg& msg) {
    std::string payload = msg.SerializeAsString();
    uint32_t sz = static_cast<uint32_t>(payload.size());
    uint32_t be = __builtin_bswap32(sz);
    std::string wire(reinterpret_cast<char*>(&be), 4);
    wire += payload;
    return wire;
}

static std::string proto_strip(const std::string& wire) {
    if (wire.size() < 4) throw std::runtime_error("Message too short");
    return wire.substr(4);
}

// ─── ET time helpers ─────────────────────────────────────────────────────────
static void current_et(int& h, int& m) {
    auto now = std::chrono::system_clock::now();
    time_t tt = std::chrono::system_clock::to_time_t(now);
    struct tm utc_tm;
    gmtime_r(&tt, &utc_tm);
    time_t et_t = tt - us_et_offset(utc_tm) * 3600;
    struct tm et_tm;
    gmtime_r(&et_t, &et_tm);
    h = et_tm.tm_hour;
    m = et_tm.tm_min;
}

static std::string today_date_str() {
    auto now = std::chrono::system_clock::now();
    time_t tt = std::chrono::system_clock::to_time_t(now);
    struct tm utc_tm;
    gmtime_r(&tt, &utc_tm);
    time_t et_t = tt - us_et_offset(utc_tm) * 3600;
    struct tm et_tm;
    gmtime_r(&et_t, &et_tm);
    char buf[16];
    std::strftime(buf, sizeof(buf), "%Y-%m-%d", &et_tm);
    return buf;
}

// ─── WebSocket helpers ────────────────────────────────────────────────────────
using WsStream = websocket::stream<beast::ssl_stream<beast::tcp_stream>>;

static asio::awaitable<std::unique_ptr<WsStream>>
connect_ws(asio::io_context& ioc, ssl::context& ssl_ctx, const std::string& url) {
    std::string host, port;
    std::string u = url;
    if (u.substr(0, 6) == "wss://") u = u.substr(6);
    auto colon = u.find(':');
    if (colon != std::string::npos) {
        host = u.substr(0, colon);
        port = u.substr(colon + 1);
    } else { host = u; port = "443"; }

    auto ex = co_await asio::this_coro::executor;
    tcp::resolver resolver(ex);
    auto results = co_await resolver.async_resolve(host, port, asio::use_awaitable);
    auto ws = std::make_unique<WsStream>(ex, ssl_ctx);
    co_await beast::get_lowest_layer(*ws).async_connect(results, asio::use_awaitable);
    {
        auto& sock = beast::get_lowest_layer(*ws).socket();
        sock.set_option(asio::ip::tcp::no_delay(true));
        sock.set_option(asio::socket_base::receive_buffer_size(1 << 20));
        sock.set_option(asio::socket_base::send_buffer_size(256 << 10));
    }
    if (!SSL_set_tlsext_host_name(ws->next_layer().native_handle(), host.c_str()))
        throw std::runtime_error("SSL SNI failed");
    co_await ws->next_layer().async_handshake(ssl::stream_base::client, asio::use_awaitable);
    ws->set_option(websocket::stream_base::decorator([](websocket::request_type& req){
        req.set(beast::http::field::user_agent, "nq_executor/1.0");
    }));
    co_await ws->async_handshake(host + ":" + port, "/", asio::use_awaitable);
    co_return ws;
}

static asio::awaitable<void> ws_write(WsStream& ws, const std::string& data) {
    ws.binary(true);
    co_await ws.async_write(asio::buffer(data), asio::use_awaitable);
}

// ─── Order plant send helper ──────────────────────────────────────────────────
// Wraps WsStream writes with mutex (called from io_context coroutine only —
// single-threaded io_context means no contention, but we keep the mutex for
// safety in case of future threading changes).
struct OrderPlant {
    std::unique_ptr<WsStream> ws;
    std::mutex                send_mu;
    bool                      connected = false;
    std::string               account_id;
    std::string               fcm_id;
    std::string               ib_id;
    std::string               trade_route = "Rithmic Order Routing";
    std::string               trade_symbol;  // front-month contract e.g. NQM6

    // Send RequestNewOrder (template 314)
    // Returns basket_id if sent, empty string on error
    std::string send_new_order(const std::string& basket_id,
                               const std::string& symbol,
                               const std::string& exchange,
                               int qty,
                               int order_type,   // 2=MKT, 1=LMT, 4=STOP_MARKET
                               bool is_buy,
                               double price,
                               const std::string& user_tag,
                               bool dry_run) {
        if (dry_run) {
            LOG("[ORDER_PLANT] [DRY_RUN] %s %s qty=%d basket=%s",
                is_buy ? "BUY" : "SELL", symbol.c_str(), qty, basket_id.c_str());
            return basket_id;
        }
        if (!connected || !ws) {
            LOG("[ORDER_PLANT] Not connected — cannot send order basket=%s", basket_id.c_str());
            return "";
        }

        rti::RequestNewOrder req;
        req.set_template_id(312);
        req.set_fcm_id(fcm_id);
        req.set_ib_id(ib_id);
        req.set_account_id(account_id);
        req.set_symbol(symbol);
        req.set_exchange(exchange);
        req.set_quantity(qty);
        req.set_order_type(order_type);
        req.set_transaction_type(is_buy ? 1 : 2);
        // Rithmic fields by order type:
        //   LIMIT (1)       -> price
        //   MARKET (2)      -> neither
        //   STOP_LIMIT (3)  -> price + trigger_price
        //   STOP_MARKET (4) -> trigger_price
        if (order_type == 1) req.set_price(price);
        if (order_type == 3) { req.set_price(price); req.set_trigger_price(price); }
        if (order_type == 4) req.set_trigger_price(price);
        // basket_id field removed from canonical proto; use user_tag for client-side tracking
        req.set_user_tag(basket_id);
        req.set_duration(rti::RequestNewOrder::DAY);
        req.set_manual_or_auto_select(rti::RequestNewOrder::MANUAL);
        req.set_trade_route(trade_route);

        try {
            std::string wire = proto_frame(req);
            // Synchronous send is OK — we're single-threaded in io_context
            beast::flat_buffer dummy;
            (void)dummy;
            // Note: async send not possible from non-coroutine context;
            // for production, queue to a write strand. For now we use a
            // blocking write (acceptable given <1 order/minute cadence).
            ws->write(asio::buffer(wire));
            LOG("[ORDER_PLANT] RequestNewOrder sent: basket=%s %s %s qty=%d",
                basket_id.c_str(), is_buy ? "BUY" : "SELL", symbol.c_str(), qty);
            return basket_id;
        } catch (std::exception& e) {
            LOG("[ORDER_PLANT] ERROR sending order: %s", e.what());
            return "";
        }
    }

    // Send RequestModifyOrder (template 314) — atomically changes stop trigger_price.
    // Returns true if sent successfully.
    bool send_modify_order(const std::string& basket_id,
                           double new_trigger_price,
                           const std::string& symbol,
                           const std::string& exchange_str,
                           bool dry_run) {
        if (dry_run) {
            LOG("[ORDER_PLANT] [DRY_RUN] Would modify stop basket=%s trigger=%.2f",
                basket_id.c_str(), new_trigger_price);
            return true;
        }
        if (!connected || !ws) {
            LOG("[ORDER_PLANT] Not connected — cannot modify order basket=%s", basket_id.c_str());
            return false;
        }
        rti::RequestModifyOrder req;
        req.set_template_id(314);
        req.set_basket_id(basket_id);
        req.set_fcm_id(fcm_id);
        req.set_ib_id(ib_id);
        req.set_account_id(account_id);
        req.set_symbol(symbol);
        req.set_exchange(exchange_str);
        req.set_trigger_price(new_trigger_price);
        try {
            ws->write(asio::buffer(proto_frame(req)));
            LOG("[ORDER_PLANT] RequestModifyOrder sent: basket=%s new_trigger=%.2f",
                basket_id.c_str(), new_trigger_price);
            return true;
        } catch (std::exception& e) {
            LOG("[ORDER_PLANT] ERROR sending modify order: %s", e.what());
            return false;
        }
    }

    // Send RequestCancelOrder (template 316)
    void send_cancel(const std::string& basket_id, const std::string& account_id_str) {
        if (!connected || !ws) return;
        rti::RequestCancelOrder req;
        req.set_template_id(316);
        req.set_basket_id(basket_id);
        req.set_account_id(account_id_str);
        req.set_fcm_id(fcm_id);
        req.set_ib_id(ib_id);
        try {
            ws->write(asio::buffer(proto_frame(req)));
            LOG("[ORDER_PLANT] RequestCancelOrder sent: basket=%s", basket_id.c_str());
        } catch (std::exception& e) {
            LOG("[ORDER_PLANT] ERROR sending cancel: %s", e.what());
        }
    }
};

// ─── Main executor coroutine ──────────────────────────────────────────────────
// risk, strategy, and today are owned by main() and survive reconnects.
asio::awaitable<void> run_executor(const OrbConfig& orb_cfg,
                                   asio::io_context& ioc_ref,
                                   RiskManager& risk,
                                   OrbStrategy& strategy,
                                   std::string& today,
                                   Position& carried_pos) {
    // ── Component construction ────────────────────────────────────────────────
    LatencyLogger lat;
    OrderManager  order_mgr(orb_cfg, risk, lat);

    // ── Reconnect reconciliation (#2) ─────────────────────────────────────────
    // If the previous session ended with an open position (e.g. disconnect while
    // LONG), the exchange stop order may or may not have fired. We cannot query
    // the exchange here, so we halt new entries and force a manual check.
    if (carried_pos.state != PosState::FLAT) {
        LOG("[EXECUTOR] CRITICAL: reconnecting with non-flat carried position "
            "(state=%d dir=%s entry=%.2f sl=%.2f) — halting new entries. "
            "Verify exchange position manually; delete halt if flat.",
            (int)carried_pos.state,
            carried_pos.direction == OrbSignal::BUY ? "LONG" : "SHORT",
            carried_pos.entry_price, carried_pos.sl_price);
        strategy.halt_trading("reconnect_unreconciled_position");
    }
    carried_pos = Position{};  // reset; will be populated again at session end

    // Wire strategy → order_mgr
    strategy.set_signal_callback(
        [&](OrbSignal sig, double price, const std::string& reason) {
            order_mgr.on_signal(sig, price, reason);
        }
    );

    // ── DB setup ──────────────────────────────────────────────────────────────
    std::unique_ptr<OrbDB> db;
    try {
        db = std::make_unique<OrbDB>(orb_cfg.pg_connstr(), orb_cfg.symbol);
        LOG("[EXECUTOR] OrbDB connected");
        // Seed risk manager with historical P&L so consistency cap is correct
        if (today.empty()) {  // only on first startup, not reconnects
            double hist_pnl = db->get_total_pnl();
            risk.seed_total_profit(hist_pnl);
        }
    } catch (std::exception& e) {
        LOG("[EXECUTOR] WARNING: OrbDB failed (%s) — trades will not be persisted", e.what());
    }

    // ── Order plant setup ─────────────────────────────────────────────────────
    auto order_plant = std::make_shared<OrderPlant>();
    order_plant->account_id = ""; // populated after login
    order_plant->fcm_id     = ""; // populated after login
    order_plant->ib_id      = ""; // populated after login

    // Wire order_mgr → order_plant (uses order_plant->trade_symbol for the specific contract)
    order_mgr.set_order_callback(
        [&order_plant, &orb_cfg](const std::string& basket_id,
                                 const std::string& /*symbol*/,
                                 const std::string& exchange,
                                 int qty, int order_type, bool is_buy,
                                 double price, const std::string& user_tag) -> bool {
            const std::string& sym = order_plant->trade_symbol.empty()
                                   ? orb_cfg.symbol : order_plant->trade_symbol;
            std::string result = order_plant->send_new_order(
                basket_id, sym, exchange, qty, order_type,
                is_buy, price, user_tag, orb_cfg.dry_run);
            return !result.empty();
        }
    );
    order_mgr.set_cancel_callback(
        [&order_plant](const std::string& basket_id) {
            order_plant->send_cancel(basket_id, order_plant->account_id);
        }
    );
    order_mgr.set_modify_callback(
        [&order_plant, &orb_cfg](const std::string& basket_id, double new_trigger) -> bool {
            const std::string& sym = order_plant->trade_symbol.empty()
                                   ? orb_cfg.symbol : order_plant->trade_symbol;
            return order_plant->send_modify_order(
                basket_id, new_trigger, sym, orb_cfg.exchange, orb_cfg.dry_run);
        }
    );

    // ── MD plant connection ────────────────────────────────────────────────────
    LOG("[EXECUTOR] Connecting to MD plant: %s", orb_cfg.md_url.c_str());

    ssl::context ssl_ctx(ssl::context::tls_client);
    ssl_ctx.load_verify_file("certs/rithmic_ssl_cert_auth_params");
    ssl_ctx.set_verify_mode(ssl::verify_peer);

    auto ex = co_await asio::this_coro::executor;
    asio::io_context& ioc = static_cast<asio::io_context&>(ex.context());

    std::unique_ptr<WsStream> md_ws;
    try {
        md_ws = co_await connect_ws(ioc, ssl_ctx, orb_cfg.md_url);
        LOG("[EXECUTOR] MD plant WS connected");
    } catch (std::exception& e) {
        LOG("[EXECUTOR] FATAL: MD plant connect failed: %s", e.what());
        co_return;
    }

    // System info
    {
        rti::RequestRithmicSystemInfo req;
        req.set_template_id(16);
        co_await ws_write(*md_ws, proto_frame(req));
        beast::flat_buffer buf;
        for (;;) {
            buf.clear();
            co_await md_ws->async_read(buf, asio::use_awaitable);
            auto payload = proto_strip(beast::buffers_to_string(buf.data()));
            rti::Base base;
            base.ParseFromString(payload);
            if (base.template_id() == 17) break;
        }
        LOG("[EXECUTOR] System info OK");
    }

    // Rithmic protocol: close probe connection, reconnect for login
    try { md_ws->close(websocket::close_code::normal); } catch (...) {}
    md_ws.reset();
    try {
        md_ws = co_await connect_ws(ioc, ssl_ctx, orb_cfg.md_url);
        LOG("[EXECUTOR] MD plant reconnected for login");
    } catch (std::exception& e) {
        LOG("[EXECUTOR] FATAL: MD plant reconnect failed: %s", e.what());
        co_return;
    }

    // MD plant login — use AMP credentials (separate session from Legends ORDER_PLANT)
    {
        LOG("[EXECUTOR] MD Login: user=%s system=%s",
            orb_cfg.md_user.c_str(), orb_cfg.md_system_name.c_str());
        rti::RequestLogin req;
        req.set_template_id(10);
        req.set_template_version("3.9");
        req.set_user(orb_cfg.md_user);
        req.set_password(orb_cfg.md_password);
        req.set_system_name(orb_cfg.md_system_name);
        req.set_app_name(orb_cfg.app_name + "-MD");
        req.set_app_version(orb_cfg.app_version);
        req.set_infra_type(rti::RequestLogin::TICKER_PLANT);
        co_await ws_write(*md_ws, proto_frame(req));

        beast::flat_buffer buf;
        for (;;) {
            buf.clear();
            co_await md_ws->async_read(buf, asio::use_awaitable);
            auto payload = proto_strip(beast::buffers_to_string(buf.data()));
            rti::Base base;
            base.ParseFromString(payload);
            if (base.template_id() == 11) {
                rti::ResponseLogin resp;
                resp.ParseFromString(payload);
                bool ok = !resp.rp_code().empty() && resp.rp_code(0) == "0";
                if (!ok) {
                    LOG("[EXECUTOR] FATAL: MD login failed");
                    co_return;
                }
                double hb_interval = resp.heartbeat_interval();
                LOG("[EXECUTOR] MD plant login OK (heartbeat_interval=%.0fs)", hb_interval);
                break;
            }
        }
        // Server requires immediate heartbeat after login
        {
            rti::RequestHeartbeat hb;
            hb.set_template_id(18);
            auto ts = std::chrono::system_clock::now().time_since_epoch();
            hb.set_ssboe(static_cast<int32_t>(
                std::chrono::duration_cast<std::chrono::seconds>(ts).count()));
            co_await ws_write(*md_ws, proto_frame(hb));
        }
    }

    // Use configured contract symbol (set trade_contract in config for the front month, e.g. NQM6)
    std::string trade_symbol = orb_cfg.trade_contract.empty() ? orb_cfg.symbol : orb_cfg.trade_contract;
    order_plant->trade_symbol = trade_symbol;
    LOG("[EXECUTOR] Trading contract: %s", trade_symbol.c_str());

    // Subscribe to NQ last trade
    {
        rti::RequestMarketDataUpdate req;
        req.set_template_id(100);
        req.set_symbol(trade_symbol);
        req.set_exchange(orb_cfg.exchange);
        req.set_request(rti::RequestMarketDataUpdate::SUBSCRIBE);
        req.set_update_bits(1);  // LAST_TRADE
        co_await ws_write(*md_ws, proto_frame(req));
        LOG("[EXECUTOR] Subscribed to %s/%s last trade",
            trade_symbol.c_str(), orb_cfg.exchange.c_str());
    }

    // ── ORDER_PLANT connection (live mode only) ───────────────────────────────
    std::unique_ptr<WsStream> op_ws;
    if (!orb_cfg.dry_run) {
        LOG("[EXECUTOR] Connecting to ORDER_PLANT: %s", orb_cfg.rithmic_url.c_str());
        try {
            // System info probe
            auto probe = co_await connect_ws(ioc, ssl_ctx, orb_cfg.rithmic_url);
            {
                rti::RequestRithmicSystemInfo req;
                req.set_template_id(16);
                co_await ws_write(*probe, proto_frame(req));
                beast::flat_buffer buf;
                for (;;) {
                    buf.clear();
                    co_await probe->async_read(buf, asio::use_awaitable);
                    auto payload = proto_strip(beast::buffers_to_string(buf.data()));
                    rti::Base base;
                    base.ParseFromString(payload);
                    if (base.template_id() == 17) break;
                }
            }
            try { probe->close(websocket::close_code::normal); } catch (...) {}
            probe.reset();

            // Reconnect for login
            op_ws = co_await connect_ws(ioc, ssl_ctx, orb_cfg.rithmic_url);
            {
                rti::RequestLogin req;
                req.set_template_id(10);
                req.set_template_version("3.9");
                req.set_user(orb_cfg.rithmic_user);
                req.set_password(orb_cfg.rithmic_password);
                req.set_system_name(orb_cfg.rithmic_system_name);
                req.set_app_name(orb_cfg.app_name);
                req.set_app_version(orb_cfg.app_version);
                req.set_infra_type(rti::RequestLogin::ORDER_PLANT);
                co_await ws_write(*op_ws, proto_frame(req));

                beast::flat_buffer buf;
                for (;;) {
                    buf.clear();
                    co_await op_ws->async_read(buf, asio::use_awaitable);
                    auto payload = proto_strip(beast::buffers_to_string(buf.data()));
                    rti::Base base;
                    base.ParseFromString(payload);
                    if (base.template_id() == 11) {
                        rti::ResponseLogin resp;
                        resp.ParseFromString(payload);
                        bool ok = !resp.rp_code().empty() && resp.rp_code(0) == "0";
                        if (!ok) {
                            LOG("[EXECUTOR] FATAL: ORDER_PLANT login failed — rp_code=%s",
                                resp.rp_code().empty() ? "?" : resp.rp_code(0).c_str());
                            co_return;
                        }
                        LOG("[EXECUTOR] ORDER_PLANT login OK");
                        break;
                    }
                }
            }
            // Server requires immediate heartbeat after login
            {
                rti::RequestHeartbeat hb;
                hb.set_template_id(18);
                auto ts = std::chrono::system_clock::now().time_since_epoch();
                hb.set_ssboe(static_cast<int32_t>(
                    std::chrono::duration_cast<std::chrono::seconds>(ts).count()));
                co_await ws_write(*op_ws, proto_frame(hb));
            }
            order_plant->ws         = std::move(op_ws);
            order_plant->account_id  = orb_cfg.account_id;
            order_plant->fcm_id      = orb_cfg.fcm_id;
            order_plant->ib_id       = orb_cfg.ib_id;
            order_plant->trade_route = orb_cfg.trade_route;
            order_plant->connected   = true;
            LOG("[EXECUTOR] ORDER_PLANT connected — live orders enabled (account=%s)",
                orb_cfg.account_id.c_str());

            // Subscribe to order updates (template 308 = RequestSubscribeForOrderUpdates).
            // Without this subscription Rithmic will NOT deliver tid=351/352 notifications.
            {
                rti::RequestSubscribeForOrderUpdates sub;
                sub.set_template_id(308);
                sub.set_fcm_id(orb_cfg.fcm_id);
                sub.set_ib_id(orb_cfg.ib_id);
                sub.set_account_id(orb_cfg.account_id);
                try {
                    order_plant->ws->write(asio::buffer(proto_frame(sub)));
                    LOG("[EXECUTOR] Sent RequestSubscribeForOrderUpdates (tid=308)");
                } catch (std::exception& e) {
                    LOG("[EXECUTOR] WARNING: Failed to send order update subscription: %s", e.what());
                }
            }


        } catch (std::exception& e) {
            LOG("[EXECUTOR] FATAL: ORDER_PLANT connect failed: %s", e.what());
            co_return;
        }
    }

    // ── ORDER_PLANT fill receive loop ─────────────────────────────────────────
    auto op_loop = [&]() -> asio::awaitable<void> {
        if (!order_plant->connected || !order_plant->ws) co_return;
        beast::flat_buffer buf;
        while (g_running) {
            buf.clear();
            try {
                co_await order_plant->ws->async_read(buf, asio::use_awaitable);
            } catch (std::exception& e) {
                if (!g_running) co_return;
                LOG("[EXECUTOR] ORDER_PLANT read error: %s — triggering full reconnect", e.what());
                ioc_ref.stop();  // kill md_loop and all timers → outer loop reconnects
                co_return;
            }
            auto payload = proto_strip(beast::buffers_to_string(buf.data()));
            rti::Base base;
            if (!base.ParseFromString(payload)) continue;

            int tid = base.template_id();
            LOG("[EXECUTOR] op_loop tid=%d len=%zu", tid, payload.size());

            if (tid == 18) {
                // Server-sent RequestHeartbeat — respond with ResponseHeartbeat (tid=19)
                rti::ResponseHeartbeat hb_resp;
                hb_resp.set_template_id(19);
                try {
                    co_await ws_write(*order_plant->ws, proto_frame(hb_resp));
                } catch (...) {
                    LOG("[EXECUTOR] ORDER_PLANT heartbeat response send failed");
                }
            } else if (tid == 19) {
                // ResponseHeartbeat — server acked our heartbeat, no action
            } else if (tid == 309) {
                // ResponseSubscribeForOrderUpdates — subscription ack
                rti::ResponseSubscribeForOrderUpdates resp;
                resp.ParseFromString(payload);
                std::string rpc = resp.rp_code().empty() ? "?" : resp.rp_code(0);
                LOG("[EXECUTOR] Order update subscription %s (rp_code=%s)",
                    rpc == "0" ? "OK" : "FAILED", rpc.c_str());

            } else if (tid == 313 || tid == 315) {
                // 313 = preliminary gateway ack (rq_handler_rp_code only)
                // 315 = final response for both RequestNewOrder and RequestModifyOrder
                rti::ResponseNewOrder resp;
                resp.ParseFromString(payload);
                if (resp.rp_code().empty()) {
                    LOG("[EXECUTOR] ResponseNewOrder (ack) basket=%s",
                        resp.basket_id().c_str());
                } else {
                    std::string rpc = resp.rp_code(0);
                    // If a modify is in-flight, treat tid=315 as the modify response
                    if (tid == 315 && order_mgr.has_pending_modify()) {
                        bool accepted = (rpc == "0");
                        LOG("[EXECUTOR] ResponseModifyOrder rp_code=%s (%s)",
                            rpc.c_str(), accepted ? "ACK" : "REJECT");
                        order_mgr.on_modify_response(accepted, rpc);
                    } else {
                        LOG("[EXECUTOR] ResponseNewOrder basket=%s rp_code=%s",
                            resp.basket_id().c_str(), rpc.c_str());
                        if (rpc != "0") {
                            LOG("[EXECUTOR] Order REJECTED at gateway: basket=%s code=%s",
                                resp.basket_id().c_str(), rpc.c_str());
                            order_mgr.on_order_rejected(resp.basket_id(),
                                                        "gateway_reject_" + rpc);
                        }
                    }
                }

            } else if (tid == 351) {
                // RithmicOrderNotification — internal acks and, for Legends/paper routing,
                // the authoritative fill (notify_type=15 COMPLETE with total_fill_size>0).
                rti::RithmicOrderNotification notif;
                if (!notif.ParseFromString(payload)) continue;
                LOG("[EXECUTOR] RithmicOrderNotification basket=%s notify_type=%d status=%s "
                    "avg_fill=%.2f total_fill=%d",
                    notif.basket_id().c_str(), (int)notif.notify_type(), notif.status().c_str(),
                    notif.avg_fill_price(), notif.total_fill_size());
                // When our stop order reaches the exchange, capture the server basket_id.
                if (order_mgr.is_stop_basket(notif.user_tag()) && !notif.basket_id().empty()) {
                    order_mgr.set_stop_server_basket(notif.basket_id());
                }
                // Legends routing delivers fills as COMPLETE (15) on tid=351 rather than
                // ExchangeOrderNotification (352). Detect by total_fill_size > 0.
                if (notif.total_fill_size() > 0 && notif.avg_fill_price() > 0.0) {
                    const std::string& client_id = notif.user_tag();
                    bool is_entry = order_mgr.is_entry_basket(client_id);
                    bool is_stop  = order_mgr.is_stop_basket(client_id);
                    if (is_entry || is_stop) {
                        LOG("[EXECUTOR] tid=351 fill detected: client=%s avg_fill=%.2f qty=%d "
                            "entry=%d stop=%d",
                            client_id.c_str(), notif.avg_fill_price(),
                            notif.total_fill_size(), (int)is_entry, (int)is_stop);
                        order_mgr.on_fill_notification(client_id,
                                                       notif.avg_fill_price(),
                                                       notif.total_fill_size(),
                                                       is_entry && !is_stop);
                        flush_position(db.get(), today, order_mgr, strategy,
                                       order_plant->connected, orb_cfg.point_value);
                    }
                } else if ((int)notif.notify_type() == 15 && notif.total_fill_size() == 0) {
                    // COMPLETE with no fill = order cancelled/rejected by routing or risk rules.
                    const std::string& client_id = notif.user_tag();
                    if (order_mgr.is_entry_basket(client_id)) {
                        LOG("[EXECUTOR] tid=351 order CANCELLED (no fill): client=%s status=%s — "
                            "returning to FLAT (possible pre-market or risk restriction)",
                            client_id.c_str(), notif.status().c_str());
                        order_mgr.on_order_rejected(client_id, "cancelled_no_fill");
                        flush_position(db.get(), today, order_mgr, strategy,
                                       order_plant->connected, orb_cfg.point_value);
                    }
                }

            } else if (tid == 352) {
                // ExchangeOrderNotification — actual exchange fills, rejects, and cancels.
                // This is the authoritative source for fill_price and fill_size.
                rti::ExchangeOrderNotification notif;
                if (!notif.ParseFromString(payload)) continue;
                int notify_type = (int)notif.notify_type();
                LOG("[EXECUTOR] ExchangeOrderNotification type=%d basket=%s "
                    "fill_px=%.2f fill_qty=%d status=%s",
                    notify_type,
                    notif.basket_id().c_str(),
                    notif.fill_price(),
                    notif.fill_size(),
                    notif.status().c_str());

                // ExchangeOrderNotification::NotifyType::FILL = 5
                // Correlate fills via user_tag (our client-side tracking ID).
                // Rithmic assigns its own basket_id on the response and echoes
                // our user_tag in every notification.
                if (notify_type == 5) {
                    const std::string& client_id = notif.user_tag();
                    bool is_entry = order_mgr.is_entry_basket(client_id);
                    bool is_stop  = order_mgr.is_stop_basket(client_id);
                    if (is_stop) {
                        LOG("[EXECUTOR] Exchange STOP filled client_id=%s (server=%s) px=%.2f — treating as exit",
                            client_id.c_str(), notif.basket_id().c_str(), notif.fill_price());
                    }
                    order_mgr.on_fill_notification(client_id,
                                                   notif.fill_price(),
                                                   notif.fill_size(),
                                                   is_entry && !is_stop);
                    flush_position(db.get(), today, order_mgr, strategy,
                                   order_plant->connected, orb_cfg.point_value);
                } else if (notify_type == 2) { // MODIFY ACK
                    LOG("[EXECUTOR] Stop MODIFIED by exchange: client=%s server=%s — trail ACKed",
                        notif.user_tag().c_str(), notif.basket_id().c_str());
                } else if (notify_type == 6) { // REJECT
                    LOG("[EXECUTOR] Order REJECTED by exchange: client=%s server=%s status=%s",
                        notif.user_tag().c_str(), notif.basket_id().c_str(), notif.status().c_str());
                    order_mgr.on_order_rejected(notif.user_tag(), notif.status());
                } else if (notify_type == 3) { // CANCEL
                    LOG("[EXECUTOR] Order CANCELLED by exchange: client=%s server=%s",
                        notif.user_tag().c_str(), notif.basket_id().c_str());
                    order_mgr.on_cancel_confirmed(notif.user_tag());
                }

            }
        }
    };

    // Session setup — on reconnect, today/risk/strategy already have the day's state.
    // Only initialize today on first run (empty string signals first call).
    if (today.empty()) {
        today = today_date_str();
        strategy.reset_session();
        risk.reset_daily();
    }
    LOG("[EXECUTOR] Session date: %s  dry_run=%s",
        today.c_str(), orb_cfg.dry_run ? "TRUE" : "FALSE");
    if (orb_cfg.dry_run)
        LOG("[EXECUTOR] *** DRY RUN — no real orders will be sent ***");

    // ── Heartbeat timer ───────────────────────────────────────────────────────
    asio::steady_timer hb_timer(ex);
    auto heartbeat_loop = [&]() -> asio::awaitable<void> {
        while (g_running) {
            hb_timer.expires_after(std::chrono::seconds(5));
            co_await hb_timer.async_wait(asio::use_awaitable);
            if (!g_running) co_return;
            rti::RequestHeartbeat hb;
            hb.set_template_id(18);
            auto ts = std::chrono::system_clock::now().time_since_epoch();
            hb.set_ssboe(static_cast<int32_t>(
                std::chrono::duration_cast<std::chrono::seconds>(ts).count()));
            if (md_ws) {
                try {
                    co_await ws_write(*md_ws, proto_frame(hb));
                } catch (...) {
                    LOG("[EXECUTOR] Heartbeat send failed on MD — WS may be closed");
                }
            }
            // Also heartbeat ORDER_PLANT — Rithmic drops idle connections in ~2 min
            if (order_plant->connected && order_plant->ws) {
                try {
                    co_await ws_write(*order_plant->ws, proto_frame(hb));
                } catch (...) {
                    LOG("[EXECUTOR] Heartbeat send failed on ORDER_PLANT");
                }
            }
        }
    };

    // ── EOD/trail check timer (1-second tick) ─────────────────────────────────
    asio::steady_timer eod_timer(ex);
    auto eod_loop = [&]() -> asio::awaitable<void> {
        int pos_write_counter = 0;  // flush live_position every 5 ticks
        while (g_running) {
            eod_timer.expires_after(std::chrono::seconds(1));
            co_await eod_timer.async_wait(asio::use_awaitable);

            // Deferred flatten from signal handler (#5 — signal handler is mutex-free)
            if (g_flatten_requested.exchange(false)) {
                LOG("[EXECUTOR] Kill signal — flattening position");
                order_mgr.flatten_now("kill_signal");
            }

            if (!g_running) co_return;

            int et_h, et_m;
            current_et(et_h, et_m);
            strategy.check_eod(et_h, et_m);

            // Date rollover check
            std::string new_date = today_date_str();
            if (new_date != today) {
                today = new_date;
                strategy.reset_session();
                risk.reset_daily();
                pos_write_counter = 0;
                LOG("[EXECUTOR] New trading day: %s", today.c_str());
            }

            // Flush session state to DB
            if (db && db->is_connected()) {
                const auto& sess = strategy.session();
                try {
                    // Real account balance = starting equity (50k) + all-time realized P&L
                    double cur_equity = 50000.0 + risk.total_profit();
                    db->upsert_session(today,
                        sess.orb_set ? strategy.orb_high() : 0.0,
                        sess.orb_set ? strategy.orb_low()  : 0.0,
                        sess.trades_today,
                        risk.daily_pnl(),
                        risk.peak_equity(),
                        risk.halted(),
                        "");
                    db->write_account_equity(today, cur_equity);
                } catch (std::exception& e) {
                    LOG("[EXECUTOR] DB upsert_session failed: %s", e.what());
                    db->reconnect();
                }
            }

            // Check for completed trades → write to DB + immediately flush position
            Position completed;
            if (order_mgr.pop_trade_completed(completed)) {
                strategy.notify_trade_filled(completed.direction);
                if (db && db->is_connected()) {
                    try {
                        db->write_trade(completed,
                                        order_mgr.last_entry_lat(),
                                        order_mgr.last_exit_lat(),
                                        today);
                    } catch (std::exception& e) {
                        LOG("[EXECUTOR] DB write_trade failed: %s", e.what());
                        db->reconnect();
                    }
                }
                // Immediate position flush after trade close so UI sees FLAT right away
                flush_position(db.get(), today, order_mgr, strategy,
                               order_plant->connected, orb_cfg.point_value);
                pos_write_counter = 0;
            }

            // Periodic position flush every 5 seconds
            if (++pos_write_counter >= 5) {
                pos_write_counter = 0;
                flush_position(db.get(), today, order_mgr, strategy,
                               order_plant->connected, orb_cfg.point_value);
            }
        }
    };

    // ── Legends TICKER_PLANT loop (price comparison only) ────────────────────
    // Connects with Legends credentials to a second TICKER_PLANT session and
    // writes legends_price to live_position for side-by-side comparison with the
    // AMP price. Expects FORCED LOGOUTs (Legends allows one session; ORDER_PLANT
    // holds it), so it reconnects after each kick without disrupting the main loop.
    // NOTE: co_await must never be inside a catch block (C++20 restriction) —
    //       errors set a flag, then the timer is awaited outside the catch.
    auto legends_md_loop = [&]() -> asio::awaitable<void> {
        while (g_running) {
            int retry_secs = 0;
            std::unique_ptr<WsStream> l_ws;

            // ── connect (probe) ──────────────────────────────────────────────
            try { l_ws = co_await connect_ws(ioc, ssl_ctx, orb_cfg.rithmic_url); }
            catch (...) { LOG("[LEGENDS_MD] Connect failed"); retry_secs = 1; }
            if (retry_secs) {
                asio::steady_timer t(ex); t.expires_after(std::chrono::seconds(retry_secs));
                co_await t.async_wait(asio::use_awaitable); continue;
            }

            // ── system info probe ────────────────────────────────────────────
            try {
                rti::RequestRithmicSystemInfo req; req.set_template_id(16);
                co_await ws_write(*l_ws, proto_frame(req));
                beast::flat_buffer buf;
                for (;;) {
                    buf.clear();
                    co_await l_ws->async_read(buf, asio::use_awaitable);
                    auto pl = proto_strip(beast::buffers_to_string(buf.data()));
                    rti::Base b; b.ParseFromString(pl);
                    if (b.template_id() == 17) break;
                }
            } catch (...) { retry_secs = 1; }
            if (retry_secs) {
                asio::steady_timer t(ex); t.expires_after(std::chrono::seconds(retry_secs));
                co_await t.async_wait(asio::use_awaitable); continue;
            }

            // ── reconnect for login ──────────────────────────────────────────
            try { l_ws->close(websocket::close_code::normal); } catch (...) {}
            l_ws.reset();
            try { l_ws = co_await connect_ws(ioc, ssl_ctx, orb_cfg.rithmic_url); }
            catch (...) { retry_secs = 1; }
            if (retry_secs) {
                asio::steady_timer t(ex); t.expires_after(std::chrono::seconds(retry_secs));
                co_await t.async_wait(asio::use_awaitable); continue;
            }

            // ── login ────────────────────────────────────────────────────────
            bool login_ok = false;
            try {
                rti::RequestLogin req;
                req.set_template_id(10); req.set_template_version("3.9");
                req.set_user(orb_cfg.rithmic_user);
                req.set_password(orb_cfg.rithmic_password);
                req.set_system_name(orb_cfg.rithmic_system_name);
                req.set_app_name(orb_cfg.app_name);
                req.set_app_version(orb_cfg.app_version);
                req.set_infra_type(rti::RequestLogin::TICKER_PLANT);
                co_await ws_write(*l_ws, proto_frame(req));
                beast::flat_buffer buf;
                for (;;) {
                    buf.clear();
                    co_await l_ws->async_read(buf, asio::use_awaitable);
                    auto pl = proto_strip(beast::buffers_to_string(buf.data()));
                    rti::Base b; b.ParseFromString(pl);
                    if (b.template_id() == 11) {
                        rti::ResponseLogin resp; resp.ParseFromString(pl);
                        login_ok = !resp.rp_code().empty() && resp.rp_code(0) == "0";
                        break;
                    }
                }
            } catch (...) { login_ok = false; }
            if (!login_ok) {
                LOG("[LEGENDS_MD] Login failed — retry in 1s");
                asio::steady_timer t(ex); t.expires_after(std::chrono::seconds(30));
                co_await t.async_wait(asio::use_awaitable); continue;
            }
            LOG("[LEGENDS_MD] Legends TICKER_PLANT connected");

            // Heartbeat immediately after login
            try {
                rti::RequestHeartbeat hb; hb.set_template_id(18);
                auto ts = std::chrono::system_clock::now().time_since_epoch();
                hb.set_ssboe(static_cast<int32_t>(
                    std::chrono::duration_cast<std::chrono::seconds>(ts).count()));
                co_await ws_write(*l_ws, proto_frame(hb));
            } catch (...) {}

            // ── subscribe ────────────────────────────────────────────────────
            bool sub_ok = true;
            try {
                rti::RequestMarketDataUpdate sub;
                sub.set_template_id(100);
                sub.set_symbol(trade_symbol);
                sub.set_exchange(orb_cfg.exchange);
                sub.set_request(rti::RequestMarketDataUpdate::SUBSCRIBE);
                sub.set_update_bits(1);  // LAST_TRADE
                co_await ws_write(*l_ws, proto_frame(sub));
            } catch (...) { sub_ok = false; }
            if (!sub_ok) {
                asio::steady_timer t(ex); t.expires_after(std::chrono::seconds(30));
                co_await t.async_wait(asio::use_awaitable); continue;
            }

            // ── tick read loop ───────────────────────────────────────────────
            beast::flat_buffer buf;
            bool read_err = false;
            int  delay_after = 0;
            while (g_running && !read_err) {
                buf.clear(); read_err = false;
                try { co_await l_ws->async_read(buf, asio::use_awaitable); }
                catch (std::exception& e) {
                    LOG("[LEGENDS_MD] Read error: %s — reconnecting", e.what());
                    read_err = true; delay_after = 1;
                }
                if (read_err) break;

                auto payload = proto_strip(beast::buffers_to_string(buf.data()));
                rti::Base base; if (!base.ParseFromString(payload)) continue;
                int tid = base.template_id();

                if (tid == 150) {
                    rti::LastTrade lt; if (!lt.ParseFromString(payload)) continue;
                    if (lt.trade_price() <= 0.0 || lt.trade_size() <= 0) continue;
                    if (db) db->write_legends_price(today, lt.trade_price());
                } else if (tid == 77) {
                    LOG("[LEGENDS_MD] FORCED LOGOUT — reconnecting in 30s");
                    read_err = true; delay_after = 1;
                } else if (tid == 18) {
                    rti::ResponseHeartbeat hb_resp; hb_resp.set_template_id(19);
                    bool hb_ok = true;
                    try { co_await ws_write(*l_ws, proto_frame(hb_resp)); }
                    catch (...) { hb_ok = false; }
                    if (!hb_ok) { read_err = true; delay_after = 1; }
                }
            }
            if (delay_after > 0) {
                asio::steady_timer t(ex); t.expires_after(std::chrono::seconds(delay_after));
                co_await t.async_wait(asio::use_awaitable);
            }
        }
    };

    // ── Main MD receive loop (self-reconnecting) ──────────────────────────────
    // Never co_return on disconnect — reconnects internally so ORDER_PLANT stays
    // alive. md_ws is reset to nullptr on error; the inner reconnect loop restores
    // it before the next read.
    auto md_loop = [&]() -> asio::awaitable<void> {
        beast::flat_buffer buf;
        while (g_running) {
            // ── reconnect if md_ws is down ─────────────────────────────────
            while (g_running && !md_ws) {
                LOG("[EXECUTOR] MD: reconnecting...");
                bool login_ok = false;
                try {
                    // system info probe (Rithmic protocol: probe → close → reconnect → login)
                    {
                        auto probe = co_await connect_ws(ioc, ssl_ctx, orb_cfg.md_url);
                        rti::RequestRithmicSystemInfo req; req.set_template_id(16);
                        co_await ws_write(*probe, proto_frame(req));
                        beast::flat_buffer rb;
                        for (;;) {
                            rb.clear();
                            co_await probe->async_read(rb, asio::use_awaitable);
                            auto pl = proto_strip(beast::buffers_to_string(rb.data()));
                            rti::Base b; b.ParseFromString(pl);
                            if (b.template_id() == 17) break;
                        }
                        try { probe->close(websocket::close_code::normal); } catch (...) {}
                    }
                    md_ws = co_await connect_ws(ioc, ssl_ctx, orb_cfg.md_url);
                    {
                        rti::RequestLogin req; req.set_template_id(10);
                        req.set_template_version("3.9");
                        req.set_user(orb_cfg.md_user);
                        req.set_password(orb_cfg.md_password);
                        req.set_system_name(orb_cfg.md_system_name);
                        req.set_app_name(orb_cfg.app_name + "-MD");
                        req.set_app_version(orb_cfg.app_version);
                        req.set_infra_type(rti::RequestLogin::TICKER_PLANT);
                        co_await ws_write(*md_ws, proto_frame(req));
                        beast::flat_buffer lb;
                        for (;;) {
                            lb.clear();
                            co_await md_ws->async_read(lb, asio::use_awaitable);
                            auto pl = proto_strip(beast::buffers_to_string(lb.data()));
                            rti::Base b; b.ParseFromString(pl);
                            if (b.template_id() == 11) {
                                rti::ResponseLogin resp; resp.ParseFromString(pl);
                                login_ok = !resp.rp_code().empty() && resp.rp_code(0) == "0";
                                if (!login_ok)
                                    LOG("[EXECUTOR] MD reconnect: login failed");
                                break;
                            }
                        }
                    }
                    if (login_ok) {
                        // immediate heartbeat required after login
                        rti::RequestHeartbeat hb; hb.set_template_id(18);
                        auto ts = std::chrono::system_clock::now().time_since_epoch();
                        hb.set_ssboe(static_cast<int32_t>(
                            std::chrono::duration_cast<std::chrono::seconds>(ts).count()));
                        co_await ws_write(*md_ws, proto_frame(hb));
                        // re-subscribe to last trade
                        rti::RequestMarketDataUpdate sub; sub.set_template_id(100);
                        sub.set_symbol(trade_symbol);
                        sub.set_exchange(orb_cfg.exchange);
                        sub.set_request(rti::RequestMarketDataUpdate::SUBSCRIBE);
                        sub.set_update_bits(1);
                        co_await ws_write(*md_ws, proto_frame(sub));
                        LOG("[EXECUTOR] MD reconnect OK — re-subscribed to %s", trade_symbol.c_str());
                    } else {
                        try { md_ws->close(websocket::close_code::normal); } catch (...) {}
                        md_ws.reset();
                    }
                } catch (std::exception& e) {
                    LOG("[EXECUTOR] MD reconnect error: %s", e.what());
                    if (md_ws) {
                        try { md_ws->close(websocket::close_code::normal); } catch (...) {}
                        md_ws.reset();
                    }
                }
                if (!md_ws) {
                    asio::steady_timer t(ex);
                    t.expires_after(std::chrono::seconds(5));
                    co_await t.async_wait(asio::use_awaitable);
                }
            }
            if (!g_running) co_return;

            // ── read one message ───────────────────────────────────────────
            buf.clear();
            bool read_error = false;
            try {
                co_await md_ws->async_read(buf, asio::use_awaitable);
            } catch (std::exception& e) {
                if (!g_running) co_return;
                LOG("[EXECUTOR] MD read error: %s — reconnecting", e.what());
                try { md_ws->close(websocket::close_code::normal); } catch (...) {}
                md_ws.reset();
                read_error = true;
            }
            if (read_error) continue;  // re-enters reconnect block above

            auto payload = proto_strip(beast::buffers_to_string(buf.data()));
            rti::Base base;
            if (!base.ParseFromString(payload)) continue;

            int tid = base.template_id();

            if (tid == 150) {
                // LastTrade
                rti::LastTrade lt;
                if (!lt.ParseFromString(payload)) continue;

                // Filter zero-price / zero-size ticks (Rithmic heartbeat events)
                if (lt.trade_price() <= 0.0 || lt.trade_size() <= 0) continue;

                int64_t ts_us = static_cast<int64_t>(lt.ssboe()) * 1'000'000LL
                              + lt.usecs();
                OrbTick tick{ts_us, lt.trade_price(), lt.trade_size(),
                             lt.aggressor() == rti::LastTrade::BUY};

                // Check trailing stop on every tick
                order_mgr.check_trail_and_stop(tick.price);

                // Feed strategy
                strategy.on_tick(tick);

            } else if (tid == 18) {
                // Server-sent RequestHeartbeat — must respond with ResponseHeartbeat (tid=19)
                rti::ResponseHeartbeat hb_resp;
                hb_resp.set_template_id(19);
                try {
                    co_await ws_write(*md_ws, proto_frame(hb_resp));
                } catch (...) {
                    LOG("[EXECUTOR] MD heartbeat response send failed");
                }
            } else if (tid == 19) {
                // ResponseHeartbeat — server acked our heartbeat, no action needed
            } else if (tid == 101) {
                // ResponseMarketDataUpdate — subscription ack
                rti::ResponseMarketDataUpdate resp;
                resp.ParseFromString(payload);
                std::string rpc = resp.rp_code().empty() ? "?" : resp.rp_code(0);
                LOG("[EXECUTOR] MD subscription %s (rp_code=%s)",
                    rpc == "0" ? "OK" : "FAILED", rpc.c_str());
            } else if (tid == 77) {
                // ForcedLogout — server is closing this session; trigger reconnect
                LOG("[EXECUTOR] MD: FORCED LOGOUT (tid=77) — reconnecting MD without touching ORDER_PLANT");
                try { md_ws->close(websocket::close_code::normal); } catch (...) {}
                md_ws.reset();
            } else if (tid == 11) {
                LOG("[EXECUTOR] Login response on MD loop (template 11) — ignoring");
            } else {
                LOG("[EXECUTOR] MD: unhandled tid=%d len=%zu", tid, payload.size());
            }
        }
    };

    // Run all coroutines concurrently
    // (In a real deployment we'd use parallel_group; for simplicity we spawn
    //  as separate tasks on the same io_context — C++20 coroutines co_spawn)
    asio::co_spawn(ex, heartbeat_loop(),  asio::detached);
    asio::co_spawn(ex, eod_loop(),        asio::detached);
    asio::co_spawn(ex, op_loop(),         asio::detached);
    // legends_md_loop disabled — Legends single-session limit causes FORCED LOGOUT
    // loop that destabilises the main MD (AMP) feed when ORDER_PLANT is active.
    // asio::co_spawn(ex, legends_md_loop(), asio::detached);

    // Seed ORB range from env vars (use after restart when range was lost)
    {
        const char* sh = std::getenv("ORB_SEED_HIGH");
        const char* sl = std::getenv("ORB_SEED_LOW");
        if (sh && sl) {
            strategy.seed_orb_range(std::stod(sh), std::stod(sl));
        }
    }

    // Test-order hook: fire one BUY+SELL round-trip when NQ_FIRE_TEST_ORDER is set
    if (std::getenv("NQ_FIRE_TEST_ORDER") != nullptr) {
        LOG("[TEST] NQ_FIRE_TEST_ORDER set — firing BUY %s MKT qty=1", orb_cfg.symbol.c_str());
        std::string buy_basket = "NQ-testbuy-" + std::to_string(std::time(nullptr));
        order_plant->send_new_order(buy_basket, orb_cfg.symbol, orb_cfg.exchange,
                                    1, 2, true, 0.0, "test-buy", false);
        {
            asio::steady_timer t(ex, std::chrono::seconds(4));
            co_await t.async_wait(asio::use_awaitable);
        }
        if (std::getenv("NQ_TEST_BUY_ONLY") != nullptr) {
            LOG("[TEST] NQ_TEST_BUY_ONLY — skipping SELL; position remains LONG");
        } else {
        LOG("[TEST] Firing SELL %s MKT qty=1 to flatten", orb_cfg.symbol.c_str());
        std::string sell_basket = "NQ-testsell-" + std::to_string(std::time(nullptr));
        order_plant->send_new_order(sell_basket, orb_cfg.symbol, orb_cfg.exchange,
                                    1, 2, false, 0.0, "test-sell", false);
        }
        {
            asio::steady_timer t(ex, std::chrono::seconds(5));
            co_await t.async_wait(asio::use_awaitable);
        }
        LOG("[TEST] Round-trip complete — exiting");
        g_running = false;
        ioc_ref.stop();
        co_return;
    }

    co_await md_loop();

    carried_pos = order_mgr.position_snapshot();  // preserve state across reconnects (#2)
    LOG("[EXECUTOR] Main loop exited — carried_pos.state=%d", (int)carried_pos.state);
    ioc_ref.stop();  // unblock ioc.run() so outer reconnect loop can restart
}

// ─── Entry point ──────────────────────────────────────────────────────────────
int main(int argc, char* argv[]) {
    std::signal(SIGINT,  handle_signal);
    std::signal(SIGTERM, handle_signal);

    // ── Parse args ────────────────────────────────────────────────────────────
    std::string config_path = "config/orb_config.json";
    bool force_dry_run = false;

    for (int i = 1; i < argc; ++i) {
        if (std::strcmp(argv[i], "--config") == 0 && i + 1 < argc)
            config_path = argv[++i];
        else if (std::strcmp(argv[i], "--dry-run") == 0)
            force_dry_run = true;
    }

    LOG("[EXECUTOR] NQ ORB Execution Engine starting");
    LOG("[EXECUTOR] Config: %s", config_path.c_str());

    // ── Load ORB config ───────────────────────────────────────────────────────
    OrbConfig orb_cfg;
    try {
        orb_cfg = OrbConfig::from_file(config_path);
    } catch (std::exception& e) {
        std::fprintf(stderr, "FATAL: Cannot load config: %s\n", e.what());
        return 1;
    }
    if (force_dry_run) orb_cfg.dry_run = true;

    LOG("[EXECUTOR] symbol=%s exchange=%s orb_min=%d sl=%.1fpts trail_step=%.1fpts",
        orb_cfg.symbol.c_str(), orb_cfg.exchange.c_str(),
        orb_cfg.orb_minutes, orb_cfg.sl_points, orb_cfg.trail_step);
    LOG("[EXECUTOR] max_daily_trades=%d last_entry_hour=%d dry_run=%s",
        orb_cfg.max_daily_trades, orb_cfg.last_entry_hour,
        orb_cfg.dry_run ? "true" : "false");
    LOG("[EXECUTOR] risk: trailing_dd_cap=$%.0f consistency_cap=%.0f%%",
        orb_cfg.trailing_drawdown_cap, orb_cfg.consistency_cap_pct * 100.0);

    // ── Validate Legends credentials ─────────────────────────────────────────
    if (orb_cfg.rithmic_user.empty()) {
        std::fprintf(stderr, "Config error: RITHMIC_LEGENDS_USER not set\n"); return 1;
    }
    if (orb_cfg.rithmic_password.empty()) {
        std::fprintf(stderr, "Config error: RITHMIC_LEGENDS_PASSWORD not set\n"); return 1;
    }

    // ── Validate config sanity ────────────────────────────────────────────────
    if (orb_cfg.sl_points <= 0) {
        std::fprintf(stderr, "FATAL: sl_points must be > 0\n");
        return 1;
    }
    if (orb_cfg.trailing_drawdown_cap <= 0) {
        std::fprintf(stderr, "FATAL: trailing_drawdown_cap must be > 0\n");
        return 1;
    }

    // ── Run ───────────────────────────────────────────────────────────────────
    // Hoist session components so they survive reconnects.
    RiskManager  risk(orb_cfg);
    OrbStrategy  strategy(orb_cfg);
    std::string  today;        // empty = first run; triggers reset_session/reset_daily
    Position     carried_pos;  // non-FLAT on reconnect → halt + warn (#2)

    int exit_code = 0;
    while (g_running) {
        try {
            asio::io_context ioc(1);
            asio::co_spawn(ioc,
                run_executor(orb_cfg, ioc, risk, strategy, today, carried_pos),
                [&](std::exception_ptr ep) {
                    if (ep) {
                        try { std::rethrow_exception(ep); }
                        catch (std::exception& e) {
                            LOG("[EXECUTOR] Coroutine exception: %s", e.what());
                        }
                        g_running = false;  // unhandled exception — stop
                    }
                    // Normal exit (MD disconnect) → outer while loop reconnects
                });
            ioc.run();
        } catch (std::exception& e) {
            LOG("[EXECUTOR] io_context exception: %s", e.what());
            exit_code = 1;
        }

        if (g_running) {
            LOG("[EXECUTOR] Reconnecting in 10s...");
            std::this_thread::sleep_for(std::chrono::seconds(10));
        }
    }

    LOG("[EXECUTOR] Shutdown complete");
    return exit_code;
}
