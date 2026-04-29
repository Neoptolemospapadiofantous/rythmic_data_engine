#include "collector.hpp"
#include "log.hpp"
#include "validator.hpp"

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/redirect_error.hpp>
#include <boost/asio/steady_timer.hpp>
#include <stdexcept>

// ── Collector ──────────────────────────────────────────────────────

Collector::Collector(const Config& cfg) : cfg_(cfg) {
    db_    = std::make_unique<TickDB>(cfg_.pg_connstr());
    audit_ = std::make_unique<AuditLog>(db_->conn());
    wal_   = std::make_unique<Wal>(cfg_.wal_path());
    sentinel_ = std::make_unique<DataSentinel>();

    auto count = db_->row_count();
    LOG("PostgreSQL connected (%lld existing ticks)", (long long)count);

    // Start a new DB session
    session_id_ = db_->start_session("collect");
    LOG("Session started (id=%lld)", (long long)session_id_);

    audit_->info("collector.start",
                 "existing_ticks=" + std::to_string(count) +
                 " session_id=" + std::to_string(session_id_));

    // Replay any ticks that were written to WAL but not flushed (crash recovery)
    auto replayed = wal_->replay();
    if (!replayed.empty()) {
        LOG("WAL replay: %zu ticks recovered from crash", replayed.size());
        try {
            int n = db_->write(replayed);
            wal_->commit();
            LOG("WAL replay: %d ticks written to DB", n);
            audit_->info("wal.replay", "recovered=" + std::to_string(n));
        } catch (std::exception& e) {
            LOG("WAL replay DB write failed: %s (ticks kept in WAL)", e.what());
        }
    }

    client_ = std::make_unique<RithmicClient>(ioc_, cfg_);
    client_->set_on_tick([this](TickRow r)  { on_tick(std::move(r));  });
    client_->set_on_bbo([this](BBORow r)    { on_bbo(std::move(r));   });
    client_->set_on_depth([this](DepthRow r){ on_depth(std::move(r)); });

    last_flush_         = std::chrono::steady_clock::now();
    last_audit_flush_   = std::chrono::steady_clock::now();
    last_metrics_flush_ = std::chrono::steady_clock::now();
    last_bbo_flush_     = std::chrono::steady_clock::now();
    last_depth_flush_   = std::chrono::steady_clock::now();
}

Collector::~Collector() { stop(); }

// ── on_tick ────────────────────────────────────────────────────────

void Collector::on_tick(TickRow row) {
    // Validate before buffering — reject garbage data early
    std::string reason;
    if (!TickValidator::valid(row, &reason)) {
        LOG("  Tick rejected [%s/%s price=%.2f size=%lld]: %s",
            row.symbol.c_str(), row.exchange.c_str(),
            row.price, (long long)row.size, reason.c_str());
        ++rejected_total_;
        return;
    }

    // Economic plausibility checks (stateful — price jumps, gaps, volume spikes)
    sentinel_->observe_tick(row.price, row.size, row.ts_micros);

    bool need_flush = false;
    {
        std::lock_guard lock(buf_mu_);
        buf_.push_back(std::move(row));
        ++session_total_;
        double elapsed = std::chrono::duration<double>(
            std::chrono::steady_clock::now() - last_flush_).count();
        need_flush = (static_cast<int>(buf_.size()) >= FLUSH_EVERY_N ||
                      elapsed >= FLUSH_EVERY_SEC);
    }
    if (need_flush) flush();
}

// ── ensure_db_connected ────────────────────────────────────────────

void Collector::ensure_db_connected() {
    if (!db_->is_connected()) {
        LOG("  DB disconnected — attempting reconnect...");
        db_->reconnect();
    }
}

// ── flush ──────────────────────────────────────────────────────────

int Collector::flush() {
    std::vector<TickRow> batch;
    {
        std::lock_guard lock(buf_mu_);
        if (buf_.empty()) return 0;
        batch.swap(buf_);
        last_flush_ = std::chrono::steady_clock::now();
    }

    // Step 1: check for accumulated missed batches BEFORE appending new one
    bool was_dirty = wal_->dirty();

    // Step 2: durably append current batch to WAL (fdatasync)
    try {
        wal_->write_batch(batch);
    } catch (std::exception& e) {
        LOG("  WAL write failed: %s — ticks may be lost on crash", e.what());
    }

    // Step 3: drain into DB.
    try {
        ensure_db_connected();
        int n;
        if (was_dirty) {
            auto pending = wal_->replay();
            n = db_->write(pending);
        } else {
            n = db_->write(batch);
        }
        wal_->commit();

        LOG("  Wrote %d ticks (session=%lld rejected=%lld)",
            n, (long long)session_total_.load(),
               (long long)rejected_total_.load());
        audit_->info("ticks.written",
                     "count=" + std::to_string(n) +
                     " batch=" + std::to_string(batch.size()));

        // Periodic flushes
        auto now = std::chrono::steady_clock::now();

        double ae = std::chrono::duration<double>(now - last_audit_flush_).count();
        if (ae >= 60.0) {
            audit_->flush();
            last_audit_flush_ = now;
        }

        double me = std::chrono::duration<double>(now - last_metrics_flush_).count();
        if (me >= METRICS_FLUSH_SEC) {
            flush_sentinel();
            flush_metrics();
            last_metrics_flush_ = now;
        }

        return n;

    } catch (std::exception& e) {
        LOG("  DB write failed: %s — %zu ticks held in WAL", e.what(), batch.size());
        audit_->error("ticks.write_error", e.what());
        return 0;
    }
}

// ── on_bbo ─────────────────────────────────────────────────────────

void Collector::on_bbo(BBORow row) {
    // BBO sentinel checks (bid-ask inversion, wide spread)
    sentinel_->observe_bbo(row.bid_price, row.ask_price);

    bool need_flush = false;
    {
        std::lock_guard lock(bbo_mu_);
        bbo_buf_.push_back(std::move(row));
        ++bbo_total_;
        double elapsed = std::chrono::duration<double>(
            std::chrono::steady_clock::now() - last_bbo_flush_).count();
        need_flush = (static_cast<int>(bbo_buf_.size()) >= BBO_FLUSH_EVERY_N ||
                      elapsed >= BBO_FLUSH_EVERY_SEC);
    }
    if (need_flush) flush_bbo();
}

// ── flush_bbo ──────────────────────────────────────────────────────

int Collector::flush_bbo() {
    std::vector<BBORow> batch;
    {
        std::lock_guard lock(bbo_mu_);
        if (bbo_buf_.empty()) return 0;
        batch.swap(bbo_buf_);
        last_bbo_flush_ = std::chrono::steady_clock::now();
    }
    try {
        ensure_db_connected();
        int n = db_->write_bbo(batch);
        LOG("  Wrote %d BBO rows", n);
        return n;
    } catch (std::exception& e) {
        LOG("  BBO DB write failed: %s — %zu rows dropped", e.what(), batch.size());
        audit_->error("bbo.write_error", e.what());
        return 0;
    }
}

// ── on_depth ───────────────────────────────────────────────────────

void Collector::on_depth(DepthRow row) {
    bool need_flush = false;
    {
        std::lock_guard lock(depth_mu_);
        depth_buf_.push_back(std::move(row));
        ++depth_total_;
        double elapsed = std::chrono::duration<double>(
            std::chrono::steady_clock::now() - last_depth_flush_).count();
        need_flush = (static_cast<int>(depth_buf_.size()) >= DEPTH_FLUSH_EVERY_N ||
                      elapsed >= DEPTH_FLUSH_EVERY_SEC);
    }
    if (need_flush) flush_depth();
}

// ── flush_depth ────────────────────────────────────────────────────

int Collector::flush_depth() {
    std::vector<DepthRow> batch;
    {
        std::lock_guard lock(depth_mu_);
        if (depth_buf_.empty()) return 0;
        batch.swap(depth_buf_);
        last_depth_flush_ = std::chrono::steady_clock::now();
    }
    try {
        ensure_db_connected();
        int n = db_->write_depth(batch);
        LOG("  Wrote %d depth rows", n);
        return n;
    } catch (std::exception& e) {
        LOG("  depth DB write failed: %s — %zu rows dropped", e.what(), batch.size());
        audit_->error("depth.write_error", e.what());
        return 0;
    }
}

// ── flush_sentinel — drain alerts from DataSentinel to DB ─────────

void Collector::flush_sentinel() {
    auto alerts = sentinel_->drain_alerts();
    if (alerts.empty()) return;

    std::vector<SentinelAlertRow> rows;
    rows.reserve(alerts.size());
    for (auto& a : alerts) {
        rows.push_back({session_id_, a.check, a.severity, a.message, a.value});
    }

    try {
        db_->write_sentinel_alerts(rows);
        LOG("  Flushed %zu sentinel alerts", alerts.size());
    } catch (std::exception& e) {
        LOG("  Sentinel alert flush failed: %s", e.what());
    }
}

// ── flush_metrics — write quality metrics snapshot ─────────────────

void Collector::flush_metrics() {
    try {
        std::vector<QualityMetric> ms;
        ms.push_back({"session_ticks",    static_cast<double>(session_total_.load()), ""});
        ms.push_back({"session_rejected", static_cast<double>(rejected_total_.load()), ""});
        ms.push_back({"session_bbo",      static_cast<double>(bbo_total_.load()), ""});
        ms.push_back({"session_depth",    static_cast<double>(depth_total_.load()), ""});
        ms.push_back({"sentinel_alerts",  static_cast<double>(sentinel_->alert_count()), ""});
        ms.push_back({"sentinel_gaps",    static_cast<double>(sentinel_->gap_count()), ""});

        double reject_rate = session_total_.load() > 0
            ? static_cast<double>(rejected_total_.load()) / static_cast<double>(session_total_.load() + rejected_total_.load()) * 100.0
            : 0.0;
        ms.push_back({"rejection_rate_pct", reject_rate, ""});

        db_->write_metrics(ms);
    } catch (std::exception& e) {
        LOG("  Metrics flush failed: %s", e.what());
    }
}

// ── status logging ─────────────────────────────────────────────────

asio::awaitable<void> Collector::status_log_coro() {
    auto ex = co_await asio::this_coro::executor;
    asio::steady_timer t(ex);
    while (running_) {
        t.expires_after(std::chrono::seconds(60));
        boost::system::error_code ec;
        co_await t.async_wait(asio::redirect_error(use_awaitable, ec));
        if (ec || !running_) co_return;
        try {
            auto s = db_->summary();
            LOG("  ticks=%lld  session=%lld  rejected=%lld  bbo=%lld  depth=%lld  alerts=%lld  latest=%s  price=%s",
                (long long)s.tick_count,
                (long long)session_total_.load(),
                (long long)rejected_total_.load(),
                (long long)bbo_total_.load(),
                (long long)depth_total_.load(),
                (long long)sentinel_->alert_count(),
                s.latest.c_str(),
                s.price ? std::to_string(*s.price).c_str() : "n/a");
            audit_->flush();
        } catch (...) {}
    }
}

void Collector::status_log() {
    asio::co_spawn(ioc_, status_log_coro(), asio::detached);
}

// ── run / stop ─────────────────────────────────────────────────────

void Collector::run() {
    auto errs = cfg_.validate();
    if (!errs.empty()) {
        for (auto& e : errs) LOG("Config error: %s", e.c_str());
        throw std::runtime_error("Invalid config — check .env");
    }

    status_log();

    asio::co_spawn(ioc_, client_->run(), [this](std::exception_ptr ep) {
        if (ep) {
            try { std::rethrow_exception(ep); }
            catch (std::exception& e) {
                LOG("Client error: %s", e.what());
                audit_->error("connection.lost", e.what());
            }
        }
        ioc_.stop();
    });

    ioc_.run();

    // Final flushes before shutdown
    flush();
    flush_bbo();
    flush_depth();
    flush_sentinel();
    flush_metrics();

    // Close session in DB
    try {
        db_->end_session(session_id_,
                         session_total_.load(), bbo_total_.load(), depth_total_.load(),
                         rejected_total_.load(), sentinel_->gap_count(),
                         sentinel_->alert_count());
        LOG("Session %lld closed", (long long)session_id_);
    } catch (std::exception& e) {
        LOG("Failed to close session: %s", e.what());
    }

    audit_->info("collector.stop",
                 "session_id=" + std::to_string(session_id_) +
                 " ticks=" + std::to_string(session_total_.load()) +
                 " rejected=" + std::to_string(rejected_total_.load()));
    audit_->flush();
    LOG("Collector stopped.");
}

void Collector::stop() {
    if (running_.exchange(false)) {
        client_->stop();
        ioc_.stop();
    }
}
