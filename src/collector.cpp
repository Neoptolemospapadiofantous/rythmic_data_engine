#include "collector.hpp"
#include "log.hpp"

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/steady_timer.hpp>
#include <stdexcept>

// ── Collector ──────────────────────────────────────────────────────

Collector::Collector(const Config& cfg) : cfg_(cfg) {
    db_    = std::make_unique<TickDB>(cfg_.pg_connstr());
    audit_ = std::make_unique<AuditLog>(db_->conn());

    auto count = db_->row_count();
    LOG("PostgreSQL connected (%lld existing ticks)", (long long)count);
    audit_->info("collector.start",
                 "existing_ticks=" + std::to_string(count));

    client_ = std::make_unique<RithmicClient>(ioc_, cfg_);
    client_->set_on_tick([this](TickRow r) { on_tick(std::move(r)); });

    last_flush_       = std::chrono::steady_clock::now();
    last_audit_flush_ = std::chrono::steady_clock::now();
}

Collector::~Collector() { stop(); }

// ── on_tick ────────────────────────────────────────────────────────

void Collector::on_tick(TickRow row) {
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

// ── flush ──────────────────────────────────────────────────────────

int Collector::flush() {
    std::vector<TickRow> batch;
    {
        std::lock_guard lock(buf_mu_);
        if (buf_.empty()) return 0;
        batch.swap(buf_);
        last_flush_ = std::chrono::steady_clock::now();
    }
    try {
        int n = db_->write(batch);
        LOG("  Wrote %d ticks (session total: %lld)",
            n, (long long)session_total_.load());
        audit_->info("ticks.written",
                     "count=" + std::to_string(n) +
                     " batch=" + std::to_string(batch.size()));

        // Flush audit events periodically
        double audit_elapsed = std::chrono::duration<double>(
            std::chrono::steady_clock::now() - last_audit_flush_).count();
        if (audit_elapsed >= 60.0) {
            audit_->flush();
            last_audit_flush_ = std::chrono::steady_clock::now();
        }
        return n;
    } catch (std::exception& e) {
        LOG("  DB write error: %s", e.what());
        audit_->error("ticks.write_error", e.what());
        audit_->flush();
        return 0;
    }
}

// ── status logging ─────────────────────────────────────────────────

void Collector::status_log() {
    asio::steady_timer t(ioc_);
    std::function<void()> schedule = [&] {
        t.expires_after(std::chrono::seconds(60));
        t.async_wait([&](boost::system::error_code ec) {
            if (ec || !running_) return;
            try {
                auto s = db_->summary();
                LOG("  ticks=%lld  session=%lld  latest=%s  price=%s",
                    (long long)s.tick_count,
                    (long long)session_total_.load(),
                    s.latest.c_str(),
                    s.price ? std::to_string(*s.price).c_str() : "n/a");
                audit_->flush();
            } catch (...) {}
            schedule();
        });
    };
    schedule();
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

    flush();
    audit_->info("collector.stop");
    audit_->flush();
    LOG("Collector stopped.");
}

void Collector::stop() {
    if (running_.exchange(false)) {
        client_->stop();
        ioc_.stop();
    }
}
