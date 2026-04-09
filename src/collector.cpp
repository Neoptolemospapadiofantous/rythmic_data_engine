#include "collector.hpp"
#include "log.hpp"
#include "validator.hpp"

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/steady_timer.hpp>
#include <stdexcept>

// ── Collector ──────────────────────────────────────────────────────

Collector::Collector(const Config& cfg) : cfg_(cfg) {
    db_    = std::make_unique<TickDB>(cfg_.pg_connstr());
    audit_ = std::make_unique<AuditLog>(db_->conn());
    wal_   = std::make_unique<Wal>(cfg_.wal_path());

    auto count = db_->row_count();
    LOG("PostgreSQL connected (%lld existing ticks)", (long long)count);
    audit_->info("collector.start",
                 "existing_ticks=" + std::to_string(count));

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
    client_->set_on_tick([this](TickRow r) { on_tick(std::move(r)); });

    last_flush_       = std::chrono::steady_clock::now();
    last_audit_flush_ = std::chrono::steady_clock::now();
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

    // Step 1: check for accumulated missed batches BEFORE appending new one
    bool was_dirty = wal_->dirty();

    // Step 2: durably append current batch to WAL (fdatasync)
    try {
        wal_->write_batch(batch);
    } catch (std::exception& e) {
        LOG("  WAL write failed: %s — ticks may be lost on crash", e.what());
        // Still attempt DB write; worst case: crash between here and DB commit
        // loses this batch only.
    }

    // Step 3: drain into DB.
    //
    // If WAL was already dirty (previous flush failed), replay the full WAL so
    // we catch up all accumulated missed batches in one shot.
    // In the normal case (WAL was clean) just write the current batch directly —
    // avoids a redundant file read.
    //
    // Non-blocking: no sleep.  If DB is down, log and return; WAL holds the data.
    try {
        if (!db_->is_connected()) {
            LOG("  DB disconnected — attempting reconnect...");
            db_->reconnect();
        }
        int n;
        if (was_dirty) {
            auto pending = wal_->replay();   // old failures + current batch
            n = db_->write(pending);
        } else {
            n = db_->write(batch);           // fast path: no extra file read
        }
        wal_->commit();

        LOG("  Wrote %d ticks (session=%lld rejected=%lld)",
            n, (long long)session_total_.load(),
               (long long)rejected_total_.load());
        audit_->info("ticks.written",
                     "count=" + std::to_string(n) +
                     " batch=" + std::to_string(batch.size()));

        // Flush audit events periodically
        double ae = std::chrono::duration<double>(
            std::chrono::steady_clock::now() - last_audit_flush_).count();
        if (ae >= 60.0) {
            audit_->flush();
            last_audit_flush_ = std::chrono::steady_clock::now();
        }
        return n;

    } catch (std::exception& e) {
        LOG("  DB write failed: %s — %zu ticks held in WAL", e.what(), batch.size());
        audit_->error("ticks.write_error", e.what());
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
