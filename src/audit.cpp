#include "audit.hpp"
#include "log.hpp"

#include <cstdio>
#include <ctime>
#include <stdexcept>

// ── helpers ────────────────────────────────────────────────────────

std::string AuditLog::now_iso() {
    auto t = std::time(nullptr);
    struct tm tm_utc;
    gmtime_r(&t, &tm_utc);
    char buf[32];
    std::strftime(buf, sizeof(buf), "%Y-%m-%dT%H:%M:%SZ", &tm_utc);
    return buf;
}

std::string AuditLog::sev_str(Severity s) {
    switch (s) {
        case Severity::WARN:  return "WARN";
        case Severity::ERROR: return "ERROR";
        default:              return "INFO";
    }
}

// ── AuditLog ───────────────────────────────────────────────────────

AuditLog::AuditLog(PGconn* conn) : conn_(conn) {}

void AuditLog::log(const std::string& event,
                   const std::string& details,
                   Severity           severity) {
    std::lock_guard lock(mu_);
    buf_.push_back({now_iso(), event, sev_str(severity), details});
}

void AuditLog::info (const std::string& e, const std::string& d) {
    log(e, d, Severity::INFO);
}
void AuditLog::warn (const std::string& e, const std::string& d) {
    log(e, d, Severity::WARN);
}
void AuditLog::error(const std::string& e, const std::string& d) {
    log(e, d, Severity::ERROR);
}

int AuditLog::pending() const {
    std::lock_guard lock(mu_);
    return static_cast<int>(buf_.size());
}

// ── flush ──────────────────────────────────────────────────────────

void AuditLog::flush() {
    std::vector<Event> batch;
    {
        std::lock_guard lock(mu_);
        if (buf_.empty()) return;
        batch.swap(buf_);
    }

    // Build a single multi-row INSERT
    std::string sql =
        "INSERT INTO audit_log (ts, event, severity, details) VALUES ";

    for (size_t i = 0; i < batch.size(); ++i) {
        if (i) sql += ',';
        auto esc_details = [&](const std::string& s) {
            std::string out;
            out.reserve(s.size() + 2);
            out += '\'';
            for (char c : s) {
                if (c == '\'') out += "''";
                else           out += c;
            }
            out += '\'';
            return out;
        };
        sql += "('" + batch[i].ts + "',"
               "'" + batch[i].event + "',"
               "'" + batch[i].severity + "',"
               + esc_details(batch[i].details) + ")";
    }

    PGresult* res = PQexec(conn_, sql.c_str());
    if (!res || PQresultStatus(res) != PGRES_COMMAND_OK) {
        LOG("Audit flush error: %s",
            res ? PQresultErrorMessage(res) : "null result");
        // Re-queue the events so they aren't lost
        std::lock_guard lock(mu_);
        buf_.insert(buf_.begin(), batch.begin(), batch.end());
    }
    if (res) PQclear(res);
}
