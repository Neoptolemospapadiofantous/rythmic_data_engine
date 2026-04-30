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
    if (buf_.size() >= MAX_BUF) buf_.erase(buf_.begin());  // drop oldest to prevent OOM
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

    // Build parallel param arrays for PQexecParams
    const int n = static_cast<int>(batch.size());
    std::vector<const char*> p_ts(n), p_ev(n), p_sev(n), p_det(n);
    for (int i = 0; i < n; ++i) {
        p_ts[i]  = batch[i].ts.c_str();
        p_ev[i]  = batch[i].event.c_str();
        p_sev[i] = batch[i].severity.c_str();
        p_det[i] = batch[i].details.c_str();
    }

    // Use UNNEST to insert all rows in one parameterized statement
    // $1..$4 are text[] arrays
    const char* sql =
        "INSERT INTO audit_log (ts, event, severity, details)"
        " SELECT * FROM unnest($1::timestamptz[], $2::varchar[], $3::varchar[], $4::text[])";

    // Helper: wrap vector<const char*> into '{val1,val2,...}' quoted array string
    auto make_pg_array = [](const std::vector<const char*>& vals) {
        std::string arr = "{";
        for (size_t i = 0; i < vals.size(); ++i) {
            if (i) arr += ',';
            arr += '"';
            for (const char* p = vals[i]; *p; ++p) {
                if (*p == '"' || *p == '\\') arr += '\\';
                arr += *p;
            }
            arr += '"';
        }
        arr += '}';
        return arr;
    };

    std::string a_ts  = make_pg_array(p_ts);
    std::string a_ev  = make_pg_array(p_ev);
    std::string a_sev = make_pg_array(p_sev);
    std::string a_det = make_pg_array(p_det);

    const char* params[4] = { a_ts.c_str(), a_ev.c_str(), a_sev.c_str(), a_det.c_str() };
    PGresult* res = PQexecParams(conn_, sql, 4, nullptr, params, nullptr, nullptr, 0);

    if (!res || PQresultStatus(res) != PGRES_COMMAND_OK) {
        LOG("Audit flush error: %s",
            res ? PQresultErrorMessage(res) : "null result");
        // Re-queue the events so they aren't lost
        std::lock_guard lock(mu_);
        buf_.insert(buf_.begin(), batch.begin(), batch.end());
    }
    if (res) PQclear(res);
}
