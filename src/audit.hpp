#pragma once
#include <chrono>
#include <mutex>
#include <string>
#include <vector>

#include <libpq-fe.h>

// Async audit logger — writes structured events to the audit_log table.
//
// Events are buffered in memory and flushed in batches to avoid blocking
// the collector hot path. Call flush() periodically (e.g., every minute).
//
// Standard events:
//   collector.start / collector.stop
//   connection.established / connection.lost
//   ticks.written   details: "count=N"
//   error           details: "<message>"
class AuditLog {
public:
    enum class Severity { INFO, WARN, ERROR };

    explicit AuditLog(PGconn* conn);

    // Non-blocking — appends to in-memory buffer
    void log(const std::string& event,
             const std::string& details  = "",
             Severity           severity = Severity::INFO);

    void info (const std::string& event, const std::string& details = "");
    void warn (const std::string& event, const std::string& details = "");
    void error(const std::string& event, const std::string& details = "");

    // Flush pending events to PostgreSQL (call from collector loop)
    void flush();

    // Number of events pending in buffer
    int pending() const;

private:
    struct Event {
        std::string ts;         // ISO 8601
        std::string event;
        std::string severity;
        std::string details;
    };

    static std::string now_iso();
    static std::string sev_str(Severity s);

    PGconn*           conn_;
    mutable std::mutex mu_;
    std::vector<Event> buf_;
};
