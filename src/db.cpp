#include "db.hpp"
#include "log.hpp"

#include <cassert>
#include <cstdio>
#include <cstring>
#include <ctime>
#include <stdexcept>
#include <string>

// ── helpers ────────────────────────────────────────────────────────

static void pg_check(PGresult* res, const char* ctx) {
    if (!res) throw std::runtime_error(std::string("libpq: null result in ") + ctx);
    auto s = PQresultStatus(res);
    if (s != PGRES_COMMAND_OK && s != PGRES_TUPLES_OK) {
        std::string msg = PQresultErrorMessage(res);
        PQclear(res);
        throw std::runtime_error(std::string("DB error [") + ctx + "]: " + msg);
    }
}

std::string TickDB::format_ts(int64_t ts_micros) {
    time_t secs   = static_cast<time_t>(ts_micros / 1'000'000);
    int    micros = static_cast<int>(ts_micros % 1'000'000);
    struct tm t;
    gmtime_r(&secs, &t);
    char buf[32];
    std::strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", &t);
    char result[48];
    std::snprintf(result, sizeof(result), "%s.%06d+00", buf, micros);
    return result;
}

// ── constructor / destructor ───────────────────────────────────────

TickDB::TickDB(const std::string& connstr, bool read_only)
    : connstr_(connstr), read_only_(read_only)
{
    conn_ = PQconnectdb(connstr.c_str());
    if (PQstatus(conn_) != CONNECTION_OK)
        throw std::runtime_error(
            std::string("PostgreSQL connection failed: ") + PQerrorMessage(conn_));

    if (!read_only_)
        ensure_schema();
}

TickDB::~TickDB() { close(); }

void TickDB::close() {
    if (conn_) { PQfinish(conn_); conn_ = nullptr; }
}

// ── exec ───────────────────────────────────────────────────────────

void TickDB::exec(const char* sql) {
    PGresult* res = PQexec(conn_, sql);
    pg_check(res, sql);
    PQclear(res);
}

// ── ensure_schema ──────────────────────────────────────────────────

void TickDB::ensure_schema() {
    // Ticks hypertable — stores every raw trade print
    exec(R"(
        CREATE TABLE IF NOT EXISTS ticks (
            ts_event  TIMESTAMPTZ      NOT NULL,
            symbol    VARCHAR(32)      NOT NULL DEFAULT 'NQ',
            exchange  VARCHAR(32)      NOT NULL DEFAULT 'CME',
            price     DOUBLE PRECISION NOT NULL,
            size      BIGINT           NOT NULL,
            side      CHAR(1),
            is_buy    BOOLEAN,
            source    VARCHAR(32)      DEFAULT 'amp_rithmic'
        );
    )");

    // Add columns to existing tables (idempotent — safe to run every start)
    {
        PGresult* r;
        r = PQexec(conn_, "ALTER TABLE ticks ADD COLUMN IF NOT EXISTS symbol   VARCHAR(32) NOT NULL DEFAULT 'NQ';");  if (r) PQclear(r);
        r = PQexec(conn_, "ALTER TABLE ticks ADD COLUMN IF NOT EXISTS exchange VARCHAR(32) NOT NULL DEFAULT 'CME';"); if (r) PQclear(r);
    }

    // Create hypertable (idempotent)
    {
        PGresult* res = PQexec(conn_,
            "SELECT create_hypertable('ticks','ts_event',"
            "  if_not_exists => TRUE, migrate_data => TRUE);");
        if (res) PQclear(res);
    }

    // Unique index: (symbol, exchange, ts_event) — two instruments can tick
    // at the same microsecond; old single-column index dropped if present
    {
        PGresult* r;
        r = PQexec(conn_, "DROP INDEX IF EXISTS idx_ticks_ts_unique;");
        if (r) PQclear(r);
    }
    exec(R"(
        CREATE UNIQUE INDEX IF NOT EXISTS idx_ticks_unique
            ON ticks(symbol, exchange, ts_event);
    )");

    // Compression — non-fatal (requires TimescaleDB; skipped if unavailable)
    {
        PGresult* res = PQexec(conn_,
            "ALTER TABLE ticks SET ("
            "  timescaledb.compress,"
            "  timescaledb.compress_orderby = 'ts_event'"
            ");");
        if (res) PQclear(res);  // ignore: already set, or TimescaleDB not loaded
    }

    // ── Continuous aggregates (OHLCV bars) ────────────────────────
    // These require TimescaleDB.  Wrapped in non-throwing PQexec so the
    // engine still runs on plain PostgreSQL (bars just won't be available).
    auto ts_exec = [&](const char* sql) {
        PGresult* r = PQexec(conn_, sql);
        if (r) PQclear(r);
    };

    ts_exec(R"(
        CREATE MATERIALIZED VIEW IF NOT EXISTS bars_1min
        WITH (timescaledb.continuous) AS
        SELECT
            time_bucket('1 minute', ts_event) AS ts,
            first(price, ts_event)             AS open,
            MAX(price)                         AS high,
            MIN(price)                         AS low,
            last(price,  ts_event)             AS close,
            SUM(size)                          AS volume
        FROM ticks
        GROUP BY 1
        WITH NO DATA;
    )");

    ts_exec(R"(
        CREATE MATERIALIZED VIEW IF NOT EXISTS bars_5min
        WITH (timescaledb.continuous) AS
        SELECT
            time_bucket('5 minutes', ts_event) AS ts,
            first(price, ts_event)              AS open,
            MAX(price)                          AS high,
            MIN(price)                          AS low,
            last(price,  ts_event)              AS close,
            SUM(size)                           AS volume
        FROM ticks
        GROUP BY 1
        WITH NO DATA;
    )");

    ts_exec(R"(
        CREATE MATERIALIZED VIEW IF NOT EXISTS bars_15min
        WITH (timescaledb.continuous) AS
        SELECT
            time_bucket('15 minutes', ts_event) AS ts,
            first(price, ts_event)               AS open,
            MAX(price)                           AS high,
            MIN(price)                           AS low,
            last(price,  ts_event)               AS close,
            SUM(size)                            AS volume
        FROM ticks
        GROUP BY 1
        WITH NO DATA;
    )");

    // Refresh policies (also TimescaleDB-only — ignore errors silently)
    auto add_policy = [&](const char* view, const char* bucket) {
        char sql[512];
        std::snprintf(sql, sizeof(sql),
            "SELECT add_continuous_aggregate_policy('%s',"
            "  start_offset => INTERVAL '1 hour',"
            "  end_offset   => INTERVAL '%s',"
            "  schedule_interval => INTERVAL '%s',"
            "  if_not_exists => TRUE);",
            view, bucket, bucket);
        PGresult* r = PQexec(conn_, sql);
        if (r) PQclear(r);
    };
    add_policy("bars_1min",  "1 minute");
    add_policy("bars_5min",  "5 minutes");
    add_policy("bars_15min", "15 minutes");

    // ── Audit log table ────────────────────────────────────────────
    exec(R"(
        CREATE TABLE IF NOT EXISTS audit_log (
            id        BIGSERIAL PRIMARY KEY,
            ts        TIMESTAMPTZ DEFAULT NOW(),
            event     VARCHAR(64)  NOT NULL,
            severity  VARCHAR(8)   DEFAULT 'INFO',
            details   TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_audit_ts ON audit_log(ts);
    )");

    LOG("Schema ready (TimescaleDB hypertable + continuous aggregates)");
}

// ── write ──────────────────────────────────────────────────────────

int TickDB::write(const std::vector<TickRow>& rows) {
    if (rows.empty()) return 0;

    // Build array literals for UNNEST batch insert
    std::string ts_arr, sym_arr, exch_arr, price_arr, size_arr, side_arr, is_buy_arr, src_arr;
    ts_arr.reserve(rows.size() * 32);

    for (size_t i = 0; i < rows.size(); ++i) {
        if (i) {
            ts_arr    += ','; sym_arr  += ','; exch_arr   += ',';
            price_arr += ','; size_arr += ','; side_arr   += ',';
            is_buy_arr+= ','; src_arr  += ',';
        }
        ts_arr    += '"' + format_ts(rows[i].ts_micros) + '"';
        sym_arr   += '"' + rows[i].symbol   + '"';
        exch_arr  += '"' + rows[i].exchange + '"';
        price_arr += std::to_string(rows[i].price);
        size_arr  += std::to_string(rows[i].size);
        side_arr  += (rows[i].is_buy ? "\"B\"" : "\"A\"");
        is_buy_arr+= (rows[i].is_buy ? "true"  : "false");
        src_arr   += "\"amp_rithmic\"";
    }

    ts_arr    = '{' + ts_arr    + '}';
    sym_arr   = '{' + sym_arr   + '}';
    exch_arr  = '{' + exch_arr  + '}';
    price_arr = '{' + price_arr + '}';
    size_arr  = '{' + size_arr  + '}';
    side_arr  = '{' + side_arr  + '}';
    is_buy_arr= '{' + is_buy_arr+ '}';
    src_arr   = '{' + src_arr   + '}';

    const char* sql =
        "INSERT INTO ticks (ts_event, symbol, exchange, price, size, side, is_buy, source)"
        " SELECT * FROM unnest("
        "   $1::timestamptz[],"
        "   $2::varchar[],"
        "   $3::varchar[],"
        "   $4::float8[],"
        "   $5::int8[],"
        "   $6::char[],"
        "   $7::bool[],"
        "   $8::varchar[]"
        " ) ON CONFLICT (symbol, exchange, ts_event) DO NOTHING";

    const char* params[8] = {
        ts_arr.c_str(), sym_arr.c_str(), exch_arr.c_str(),
        price_arr.c_str(), size_arr.c_str(),
        side_arr.c_str(), is_buy_arr.c_str(), src_arr.c_str()
    };

    PGresult* res = PQexecParams(conn_, sql, 8, nullptr,
                                 params, nullptr, nullptr, 0);
    pg_check(res, "write ticks");

    const char* tag = PQcmdTuples(res);
    int inserted = tag ? std::atoi(tag) : 0;
    PQclear(res);
    return inserted;
}

// ── read ───────────────────────────────────────────────────────────

int64_t TickDB::row_count() {
    PGresult* res = PQexec(conn_, "SELECT COUNT(*) FROM ticks");
    pg_check(res, "row_count");
    int64_t n = std::stoll(PQgetvalue(res, 0, 0));
    PQclear(res);
    return n;
}

std::optional<double> TickDB::latest_price() {
    PGresult* res = PQexec(conn_,
        "SELECT price FROM ticks ORDER BY ts_event DESC LIMIT 1");
    pg_check(res, "latest_price");
    std::optional<double> val;
    if (PQntuples(res) > 0)
        val = std::stod(PQgetvalue(res, 0, 0));
    PQclear(res);
    return val;
}

DBSummary TickDB::summary() {
    DBSummary s;
    s.connstr    = connstr_;
    s.tick_count = row_count();
    s.price      = latest_price();

    PGresult* res = PQexec(conn_,
        "SELECT MIN(ts_event)::text, MAX(ts_event)::text FROM ticks");
    if (res && PQresultStatus(res) == PGRES_TUPLES_OK && PQntuples(res) > 0) {
        if (!PQgetisnull(res, 0, 0)) s.earliest = PQgetvalue(res, 0, 0);
        if (!PQgetisnull(res, 0, 1)) s.latest   = PQgetvalue(res, 0, 1);
    }
    if (res) PQclear(res);
    return s;
}
