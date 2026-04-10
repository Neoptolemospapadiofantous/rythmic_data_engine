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

void TickDB::reconnect() {
    PQreset(conn_);
    if (PQstatus(conn_) != CONNECTION_OK)
        throw std::runtime_error(
            std::string("PostgreSQL reconnect failed: ") + PQerrorMessage(conn_));
    if (!read_only_)
        ensure_schema();
    LOG("PostgreSQL reconnected");
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

    // Unique index: (symbol, exchange, ts_event, price, size).
    // IF NOT EXISTS makes this a no-op on every normal startup — only builds
    // the index the very first time (or after explicit DROP).
    // Old narrower legacy index (3-col) dropped once on first run.
    {
        PGresult* r = PQexec(conn_, "DROP INDEX IF EXISTS idx_ticks_ts_unique;");
        if (r) PQclear(r);
    }
    exec(R"(
        CREATE UNIQUE INDEX IF NOT EXISTS idx_ticks_unique
            ON ticks(symbol, exchange, ts_event, price, size);
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

    // ── BBO hypertable — top-of-book snapshots ─────────────────────
    exec(R"(
        CREATE TABLE IF NOT EXISTS bbo (
            ts_event   TIMESTAMPTZ      NOT NULL,
            symbol     VARCHAR(32)      NOT NULL DEFAULT 'NQ',
            exchange   VARCHAR(32)      NOT NULL DEFAULT 'CME',
            bid_price  DOUBLE PRECISION,
            bid_size   INTEGER,
            bid_orders INTEGER,
            ask_price  DOUBLE PRECISION,
            ask_size   INTEGER,
            ask_orders INTEGER,
            source     VARCHAR(32)      DEFAULT 'amp_rithmic'
        );
    )");
    {
        PGresult* res = PQexec(conn_,
            "SELECT create_hypertable('bbo','ts_event',"
            "  if_not_exists => TRUE, migrate_data => TRUE);");
        if (res) PQclear(res);
    }
    {
        PGresult* r = PQexec(conn_,
            "CREATE INDEX IF NOT EXISTS idx_bbo_ts ON bbo(ts_event DESC);");
        if (r) PQclear(r);
    }

    // ── depth_by_order hypertable — L3 MBO event stream ───────────
    exec(R"(
        CREATE TABLE IF NOT EXISTS depth_by_order (
            ts_event          TIMESTAMPTZ NOT NULL,
            source_ns         BIGINT,
            symbol            VARCHAR(32) NOT NULL DEFAULT 'NQ',
            exchange          VARCHAR(32) NOT NULL DEFAULT 'CME',
            sequence_number   BIGINT,
            update_type       SMALLINT,
            transaction_type  SMALLINT,
            depth_price       DOUBLE PRECISION NOT NULL,
            prev_depth_price  DOUBLE PRECISION,
            depth_size        INTEGER,
            exchange_order_id VARCHAR(64),
            source            VARCHAR(32) DEFAULT 'amp_rithmic'
        );
    )");
    {
        PGresult* res = PQexec(conn_,
            "SELECT create_hypertable('depth_by_order','ts_event',"
            "  if_not_exists => TRUE, migrate_data => TRUE);");
        if (res) PQclear(res);
    }
    {
        PGresult* r;
        r = PQexec(conn_,
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_depth_unique"
            " ON depth_by_order(symbol, exchange, source_ns)"
            " WHERE source_ns IS NOT NULL;");
        if (r) PQclear(r);
        r = PQexec(conn_,
            "CREATE INDEX IF NOT EXISTS idx_depth_ts ON depth_by_order(ts_event DESC);");
        if (r) PQclear(r);
    }

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
    // Reserve capacity to avoid reallocation (ts string dominates at ~32 chars each)
    const std::size_t N = rows.size();
    ts_arr.reserve(N * 34);  sym_arr.reserve(N * 8);  exch_arr.reserve(N * 8);
    price_arr.reserve(N * 12); size_arr.reserve(N * 8); side_arr.reserve(N * 6);
    is_buy_arr.reserve(N * 6); src_arr.reserve(N * 16);

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
        " ) ON CONFLICT (symbol, exchange, ts_event, price, size) DO NOTHING";

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

// ── write_bbo ─────────────────────────────────────────────────────

int TickDB::write_bbo(const std::vector<BBORow>& rows) {
    if (rows.empty()) return 0;

    const std::size_t N = rows.size();
    std::string ts_arr, sym_arr, exch_arr;
    std::string bid_p_arr, bid_s_arr, bid_o_arr;
    std::string ask_p_arr, ask_s_arr, ask_o_arr;
    std::string src_arr;

    ts_arr.reserve(N * 34); sym_arr.reserve(N * 8); exch_arr.reserve(N * 8);
    bid_p_arr.reserve(N * 12); bid_s_arr.reserve(N * 8); bid_o_arr.reserve(N * 8);
    ask_p_arr.reserve(N * 12); ask_s_arr.reserve(N * 8); ask_o_arr.reserve(N * 8);
    src_arr.reserve(N * 16);

    for (std::size_t i = 0; i < N; ++i) {
        if (i) {
            ts_arr   += ','; sym_arr  += ','; exch_arr += ',';
            bid_p_arr+= ','; bid_s_arr+= ','; bid_o_arr+= ',';
            ask_p_arr+= ','; ask_s_arr+= ','; ask_o_arr+= ',';
            src_arr  += ',';
        }
        ts_arr    += '"' + format_ts(rows[i].ts_micros)  + '"';
        sym_arr   += '"' + rows[i].symbol   + '"';
        exch_arr  += '"' + rows[i].exchange + '"';
        bid_p_arr += std::to_string(rows[i].bid_price);
        bid_s_arr += std::to_string(rows[i].bid_size);
        bid_o_arr += std::to_string(rows[i].bid_orders);
        ask_p_arr += std::to_string(rows[i].ask_price);
        ask_s_arr += std::to_string(rows[i].ask_size);
        ask_o_arr += std::to_string(rows[i].ask_orders);
        src_arr   += "\"amp_rithmic\"";
    }

    ts_arr    = '{' + ts_arr    + '}';
    sym_arr   = '{' + sym_arr   + '}';
    exch_arr  = '{' + exch_arr  + '}';
    bid_p_arr = '{' + bid_p_arr + '}';
    bid_s_arr = '{' + bid_s_arr + '}';
    bid_o_arr = '{' + bid_o_arr + '}';
    ask_p_arr = '{' + ask_p_arr + '}';
    ask_s_arr = '{' + ask_s_arr + '}';
    ask_o_arr = '{' + ask_o_arr + '}';
    src_arr   = '{' + src_arr   + '}';

    const char* sql =
        "INSERT INTO bbo"
        " (ts_event, symbol, exchange, bid_price, bid_size, bid_orders,"
        "  ask_price, ask_size, ask_orders, source)"
        " SELECT * FROM unnest("
        "   $1::timestamptz[],"
        "   $2::varchar[],"
        "   $3::varchar[],"
        "   $4::float8[],"
        "   $5::int4[],"
        "   $6::int4[],"
        "   $7::float8[],"
        "   $8::int4[],"
        "   $9::int4[],"
        "   $10::varchar[]"
        " ) ON CONFLICT DO NOTHING";

    const char* params[10] = {
        ts_arr.c_str(), sym_arr.c_str(), exch_arr.c_str(),
        bid_p_arr.c_str(), bid_s_arr.c_str(), bid_o_arr.c_str(),
        ask_p_arr.c_str(), ask_s_arr.c_str(), ask_o_arr.c_str(),
        src_arr.c_str()
    };

    PGresult* res = PQexecParams(conn_, sql, 10, nullptr,
                                 params, nullptr, nullptr, 0);
    pg_check(res, "write bbo");

    const char* tag = PQcmdTuples(res);
    int inserted = tag ? std::atoi(tag) : 0;
    PQclear(res);
    return inserted;
}

// ── write_depth ────────────────────────────────────────────────────

int TickDB::write_depth(const std::vector<DepthRow>& rows) {
    if (rows.empty()) return 0;

    const std::size_t N = rows.size();
    std::string ts_arr, src_ns_arr, sym_arr, exch_arr;
    std::string seq_arr, upd_arr, txn_arr;
    std::string dp_arr, pdp_arr, ds_arr, eoid_arr, src_arr;

    ts_arr.reserve(N * 34); sym_arr.reserve(N * 8); exch_arr.reserve(N * 8);

    for (std::size_t i = 0; i < N; ++i) {
        if (i) {
            ts_arr    += ','; src_ns_arr+= ','; sym_arr  += ','; exch_arr += ',';
            seq_arr   += ','; upd_arr   += ','; txn_arr  += ',';
            dp_arr    += ','; pdp_arr   += ','; ds_arr   += ',';
            eoid_arr  += ','; src_arr   += ',';
        }
        ts_arr     += '"' + format_ts(rows[i].ts_micros) + '"';
        src_ns_arr += std::to_string(rows[i].source_ns);
        sym_arr    += '"' + rows[i].symbol   + '"';
        exch_arr   += '"' + rows[i].exchange + '"';
        seq_arr    += std::to_string(rows[i].sequence_number);
        upd_arr    += std::to_string(static_cast<int>(rows[i].update_type));
        txn_arr    += std::to_string(static_cast<int>(rows[i].transaction_type));
        dp_arr     += std::to_string(rows[i].depth_price);
        pdp_arr    += std::to_string(rows[i].prev_depth_price);
        ds_arr     += std::to_string(rows[i].depth_size);
        eoid_arr   += '"' + rows[i].exchange_order_id + '"';
        src_arr    += "\"amp_rithmic\"";
    }

    ts_arr     = '{' + ts_arr     + '}';
    src_ns_arr = '{' + src_ns_arr + '}';
    sym_arr    = '{' + sym_arr    + '}';
    exch_arr   = '{' + exch_arr   + '}';
    seq_arr    = '{' + seq_arr    + '}';
    upd_arr    = '{' + upd_arr    + '}';
    txn_arr    = '{' + txn_arr    + '}';
    dp_arr     = '{' + dp_arr     + '}';
    pdp_arr    = '{' + pdp_arr    + '}';
    ds_arr     = '{' + ds_arr     + '}';
    eoid_arr   = '{' + eoid_arr   + '}';
    src_arr    = '{' + src_arr    + '}';

    const char* sql =
        "INSERT INTO depth_by_order"
        " (ts_event, source_ns, symbol, exchange,"
        "  sequence_number, update_type, transaction_type,"
        "  depth_price, prev_depth_price, depth_size,"
        "  exchange_order_id, source)"
        " SELECT * FROM unnest("
        "   $1::timestamptz[],"
        "   $2::int8[],"
        "   $3::varchar[],"
        "   $4::varchar[],"
        "   $5::int8[],"
        "   $6::int2[],"
        "   $7::int2[],"
        "   $8::float8[],"
        "   $9::float8[],"
        "   $10::int4[],"
        "   $11::varchar[],"
        "   $12::varchar[]"
        " ) ON CONFLICT (symbol, exchange, source_ns)"
        "   WHERE source_ns IS NOT NULL DO NOTHING";

    const char* params[12] = {
        ts_arr.c_str(), src_ns_arr.c_str(), sym_arr.c_str(), exch_arr.c_str(),
        seq_arr.c_str(), upd_arr.c_str(), txn_arr.c_str(),
        dp_arr.c_str(), pdp_arr.c_str(), ds_arr.c_str(),
        eoid_arr.c_str(), src_arr.c_str()
    };

    PGresult* res = PQexecParams(conn_, sql, 12, nullptr,
                                 params, nullptr, nullptr, 0);
    pg_check(res, "write depth");

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
