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
    {
        // Unique index required for ON CONFLICT DO NOTHING to actually dedup
        PGresult* r = PQexec(conn_,
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_bbo_unique"
            " ON bbo(symbol, exchange, ts_event);");
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
            source    VARCHAR(32)  DEFAULT 'engine',
            event     VARCHAR(64)  NOT NULL,
            severity  VARCHAR(8)   DEFAULT 'INFO',
            details   TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_audit_ts ON audit_log(ts);
        CREATE INDEX IF NOT EXISTS idx_audit_sev ON audit_log(severity, ts DESC);
    )");
    // Add source column if missing (safe migration for existing installs)
    {
        PGresult* r = PQexec(conn_,
            "ALTER TABLE audit_log ADD COLUMN IF NOT EXISTS source VARCHAR(32) DEFAULT 'engine';");
        if (r) PQclear(r);
    }

    // ── Quality metrics — time-series health snapshots ─────────────
    exec(R"(
        CREATE TABLE IF NOT EXISTS quality_metrics (
            id           BIGSERIAL    PRIMARY KEY,
            ts           TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
            metric       VARCHAR(64)  NOT NULL,
            value        DOUBLE PRECISION NOT NULL,
            labels_json  TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_qm_metric_ts
            ON quality_metrics(metric, ts DESC);
    )");

    // ── Session tracking — supports both collector and trading sessions
    // Schema aligned with bot's SQLite sessions table (python/db/models.py)
    exec(R"(
        CREATE TABLE IF NOT EXISTS sessions (
            id             BIGSERIAL    PRIMARY KEY,
            strategy       VARCHAR(32)  NOT NULL DEFAULT 'micro_orb',
            mode           VARCHAR(16)  NOT NULL DEFAULT 'collect',
            started_at     TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
            ended_at       TIMESTAMPTZ,
            params_json    TEXT,
            report_json    TEXT,
            summary_json   TEXT,
            total_pnl      DOUBLE PRECISION DEFAULT 0.0,
            total_trades   INTEGER      DEFAULT 0,
            win_rate       DOUBLE PRECISION DEFAULT 0.0,
            sharpe         DOUBLE PRECISION,
            max_drawdown   DOUBLE PRECISION,
            profit_factor  DOUBLE PRECISION,
            tick_count     BIGINT       DEFAULT 0,
            bbo_count      BIGINT       DEFAULT 0,
            depth_count    BIGINT       DEFAULT 0,
            rejected_count BIGINT       DEFAULT 0,
            gap_count      BIGINT       DEFAULT 0,
            alert_count    BIGINT       DEFAULT 0,
            notes          TEXT,
            created_at     TIMESTAMPTZ  NOT NULL DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS idx_sessions_started
            ON sessions(started_at DESC);
        CREATE INDEX IF NOT EXISTS idx_sessions_strategy
            ON sessions(strategy, mode);
    )");
    // Add columns that may be missing on existing installs
    {
        PGresult* r;
        r = PQexec(conn_, "ALTER TABLE sessions ADD COLUMN IF NOT EXISTS strategy      VARCHAR(32) NOT NULL DEFAULT 'micro_orb';"); if (r) PQclear(r);
        r = PQexec(conn_, "ALTER TABLE sessions ADD COLUMN IF NOT EXISTS report_json   TEXT;");                                     if (r) PQclear(r);
        r = PQexec(conn_, "ALTER TABLE sessions ADD COLUMN IF NOT EXISTS total_pnl     DOUBLE PRECISION DEFAULT 0.0;");             if (r) PQclear(r);
        r = PQexec(conn_, "ALTER TABLE sessions ADD COLUMN IF NOT EXISTS total_trades  INTEGER DEFAULT 0;");                        if (r) PQclear(r);
        r = PQexec(conn_, "ALTER TABLE sessions ADD COLUMN IF NOT EXISTS win_rate      DOUBLE PRECISION DEFAULT 0.0;");             if (r) PQclear(r);
        r = PQexec(conn_, "ALTER TABLE sessions ADD COLUMN IF NOT EXISTS sharpe        DOUBLE PRECISION;");                         if (r) PQclear(r);
        r = PQexec(conn_, "ALTER TABLE sessions ADD COLUMN IF NOT EXISTS max_drawdown  DOUBLE PRECISION;");                         if (r) PQclear(r);
        r = PQexec(conn_, "ALTER TABLE sessions ADD COLUMN IF NOT EXISTS profit_factor DOUBLE PRECISION;");                         if (r) PQclear(r);
        r = PQexec(conn_, "ALTER TABLE sessions ADD COLUMN IF NOT EXISTS created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW();");        if (r) PQclear(r);
    }

    // ── Sentinel alerts — structured anomaly events ────────────────
    exec(R"(
        CREATE TABLE IF NOT EXISTS sentinel_alerts (
            id           BIGSERIAL    PRIMARY KEY,
            ts           TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
            session_id   BIGINT       REFERENCES sessions(id),
            check_name   VARCHAR(64)  NOT NULL,
            severity     VARCHAR(8)   NOT NULL DEFAULT 'WARN',
            message      TEXT,
            value        DOUBLE PRECISION
        );
        CREATE INDEX IF NOT EXISTS idx_sentinel_ts
            ON sentinel_alerts(ts DESC);
        CREATE INDEX IF NOT EXISTS idx_sentinel_check
            ON sentinel_alerts(check_name, ts DESC);
    )");

    // ── Loss limits — bot writes limits, engine reads them ─────────
    exec(R"(
        CREATE TABLE IF NOT EXISTS loss_limits (
            id                BIGSERIAL    PRIMARY KEY,
            symbol            VARCHAR(32)  NOT NULL DEFAULT 'NQ',
            daily_loss_limit  DOUBLE PRECISION DEFAULT -1000.0,
            weekly_loss_limit DOUBLE PRECISION DEFAULT -1800.0,
            max_drawdown      DOUBLE PRECISION DEFAULT 0.0,
            max_daily_trades  INTEGER          DEFAULT 3,
            active            BOOLEAN          DEFAULT TRUE,
            updated_at        TIMESTAMPTZ      NOT NULL DEFAULT NOW(),
            notes             TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_loss_limits_symbol
            ON loss_limits(symbol, active, updated_at DESC);
    )");

    // ── Trade log — bot writes trades for engine to audit ──────────
    // Schema aligned with bot's SQLite trades table (python/db/models.py)
    exec(R"(
        CREATE TABLE IF NOT EXISTS trade_log (
            id              BIGSERIAL    PRIMARY KEY,
            session_id      BIGINT,
            strategy        VARCHAR(32)  NOT NULL DEFAULT 'micro_orb',
            mode            VARCHAR(16)  NOT NULL DEFAULT 'backtest',
            trade_date      DATE         NOT NULL,
            entry_time      TIMESTAMPTZ  NOT NULL,
            exit_time       TIMESTAMPTZ,
            symbol          VARCHAR(32)  NOT NULL DEFAULT 'NQ',
            direction       VARCHAR(8)   NOT NULL DEFAULT 'long',
            entry_price     DOUBLE PRECISION NOT NULL,
            exit_price      DOUBLE PRECISION,
            quantity        INTEGER      NOT NULL DEFAULT 1,
            gross_pnl       DOUBLE PRECISION DEFAULT 0.0,
            commission      DOUBLE PRECISION DEFAULT 4.0,
            slippage        DOUBLE PRECISION DEFAULT 0.0,
            net_pnl         DOUBLE PRECISION DEFAULT 0.0,
            points          DOUBLE PRECISION DEFAULT 0.0,
            ticks           DOUBLE PRECISION DEFAULT 0.0,
            exit_reason     VARCHAR(32),
            params_json     TEXT,
            features_json   TEXT,
            created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS idx_trade_log_date
            ON trade_log(trade_date DESC);
        CREATE INDEX IF NOT EXISTS idx_trade_log_strategy
            ON trade_log(strategy, mode);
        CREATE INDEX IF NOT EXISTS idx_trade_log_entry_time
            ON trade_log(entry_time DESC);
        CREATE UNIQUE INDEX IF NOT EXISTS idx_trade_log_entry
            ON trade_log(session_id, entry_time);
    )");
    // Add columns that may be missing on existing installs
    {
        PGresult* r;
        r = PQexec(conn_, "ALTER TABLE trade_log ADD COLUMN IF NOT EXISTS strategy   VARCHAR(32) NOT NULL DEFAULT 'micro_orb';"); if (r) PQclear(r);
        r = PQexec(conn_, "ALTER TABLE trade_log ADD COLUMN IF NOT EXISTS mode       VARCHAR(16) NOT NULL DEFAULT 'backtest';");  if (r) PQclear(r);
        r = PQexec(conn_, "ALTER TABLE trade_log ADD COLUMN IF NOT EXISTS slippage   DOUBLE PRECISION DEFAULT 0.0;");            if (r) PQclear(r);
        r = PQexec(conn_, "ALTER TABLE trade_log ADD COLUMN IF NOT EXISTS points     DOUBLE PRECISION DEFAULT 0.0;");            if (r) PQclear(r);
        r = PQexec(conn_, "ALTER TABLE trade_log ADD COLUMN IF NOT EXISTS ticks      DOUBLE PRECISION DEFAULT 0.0;");            if (r) PQclear(r);
        r = PQexec(conn_, "ALTER TABLE trade_log ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT NOW();");       if (r) PQclear(r);
    }

    // ── Daily stats — bot writes daily P&L aggregates ───────────────
    // Schema aligned with bot's SQLite daily_stats table
    exec(R"(
        CREATE TABLE IF NOT EXISTS daily_stats (
            id              BIGSERIAL    PRIMARY KEY,
            strategy        VARCHAR(32)  NOT NULL DEFAULT 'micro_orb',
            mode            VARCHAR(16)  NOT NULL DEFAULT 'backtest',
            stat_date       DATE         NOT NULL,
            total_pnl       DOUBLE PRECISION DEFAULT 0.0,
            trade_count     INTEGER      DEFAULT 0,
            win_count       INTEGER      DEFAULT 0,
            loss_count      INTEGER      DEFAULT 0,
            win_rate        DOUBLE PRECISION DEFAULT 0.0,
            avg_win         DOUBLE PRECISION DEFAULT 0.0,
            avg_loss        DOUBLE PRECISION DEFAULT 0.0,
            max_win         DOUBLE PRECISION DEFAULT 0.0,
            max_loss        DOUBLE PRECISION DEFAULT 0.0,
            profit_factor   DOUBLE PRECISION,
            max_drawdown    DOUBLE PRECISION DEFAULT 0.0,
            session_id      BIGINT,
            created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
            UNIQUE(strategy, mode, stat_date, session_id)
        );
        CREATE INDEX IF NOT EXISTS idx_daily_date
            ON daily_stats(stat_date DESC);
        CREATE INDEX IF NOT EXISTS idx_daily_strategy
            ON daily_stats(strategy, mode);
    )");

    // ── Orders — bot writes order lifecycle for live/paper trading ──
    // Schema aligned with bot's SQLite orders table
    exec(R"(
        CREATE TABLE IF NOT EXISTS orders (
            id              BIGSERIAL    PRIMARY KEY,
            trade_id        BIGINT,
            session_id      BIGINT,
            order_type      VARCHAR(16)  NOT NULL DEFAULT 'market',
            side            VARCHAR(8)   NOT NULL,
            quantity        INTEGER      NOT NULL DEFAULT 1,
            price           DOUBLE PRECISION,
            fill_price      DOUBLE PRECISION,
            status          VARCHAR(16)  NOT NULL DEFAULT 'pending',
            broker_order_id VARCHAR(64),
            submitted_at    TIMESTAMPTZ,
            filled_at       TIMESTAMPTZ,
            cancelled_at    TIMESTAMPTZ,
            error_msg       TEXT,
            created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS idx_orders_trade
            ON orders(trade_id);
        CREATE INDEX IF NOT EXISTS idx_orders_status
            ON orders(status);
        CREATE INDEX IF NOT EXISTS idx_orders_session
            ON orders(session_id);
    )");

    // ── Gate results — records pipeline gate pass/fail ──────────────
    exec(R"(
        CREATE TABLE IF NOT EXISTS gate_results (
            id            BIGSERIAL    PRIMARY KEY,
            ts            TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
            gate_name     VARCHAR(64)  NOT NULL,
            status        VARCHAR(16)  NOT NULL DEFAULT 'pending',
            threshold     DOUBLE PRECISION,
            actual_value  DOUBLE PRECISION,
            details_json  TEXT,
            session_id    BIGINT
        );
        CREATE INDEX IF NOT EXISTS idx_gate_results_ts
            ON gate_results(ts DESC);
        CREATE INDEX IF NOT EXISTS idx_gate_results_name
            ON gate_results(gate_name, ts DESC);
    )");

    // ── Gate-ready SQL views (non-fatal — require sufficient data) ──
    ts_exec(R"(
        CREATE OR REPLACE VIEW v_daily_pnl AS
        SELECT
            trade_date,
            symbol,
            COUNT(*)                   AS trade_count,
            SUM(net_pnl)               AS daily_pnl,
            SUM(CASE WHEN net_pnl > 0 THEN 1 ELSE 0 END) AS wins,
            SUM(CASE WHEN net_pnl < 0 THEN 1 ELSE 0 END) AS losses,
            MAX(net_pnl)               AS best_trade,
            MIN(net_pnl)               AS worst_trade,
            SUM(SUM(net_pnl)) OVER (ORDER BY trade_date) AS cumulative_pnl
        FROM trade_log
        GROUP BY trade_date, symbol
        ORDER BY trade_date;
    )");

    ts_exec(R"(
        CREATE OR REPLACE VIEW v_equity_curve AS
        SELECT
            entry_time,
            net_pnl,
            SUM(net_pnl) OVER (ORDER BY entry_time) AS equity
        FROM trade_log
        ORDER BY entry_time;
    )");

    ts_exec(R"(
        CREATE OR REPLACE VIEW v_loss_limit_status AS
        SELECT
            ll.symbol,
            ll.daily_loss_limit,
            ll.weekly_loss_limit,
            ll.max_drawdown,
            ll.max_daily_trades,
            COALESCE(d.daily_pnl, 0)   AS today_pnl,
            COALESCE(d.trade_count, 0)  AS today_trades,
            COALESCE(w.weekly_pnl, 0)   AS week_pnl,
            CASE WHEN COALESCE(d.daily_pnl, 0)  <= ll.daily_loss_limit  THEN TRUE ELSE FALSE END AS daily_breached,
            CASE WHEN COALESCE(w.weekly_pnl, 0)  <= ll.weekly_loss_limit THEN TRUE ELSE FALSE END AS weekly_breached,
            CASE WHEN COALESCE(d.trade_count, 0) >= ll.max_daily_trades  THEN TRUE ELSE FALSE END AS trade_limit_hit
        FROM loss_limits ll
        LEFT JOIN (
            SELECT symbol, SUM(net_pnl) AS daily_pnl, COUNT(*) AS trade_count
            FROM trade_log
            WHERE trade_date = CURRENT_DATE
            GROUP BY symbol
        ) d ON d.symbol = ll.symbol
        LEFT JOIN (
            SELECT symbol, SUM(net_pnl) AS weekly_pnl
            FROM trade_log
            WHERE trade_date >= date_trunc('week', CURRENT_DATE)
            GROUP BY symbol
        ) w ON w.symbol = ll.symbol
        WHERE ll.active = TRUE;
    )");

    // ── Feature store views (require TimescaleDB bars) ──────────────
    ts_exec(R"(
        CREATE MATERIALIZED VIEW IF NOT EXISTS mv_bar_features_1min AS
        SELECT
            ts,
            open, high, low, close, volume,
            close - open                                         AS body,
            high - low                                           AS range,
            CASE WHEN LAG(close) OVER (ORDER BY ts) > 0
                 THEN (close - LAG(close) OVER (ORDER BY ts))
                      / LAG(close) OVER (ORDER BY ts) * 100.0
                 ELSE 0 END                                      AS return_pct,
            AVG(volume) OVER (ORDER BY ts ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS vol_ma20,
            STDDEV(close) OVER (ORDER BY ts ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS close_std20,
            AVG(close)  OVER (ORDER BY ts ROWS BETWEEN 19 PRECEDING AND CURRENT ROW)  AS close_ma20,
            AVG(close)  OVER (ORDER BY ts ROWS BETWEEN 59 PRECEDING AND CURRENT ROW)  AS close_ma60,
            (high - low) / NULLIF(AVG(high - low) OVER (ORDER BY ts ROWS BETWEEN 19 PRECEDING AND CURRENT ROW), 0) AS range_ratio
        FROM bars_1min
        ORDER BY ts
        WITH NO DATA;
    )");

    ts_exec(R"(
        CREATE OR REPLACE VIEW v_regime_labels AS
        SELECT
            ts,
            close,
            close_std20,
            close_ma20,
            NTILE(5) OVER (ORDER BY close_std20)  AS vol_quintile,
            CASE
                WHEN close > close_ma60 AND close > close_ma20 THEN 'uptrend'
                WHEN close < close_ma60 AND close < close_ma20 THEN 'downtrend'
                WHEN close_std20 > close_ma20 * 0.005          THEN 'volatile'
                ELSE 'choppy'
            END AS regime
        FROM mv_bar_features_1min
        WHERE close_ma60 IS NOT NULL;
    )");

    ts_exec(R"(
        CREATE OR REPLACE VIEW v_walk_forward_windows AS
        SELECT
            w.window_id,
            w.train_start,
            w.train_end,
            w.test_start,
            w.test_end
        FROM (
            SELECT
                ROW_NUMBER() OVER () AS window_id,
                ts AS train_start,
                ts + INTERVAL '90 days' AS train_end,
                ts + INTERVAL '90 days' AS test_start,
                ts + INTERVAL '120 days' AS test_end
            FROM generate_series(
                (SELECT MIN(ts) FROM bars_1min),
                (SELECT MAX(ts) - INTERVAL '120 days' FROM bars_1min),
                INTERVAL '30 days'
            ) AS ts
        ) w;
    )");

    ts_exec(R"(
        CREATE OR REPLACE VIEW v_oot_partition AS
        SELECT
            (SELECT MAX(ts) - INTERVAL '30 days' FROM bars_1min) AS holdout_start,
            (SELECT MAX(ts) FROM bars_1min)                       AS holdout_end,
            (SELECT MIN(ts) FROM bars_1min)                       AS data_start,
            (SELECT MAX(ts) - INTERVAL '30 days' FROM bars_1min) AS pipeline_end;
    )");

    LOG("Schema ready (TimescaleDB hypertable + continuous aggregates + lifecycle tables)");
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
        " ) ON CONFLICT (symbol, exchange, ts_event) DO NOTHING";

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

// ── session lifecycle ──────────────────────────────────────────────

int64_t TickDB::start_session(const std::string& mode,
                               const std::string& params_json) {
    std::string sql =
        "INSERT INTO sessions (mode, params_json) VALUES ($1, $2) RETURNING id";
    const char* params[2] = { mode.c_str(),
                              params_json.empty() ? nullptr : params_json.c_str() };
    int lengths[2] = { 0, 0 };
    int formats[2] = { 0, 0 };

    PGresult* res = PQexecParams(conn_, sql.c_str(), 2, nullptr,
                                  params, lengths, formats, 0);
    pg_check(res, "start_session");
    int64_t id = std::stoll(PQgetvalue(res, 0, 0));
    PQclear(res);
    return id;
}

void TickDB::end_session(int64_t session_id, int64_t ticks, int64_t bbo,
                          int64_t depth, int64_t rejected, int64_t gaps,
                          int64_t alerts, const std::string& summary_json) {
    std::string sql =
        "UPDATE sessions SET ended_at = NOW(),"
        " tick_count = $1, bbo_count = $2, depth_count = $3,"
        " rejected_count = $4, gap_count = $5, alert_count = $6,"
        " summary_json = $7"
        " WHERE id = $8";

    std::string s_ticks    = std::to_string(ticks);
    std::string s_bbo      = std::to_string(bbo);
    std::string s_depth    = std::to_string(depth);
    std::string s_rejected = std::to_string(rejected);
    std::string s_gaps     = std::to_string(gaps);
    std::string s_alerts   = std::to_string(alerts);
    std::string s_id       = std::to_string(session_id);

    const char* params[8] = {
        s_ticks.c_str(), s_bbo.c_str(), s_depth.c_str(),
        s_rejected.c_str(), s_gaps.c_str(), s_alerts.c_str(),
        summary_json.empty() ? nullptr : summary_json.c_str(),
        s_id.c_str()
    };

    PGresult* res = PQexecParams(conn_, sql.c_str(), 8, nullptr,
                                  params, nullptr, nullptr, 0);
    pg_check(res, "end_session");
    PQclear(res);
}

// ── quality metrics ───────────────────────────────────────────────

void TickDB::write_metric(const QualityMetric& m) {
    const char* sql =
        "INSERT INTO quality_metrics (metric, value, labels_json)"
        " VALUES ($1, $2, $3)";
    std::string s_val = std::to_string(m.value);
    const char* params[3] = {
        m.metric.c_str(), s_val.c_str(),
        m.labels_json.empty() ? nullptr : m.labels_json.c_str()
    };
    PGresult* res = PQexecParams(conn_, sql, 3, nullptr,
                                  params, nullptr, nullptr, 0);
    if (res) {
        if (PQresultStatus(res) != PGRES_COMMAND_OK)
            LOG("write_metric error: %s", PQresultErrorMessage(res));
        PQclear(res);
    }
}

void TickDB::write_metrics(const std::vector<QualityMetric>& ms) {
    for (auto& m : ms) write_metric(m);
}

// ── sentinel alerts ───────────────────────────────────────────────

void TickDB::write_sentinel_alerts(const std::vector<SentinelAlertRow>& alerts) {
    if (alerts.empty()) return;

    std::string sql =
        "INSERT INTO sentinel_alerts (session_id, check_name, severity, message, value) VALUES ";
    for (size_t i = 0; i < alerts.size(); ++i) {
        if (i) sql += ',';
        auto& a = alerts[i];
        sql += "(" + std::to_string(a.session_id) + ",'" + a.check_name + "','"
             + a.severity + "','" + a.message + "'," + std::to_string(a.value) + ")";
    }

    PGresult* res = PQexec(conn_, sql.c_str());
    if (res) {
        if (PQresultStatus(res) != PGRES_COMMAND_OK)
            LOG("write_sentinel_alerts error: %s", PQresultErrorMessage(res));
        PQclear(res);
    }
}

// ── gate results ──────────────────────────────────────────────────

void TickDB::write_gate_result(const GateResult& g) {
    const char* sql =
        "INSERT INTO gate_results (gate_name, status, threshold, actual_value, details_json, session_id)"
        " VALUES ($1, $2, $3, $4, $5, $6)";
    std::string s_thresh = std::to_string(g.threshold);
    std::string s_actual = std::to_string(g.actual);
    std::string s_sid    = std::to_string(g.session_id);
    const char* params[6] = {
        g.gate_name.c_str(), g.status.c_str(),
        s_thresh.c_str(), s_actual.c_str(),
        g.details_json.empty() ? nullptr : g.details_json.c_str(),
        s_sid.c_str()
    };
    PGresult* res = PQexecParams(conn_, sql, 6, nullptr,
                                  params, nullptr, nullptr, 0);
    if (res) {
        if (PQresultStatus(res) != PGRES_COMMAND_OK)
            LOG("write_gate_result error: %s", PQresultErrorMessage(res));
        PQclear(res);
    }
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
