#pragma once
/*  ═══════════════════════════════════════════════════════════════════════════
    orb_db.hpp — PostgreSQL persistence for NQ ORB execution engine

    Schema:
      live_trades    — one row per closed trade (entry, exit, PnL, latency)
      live_sessions  — one row per trading day (ORB levels, risk state)

    Uses libpq (already a dependency in the existing rithmic_engine).
    Not thread-safe — call from a single writer thread.
    ═══════════════════════════════════════════════════════════════════════════ */
#include "orb_config.hpp"
#include "order_manager.hpp"
#include "orb_strategy.hpp"
#include "log.hpp"
#include <libpq-fe.h>
#include <cstdlib>
#include <stdexcept>
#include <string>
#include <ctime>

class OrbDB {
public:
    explicit OrbDB(const std::string& connstr) : connstr_(connstr) {
        conn_ = PQconnectdb(connstr_.c_str());
        if (!conn_ || PQstatus(conn_) != CONNECTION_OK)
            throw std::runtime_error(std::string("OrbDB connect failed: ") +
                                     (conn_ ? PQerrorMessage(conn_) : "null conn"));
        ensure_schema();
    }

    ~OrbDB() {
        if (conn_) PQfinish(conn_);
    }

    OrbDB(const OrbDB&)            = delete;
    OrbDB& operator=(const OrbDB&) = delete;

    bool is_connected() const { return conn_ && PQstatus(conn_) == CONNECTION_OK; }

    void reconnect() {
        PQreset(conn_);
        if (PQstatus(conn_) != CONNECTION_OK)
            LOG("[ORBDB] Reconnect failed: %s", PQerrorMessage(conn_));
        else
            LOG("[ORBDB] Reconnected to PostgreSQL");
    }

    // ── Write a completed trade ───────────────────────────────────────────────
    void write_trade(const Position& pos,
                     const TradeLatency& entry_lat,
                     const TradeLatency& exit_lat,
                     const std::string& trade_date) {
        std::string direction = (pos.direction == OrbSignal::BUY) ? "LONG" : "SHORT";

        // When fill_ts_ns is 0 (latency record not captured), fall back to NOW()
        // to avoid recording 1970-01-01 as the exit timestamp.
        std::string exit_time_sql = (exit_lat.fill_ts_ns > 0)
            ? ("to_timestamp(" + std::to_string(exit_lat.fill_ts_ns / 1000)
               + "::bigint / 1000000.0)")
            : "NOW()";

        char sql[2048];
        std::snprintf(sql, sizeof(sql),
            "INSERT INTO live_trades"
            "(trade_date, direction, entry_time, exit_time, entry_price, exit_price, "
            " sl_price, qty, pnl_points, pnl_usd, exit_reason, "
            " signal_to_submit_us, submit_to_fill_ms, entry_slippage_ticks, exit_slippage_ticks, "
            " mae_pts, mfe_pts, trigger_price, fill_price) "
            "VALUES "
            "('%s', '%s', "
            " to_timestamp(%lld::bigint / 1000000.0),"
            " %s, "
            " %.4f, %.4f, %.4f, %d, %.4f, %.4f, '%s', "
            " %lld, %lld, %d, %d, "
            " %.4f, %.4f, %.4f, %.4f)",
            trade_date.c_str(),
            direction.c_str(),
            (long long)(entry_lat.fill_ts_ns / 1000),   // ns → us
            exit_time_sql.c_str(),
            pos.entry_price, pos.exit_price, pos.sl_price, pos.qty,
            pos.pnl_points, pos.pnl_usd, pos.exit_reason.c_str(),
            (long long)entry_lat.signal_to_submit_us,
            (long long)exit_lat.submit_to_fill_ms,
            entry_lat.slippage_ticks,
            exit_lat.slippage_ticks,
            pos.mae, pos.mfe,
            pos.trigger_price, pos.fill_price_actual
        );

        exec(sql);
        LOG("[ORBDB] Trade written: %s %.4f→%.4f pnl=%.2f mae=%.2f mfe=%.2f slip=%.2fpts",
            direction.c_str(), pos.entry_price, pos.exit_price, pos.pnl_usd,
            pos.mae, pos.mfe,
            pos.trigger_price > 0.0 ? std::abs(pos.fill_price_actual - pos.trigger_price) : 0.0);
    }

    // ── Upsert session row for today ──────────────────────────────────────────
    void upsert_session(const std::string& trade_date,
                        double orb_high,
                        double orb_low,
                        int trades_taken,
                        double daily_pnl,
                        double peak_equity,
                        bool risk_halted,
                        const std::string& halt_reason) {
        char sql[1024];
        std::snprintf(sql, sizeof(sql),
            "INSERT INTO live_sessions"
            "(session_date, orb_high, orb_low, orb_range, trades_taken, "
            " daily_pnl_usd, peak_equity, risk_halted, halt_reason) "
            "VALUES ('%s', %.4f, %.4f, %.4f, %d, %.4f, %.4f, %s, '%s') "
            "ON CONFLICT (session_date) DO UPDATE SET "
            " orb_high=EXCLUDED.orb_high, orb_low=EXCLUDED.orb_low, "
            " orb_range=EXCLUDED.orb_range, trades_taken=EXCLUDED.trades_taken, "
            " daily_pnl_usd=EXCLUDED.daily_pnl_usd, peak_equity=EXCLUDED.peak_equity, "
            " risk_halted=EXCLUDED.risk_halted, halt_reason=EXCLUDED.halt_reason",
            trade_date.c_str(),
            orb_high, orb_low, (orb_high - orb_low),
            trades_taken, daily_pnl, peak_equity,
            risk_halted ? "TRUE" : "FALSE",
            halt_reason.c_str()
        );
        exec(sql);
    }

    // ── Update account equity for today's session (called every 60s) ─────────
    void write_account_equity(const std::string& trade_date, double equity) {
        if (!is_connected()) {
            reconnect();
            if (!is_connected()) return;
        }
        char sql[512];
        std::snprintf(sql, sizeof(sql),
            "INSERT INTO live_sessions(session_date, account_equity) "
            "VALUES ('%s', %.4f) "
            "ON CONFLICT (session_date) DO UPDATE SET account_equity=EXCLUDED.account_equity",
            trade_date.c_str(), equity);
        try {
            exec(sql);
            LOG("[ORBDB] Account equity updated: $%.2f", equity);
        } catch (std::exception& e) {
            LOG("[ORBDB] write_account_equity failed: %s", e.what());
        }
    }

    // ── Upsert live position row ──────────────────────────────────────────────
    // Lightweight single-row UPSERT keyed on session_date.
    // If the libpq call fails, logs a warning and returns without throwing so
    // the trading loop is never disrupted.
    void write_position(const std::string& session_date,
                        const std::string& state,       // FLAT / PENDING_ENTRY / LONG / SHORT / PENDING_EXIT
                        const std::string& direction,   // "LONG" / "SHORT" / ""
                        double entry_price,
                        const std::string& entry_time,  // ISO-8601 UTC or ""
                        double current_price,
                        double unrealized_pts,
                        double unrealized_usd,
                        double sl_price,
                        double orb_high,
                        double orb_low,
                        bool   orb_set,
                        int    trades_today,
                        bool   md_connected,
                        bool   op_connected) {
        if (!is_connected()) {
            reconnect();
            if (!is_connected()) {
                LOG("[ORBDB] write_position skipped — not connected");
                return;
            }
        }

        // Build nullable TEXT literals for optional fields
        std::string dir_lit   = direction.empty()  ? "NULL" : ("'" + direction  + "'");
        std::string etime_lit = entry_time.empty() ? "NULL"
            : ("to_timestamp('" + entry_time + "', 'YYYY-MM-DD HH24:MI:SS') AT TIME ZONE 'UTC'");

        // entry_price / sl_price are 0 when FLAT — store NULL for clarity
        std::string eprice_lit = (entry_price == 0.0) ? "NULL" : std::to_string(entry_price);
        std::string sl_lit     = (sl_price    == 0.0) ? "NULL" : std::to_string(sl_price);

        char sql[2048];
        std::snprintf(sql, sizeof(sql),
            "INSERT INTO live_position "
            "(session_date, state, direction, entry_price, entry_time, "
            " current_price, unrealized_pnl_pts, unrealized_pnl_usd, "
            " sl_price, orb_high, orb_low, orb_set, "
            " trades_today, md_connected, op_connected, last_updated) "
            "VALUES "
            "('%s', '%s', %s, %s, %s, "
            " %.4f, %.4f, %.4f, "
            " %s, %.4f, %.4f, %s, "
            " %d, %s, %s, NOW()) "
            "ON CONFLICT (session_date) DO UPDATE SET "
            " state=EXCLUDED.state, direction=EXCLUDED.direction, "
            " entry_price=EXCLUDED.entry_price, entry_time=EXCLUDED.entry_time, "
            " current_price=EXCLUDED.current_price, "
            " unrealized_pnl_pts=EXCLUDED.unrealized_pnl_pts, "
            " unrealized_pnl_usd=EXCLUDED.unrealized_pnl_usd, "
            " sl_price=EXCLUDED.sl_price, "
            " orb_high=EXCLUDED.orb_high, orb_low=EXCLUDED.orb_low, orb_set=EXCLUDED.orb_set, "
            " trades_today=EXCLUDED.trades_today, "
            " md_connected=EXCLUDED.md_connected, op_connected=EXCLUDED.op_connected, "
            " last_updated=NOW()",
            session_date.c_str(),
            state.c_str(),
            dir_lit.c_str(),
            eprice_lit.c_str(),
            etime_lit.c_str(),
            current_price,
            unrealized_pts,
            unrealized_usd,
            sl_lit.c_str(),
            orb_high,
            orb_low,
            orb_set ? "TRUE" : "FALSE",
            trades_today,
            md_connected ? "TRUE" : "FALSE",
            op_connected ? "TRUE" : "FALSE"
        );

        try {
            exec(sql);
        } catch (std::exception& e) {
            LOG("[ORBDB] write_position failed: %s", e.what());
        }
    }

    // ── Get total historical P&L (for seeding RiskManager on startup) ─────────
    double get_total_pnl() {
        if (!is_connected()) reconnect();
        PGresult* res = PQexec(conn_,
            "SELECT COALESCE(SUM(pnl_usd), 0.0) FROM live_trades");
        if (!res || PQresultStatus(res) != PGRES_TUPLES_OK) {
            LOG("[ORBDB] get_total_pnl failed: %s", PQerrorMessage(conn_));
            if (res) PQclear(res);
            return 0.0;
        }
        double total = (PQntuples(res) > 0) ? std::atof(PQgetvalue(res, 0, 0)) : 0.0;
        PQclear(res);
        LOG("[ORBDB] Historical total_pnl=%.2f", total);
        return total;
    }

    // ── Read last N trade rows (for monitoring) ───────────────────────────────
    void print_recent_trades(int n = 10) {
        char sql[256];
        std::snprintf(sql, sizeof(sql),
            "SELECT trade_date, direction, entry_price, exit_price, pnl_usd, exit_reason "
            "FROM live_trades ORDER BY entry_time DESC LIMIT %d", n);

        PGresult* res = PQexec(conn_, sql);
        if (!res || PQresultStatus(res) != PGRES_TUPLES_OK) {
            LOG("[ORBDB] print_recent_trades failed: %s", PQerrorMessage(conn_));
            if (res) PQclear(res);
            return;
        }
        int rows = PQntuples(res);
        LOG("[ORBDB] Recent trades (%d):", rows);
        for (int i = 0; i < rows; ++i) {
            LOG("  %s %s entry=%.2f exit=%.2f pnl=$%.2f reason=%s",
                PQgetvalue(res, i, 0),
                PQgetvalue(res, i, 1),
                std::atof(PQgetvalue(res, i, 2)),
                std::atof(PQgetvalue(res, i, 3)),
                std::atof(PQgetvalue(res, i, 4)),
                PQgetvalue(res, i, 5));
        }
        PQclear(res);
    }

private:
    void ensure_schema() {
        exec(R"(
            CREATE TABLE IF NOT EXISTS live_trades (
                id                      BIGSERIAL PRIMARY KEY,
                trade_date              DATE NOT NULL,
                direction               TEXT NOT NULL,
                entry_time              TIMESTAMPTZ NOT NULL,
                exit_time               TIMESTAMPTZ,
                entry_price             DOUBLE PRECISION NOT NULL,
                exit_price              DOUBLE PRECISION,
                sl_price                DOUBLE PRECISION NOT NULL,
                qty                     INT NOT NULL,
                pnl_points              DOUBLE PRECISION,
                pnl_usd                 DOUBLE PRECISION,
                exit_reason             TEXT,
                signal_to_submit_us     BIGINT,
                submit_to_fill_ms       BIGINT,
                entry_slippage_ticks    INT,
                exit_slippage_ticks     INT,
                mae_pts                 DOUBLE PRECISION,
                mfe_pts                 DOUBLE PRECISION,
                trigger_price           DOUBLE PRECISION,
                fill_price              DOUBLE PRECISION
            )
        )");

        // Add new columns to existing table if they don't exist yet (idempotent)
        exec("ALTER TABLE live_trades ADD COLUMN IF NOT EXISTS mae_pts        DOUBLE PRECISION");
        exec("ALTER TABLE live_trades ADD COLUMN IF NOT EXISTS mfe_pts        DOUBLE PRECISION");
        exec("ALTER TABLE live_trades ADD COLUMN IF NOT EXISTS trigger_price  DOUBLE PRECISION");
        exec("ALTER TABLE live_trades ADD COLUMN IF NOT EXISTS fill_price     DOUBLE PRECISION");

        exec(R"(
            CREATE TABLE IF NOT EXISTS live_sessions (
                session_date    DATE PRIMARY KEY,
                orb_high        DOUBLE PRECISION,
                orb_low         DOUBLE PRECISION,
                orb_range       DOUBLE PRECISION,
                trades_taken    INT DEFAULT 0,
                daily_pnl_usd   DOUBLE PRECISION DEFAULT 0,
                peak_equity     DOUBLE PRECISION,
                risk_halted     BOOLEAN DEFAULT FALSE,
                halt_reason     TEXT,
                account_equity  DOUBLE PRECISION
            )
        )");

        // Add account_equity column to existing live_sessions if not present
        exec("ALTER TABLE live_sessions ADD COLUMN IF NOT EXISTS account_equity DOUBLE PRECISION");

        exec(R"(
            CREATE TABLE IF NOT EXISTS live_position (
                id                  SERIAL PRIMARY KEY,
                session_date        DATE NOT NULL,
                state               TEXT NOT NULL DEFAULT 'FLAT',
                direction           TEXT,
                entry_price         DOUBLE PRECISION,
                entry_time          TIMESTAMPTZ,
                current_price       DOUBLE PRECISION,
                unrealized_pnl_pts  DOUBLE PRECISION,
                unrealized_pnl_usd  DOUBLE PRECISION,
                sl_price            DOUBLE PRECISION,
                orb_high            DOUBLE PRECISION,
                orb_low             DOUBLE PRECISION,
                orb_set             BOOLEAN DEFAULT FALSE,
                trades_today        INTEGER DEFAULT 0,
                md_connected        BOOLEAN DEFAULT FALSE,
                op_connected        BOOLEAN DEFAULT FALSE,
                last_updated        TIMESTAMPTZ DEFAULT NOW()
            )
        )");

        exec(R"(
            CREATE UNIQUE INDEX IF NOT EXISTS live_position_date_idx
            ON live_position(session_date)
        )");

        LOG("[ORBDB] Schema verified (live_trades: +mae_pts/mfe_pts/trigger_price/fill_price, "
            "live_sessions: +account_equity, live_position)");
    }

    void exec(const char* sql) {
        if (!is_connected()) reconnect();
        PGresult* res = PQexec(conn_, sql);
        if (!res) {
            LOG("[ORBDB] PQexec returned null: %s", PQerrorMessage(conn_));
            return;
        }
        ExecStatusType status = PQresultStatus(res);
        if (status != PGRES_COMMAND_OK && status != PGRES_TUPLES_OK) {
            std::string err = PQerrorMessage(conn_);
            PQclear(res);
            throw std::runtime_error("[ORBDB] SQL error: " + err + " | SQL: " +
                                     std::string(sql).substr(0, 200));
        }
        PQclear(res);
    }

    std::string connstr_;
    PGconn*     conn_ = nullptr;
};
