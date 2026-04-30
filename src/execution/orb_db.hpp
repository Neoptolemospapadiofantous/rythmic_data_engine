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
    explicit OrbDB(const std::string& connstr,
                   const std::string& instrument = "MNQ")
        : connstr_(connstr), instrument_(instrument) {
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

        // Convert entry timestamp ns → us as decimal string for to_timestamp()
        char p4[32];
        snprintf(p4, sizeof(p4), "%lld", (long long)(entry_lat.fill_ts_ns / 1000));

        // Exit timestamp: NULL when not captured; SQL uses COALESCE(..., NOW())
        char p5_buf[32];
        const char* p5 = nullptr;
        if (exit_lat.fill_ts_ns > 0) {
            snprintf(p5_buf, sizeof(p5_buf), "%lld", (long long)(exit_lat.fill_ts_ns / 1000));
            p5 = p5_buf;
        }

        char ep[32], xp[32], sl[32], qty[16];
        char ppts[32], pusd[32];
        char ssus[32], sfms[32], eslip[16], xslip[16];
        char mae[32], mfe[32], trig[32], fill[32];

        snprintf(ep,    sizeof(ep),    "%.4f", pos.entry_price);
        snprintf(xp,    sizeof(xp),    "%.4f", pos.exit_price);
        snprintf(sl,    sizeof(sl),    "%.4f", pos.sl_price);
        snprintf(qty,   sizeof(qty),   "%d",   pos.qty);
        snprintf(ppts,  sizeof(ppts),  "%.4f", pos.pnl_points);
        snprintf(pusd,  sizeof(pusd),  "%.4f", pos.pnl_usd);
        snprintf(ssus,  sizeof(ssus),  "%lld", (long long)entry_lat.signal_to_submit_us);
        snprintf(sfms,  sizeof(sfms),  "%lld", (long long)exit_lat.submit_to_fill_ms);
        snprintf(eslip, sizeof(eslip), "%d",   entry_lat.slippage_ticks);
        snprintf(xslip, sizeof(xslip), "%d",   exit_lat.slippage_ticks);
        snprintf(mae,   sizeof(mae),   "%.4f", pos.mae);
        snprintf(mfe,   sizeof(mfe),   "%.4f", pos.mfe);
        snprintf(trig,  sizeof(trig),  "%.4f", pos.trigger_price);
        snprintf(fill,  sizeof(fill),  "%.4f", pos.fill_price_actual);

        const char* params[20] = {
            instrument_.c_str(),       // $1  instrument
            trade_date.c_str(),        // $2  trade_date
            direction.c_str(),         // $3  direction
            p4,                        // $4  entry_time_us
            p5,                        // $5  exit_time_us (NULL → COALESCE to NOW())
            ep,                        // $6  entry_price
            xp,                        // $7  exit_price
            sl,                        // $8  sl_price
            qty,                       // $9  qty
            ppts,                      // $10 pnl_points
            pusd,                      // $11 pnl_usd
            pos.exit_reason.c_str(),   // $12 exit_reason
            ssus,                      // $13 signal_to_submit_us
            sfms,                      // $14 submit_to_fill_ms
            eslip,                     // $15 entry_slippage_ticks
            xslip,                     // $16 exit_slippage_ticks
            mae,                       // $17 mae_pts
            mfe,                       // $18 mfe_pts
            trig,                      // $19 trigger_price
            fill                       // $20 fill_price
        };

        exec_params(
            "INSERT INTO live_trades"
            "(instrument, trade_date, direction, entry_time, exit_time, entry_price, exit_price,"
            " sl_price, qty, pnl_points, pnl_usd, exit_reason,"
            " signal_to_submit_us, submit_to_fill_ms, entry_slippage_ticks, exit_slippage_ticks,"
            " mae_pts, mfe_pts, trigger_price, fill_price)"
            " VALUES"
            "($1, $2::date, $3,"
            " to_timestamp($4::bigint / 1000000.0),"
            " COALESCE(to_timestamp($5::bigint / 1000000.0), NOW()),"
            " $6::double precision, $7::double precision, $8::double precision, $9::int,"
            " $10::double precision, $11::double precision, $12,"
            " $13::bigint, $14::bigint, $15::int, $16::int,"
            " $17::double precision, $18::double precision,"
            " $19::double precision, $20::double precision)",
            20, params);

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
        char p4[32], p5[32], p6[32], p7[16], p8[32], p9[32];
        snprintf(p4, sizeof(p4), "%.4f", orb_high);
        snprintf(p5, sizeof(p5), "%.4f", orb_low);
        snprintf(p6, sizeof(p6), "%.4f", orb_high - orb_low);
        snprintf(p7, sizeof(p7), "%d",   trades_taken);
        snprintf(p8, sizeof(p8), "%.4f", daily_pnl);
        snprintf(p9, sizeof(p9), "%.4f", peak_equity);
        const char* p10 = risk_halted ? "t" : "f";

        const char* params[11] = {
            trade_date.c_str(),    // $1  session_date
            instrument_.c_str(),   // $2  instrument
            strategy_.c_str(),     // $3  strategy
            p4,                    // $4  orb_high
            p5,                    // $5  orb_low
            p6,                    // $6  orb_range
            p7,                    // $7  trades_taken
            p8,                    // $8  daily_pnl_usd
            p9,                    // $9  peak_equity
            p10,                   // $10 risk_halted
            halt_reason.c_str()    // $11 halt_reason
        };

        exec_params(
            "INSERT INTO live_sessions"
            "(session_date, instrument, strategy, orb_high, orb_low, orb_range, trades_taken,"
            " daily_pnl_usd, peak_equity, risk_halted, halt_reason)"
            " VALUES ($1::date, $2, $3,"
            " $4::double precision, $5::double precision, $6::double precision, $7::int,"
            " $8::double precision, $9::double precision, $10::boolean, $11)"
            " ON CONFLICT (session_date, instrument, strategy) DO UPDATE SET"
            " orb_high=EXCLUDED.orb_high, orb_low=EXCLUDED.orb_low,"
            " orb_range=EXCLUDED.orb_range, trades_taken=EXCLUDED.trades_taken,"
            " daily_pnl_usd=EXCLUDED.daily_pnl_usd, peak_equity=EXCLUDED.peak_equity,"
            " risk_halted=EXCLUDED.risk_halted, halt_reason=EXCLUDED.halt_reason",
            11, params);
    }

    // ── Update account equity for today's session ────────────────────────────
    void write_account_equity(const std::string& trade_date, double equity) {
        if (!is_connected()) {
            reconnect();
            if (!is_connected()) return;
        }
        // Only log on first write or when value changes by more than $0.01
        bool should_log = (last_written_equity_ < 0.0 ||
                           std::abs(equity - last_written_equity_) >= 0.01);

        char p4[32];
        snprintf(p4, sizeof(p4), "%.4f", equity);

        const char* params[4] = {
            trade_date.c_str(),   // $1  session_date
            instrument_.c_str(),  // $2  instrument
            strategy_.c_str(),    // $3  strategy
            p4                    // $4  account_equity
        };

        try {
            exec_params(
                "INSERT INTO live_sessions(session_date, instrument, strategy, account_equity)"
                " VALUES ($1::date, $2, $3, $4::double precision)"
                " ON CONFLICT (session_date, instrument, strategy)"
                " DO UPDATE SET account_equity=EXCLUDED.account_equity",
                4, params);
            if (should_log) {
                LOG("[ORBDB] Account equity updated: $%.2f", equity);
                last_written_equity_ = equity;
            }
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

        // Nullable text params: pass nullptr so PostgreSQL receives SQL NULL
        const char* dir_param   = direction.empty()  ? nullptr : direction.c_str();
        const char* etime_param = entry_time.empty() ? nullptr : entry_time.c_str();

        // entry_price / sl_price are 0 when FLAT — store NULL for clarity
        char ep_buf[32], sl_buf[32];
        const char* ep_param = nullptr;
        const char* sl_param = nullptr;
        if (entry_price != 0.0) {
            snprintf(ep_buf, sizeof(ep_buf), "%.4f", entry_price);
            ep_param = ep_buf;
        }
        if (sl_price != 0.0) {
            snprintf(sl_buf, sizeof(sl_buf), "%.4f", sl_price);
            sl_param = sl_buf;
        }

        char cp[32], upts[32], uusd[32], oh[32], ol[32], tt[16];
        snprintf(cp,   sizeof(cp),   "%.4f", current_price);
        snprintf(upts, sizeof(upts), "%.4f", unrealized_pts);
        snprintf(uusd, sizeof(uusd), "%.4f", unrealized_usd);
        snprintf(oh,   sizeof(oh),   "%.4f", orb_high);
        snprintf(ol,   sizeof(ol),   "%.4f", orb_low);
        snprintf(tt,   sizeof(tt),   "%d",   trades_today);
        const char* orb_set_s    = orb_set      ? "t" : "f";
        const char* md_conn_s    = md_connected  ? "t" : "f";
        const char* op_conn_s    = op_connected  ? "t" : "f";

        const char* params[17] = {
            session_date.c_str(),  // $1  session_date
            instrument_.c_str(),   // $2  instrument
            strategy_.c_str(),     // $3  strategy
            state.c_str(),         // $4  state
            dir_param,             // $5  direction (NULL if empty)
            ep_param,              // $6  entry_price (NULL if FLAT)
            etime_param,           // $7  entry_time (NULL if empty)
            cp,                    // $8  current_price
            upts,                  // $9  unrealized_pnl_pts
            uusd,                  // $10 unrealized_pnl_usd
            sl_param,              // $11 sl_price (NULL if FLAT)
            oh,                    // $12 orb_high
            ol,                    // $13 orb_low
            orb_set_s,             // $14 orb_set
            tt,                    // $15 trades_today
            md_conn_s,             // $16 md_connected
            op_conn_s              // $17 op_connected
        };

        try {
            exec_params(
                "INSERT INTO live_position"
                " (session_date, instrument, strategy, state, direction, entry_price, entry_time,"
                "  current_price, unrealized_pnl_pts, unrealized_pnl_usd,"
                "  sl_price, orb_high, orb_low, orb_set,"
                "  trades_today, md_connected, op_connected, last_updated)"
                " VALUES"
                " ($1::date, $2, $3, $4, $5,"
                "  $6::double precision,"
                /* entry_time: to_timestamp(NULL,...) yields NULL — correct for FLAT state */
                "  to_timestamp($7, 'YYYY-MM-DD HH24:MI:SS') AT TIME ZONE 'UTC',"
                "  $8::double precision, $9::double precision, $10::double precision,"
                "  $11::double precision, $12::double precision, $13::double precision,"
                "  $14::boolean, $15::int, $16::boolean, $17::boolean, NOW())"
                " ON CONFLICT (session_date, instrument, strategy) DO UPDATE SET"
                "  state=EXCLUDED.state, direction=EXCLUDED.direction,"
                "  entry_price=EXCLUDED.entry_price, entry_time=EXCLUDED.entry_time,"
                "  current_price=EXCLUDED.current_price,"
                "  unrealized_pnl_pts=EXCLUDED.unrealized_pnl_pts,"
                "  unrealized_pnl_usd=EXCLUDED.unrealized_pnl_usd,"
                "  sl_price=EXCLUDED.sl_price,"
                "  orb_high=EXCLUDED.orb_high, orb_low=EXCLUDED.orb_low,"
                "  orb_set=EXCLUDED.orb_set,"
                "  trades_today=EXCLUDED.trades_today,"
                "  md_connected=EXCLUDED.md_connected,"
                "  op_connected=EXCLUDED.op_connected,"
                "  last_updated=NOW()",
                17, params);
        } catch (std::exception& e) {
            LOG("[ORBDB] write_position failed: %s", e.what());
        }
    }

    // ── Write Legends TICKER_PLANT price for comparison ──────────────────────
    void write_legends_price(const std::string& session_date, double price) {
        if (!is_connected()) { reconnect(); if (!is_connected()) return; }

        char p1[32];
        snprintf(p1, sizeof(p1), "%.4f", price);

        const char* params[4] = {
            p1,                   // $1  legends_price
            session_date.c_str(), // $2  session_date
            instrument_.c_str(),  // $3  instrument
            strategy_.c_str()     // $4  strategy
        };

        try {
            exec_params(
                "UPDATE live_position"
                " SET legends_price=$1::double precision, last_updated=NOW()"
                " WHERE session_date=$2::date AND instrument=$3 AND strategy=$4",
                4, params);
        } catch (std::exception& e) {
            LOG("[ORBDB] write_legends_price failed: %s", e.what());
        }
    }

    // ── Get total historical P&L (for seeding RiskManager on startup) ─────────
    double get_total_pnl() {
        if (!is_connected()) reconnect();

        const char* params[2] = {
            instrument_.c_str(),  // $1
            strategy_.c_str()     // $2
        };

        PGresult* res = exec_params_query(
            "SELECT COALESCE(SUM(pnl_usd), 0.0) FROM live_trades"
            " WHERE instrument=$1 AND strategy=$2",
            2, params);

        if (!res) return 0.0;
        double total = (PQntuples(res) > 0) ? std::atof(PQgetvalue(res, 0, 0)) : 0.0;
        PQclear(res);
        LOG("[ORBDB] Historical total_pnl=%.2f", total);
        return total;
    }

    // ── Read last N trade rows (for monitoring) ───────────────────────────────
    void print_recent_trades(int n = 10) {
        char p3[16];
        snprintf(p3, sizeof(p3), "%d", n);

        const char* params[3] = {
            instrument_.c_str(),  // $1
            strategy_.c_str(),    // $2
            p3                    // $3  LIMIT
        };

        PGresult* res = exec_params_query(
            "SELECT trade_date, direction, entry_price, exit_price, pnl_usd, exit_reason"
            " FROM live_trades"
            " WHERE instrument=$1 AND strategy=$2"
            " ORDER BY entry_time DESC LIMIT $3::int",
            3, params);

        if (!res) return;
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
                instrument              TEXT NOT NULL DEFAULT 'MNQ',
                strategy                TEXT NOT NULL DEFAULT 'ORB',
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

        // Add columns idempotently for existing deployments
        exec("ALTER TABLE live_trades ADD COLUMN IF NOT EXISTS instrument     TEXT NOT NULL DEFAULT 'MNQ'");
        exec("ALTER TABLE live_trades ADD COLUMN IF NOT EXISTS strategy       TEXT NOT NULL DEFAULT 'ORB'");
        exec("ALTER TABLE live_trades ADD COLUMN IF NOT EXISTS mae_pts        DOUBLE PRECISION");
        exec("ALTER TABLE live_trades ADD COLUMN IF NOT EXISTS mfe_pts        DOUBLE PRECISION");
        exec("ALTER TABLE live_trades ADD COLUMN IF NOT EXISTS trigger_price  DOUBLE PRECISION");
        exec("ALTER TABLE live_trades ADD COLUMN IF NOT EXISTS fill_price     DOUBLE PRECISION");

        exec(R"(
            CREATE TABLE IF NOT EXISTS live_sessions (
                session_date    DATE NOT NULL,
                instrument      TEXT NOT NULL DEFAULT 'MNQ',
                strategy        TEXT NOT NULL DEFAULT 'ORB',
                orb_high        DOUBLE PRECISION,
                orb_low         DOUBLE PRECISION,
                orb_range       DOUBLE PRECISION,
                trades_taken    INT DEFAULT 0,
                daily_pnl_usd   DOUBLE PRECISION DEFAULT 0,
                peak_equity     DOUBLE PRECISION,
                risk_halted     BOOLEAN DEFAULT FALSE,
                halt_reason     TEXT,
                account_equity  DOUBLE PRECISION,
                PRIMARY KEY (session_date, instrument, strategy)
            )
        )");

        exec("ALTER TABLE live_sessions ADD COLUMN IF NOT EXISTS instrument     TEXT NOT NULL DEFAULT 'MNQ'");
        exec("ALTER TABLE live_sessions ADD COLUMN IF NOT EXISTS strategy       TEXT NOT NULL DEFAULT 'ORB'");
        exec("ALTER TABLE live_sessions ADD COLUMN IF NOT EXISTS account_equity DOUBLE PRECISION");

        exec(R"(
            CREATE TABLE IF NOT EXISTS live_position (
                id                  SERIAL PRIMARY KEY,
                session_date        DATE NOT NULL,
                instrument          TEXT NOT NULL DEFAULT 'MNQ',
                strategy            TEXT NOT NULL DEFAULT 'ORB',
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

        exec("ALTER TABLE live_position ADD COLUMN IF NOT EXISTS instrument     TEXT NOT NULL DEFAULT 'MNQ'");
        exec("ALTER TABLE live_position ADD COLUMN IF NOT EXISTS strategy      TEXT NOT NULL DEFAULT 'ORB'");
        exec("ALTER TABLE live_position ADD COLUMN IF NOT EXISTS legends_price DOUBLE PRECISION");

        exec(R"(
            CREATE UNIQUE INDEX IF NOT EXISTS live_position_inst_strat_idx
            ON live_position(session_date, instrument, strategy)
        )");

        LOG("[ORBDB] Schema verified: instrument=%s strategy=%s",
            instrument_.c_str(), strategy_.c_str());
    }

    // Push current price to any LISTEN live_tick subscribers (non-throwing)
    void notify_tick(double price) {
        if (!is_connected()) return;
        char sql[80];
        snprintf(sql, sizeof(sql), "NOTIFY live_tick, '%.2f'", price);
        PGresult* res = PQexec(conn_, sql);
        if (res) PQclear(res);
    }

    // DDL-only helper — used exclusively for schema setup (no user input)
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

    // Parameterized execute for INSERT/UPDATE/DELETE — throws on error
    void exec_params(const char* sql, int n_params, const char* const* params) {
        if (!is_connected()) reconnect();
        PGresult* res = PQexecParams(conn_, sql, n_params,
                                     nullptr,  // server infers param types
                                     params,
                                     nullptr,  // text params: lengths not needed
                                     nullptr,  // all text format
                                     0);       // text result format
        if (!res) {
            // PQexecParams returns null only on severe OOM or broken connection.
            // Must throw so callers (e.g. write_trade) don't silently lose data.
            std::string err = PQerrorMessage(conn_);
            throw std::runtime_error("[ORBDB] PQexecParams returned null: " + err);
        }
        ExecStatusType status = PQresultStatus(res);
        if (status != PGRES_COMMAND_OK && status != PGRES_TUPLES_OK) {
            std::string err = PQerrorMessage(conn_);
            PQclear(res);
            throw std::runtime_error("[ORBDB] SQL error: " + err);
        }
        PQclear(res);
    }

    // Parameterized execute for SELECT — caller must PQclear() the returned result
    PGresult* exec_params_query(const char* sql, int n_params, const char* const* params) {
        if (!is_connected()) reconnect();
        PGresult* res = PQexecParams(conn_, sql, n_params,
                                     nullptr, params, nullptr, nullptr, 0);
        if (!res) {
            LOG("[ORBDB] PQexecParams returned null: %s", PQerrorMessage(conn_));
            return nullptr;
        }
        if (PQresultStatus(res) != PGRES_TUPLES_OK) {
            LOG("[ORBDB] Query failed: %s", PQerrorMessage(conn_));
            PQclear(res);
            return nullptr;
        }
        return res;
    }

    std::string connstr_;
    std::string instrument_;
    std::string strategy_ = "ORB";
    PGconn*     conn_ = nullptr;
    double      last_written_equity_ = -1.0; // suppress duplicate equity log lines
};
