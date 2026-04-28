-- migrations/001_trades.sql
-- Creates the unified trades table and session_summary table used by the
-- Python live trader. The C++ engine writes to live_trades (see orb_db.hpp);
-- sync_cpp_trades.py reconciles those rows into trades at EOD.
--
-- UP migration (run this to apply):
--   psql $DATABASE_URL -f migrations/001_trades.sql
--
-- DOWN migration (run this to roll back):
--   See "DOWN MIGRATION" section at the bottom of this file.
--
-- Rules:
--   - Additive only: no DROP COLUMN, no ALTER TYPE narrowing.
--   - All DOWN steps are commented-out to prevent accidental execution.
--   - Safe to re-run (all statements use IF NOT EXISTS / OR REPLACE).

-- ── UP MIGRATION ──────────────────────────────────────────────────

BEGIN;

-- trades: one row per completed trade (entry + exit pair).
-- Unified table for both Python-sourced and C++-sourced trades.
CREATE TABLE IF NOT EXISTS trades (
    id             BIGSERIAL       PRIMARY KEY,
    session_date   DATE            NOT NULL,
    symbol         VARCHAR(16)     NOT NULL DEFAULT 'NQ',
    direction      CHAR(5)         NOT NULL CHECK (direction IN ('LONG', 'SHORT')),
    entry_price    DOUBLE PRECISION NOT NULL,
    exit_price     DOUBLE PRECISION,
    entry_time     TIMESTAMPTZ     NOT NULL,
    exit_time      TIMESTAMPTZ,
    quantity       INTEGER         NOT NULL DEFAULT 1 CHECK (quantity > 0),
    pnl            DOUBLE PRECISION,            -- in dollars
    pnl_points     DOUBLE PRECISION,            -- in NQ points
    stop_loss      DOUBLE PRECISION,            -- SL price at time of exit
    source         VARCHAR(8)      NOT NULL DEFAULT 'python'
                                   CHECK (source IN ('python', 'cpp')),
    session_id     VARCHAR(64),                 -- FK to session_summary.session_id
    ml_prediction  DOUBLE PRECISION,            -- model output probability
    ml_confidence  DOUBLE PRECISION,            -- calibrated confidence score
    created_at     TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    updated_at     TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

-- Unique key used by sync_cpp_trades.py for idempotent upsert:
-- a trade is uniquely identified by its entry timestamp + symbol + direction.
CREATE UNIQUE INDEX IF NOT EXISTS idx_trades_upsert_key
    ON trades (symbol, entry_time, direction);

CREATE INDEX IF NOT EXISTS idx_trades_session_date
    ON trades (session_date);

CREATE INDEX IF NOT EXISTS idx_trades_session_id
    ON trades (session_id)
    WHERE session_id IS NOT NULL;

-- session_summary: one row per trading day / session.
-- Written by live_trader.py at EOD (and by sync_cpp_trades.py if missing).
CREATE TABLE IF NOT EXISTS session_summary (
    session_id      VARCHAR(64)      PRIMARY KEY,  -- e.g. "2026-04-23_python"
    date            DATE             NOT NULL,
    source          VARCHAR(8)       NOT NULL DEFAULT 'python'
                                     CHECK (source IN ('python', 'cpp', 'mixed')),
    gross_pnl       DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    trade_count     INTEGER          NOT NULL DEFAULT 0,
    win_count       INTEGER          NOT NULL DEFAULT 0,
    max_drawdown    DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    start_equity    DOUBLE PRECISION,
    end_equity      DOUBLE PRECISION,
    notes           TEXT,
    created_at      TIMESTAMPTZ      NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ      NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_session_summary_date
    ON session_summary (date);

-- Trigger to auto-update updated_at on any row change.
CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trades_updated_at ON trades;
CREATE TRIGGER trades_updated_at
    BEFORE UPDATE ON trades
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

DROP TRIGGER IF EXISTS session_summary_updated_at ON session_summary;
CREATE TRIGGER session_summary_updated_at
    BEFORE UPDATE ON session_summary
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

COMMIT;


-- ── DOWN MIGRATION ─────────────────────────────────────────────────
-- To roll back, copy and run these statements manually:
--
-- BEGIN;
-- DROP TRIGGER IF EXISTS session_summary_updated_at ON session_summary;
-- DROP TRIGGER IF EXISTS trades_updated_at ON trades;
-- DROP FUNCTION IF EXISTS set_updated_at();
-- DROP TABLE IF EXISTS session_summary;
-- DROP TABLE IF EXISTS trades;
-- COMMIT;
