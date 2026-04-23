-- migrations/002_schema_reconcile.sql
-- Reconciles the divergent schemas created by live_trader.py (_ensure_trades_schema)
-- and the canonical schema in migrations/001_trades.sql / models.py.
--
-- The canonical column names are entry_time/exit_time/pnl (not entry_ts/exit_ts/pnl_dollars).
-- The canonical session table is session_summary (not session_summaries plural).
--
-- UP migration (run this to apply):
--   psql $DATABASE_URL -f migrations/002_schema_reconcile.sql
--
-- DOWN migration: see "DOWN MIGRATION" section at the bottom.
--
-- Safe to re-run (all ALTER statements are guarded by column-existence checks).

BEGIN;

-- ── 1. Rename divergent columns in trades if they exist ─────────────────────

-- entry_ts → entry_time
DO $$ BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = 'trades' AND column_name = 'entry_ts'
    ) AND NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = 'trades' AND column_name = 'entry_time'
    ) THEN
        ALTER TABLE trades RENAME COLUMN entry_ts TO entry_time;
    END IF;
END $$;

-- exit_ts → exit_time
DO $$ BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = 'trades' AND column_name = 'exit_ts'
    ) AND NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = 'trades' AND column_name = 'exit_time'
    ) THEN
        ALTER TABLE trades RENAME COLUMN exit_ts TO exit_time;
    END IF;
END $$;

-- pnl_dollars → pnl
DO $$ BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = 'trades' AND column_name = 'pnl_dollars'
    ) AND NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = 'trades' AND column_name = 'pnl'
    ) THEN
        ALTER TABLE trades RENAME COLUMN pnl_dollars TO pnl;
    END IF;
END $$;

-- ── 2. Add missing columns to trades ────────────────────────────────────────
-- Columns present in live_trader.py schema but absent from models.py canonical schema.

ALTER TABLE trades
    ADD COLUMN IF NOT EXISTS target       DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS exit_reason  VARCHAR(32),
    ADD COLUMN IF NOT EXISTS dry_run      BOOLEAN DEFAULT NULL;

-- Columns present in models.py canonical schema but absent from live_trader.py schema.

ALTER TABLE trades
    ADD COLUMN IF NOT EXISTS quantity       INTEGER NOT NULL DEFAULT 1,
    ADD COLUMN IF NOT EXISTS source         VARCHAR(8) NOT NULL DEFAULT 'python',
    ADD COLUMN IF NOT EXISTS session_id     VARCHAR(64),
    ADD COLUMN IF NOT EXISTS ml_prediction  DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS ml_confidence  DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW();

-- Ensure pnl_points column exists (live_trader.py and models.py both use this name).
ALTER TABLE trades
    ADD COLUMN IF NOT EXISTS pnl_points DOUBLE PRECISION;

-- Ensure pnl column exists (canonical name; pnl_dollars was renamed above if present).
ALTER TABLE trades
    ADD COLUMN IF NOT EXISTS pnl DOUBLE PRECISION;

-- Ensure stop_loss column exists.
ALTER TABLE trades
    ADD COLUMN IF NOT EXISTS stop_loss DOUBLE PRECISION;

-- ── 3. Fix direction CHECK constraint width ──────────────────────────────────
-- models.py uses CHAR(5) and allows 'LONG '/'SHORT'; live_trader.py uses CHAR(5) too.
-- No change needed — both use the same CHECK(direction IN ('LONG', 'SHORT')).

-- ── 4. Ensure unique index on (symbol, entry_time, direction) ───────────────
CREATE UNIQUE INDEX IF NOT EXISTS idx_trades_upsert_key
    ON trades (symbol, entry_time, direction);

CREATE INDEX IF NOT EXISTS idx_trades_session_date
    ON trades (session_date);

CREATE INDEX IF NOT EXISTS idx_trades_session_id
    ON trades (session_id)
    WHERE session_id IS NOT NULL;

-- ── 5. Add source CHECK constraint if not present ───────────────────────────
DO $$ BEGIN
    -- Only add if there's no existing constraint on the source column
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.constraint_column_usage
        WHERE table_name = 'trades' AND column_name = 'source'
    ) THEN
        ALTER TABLE trades
            ADD CONSTRAINT trades_source_check
            CHECK (source IN ('python', 'cpp'));
    END IF;
END $$;

-- ── 6. Ensure updated_at trigger fires on trades ────────────────────────────
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

-- ── 7. Ensure canonical session_summary table exists ────────────────────────
-- Already created by 001_trades.sql, but idempotent.
CREATE TABLE IF NOT EXISTS session_summary (
    session_id      VARCHAR(64)      PRIMARY KEY,
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

DROP TRIGGER IF EXISTS session_summary_updated_at ON session_summary;
CREATE TRIGGER session_summary_updated_at
    BEFORE UPDATE ON session_summary
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- ── 8. Migrate data from session_summaries (plural) if it exists ────────────
-- live_trader.py created 'session_summaries' (plural) with a simpler schema.
-- Migrate its rows into the canonical session_summary table, then drop it.
DO $$ BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = 'public' AND table_name = 'session_summaries'
    ) THEN
        INSERT INTO session_summary (session_id, date, source, gross_pnl, trade_count,
                                     win_count, max_drawdown, notes)
        SELECT
            session_date::text || '_python' AS session_id,
            session_date                    AS date,
            'python'                        AS source,
            COALESCE(gross_pnl_usd, 0.0)   AS gross_pnl,
            COALESCE(trade_count, 0)        AS trade_count,
            0                               AS win_count,
            0.0                             AS max_drawdown,
            exit_reason                     AS notes
        FROM session_summaries
        ON CONFLICT (session_id) DO UPDATE SET
            gross_pnl   = EXCLUDED.gross_pnl,
            trade_count = EXCLUDED.trade_count,
            notes       = EXCLUDED.notes,
            updated_at  = NOW();

        -- Rename the old table out of the way rather than dropping (safer rollback).
        ALTER TABLE session_summaries RENAME TO session_summaries_migrated;
    END IF;
END $$;

COMMIT;


-- ── DOWN MIGRATION ────────────────────────────────────────────────────────────
-- To roll back, copy and run these statements manually:
--
-- BEGIN;
-- -- Rename canonical columns back to live_trader.py names (only if you want live_trader.py
-- -- to work with its original schema; normally you would update live_trader.py instead).
-- -- ALTER TABLE trades RENAME COLUMN entry_time TO entry_ts;
-- -- ALTER TABLE trades RENAME COLUMN exit_time TO exit_ts;
-- -- ALTER TABLE trades RENAME COLUMN pnl TO pnl_dollars;
-- --
-- -- Drop additive columns added by this migration.
-- ALTER TABLE trades DROP COLUMN IF EXISTS target;
-- ALTER TABLE trades DROP COLUMN IF EXISTS exit_reason;
-- ALTER TABLE trades DROP COLUMN IF EXISTS dry_run;
-- ALTER TABLE trades DROP COLUMN IF EXISTS updated_at;
-- -- Note: quantity/source/session_id/ml_prediction/ml_confidence were added here;
-- -- only drop them if they weren't present before this migration.
-- --
-- -- Restore session_summaries if it was migrated.
-- -- ALTER TABLE session_summaries_migrated RENAME TO session_summaries;
-- COMMIT;
