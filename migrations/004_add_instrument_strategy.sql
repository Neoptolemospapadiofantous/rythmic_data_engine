-- 004_add_instrument_strategy.sql
-- Add instrument + strategy columns to enable multi-instrument / multi-strategy trading.
-- Run once on Oracle VM PostgreSQL after deploying 004 build.
-- Safe to run multiple times (IF NOT EXISTS / DO $$ guards).

-- ── live_trades ──────────────────────────────────────────────────────────────
ALTER TABLE live_trades ADD COLUMN IF NOT EXISTS instrument TEXT NOT NULL DEFAULT 'MNQ';
ALTER TABLE live_trades ADD COLUMN IF NOT EXISTS strategy   TEXT NOT NULL DEFAULT 'ORB';

-- ── live_sessions ─────────────────────────────────────────────────────────────
-- Step 1: add new columns with defaults so existing rows get MNQ/ORB
ALTER TABLE live_sessions ADD COLUMN IF NOT EXISTS instrument TEXT NOT NULL DEFAULT 'MNQ';
ALTER TABLE live_sessions ADD COLUMN IF NOT EXISTS strategy   TEXT NOT NULL DEFAULT 'ORB';

-- Step 2: promote the primary key from (session_date) to (session_date, instrument, strategy)
--         Only execute if the old single-column PK still exists.
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'live_sessions_pkey'
          AND contype = 'p'
    ) THEN
        -- Check if it's still the old single-column PK
        IF (
            SELECT count(*) FROM pg_attribute a
            JOIN pg_constraint c ON a.attrelid = c.conrelid
                                AND a.attnum = ANY(c.conkey)
            WHERE c.conname = 'live_sessions_pkey'
        ) = 1 THEN
            ALTER TABLE live_sessions DROP CONSTRAINT live_sessions_pkey;
            ALTER TABLE live_sessions ADD PRIMARY KEY (session_date, instrument, strategy);
        END IF;
    ELSE
        -- No PK at all — create composite one
        ALTER TABLE live_sessions ADD PRIMARY KEY (session_date, instrument, strategy);
    END IF;
END
$$;

-- ── live_position ─────────────────────────────────────────────────────────────
ALTER TABLE live_position ADD COLUMN IF NOT EXISTS instrument TEXT NOT NULL DEFAULT 'MNQ';
ALTER TABLE live_position ADD COLUMN IF NOT EXISTS strategy   TEXT NOT NULL DEFAULT 'ORB';

-- Drop old single-column unique index and create composite one
DROP INDEX IF EXISTS live_position_date_idx;
DROP INDEX IF EXISTS live_position_inst_strat_idx;
CREATE UNIQUE INDEX IF NOT EXISTS live_position_inst_strat_idx
    ON live_position(session_date, instrument, strategy);
