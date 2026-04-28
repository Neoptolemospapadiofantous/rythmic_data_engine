-- 003_rename_live_tables.sql
-- Rename nq_* tables to live_* prefix.
-- Run once on the Oracle VM PostgreSQL database.
-- Safe to run multiple times (IF EXISTS guards each statement).

-- Rename tables
ALTER TABLE IF EXISTS nq_trades   RENAME TO live_trades;
ALTER TABLE IF EXISTS nq_session  RENAME TO live_sessions;
ALTER TABLE IF EXISTS nq_position RENAME TO live_position;

-- Rename index (must match new table name)
ALTER INDEX IF EXISTS nq_position_date_idx RENAME TO live_position_date_idx;

-- Rename primary-key and default constraints to match new names
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'nq_trades_pkey') THEN
        ALTER TABLE live_trades   RENAME CONSTRAINT nq_trades_pkey   TO live_trades_pkey;
    END IF;
    IF EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'nq_session_pkey') THEN
        ALTER TABLE live_sessions RENAME CONSTRAINT nq_session_pkey  TO live_sessions_pkey;
    END IF;
    IF EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'nq_position_pkey') THEN
        ALTER TABLE live_position RENAME CONSTRAINT nq_position_pkey TO live_position_pkey;
    END IF;
END
$$;
