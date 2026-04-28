-- 005_legends_price.sql
-- Add legends_price column to live_position for Legends TICKER_PLANT price comparison.
-- Run once on Oracle VM PostgreSQL after deploying the AMP/Legends dual-feed build.
-- Safe to run multiple times (IF NOT EXISTS guard on UP, IF EXISTS on DOWN).

-- ── UP ───────────────────────────────────────────────────────────────────────
ALTER TABLE live_position ADD COLUMN IF NOT EXISTS legends_price DOUBLE PRECISION;

-- ── DOWN (rollback) ──────────────────────────────────────────────────────────
-- ALTER TABLE live_position DROP COLUMN IF EXISTS legends_price;
