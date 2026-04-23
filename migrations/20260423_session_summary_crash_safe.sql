-- migrations/20260423_session_summary_crash_safe.sql
-- Adds crash_exit BOOLEAN column to session_summary table.
--
-- crash_exit = TRUE means the session summary was written by the crash/SIGTERM
-- handler (SessionSummary.write_crash_safe) rather than a clean EOD exit.
-- Operators can filter on this to identify sessions that need manual review.
--
-- UP migration (run this to apply):
--   psql $DATABASE_URL -f migrations/20260423_session_summary_crash_safe.sql
--
-- Safe to re-run (ADD COLUMN IF NOT EXISTS).

BEGIN;

ALTER TABLE session_summary
    ADD COLUMN IF NOT EXISTS crash_exit BOOLEAN NOT NULL DEFAULT FALSE;

COMMENT ON COLUMN session_summary.crash_exit IS
    'TRUE when written by crash/SIGTERM handler; FALSE for clean EOD exits.';

COMMIT;


-- ── DOWN MIGRATION ────────────────────────────────────────────────
-- To roll back, run manually:
--
-- BEGIN;
-- ALTER TABLE session_summary DROP COLUMN IF EXISTS crash_exit;
-- COMMIT;
