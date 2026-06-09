-- Inverse of 1780956959_session_status.up.sql. Purely additive columns +
-- index, so the down direction is a clean drop with no data reshaping.

DROP INDEX IF EXISTS sessions_derived_status_idx;

ALTER TABLE sessions DROP COLUMN IF EXISTS has_git_activity;
ALTER TABLE sessions DROP COLUMN IF EXISTS has_tool_error;
ALTER TABLE sessions DROP COLUMN IF EXISTS derived_status;
