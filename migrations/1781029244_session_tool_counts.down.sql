-- Inverse of 1781029244_session_tool_counts.up.sql. Restores has_tool_error
-- (defaulting false — the precise pre-drop values are not recoverable) and
-- drops the count columns.

ALTER TABLE sessions
    ADD COLUMN IF NOT EXISTS has_tool_error boolean NOT NULL DEFAULT false;

ALTER TABLE sessions DROP COLUMN IF EXISTS tool_error_count;
ALTER TABLE sessions DROP COLUMN IF EXISTS tool_result_count;
