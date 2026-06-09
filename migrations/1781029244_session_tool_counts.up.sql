-- Replace the has_tool_error boolean with tool_result_count /
-- tool_error_count. derived_status now applies a failure-rate rule
-- (a single recovered tool error no longer marks a whole session failed)
-- alongside the unrecovered-terminal-error rule, which needs counts rather
-- than a flag. The counts are cumulative across the session, maintained at
-- ingest the same way has_tool_error was. See pkg/sessions.DetermineStatus.

ALTER TABLE sessions
    ADD COLUMN IF NOT EXISTS tool_result_count integer NOT NULL DEFAULT 0;

ALTER TABLE sessions
    ADD COLUMN IF NOT EXISTS tool_error_count integer NOT NULL DEFAULT 0;

ALTER TABLE sessions
    DROP COLUMN IF EXISTS has_tool_error;
