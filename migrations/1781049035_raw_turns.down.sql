-- Inverse of 1781049035_raw_turns.up.sql. Dropping raw_turns discards
-- the immutable raw capture (not recoverable); the promoted request_*
-- node columns are derived copies and can be re-stamped from raw_turns
-- on a future re-derive.

ALTER TABLE nodes
    DROP COLUMN IF EXISTS request_tool_count,
    DROP COLUMN IF EXISTS request_stream,
    DROP COLUMN IF EXISTS request_temperature,
    DROP COLUMN IF EXISTS request_max_tokens,
    DROP COLUMN IF EXISTS request_system;

DROP TABLE IF EXISTS raw_turns;
