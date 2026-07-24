ALTER TABLE raw_turns
    DROP COLUMN IF EXISTS raw_response,
    DROP COLUMN IF EXISTS raw_response_encoding,
    DROP COLUMN IF EXISTS raw_response_dropped;
