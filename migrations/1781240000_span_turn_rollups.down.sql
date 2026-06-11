ALTER TABLE span_turns
    DROP COLUMN IF EXISTS main_input_tokens,
    DROP COLUMN IF EXISTS main_output_tokens,
    DROP COLUMN IF EXISTS cache_read_tokens,
    DROP COLUMN IF EXISTS cache_creation_tokens;
