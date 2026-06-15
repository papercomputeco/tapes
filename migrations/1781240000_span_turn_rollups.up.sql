-- Token rollups become the deriver's responsibility: per-turn sums
-- split main-vs-shadow, computed from span usage at emit. The v2 read
-- surface aggregates these instead of the ingest counters, making the
-- deriver the single writer of accounting.
ALTER TABLE span_turns
    ADD COLUMN IF NOT EXISTS main_input_tokens     BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS main_output_tokens    BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS cache_read_tokens     BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS cache_creation_tokens BIGINT NOT NULL DEFAULT 0;
