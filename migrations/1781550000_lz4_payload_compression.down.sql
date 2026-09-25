-- Restores the server default for new values only; values already stored
-- with lz4 keep it and remain readable.
ALTER TABLE spans_20260615
    ALTER COLUMN output_preview SET COMPRESSION pglz,
    ALTER COLUMN input_preview SET COMPRESSION pglz,
    ALTER COLUMN output SET COMPRESSION pglz,
    ALTER COLUMN input SET COMPRESSION pglz;

ALTER TABLE raw_turns
    ALTER COLUMN response SET COMPRESSION pglz,
    ALTER COLUMN raw_request SET COMPRESSION pglz;
