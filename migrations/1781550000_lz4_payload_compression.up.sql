-- Compress new payload values with lz4 instead of pglz.
--
-- The payload columns are JSONB with EXTENDED storage: a value past the
-- TOAST threshold is compressed first, then moved out of line if it is
-- still too large. Postgres compresses with pglz unless the column says
-- otherwise. On this JSON lz4 reaches a comparable ratio and decompresses
-- several times faster — and every read of a raw turn or a span is a
-- decompression, so the read side is where the change is felt.
--
-- Set per column, deliberately. The cluster's default_toast_compression is
-- left alone, so tables this migration does not name keep the server's
-- setting, and a deployment that never runs this file loses nothing.
--
-- New rows only. SET COMPRESSION changes the method for values written
-- from now on; it rewrites nothing. Existing values keep the pglz they
-- were stored with and read back correctly (the method travels with the
-- value) until something detoasts and re-stores them — a dump and restore
-- does; VACUUM FULL and CLUSTER copy compressed bytes as they are. Whether
-- and when to rewrite is an operator decision outside this migration.

ALTER TABLE raw_turns
    ALTER COLUMN raw_request SET COMPRESSION lz4,
    ALTER COLUMN response SET COMPRESSION lz4;

ALTER TABLE spans_20260615
    ALTER COLUMN input SET COMPRESSION lz4,
    ALTER COLUMN output SET COMPRESSION lz4,
    ALTER COLUMN input_preview SET COMPRESSION lz4,
    ALTER COLUMN output_preview SET COMPRESSION lz4;
