-- The cursor indexes are not dropped explicitly: the up migration does not
-- create them (see its note), and where an operator has built them by hand
-- they are dependent on derive_seq and go with the column.
ALTER TABLE spans_20260615
    DROP COLUMN IF EXISTS content_hash,
    DROP COLUMN IF EXISTS derive_seq,
    DROP COLUMN IF EXISTS fidelity;

ALTER TABLE span_turns_20260615
    DROP COLUMN IF EXISTS content_hash,
    DROP COLUMN IF EXISTS derive_seq,
    DROP COLUMN IF EXISTS fidelity;

