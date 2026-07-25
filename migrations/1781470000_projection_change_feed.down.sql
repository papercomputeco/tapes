DROP INDEX IF EXISTS spans_20260615_org_derive_seq_idx;
DROP INDEX IF EXISTS span_turns_20260615_org_derive_seq_idx;

ALTER TABLE spans_20260615
    DROP COLUMN IF EXISTS content_hash,
    DROP COLUMN IF EXISTS derive_seq,
    DROP COLUMN IF EXISTS fidelity;

ALTER TABLE span_turns_20260615
    DROP COLUMN IF EXISTS content_hash,
    DROP COLUMN IF EXISTS derive_seq,
    DROP COLUMN IF EXISTS fidelity;

DROP SEQUENCE IF EXISTS derive_seq_counter;
