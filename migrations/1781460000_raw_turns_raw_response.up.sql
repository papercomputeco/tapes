-- Give the wire lane a byte-faithful response alongside the reduced one.
--
-- raw_turns.response holds the *reduced* response: a canonical
-- llm.ChatResponse the capture adapter produced by consuming the upstream
-- stream. That reduction is lossy by design and is performed by whichever
-- adapter happened to capture the turn. Two consequences the raw layer was
-- supposed to prevent:
--
--   * A field the reducing adapter didn't model is gone for good. The raw
--     layer's premise is that every future data-model change is a re-derive
--     over existing rows, never a re-capture — but a re-derive cannot recover
--     what the reduction dropped.
--   * Two capture paths that reduce slightly differently produce different
--     stored rows for identical upstream bytes, and nothing in the schema can
--     tell you that happened.
--
-- raw_response stores the upstream response bytes verbatim, exactly as they
-- arrived, so the reduction becomes reproducible and auditable rather than
-- authoritative. The reduced `response` column stays: it is what the deriver
-- reads, and re-reducing on every derive would be wasteful.
--
-- raw_response_encoding records the Content-Encoding the bytes are stored
-- under ('identity', 'gzip', …). The bytes are stored as received rather than
-- decompressed, because "verbatim" has to mean verbatim for the column to be
-- worth having — a re-compression is not guaranteed byte-identical.
--
-- raw_response_dropped is the fidelity marker. A raw response above the
-- ingest cap is dropped rather than stored, and this flag distinguishes
-- "no raw response was ever captured for this turn" (NULL, false) from "one
-- existed and we chose not to keep it" (NULL, true). Without the flag those
-- two are the same row, and a gap reads as an absence — which is the specific
-- failure this column exists to prevent.
ALTER TABLE raw_turns
    ADD COLUMN IF NOT EXISTS raw_response          BYTEA,
    ADD COLUMN IF NOT EXISTS raw_response_encoding TEXT    NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS raw_response_dropped  BOOLEAN NOT NULL DEFAULT FALSE;
