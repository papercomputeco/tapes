-- Store span payload previews next to the payload they summarize.
--
-- The console's list and scan reads only ever want a bounded preview of
-- each span's content blocks, but a preview computed at read time still
-- has to detoast the whole input/output value first, so "preview" costs
-- strictly more than "full". TOAST detoasts per attribute, which makes a
-- sibling column the one shape where a preview read never touches the
-- payload at all.
--
-- The deriver writes both columns in the same upsert as the payload,
-- from the same 512-rune rule the API applied on the way out. They are
-- a pure function of input/output and are excluded from content_hash,
-- so writing them advances no consumer's cursor.
--
-- Nullable and deliberately not backfilled here: rows derived before
-- this migration carry NULL until a throttled batched job fills them in
-- (`tapes backfill previews`); a NULL is served as an empty preview marked
-- pending, never as an on-read fallback to the payload.

ALTER TABLE spans_20260615
    ADD COLUMN IF NOT EXISTS input_preview JSONB,
    ADD COLUMN IF NOT EXISTS output_preview JSONB;
