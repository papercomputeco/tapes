-- Change-feed and provenance columns on the span projection.
--
-- Re-derivation rewrites every row of a covered session in place, whether or
-- not anything about it changed. That makes the projection unusable as a
-- change feed: a downstream consumer (search indexer, embedder, an external
-- subscriber) has no way to ask "what is actually different since I last
-- looked" and must re-read everything after every derive pass.
--
-- content_hash is the row's identity by content: a digest over exactly the
-- columns a consumer cares about, excluding these three. Two derive passes
-- over unchanged raw produce the same hash, which is what lets the writer
-- tell a real change from an idempotent rewrite.
--
-- derive_seq is the cursor. It is stamped from a single global sequence, one
-- value per derive pass, and — critically — is only advanced on a row whose
-- content_hash actually changed. A consumer polls `derive_seq > cursor` and
-- sees genuinely-changed rows, not the whole session. Bumping it on every
-- write would make the column monotonic but useless.
--
-- fidelity records raw-bytes provenance: whether the row can be re-derived
-- faithfully from stored bytes, or only from a reduction somebody already
-- performed. It is the projection-side view of raw_turns.raw_response:
--
--   ''         no raw turn backs this row (synthetic, transcript-derived)
--   'raw'      the backing raw turn has verbatim response bytes
--   'reduced'  the backing raw turn has only an adapter's reduction
--   'degraded' verbatim bytes arrived but exceeded the cap and were dropped
--
-- 'reduced' and 'degraded' are deliberately distinct. Both mean the bytes are
-- gone, but 'reduced' is a capture path that never sent them and 'degraded' is
-- one that did and hit a limit — the first is a deployment fact, the second is
-- a tuning signal, and collapsing them hides which one you are looking at.
--
-- All three are computed in the storage layer at write time. pkg/derive stays
-- a pure function of the raw rows: provenance is a property of how the bytes
-- were captured and stored, not of the projection, and computing it inside the
-- deriver would make the deriver's output depend on storage state it must not
-- read (RFD 00007 §C).

CREATE SEQUENCE IF NOT EXISTS derive_seq_counter AS BIGINT START 1;

ALTER TABLE span_turns_20260615
    ADD COLUMN IF NOT EXISTS content_hash TEXT   NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS derive_seq   BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS fidelity     TEXT   NOT NULL DEFAULT '';

ALTER TABLE spans_20260615
    ADD COLUMN IF NOT EXISTS content_hash TEXT   NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS derive_seq   BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS fidelity     TEXT   NOT NULL DEFAULT '';

-- The feed's only access pattern: "everything after my cursor", newest last.
-- Scoped by org because every consumer is.
CREATE INDEX IF NOT EXISTS span_turns_20260615_org_derive_seq_idx
    ON span_turns_20260615 (org_id, derive_seq);

CREATE INDEX IF NOT EXISTS spans_20260615_org_derive_seq_idx
    ON spans_20260615 (org_id, derive_seq);
