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
-- content_hash actually changed, so a consumer sees genuinely-changed rows
-- rather than the whole session. Bumping it on every write would make the
-- column monotonic but useless.
--
-- IT IS NOT COMMIT-ORDERED, and a consumer that treats it as such loses data.
-- The value comes from nextval() called inside the derive transaction, so it
-- is assigned at write time, not at commit time. Two derive passes running
-- concurrently can take 5 and 6 and commit in the other order: a consumer
-- polling `derive_seq > cursor` sees 6, checkpoints 6, and never sees 5 when
-- it lands. Sequences also do not roll back, so an aborted pass burns a value
-- and leaves a permanent hole — harmless on its own, but it means gaps carry
-- no information and cannot be used to detect the reordering above.
--
-- A consumer must therefore not advance its cursor past rows whose writing
-- transaction may still be in flight. The direct way is to bound the read by
-- the oldest transaction still running:
--
--   SELECT ... FROM spans_20260615
--    WHERE org_id = $1
--      AND derive_seq > $2
--      AND xmin::text::bigint < pg_snapshot_xmin(pg_current_snapshot())
--    ORDER BY derive_seq;
--
-- which holds back rows written by uncommitted transactions until they commit
-- (mind xid wraparound on a long-lived cursor). A consumer that can tolerate
-- reprocessing can instead re-read a lag window behind its cursor and dedupe
-- on content_hash, which is idempotent by construction.
--
-- No consumer exists yet. This note is here so the first one is not written
-- against the naive poll.
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

-- No index is created here, deliberately. See the note at the bottom of this
-- file before adding one.

CREATE SEQUENCE IF NOT EXISTS derive_seq_counter AS BIGINT START 1;

ALTER TABLE span_turns_20260615
    ADD COLUMN IF NOT EXISTS content_hash TEXT   NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS derive_seq   BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS fidelity     TEXT   NOT NULL DEFAULT '';

ALTER TABLE spans_20260615
    ADD COLUMN IF NOT EXISTS content_hash TEXT   NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS derive_seq   BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS fidelity     TEXT   NOT NULL DEFAULT '';

-- ---------------------------------------------------------------------------
-- The cursor index is NOT created here, and that is the whole point of this
-- note.
--
-- The feed's access pattern is "everything after my cursor" — an index on
-- (org_id, derive_seq) per projection table. Creating it in a migration is the
-- obvious move and the wrong one, because of how migrations run here:
-- postgres.NewDriver calls migrateUp, and every serve path opens a driver. So
-- migrations execute at process start, serialised across pods by a
-- pg_advisory_lock that waits indefinitely.
--
-- On a large span table that makes either index build an outage:
--
--   * a plain CREATE INDEX takes ACCESS EXCLUSIVE, blocking reads and writes
--     to the table for the whole build;
--   * CREATE INDEX CONCURRENTLY avoids that lock but scans the table twice and
--     takes LONGER, and every starting pod still blocks on the advisory lock
--     until it finishes. Writes survive; the deployment does not.
--
-- The statements above are all metadata-only on PostgreSQL 11+ (ADD COLUMN
-- with a constant default writes no rows), so this migration stays fast on a
-- table of any size. An index would be the only slow thing in it.
--
-- Nothing needs the index yet: no change-feed consumer exists, and the feed is
-- correct without it — only the cursor query is slower. Build it when a
-- consumer does exist, as a deliberate operator step:
--
--   CREATE INDEX CONCURRENTLY IF NOT EXISTS spans_20260615_org_derive_seq_idx
--       ON spans_20260615 (org_id, derive_seq);
--
--   CREATE INDEX CONCURRENTLY IF NOT EXISTS span_turns_20260615_org_derive_seq_idx
--       ON span_turns_20260615 (org_id, derive_seq);
--
-- Run each on its own, outside any transaction — CONCURRENTLY is rejected
-- inside one, and a multi-statement string gets an implicit transaction. If a
-- concurrent build fails it leaves an INVALID index behind; drop it (also
-- CONCURRENTLY) and retry rather than leaving it to be silently ignored by the
-- planner:
--
--   SELECT c.relname FROM pg_class c JOIN pg_index i ON i.indexrelid = c.oid
--    WHERE NOT i.indisvalid;
--
-- If it is ever added as a migration instead, it must be the only statement in
-- its file, and the deploy has to tolerate a startup stall for the duration.
