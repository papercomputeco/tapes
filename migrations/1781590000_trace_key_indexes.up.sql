-- Index spans and span turns by their org-free identity.
--
-- Tapes no longer has organizations (tapes#276): every row carries the nil
-- sentinel org_id, and readers outside core — the cassettes behind the
-- tapes_v1 views — address a span as (trace_id, span_id) and a turn as
-- trace_id, never naming org_id. Every index on these tables still leads
-- with org_id, so none of them can serve those reads, and the planner has
-- no way to probe a span by key: each join from outside becomes a scan of
-- the whole projection.
--
-- The search cassette is the reader that found this. Its nearest-neighbour
-- query joins span_embeddings to tapes_v1.spans on (trace_id, span_id) and
-- to tapes_v1.span_turns on trace_id. With nothing to probe, the planner
-- abandons the HNSW index, hashes the full spans heap, and sorts every
-- embedding by distance; on a tenant with a few hundred thousand spans a
-- search runs past a minute. With these indexes each candidate the search
-- checks is a probe of a handful of keys.
--
-- These are also the keys the org_id drop will leave behind, so they
-- outlive it: once org_id is gone they are what the primary keys reduce to.
--
-- Plain CREATE INDEX, for the reason 1781560000_span_page_order_index gives:
-- an interrupted concurrent build leaves an INVALID index that IF NOT EXISTS
-- then skips for good. The build holds a SHARE lock on each table — reads
-- proceed, the deriver's writes wait — so a very large tenant stalls startup
-- for its length. An operator who would rather not pay that at deploy time
-- can build both by hand first with CREATE INDEX CONCURRENTLY under these
-- exact names, check pg_index.indisvalid, and let this file be a no-op.

CREATE INDEX IF NOT EXISTS spans_20260615_trace_span_idx
    ON spans_20260615 (trace_id, span_id);

CREATE INDEX IF NOT EXISTS span_turns_20260615_trace_idx
    ON span_turns_20260615 (trace_id);
