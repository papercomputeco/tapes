-- Adds a per-turn tool_calls rollup to span_turns so /v1/stats can sum it
-- from the narrow turn table instead of counting kind='tool' rows in the
-- wide spans table on every request (PCC-936).
--
-- The spans count was the aggregate's dominant cost: ~20x the row volume of
-- span_turns, rows carrying input/output/usage JSONB payloads, and in-place
-- re-derive rewrites clearing visibility-map bits so the (org_id, kind,
-- started_at) index degrades into random heap fetches on a cold cache.
--
-- The deriver folds the count at emit time (pkg/derive SpanTurn.ToolCalls),
-- the same single-writer path as every other turn total. The backfill below
-- covers rows derived before the column existed; the count also self-heals
-- on any later re-derive of a session.
--
-- Rolling-deploy window: a pre-change derive worker still running after this
-- migration writes turns without tool_calls, which land as the 0 default and
-- undercount /v1/stats until that session re-derives. Those rows carry a
-- content_hash computed without the column, so the next new-code derive pass
-- rewrites them — but a session that never derives again stays wrong. The
-- backfill UPDATE is therefore deliberately idempotent (the IS DISTINCT FROM
-- guard makes re-runs touch only wrong rows): after every worker is on the
-- new binary, re-run it verbatim, or run `tapes dev rederive`, to close the
-- window.
--
-- Attribution note: /v1/stats previously windowed tool calls on the tool
-- span's own started_at. Summing turn rollups windows them on the turn's
-- started_at instead, matching how every other stats figure is attributed.

ALTER TABLE span_turns_20260615
    ADD COLUMN IF NOT EXISTS tool_calls BIGINT NOT NULL DEFAULT 0;

UPDATE span_turns_20260615 t
SET tool_calls = counted.n
FROM (
    SELECT org_id, trace_id, COUNT(*) AS n
    FROM spans_20260615
    WHERE kind = 'tool'
    GROUP BY org_id, trace_id
) counted
WHERE t.org_id = counted.org_id
  AND t.trace_id = counted.trace_id
  AND t.tool_calls IS DISTINCT FROM counted.n;
