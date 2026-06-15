-- AggregateSpanStats (/v1/stats) counts tool spans in a window with a
-- correlated subquery: spans WHERE org_id = $ AND kind = 'tool' AND
-- started_at >= since AND started_at < until. The existing spans indexes
-- cover call_kind, session_id+started_at, and raw_turn_id — none of them
-- serve this predicate, so the count is a seq scan. tool is the only kind
-- this query asks for, so a partial index keeps it small.
CREATE INDEX IF NOT EXISTS spans_org_kind_started_idx
    ON spans (org_id, started_at)
    WHERE kind = 'tool';
