-- Reads by (trace_id, span_id) or trace_id stay correct without these; each
-- goes back to scanning the whole projection.
DROP INDEX IF EXISTS span_turns_20260615_trace_idx;
DROP INDEX IF EXISTS spans_20260615_trace_span_idx;
