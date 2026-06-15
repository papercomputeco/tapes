ALTER TABLE IF EXISTS spans_20260615 RENAME CONSTRAINT spans_20260615_kind_chk TO spans_kind_chk;

ALTER INDEX IF EXISTS span_links_20260615_session_idx RENAME TO span_links_session_idx;
ALTER INDEX IF EXISTS span_links_20260615_to_trace_idx RENAME TO span_links_to_trace_idx;
ALTER INDEX IF EXISTS span_links_20260615_from_trace_idx RENAME TO span_links_from_trace_idx;
ALTER INDEX IF EXISTS span_links_20260615_pkey RENAME TO span_links_pkey;

ALTER INDEX IF EXISTS spans_20260615_org_kind_started_idx RENAME TO spans_org_kind_started_idx;
ALTER INDEX IF EXISTS spans_20260615_raw_turn_idx RENAME TO spans_raw_turn_idx;
ALTER INDEX IF EXISTS spans_20260615_org_call_kind_idx RENAME TO spans_org_call_kind_idx;
ALTER INDEX IF EXISTS spans_20260615_session_idx RENAME TO spans_session_idx;
ALTER INDEX IF EXISTS spans_20260615_pkey RENAME TO spans_pkey;

ALTER INDEX IF EXISTS span_turns_20260615_org_started_idx RENAME TO span_turns_org_started_idx;
ALTER INDEX IF EXISTS span_turns_20260615_session_idx RENAME TO span_turns_session_idx;
ALTER INDEX IF EXISTS span_turns_20260615_pkey RENAME TO span_turns_pkey;

ALTER TABLE IF EXISTS span_links_20260615 RENAME TO span_links;
ALTER TABLE IF EXISTS spans_20260615 RENAME TO spans;
ALTER TABLE IF EXISTS span_turns_20260615 RENAME TO span_turns;

DELETE FROM derived_projection_schemas
WHERE compatibility_date = DATE '2026-06-15';

DROP TABLE IF EXISTS derived_projection_schemas;
