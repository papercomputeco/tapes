-- The span projection is derived data. Keep each schema generation in
-- date-suffixed physical tables so future projection schemas can coexist
-- with readable historical rows instead of mutating one live table family.

CREATE TABLE IF NOT EXISTS derived_projection_schemas (
    compatibility_date DATE PRIMARY KEY,
    span_turns_table   TEXT NOT NULL,
    spans_table        TEXT NOT NULL,
    span_links_table   TEXT NOT NULL,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO derived_projection_schemas (
    compatibility_date,
    span_turns_table,
    spans_table,
    span_links_table
) VALUES (
    DATE '2026-06-15',
    'span_turns_20260615',
    'spans_20260615',
    'span_links_20260615'
) ON CONFLICT (compatibility_date) DO NOTHING;

ALTER TABLE IF EXISTS span_turns RENAME TO span_turns_20260615;
ALTER TABLE IF EXISTS spans RENAME TO spans_20260615;
ALTER TABLE IF EXISTS span_links RENAME TO span_links_20260615;

ALTER INDEX IF EXISTS span_turns_pkey RENAME TO span_turns_20260615_pkey;
ALTER INDEX IF EXISTS span_turns_session_idx RENAME TO span_turns_20260615_session_idx;
ALTER INDEX IF EXISTS span_turns_org_started_idx RENAME TO span_turns_20260615_org_started_idx;

ALTER INDEX IF EXISTS spans_pkey RENAME TO spans_20260615_pkey;
ALTER INDEX IF EXISTS spans_session_idx RENAME TO spans_20260615_session_idx;
ALTER INDEX IF EXISTS spans_org_call_kind_idx RENAME TO spans_20260615_org_call_kind_idx;
ALTER INDEX IF EXISTS spans_raw_turn_idx RENAME TO spans_20260615_raw_turn_idx;
ALTER INDEX IF EXISTS spans_org_kind_started_idx RENAME TO spans_20260615_org_kind_started_idx;

ALTER INDEX IF EXISTS span_links_pkey RENAME TO span_links_20260615_pkey;
ALTER INDEX IF EXISTS span_links_from_trace_idx RENAME TO span_links_20260615_from_trace_idx;
ALTER INDEX IF EXISTS span_links_to_trace_idx RENAME TO span_links_20260615_to_trace_idx;
ALTER INDEX IF EXISTS span_links_session_idx RENAME TO span_links_20260615_session_idx;

ALTER TABLE IF EXISTS spans_20260615 RENAME CONSTRAINT spans_kind_chk TO spans_20260615_kind_chk;

COMMENT ON TABLE span_turns_20260615 IS 'Derived span-turn projection schema version 2026-06-15.';
COMMENT ON TABLE spans_20260615 IS 'Derived span projection schema version 2026-06-15.';
COMMENT ON TABLE span_links_20260615 IS 'Derived span-link projection schema version 2026-06-15.';
