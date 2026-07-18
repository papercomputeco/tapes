-- Persist the security-monitor verdict as a typed span column.
--
-- The disposition on a permission-check span (ALLOW/BLOCK, stage, whether
-- the reviewer reasoned) is derived data: the deriver extracts it from the
-- check response at derive time (pkg/derive.ClassifyVerdict) and writes it
-- here, so the API serves it from the column instead of re-parsing span
-- output text on every read. Null on every span that is not a
-- permission-check verdict. A re-derive backfills existing rows.
ALTER TABLE spans_20260615 ADD COLUMN IF NOT EXISTS verdict JSONB;
