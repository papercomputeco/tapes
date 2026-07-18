-- Persist the session-scoped task fold and per-kind span counts as typed
-- rollup columns.
--
-- Both were recomputed on every /v1/sessions/:id/traces read: the handler
-- replayed TaskCreate/TaskUpdate tool blocks and tallied call_kinds across
-- the whole session. They are derived facts — the deriver folds them once
-- (pkg/derive) and writes them here, so the read path and the export path
-- serve stored values. A re-derive backfills existing sessions.
--
-- tasks:       JSONB array of {id, subject, description, status, updates}
-- kind_counts: JSONB object mapping call_kind -> count
ALTER TABLE sessions ADD COLUMN IF NOT EXISTS tasks JSONB;
ALTER TABLE sessions ADD COLUMN IF NOT EXISTS kind_counts JSONB;
