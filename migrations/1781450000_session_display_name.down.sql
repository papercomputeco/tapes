-- Inverse of the display_name migration. The user-editable title is a
-- capture-independent fact (never recomputed from raw_turns), so dropping
-- the column discards any user renames; the read layer falls back to
-- derived_title/preview/name as before.
ALTER TABLE sessions DROP COLUMN IF EXISTS display_name;
