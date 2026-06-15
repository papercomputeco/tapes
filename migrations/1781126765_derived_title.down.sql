-- Inverse of the derived_title migration (recomputable from raw_turns).
ALTER TABLE sessions DROP COLUMN IF EXISTS derived_title;
