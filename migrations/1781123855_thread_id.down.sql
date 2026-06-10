-- Inverse of the thread_id migration (recomputable from raw_turns).
DROP INDEX IF EXISTS nodes_thread_idx;
ALTER TABLE nodes DROP COLUMN IF EXISTS thread_id;
