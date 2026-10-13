-- A raw-turn attribution correction is a complete replacement attribution,
-- and a fork parent is now named by (parent_harness_id,
-- parent_harness_session_id) rather than by a session id assumed to share
-- the child's harness. Record the parent's harness beside its session id so
-- a repaired cross-harness child (a codex session launched by a claude one)
-- keeps resolving its parent in the right namespace.
--
-- NULL means the correction names no parent harness: the parent, if any,
-- resolves in the corrected harness_id — exactly what every correction
-- written before this column meant. Nullable, no default, no backfill, so
-- the ALTER is a catalog-only change.
ALTER TABLE raw_turn_attribution_corrections
    ADD COLUMN IF NOT EXISTS parent_harness_id TEXT;
