-- Restore the original NO ACTION foreign keys (no ON DELETE behavior), matching
-- how 1779329142_session_tracking first declared them inline.

ALTER TABLE nodes
    DROP CONSTRAINT IF EXISTS nodes_session_id_fkey,
    ADD CONSTRAINT nodes_session_id_fkey
        FOREIGN KEY (session_id) REFERENCES sessions(id);

ALTER TABLE sessions
    DROP CONSTRAINT IF EXISTS sessions_parent_session_id_fkey,
    ADD CONSTRAINT sessions_parent_session_id_fkey
        FOREIGN KEY (parent_session_id) REFERENCES sessions(id);
