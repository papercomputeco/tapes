-- Recreate the two session foreign keys with ON DELETE CASCADE so a session
-- DELETE removes its dependent rows in one statement instead of erroring on a
-- NO ACTION constraint. This is what makes DELETE /v1/sessions/:id possible.
--
-- - nodes.session_id (added in 1779329142_session_tracking) had no ON DELETE
--   clause, so deleting a session a node points at would raise a constraint
--   violation. CASCADE drops the session's derived nodes with it. A later
--   re-derive re-projects those nodes from the immutable raw_turns log with
--   NULL attribution (the deriver never re-creates the session row itself), and
--   the product surface ignores NULL-session nodes — so the delete stays durable.
-- - sessions.parent_session_id is self-referential; CASCADE makes deleting a
--   parent recursively delete its subagent (child) sessions.
--
-- span_turns / spans / span_links already cascade on session_id
-- (1781230000_span_model), so they are untouched here.

ALTER TABLE nodes
    DROP CONSTRAINT IF EXISTS nodes_session_id_fkey,
    ADD CONSTRAINT nodes_session_id_fkey
        FOREIGN KEY (session_id) REFERENCES sessions(id) ON DELETE CASCADE;

ALTER TABLE sessions
    DROP CONSTRAINT IF EXISTS sessions_parent_session_id_fkey,
    ADD CONSTRAINT sessions_parent_session_id_fkey
        FOREIGN KEY (parent_session_id) REFERENCES sessions(id) ON DELETE CASCADE;
