-- The title-gen shadow call's output IS the session's display title
-- (the same title the harness shows in its own UI), but it never
-- reached the sessions table — the §2g disposition for title-gen is
-- "fold → session.name". It lands in its own column rather than name
-- because the envelope keeps re-sending the harness's internal name
-- (a plan slug) on every turn and would clobber it; the read layer
-- prefers derived_title when present.
ALTER TABLE sessions
    ADD COLUMN IF NOT EXISTS derived_title TEXT;
