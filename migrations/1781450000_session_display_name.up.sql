-- The user-editable display title lives in its own column, separate from
-- `name`. `name` carries the harness-supplied session name — a plan slug
-- like `clearing-example-2e`, or a harness `/rename` — which the ingest
-- envelope re-sends on EVERY turn (UpsertSession: name = COALESCE(narg,
-- name)). A console rename written into `name` is therefore clobbered by
-- the next turn's upsert on any live session, and the slug also masks the
-- deriver's title (PCC-970).
--
-- display_name is written ONLY by PATCH /v1/sessions/:id and never by
-- ingest, so a user's title survives an active session. Empty/NULL means
-- "no user title"; the read layer resolves the display title by falling
-- back display_name -> derived_title -> preview -> name.
ALTER TABLE sessions
    ADD COLUMN IF NOT EXISTS display_name TEXT;
