-- Org-wide saved sessions (PCC-815): one shared "saved" marker per session
-- per org — a team shortlist, not per-user bookmarks. A separate table
-- rather than a column on sessions because the sessions read model is a
-- pure idempotent projection of raw capture; user-authored state lives
-- beside it, same convention as skills.
--
-- saved_by is attribution only (the auth_subject of the first saver,
-- '' when the caller sent no subject header) — never an ownership gate:
-- anyone in the org can unsave. ON DELETE CASCADE keeps markers from
-- outliving a pruned session row.
CREATE TABLE IF NOT EXISTS saved_sessions (
    org_id     UUID NOT NULL,
    session_id UUID NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
    saved_by   TEXT NOT NULL DEFAULT '',
    saved_at   TIMESTAMPTZ NOT NULL,

    CONSTRAINT saved_sessions_pkey PRIMARY KEY (org_id, session_id)
);
