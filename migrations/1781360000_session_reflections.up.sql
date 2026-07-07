-- session_reflections stores the on-demand LLM reflection of a session (PCC-241): a
-- 2-3 sentence narrative of what the person/agent set out to do and how it
-- went, surfaced at the top of the console's session-detail page. One row per
-- session — regeneration upserts, latest wins. turn_count is the staleness
-- anchor: it records session.turn_count at generation time, and when the live
-- session's count climbs past it the console prompts for an updated reflection.
-- A reflection whose turn_count still matches the session is immutable, so repeat
-- generate calls return the stored row without an LLM call.
--
-- observation is an optional transferable insight (a gotcha, repeated pattern,
-- or workflow heuristic) extracted by the same LLM pass — a reflection is a chance
-- for an observation. Empty when the pass found nothing noteworthy. It is the
-- feedstock for the Dreams observational-memory queue once that backend ships.
--
-- org_id is UUID NOT NULL but unconstrained — there is no `orgs` table in this
-- repo (same convention as the sessions and skills tables). Reads/writes are
-- scoped to the caller's org via the composite PK; the nil-UUID sentinel is
-- the bucket for callers that send no X-Tapes-Org-Id header.

CREATE TABLE IF NOT EXISTS session_reflections (
    org_id       UUID NOT NULL,
    session_id   UUID NOT NULL,
    narrative    TEXT NOT NULL,
    observation  TEXT NOT NULL DEFAULT '',
    turn_count   INTEGER NOT NULL,
    model        TEXT NOT NULL DEFAULT '',
    generated_at TIMESTAMPTZ NOT NULL,

    CONSTRAINT session_reflections_pkey PRIMARY KEY (org_id, session_id)
);
