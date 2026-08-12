-- tapes_v1: the published read contract for cassettes.
--
-- A cassette's manifest declares `depends.core = "v1"` and names the views it
-- wants SELECT on; the grant plan derives `tapes_v1.<view>` from that
-- declaration (pkg/cassette/v1alpha1/grants.go). Until now the schema those
-- grants name did not exist, so every deployment granted the physical
-- projection tables instead — tables that are deliberately date-versioned
-- (1781310000_version_span_projection_tables) and therefore rotate out from
-- under any grant that names them.
--
-- These views are that contract. Deployments grant SELECT on them and never
-- on the physical tables, so a projection-generation rotation is invisible to
-- cassettes: the new generation's migration repoints the views and the grants
-- keep working.
--
-- Two properties are load-bearing:
--
--   - The column lists are explicit, not SELECT *. The view IS the v1
--     contract; a column added to a physical table later does not join the
--     contract by default, it joins when a migration deliberately re-creates
--     the view. (Postgres freezes a view's columns at creation either way —
--     the explicit list just makes the contract reviewable in this file.)
--
--   - A migration that registers a new projection generation in
--     derived_projection_schemas MUST also CREATE OR REPLACE the three
--     projection views here to front the new tables. A test pins this:
--     the views' base relations are asserted against the registry's latest
--     row, so forgetting the repoint turns the suite red instead of silently
--     starving every deployed cassette.
--
-- No GRANTs happen here. Publishing the contract is core's job; deciding who
-- reads it is the deployment's (tko grants per-cassette from the manifest's
-- grant plan, docker-compose examples do it in provision.sql).

CREATE SCHEMA IF NOT EXISTS tapes_v1;

COMMENT ON SCHEMA tapes_v1 IS
    'Published v1 read contract for cassettes. Grant SELECT on these views, never on the physical tables they front.';

-- sessions is a stable-named table (not generation-versioned), but it is
-- still fronted by a view: the contract must be uniform — one schema, one
-- grant shape — and core keeps the freedom to version sessions later without
-- a flag day for cassettes.
CREATE OR REPLACE VIEW tapes_v1.sessions AS
SELECT
    id,
    org_id,
    auth_subject,
    harness_id,
    harness_session_id,
    name,
    cwd,
    harness_version,
    parent_session_id,
    started_at,
    last_seen_at,
    ended_at,
    harness_metadata,
    total_input_tokens,
    total_output_tokens,
    total_cost_usd,
    turn_count,
    derived_status,
    has_git_activity,
    tool_result_count,
    tool_error_count,
    derived_title,
    derived_model,
    model_usage,
    total_tokens,
    duration_ns,
    tasks,
    kind_counts,
    display_name
FROM sessions;

CREATE OR REPLACE VIEW tapes_v1.spans AS
SELECT
    org_id,
    trace_id,
    span_id,
    parent_span_id,
    session_id,
    kind,
    name,
    status,
    call_kind,
    thread_id,
    model,
    stop_reason,
    started_at,
    duration_ns,
    input,
    output,
    usage,
    raw_turn_id,
    node_hash,
    seq,
    verdict,
    content_hash,
    derive_seq,
    fidelity
FROM spans_20260615;

CREATE OR REPLACE VIEW tapes_v1.span_turns AS
SELECT
    org_id,
    trace_id,
    session_id,
    user_prompt,
    synthetic,
    status,
    started_at,
    ended_at,
    duration_ns,
    total_input_tokens,
    total_output_tokens,
    total_cost_usd,
    main_input_tokens,
    main_output_tokens,
    cache_read_tokens,
    cache_creation_tokens,
    response_preview,
    source,
    content_hash,
    derive_seq,
    fidelity
FROM span_turns_20260615;

CREATE OR REPLACE VIEW tapes_v1.span_links AS
SELECT
    org_id,
    from_trace_id,
    from_span_id,
    from_io,
    to_trace_id,
    to_span_id,
    to_io,
    kind,
    session_id
FROM span_links_20260615;

COMMENT ON VIEW tapes_v1.sessions IS
    'v1 contract view over the sessions table.';
COMMENT ON VIEW tapes_v1.spans IS
    'v1 contract view over the current span projection generation (see derived_projection_schemas).';
COMMENT ON VIEW tapes_v1.span_turns IS
    'v1 contract view over the current span-turn projection generation (see derived_projection_schemas).';
COMMENT ON VIEW tapes_v1.span_links IS
    'v1 contract view over the current span-link projection generation (see derived_projection_schemas).';
