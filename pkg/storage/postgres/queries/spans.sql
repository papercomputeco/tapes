-- Span model writes. Span identity is deterministic (minted from wire
-- identity by the deriver), so re-derivation upserts the same keys in
-- place and prune removes only rows a superseded projection wrote.

-- name: UpsertSpanTurn :exec
INSERT INTO span_turns (
    org_id, trace_id, session_id, user_prompt, synthetic, status,
    started_at, ended_at, duration_ns,
    total_input_tokens, total_output_tokens,
    main_input_tokens, main_output_tokens,
    cache_read_tokens, cache_creation_tokens
) VALUES (
    $1, $2, $3, $4, $5, $6,
    $7, $8, $9,
    $10, $11,
    $12, $13,
    $14, $15
)
ON CONFLICT (org_id, trace_id) DO UPDATE SET
    session_id            = COALESCE(span_turns.session_id, EXCLUDED.session_id),
    user_prompt           = EXCLUDED.user_prompt,
    synthetic             = EXCLUDED.synthetic,
    status                = EXCLUDED.status,
    started_at            = EXCLUDED.started_at,
    ended_at              = EXCLUDED.ended_at,
    duration_ns           = EXCLUDED.duration_ns,
    total_input_tokens    = EXCLUDED.total_input_tokens,
    total_output_tokens   = EXCLUDED.total_output_tokens,
    main_input_tokens     = EXCLUDED.main_input_tokens,
    main_output_tokens    = EXCLUDED.main_output_tokens,
    cache_read_tokens     = EXCLUDED.cache_read_tokens,
    cache_creation_tokens = EXCLUDED.cache_creation_tokens;

-- name: UpsertSpan :exec
INSERT INTO spans (
    org_id, trace_id, span_id, parent_span_id, session_id,
    kind, name, status, call_kind, thread_id, model, stop_reason,
    started_at, duration_ns, input, output, usage, raw_turn_id, node_hash
) VALUES (
    $1, $2, $3, $4, $5,
    $6, $7, $8, $9, $10, $11, $12,
    $13, $14, $15, $16, $17, $18, $19
)
ON CONFLICT (org_id, trace_id, span_id) DO UPDATE SET
    parent_span_id = EXCLUDED.parent_span_id,
    session_id     = COALESCE(spans.session_id, EXCLUDED.session_id),
    kind           = EXCLUDED.kind,
    name           = EXCLUDED.name,
    status         = EXCLUDED.status,
    call_kind      = EXCLUDED.call_kind,
    thread_id      = EXCLUDED.thread_id,
    model          = EXCLUDED.model,
    stop_reason    = EXCLUDED.stop_reason,
    started_at     = EXCLUDED.started_at,
    duration_ns    = EXCLUDED.duration_ns,
    input          = EXCLUDED.input,
    output         = EXCLUDED.output,
    usage          = EXCLUDED.usage,
    raw_turn_id    = EXCLUDED.raw_turn_id,
    node_hash      = EXCLUDED.node_hash;

-- name: UpsertSpanLink :exec
INSERT INTO span_links (
    org_id, from_trace_id, from_span_id, from_io,
    to_trace_id, to_span_id, to_io, kind, session_id
) VALUES (
    $1, $2, $3, $4,
    $5, $6, $7, $8, $9
)
ON CONFLICT (org_id, from_trace_id, from_span_id, to_trace_id, to_span_id, from_io, to_io)
DO UPDATE SET
    kind       = EXCLUDED.kind,
    session_id = COALESCE(span_links.session_id, EXCLUDED.session_id);

-- name: PruneSpanTurns :execrows
-- Deterministic ids make prune a no-op on unchanged raw; rows fall out
-- only when the projection stops producing their key.
DELETE FROM span_turns
WHERE org_id = $1
  AND session_id = ANY(sqlc.arg(session_ids)::uuid[])
  AND NOT (trace_id = ANY(sqlc.arg(keep_trace_ids)::text[]));

-- name: PruneSpans :execrows
DELETE FROM spans
WHERE org_id = $1
  AND session_id = ANY(sqlc.arg(session_ids)::uuid[])
  AND NOT (trace_id || '|' || span_id = ANY(sqlc.arg(keep_keys)::text[]));

-- name: PruneSpanLinks :execrows
DELETE FROM span_links
WHERE org_id = $1
  AND session_id = ANY(sqlc.arg(session_ids)::uuid[])
  AND NOT (from_trace_id || '|' || from_span_id || '|' || to_trace_id || '|' || to_span_id || '|' || from_io || '|' || to_io
           = ANY(sqlc.arg(keep_keys)::text[]));

-- Span model reads.

-- name: ListSpanTurnsBySession :many
SELECT * FROM span_turns
WHERE session_id = $1
ORDER BY started_at ASC, trace_id ASC;

-- name: ListSpansBySession :many
SELECT * FROM spans
WHERE session_id = $1
ORDER BY trace_id ASC, started_at ASC, span_id ASC;

-- name: ListSpanLinksBySession :many
SELECT * FROM span_links
WHERE session_id = $1;

-- name: ListSpanTurns :many
SELECT * FROM span_turns
WHERE org_id = $1
  AND (sqlc.narg(cursor_started_at)::timestamptz IS NULL
       OR (started_at, trace_id) < (sqlc.narg(cursor_started_at)::timestamptz, sqlc.narg(cursor_trace_id)::text))
ORDER BY started_at DESC, trace_id DESC
LIMIT $2;

-- name: GetSpanTurn :one
SELECT * FROM span_turns
WHERE org_id = $1 AND trace_id = $2;

-- name: ListSpansByTrace :many
SELECT * FROM spans
WHERE org_id = $1 AND trace_id = $2
ORDER BY started_at ASC, span_id ASC;

-- name: ListSpanLinksByTrace :many
SELECT * FROM span_links
WHERE org_id = $1 AND (from_trace_id = $2 OR to_trace_id = $2);

-- name: ListTraceSummariesBySession :many
-- Session detail's lazy view: turn headers only, no span payloads.
SELECT t.org_id, t.trace_id, t.session_id, t.user_prompt, t.synthetic,
       t.status, t.started_at, t.ended_at, t.duration_ns,
       t.total_input_tokens, t.total_output_tokens,
       t.main_input_tokens, t.main_output_tokens,
       t.cache_read_tokens, t.cache_creation_tokens, t.total_cost_usd,
       count(s.span_id) AS span_count
FROM span_turns t
LEFT JOIN spans s ON s.org_id = t.org_id AND s.trace_id = t.trace_id
WHERE t.session_id = $1
GROUP BY t.org_id, t.trace_id
ORDER BY t.started_at ASC, t.trace_id ASC;

-- name: GetSpan :one
SELECT * FROM spans
WHERE org_id = $1 AND trace_id = $2 AND span_id = $3;
