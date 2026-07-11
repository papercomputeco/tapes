-- Span model writes. Span identity is deterministic (minted from wire
-- identity by the deriver), so re-derivation upserts the same keys in
-- place and prune removes only rows a superseded projection wrote.

-- name: UpsertSpanTurn :exec
INSERT INTO span_turns_20260615 (
    org_id, trace_id, session_id, user_prompt, response_preview, synthetic, status,
    started_at, ended_at, duration_ns,
    total_input_tokens, total_output_tokens,
    main_input_tokens, main_output_tokens,
    cache_read_tokens, cache_creation_tokens,
    total_cost_usd
) VALUES (
    $1, $2, $3, $4, $5, $6, $7,
    $8, $9, $10,
    $11, $12,
    $13, $14,
    $15, $16,
    $17
)
ON CONFLICT (org_id, trace_id) DO UPDATE SET
    session_id            = COALESCE(span_turns_20260615.session_id, EXCLUDED.session_id),
    user_prompt           = EXCLUDED.user_prompt,
    response_preview      = EXCLUDED.response_preview,
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
    cache_creation_tokens = EXCLUDED.cache_creation_tokens,
    total_cost_usd        = EXCLUDED.total_cost_usd;

-- name: UpsertSpan :exec
INSERT INTO spans_20260615 (
    org_id, trace_id, span_id, parent_span_id, session_id,
    kind, name, status, call_kind, thread_id, model, stop_reason,
    started_at, duration_ns, seq, input, output, usage, raw_turn_id, node_hash,
    verdict
) VALUES (
    $1, $2, $3, $4, $5,
    $6, $7, $8, $9, $10, $11, $12,
    $13, $14, $15, $16, $17, $18, $19, $20,
    $21
)
ON CONFLICT (org_id, trace_id, span_id) DO UPDATE SET
    parent_span_id = EXCLUDED.parent_span_id,
    session_id     = COALESCE(spans_20260615.session_id, EXCLUDED.session_id),
    kind           = EXCLUDED.kind,
    name           = EXCLUDED.name,
    status         = EXCLUDED.status,
    call_kind      = EXCLUDED.call_kind,
    thread_id      = EXCLUDED.thread_id,
    model          = EXCLUDED.model,
    stop_reason    = EXCLUDED.stop_reason,
    started_at     = EXCLUDED.started_at,
    duration_ns    = EXCLUDED.duration_ns,
    seq            = EXCLUDED.seq,
    input          = EXCLUDED.input,
    output         = EXCLUDED.output,
    usage          = EXCLUDED.usage,
    raw_turn_id    = EXCLUDED.raw_turn_id,
    node_hash      = EXCLUDED.node_hash,
    verdict        = EXCLUDED.verdict;

-- name: UpsertSpanLink :exec
INSERT INTO span_links_20260615 (
    org_id, from_trace_id, from_span_id, from_io,
    to_trace_id, to_span_id, to_io, kind, session_id
) VALUES (
    $1, $2, $3, $4,
    $5, $6, $7, $8, $9
)
ON CONFLICT (org_id, from_trace_id, from_span_id, to_trace_id, to_span_id, from_io, to_io)
DO UPDATE SET
    kind       = EXCLUDED.kind,
    session_id = COALESCE(span_links_20260615.session_id, EXCLUDED.session_id);

-- name: PruneSpanTurns :execrows
-- Deterministic ids make prune a no-op on unchanged raw; rows fall out
-- only when the projection stops producing their key.
DELETE FROM span_turns_20260615
WHERE org_id = $1
  AND session_id = ANY(sqlc.arg(session_ids)::uuid[])
  AND NOT (trace_id = ANY(sqlc.arg(keep_trace_ids)::text[]));

-- name: PruneSpans :execrows
-- Keep-set membership is a tuple test over parallel arrays, NOT a
-- delimiter-joined string: trace_id/span_id embed externally-supplied
-- wire ids (request_id, tool_use_id) that can contain any byte, so a '|'
-- delimiter would collapse distinct (trace, span) pairs and delete the
-- wrong row inside the derive tx.
DELETE FROM spans_20260615
WHERE org_id = $1
  AND session_id = ANY(sqlc.arg(session_ids)::uuid[])
  AND NOT EXISTS (
      SELECT 1
      FROM generate_subscripts(sqlc.arg(keep_trace_ids)::text[], 1) AS i
      WHERE (sqlc.arg(keep_trace_ids)::text[])[i] = spans_20260615.trace_id
        AND (sqlc.arg(keep_span_ids)::text[])[i]  = spans_20260615.span_id
  );

-- name: PruneSpanLinks :execrows
-- Same tuple-membership guard as PruneSpans: the six link key columns
-- carry wire ids and cannot be safely '|'-joined into one string.
DELETE FROM span_links_20260615
WHERE org_id = $1
  AND session_id = ANY(sqlc.arg(session_ids)::uuid[])
  AND NOT EXISTS (
      SELECT 1
      FROM generate_subscripts(sqlc.arg(keep_from_trace_ids)::text[], 1) AS i
      WHERE (sqlc.arg(keep_from_trace_ids)::text[])[i] = span_links_20260615.from_trace_id
        AND (sqlc.arg(keep_from_span_ids)::text[])[i]  = span_links_20260615.from_span_id
        AND (sqlc.arg(keep_to_trace_ids)::text[])[i]   = span_links_20260615.to_trace_id
        AND (sqlc.arg(keep_to_span_ids)::text[])[i]    = span_links_20260615.to_span_id
        AND (sqlc.arg(keep_from_ios)::text[])[i]       = span_links_20260615.from_io
        AND (sqlc.arg(keep_to_ios)::text[])[i]         = span_links_20260615.to_io
  );

-- Span model reads.

-- name: ListSpanTurnsBySession :many
SELECT * FROM span_turns_20260615
WHERE session_id = $1
ORDER BY started_at ASC, trace_id ASC;

-- name: ListSpansBySession :many
-- seq is the deriver's emit ordinal (presentation order); started_at/
-- span_id only break ties for pre-seq rows that haven't re-derived.
SELECT * FROM spans_20260615
WHERE session_id = $1
ORDER BY trace_id ASC, seq ASC, started_at ASC, span_id ASC;

-- name: ListSpanLinksBySession :many
SELECT * FROM span_links_20260615
WHERE session_id = $1;

-- name: ListSpanTurns :many
SELECT * FROM span_turns_20260615
WHERE org_id = $1
  AND (sqlc.narg(cursor_started_at)::timestamptz IS NULL
       OR (started_at, trace_id) < (sqlc.narg(cursor_started_at)::timestamptz, sqlc.narg(cursor_trace_id)::text))
ORDER BY started_at DESC, trace_id DESC
LIMIT $2;

-- name: GetSpanTurn :one
SELECT * FROM span_turns_20260615
WHERE org_id = $1 AND trace_id = $2;

-- name: ListSpansByTrace :many
SELECT * FROM spans_20260615
WHERE org_id = $1 AND trace_id = $2
ORDER BY seq ASC, started_at ASC, span_id ASC;

-- name: ListSpanLinksByTrace :many
SELECT * FROM span_links_20260615
WHERE org_id = $1 AND (from_trace_id = $2 OR to_trace_id = $2);

-- name: ListTraceSummariesBySession :many
-- Session detail's lazy view: turn headers only, no span payloads.
SELECT t.org_id, t.trace_id, t.session_id, t.user_prompt, t.response_preview, t.synthetic,
       t.status, t.started_at, t.ended_at, t.duration_ns,
       t.total_input_tokens, t.total_output_tokens,
       t.main_input_tokens, t.main_output_tokens,
       t.cache_read_tokens, t.cache_creation_tokens, t.total_cost_usd,
       count(s.span_id) AS span_count
FROM span_turns_20260615 t
LEFT JOIN spans_20260615 s ON s.org_id = t.org_id AND s.trace_id = t.trace_id
WHERE t.session_id = $1
GROUP BY t.org_id, t.trace_id
ORDER BY t.started_at ASC, t.trace_id ASC;

-- name: GetSpan :one
SELECT * FROM spans_20260615
WHERE org_id = $1 AND trace_id = $2 AND span_id = $3;

-- name: FoldSessionRollupsFromSpans :exec
-- Session-level accounting is a derive-time fold over the trace
-- rollups — the deriver is the single writer of these columns on
-- span-model sessions. The ingest path never priced wire turns, its
-- token counters double-count re-sent history (each call re-bills the
-- whole conversation), and its turn counter counts wire calls rather
-- than user-visible turns, so the span fold replaces all of them.
-- derived_model is the dominant conversation-spine model, so the
-- session overview never needs span payloads.
--
-- model_usage and derived_title are reset to NULL here for every covered
-- session and re-written afterward only for the sessions that still
-- produce one (priced/titled in Go, so they ride a per-key loop, not
-- this fold). Without the reset a re-derive that drops a session's last
-- model entry or its title would leave the previous value stale — not a
-- pure function of raw.
UPDATE sessions SET
    total_cost_usd = COALESCE(f.cost, 0),
    total_input_tokens = COALESCE(f.input_tokens, 0),
    total_output_tokens = COALESCE(f.output_tokens, 0),
    turn_count = COALESCE(f.turns, 0),
    model_usage = NULL,
    derived_title = NULL,
    derived_model = COALESCE((
        SELECT sp.model FROM spans_20260615 sp
        WHERE sp.session_id = sessions.id
          AND sp.kind = 'llm' AND sp.call_kind = 'main' AND sp.model <> ''
          -- main thread only: subagents run their own (often cheaper)
          -- model, and a fan-out of subagent calls would otherwise
          -- out-vote the user's actual conversation model.
          AND sp.thread_id = ''
        GROUP BY sp.model
        ORDER BY COUNT(*) DESC, sp.model
        LIMIT 1
    ), '')
FROM (
    SELECT s.id,
           SUM(st.total_cost_usd)      AS cost,
           SUM(st.total_input_tokens)  AS input_tokens,
           SUM(st.total_output_tokens) AS output_tokens,
           COUNT(st.trace_id)          AS turns
    FROM sessions s
    LEFT JOIN span_turns_20260615 st ON st.session_id = s.id
    WHERE s.id = ANY(sqlc.arg(session_ids)::uuid[])
    GROUP BY s.id
) f
WHERE sessions.id = f.id;

-- name: AggregateSpanStats :one
-- /v1/stats from the span layer: trace-grain rollups summed over the
-- window, so the numbers agree with what the session detail and trace
-- views show. The node-layer aggregate this replaces summed per-call
-- usage, which re-bills the conversation history on every call.
--
--   turn_count        = traces started in the window
--   total_duration_ns = SUM of trace durations — agent time, not the
--                       wall-clock MAX-MIN window (idle time between
--                       turns no longer counts)
--   tool_calls        = tool spans_20260615 started in the window
WITH matched AS (
    SELECT t.org_id, t.trace_id, t.session_id, t.duration_ns,
           t.total_input_tokens, t.total_output_tokens,
           t.cache_read_tokens, t.cache_creation_tokens, t.total_cost_usd
    FROM span_turns_20260615 t
    WHERE t.org_id = sqlc.arg(org_id)
      AND (sqlc.narg(since_filter)::timestamptz IS NULL OR t.started_at >= sqlc.narg(since_filter)::timestamptz)
      AND (sqlc.narg(until_filter)::timestamptz IS NULL OR t.started_at < sqlc.narg(until_filter)::timestamptz)
)
SELECT
    COUNT(*)::bigint                                        AS turn_count,
    COUNT(DISTINCT session_id)::bigint                      AS session_count,
    COUNT(DISTINCT session_id) FILTER (
        WHERE EXISTS (
            SELECT 1 FROM sessions s
            WHERE s.id = matched.session_id AND s.derived_status = 'completed'
        )
    )::bigint                                               AS completed_count,
    COALESCE(SUM(total_input_tokens), 0)::bigint            AS input_tokens,
    COALESCE(SUM(total_output_tokens), 0)::bigint           AS output_tokens,
    COALESCE(SUM(cache_creation_tokens), 0)::bigint         AS cache_creation_tokens,
    COALESCE(SUM(cache_read_tokens), 0)::bigint             AS cache_read_tokens,
    COALESCE(SUM(duration_ns), 0)::bigint                   AS total_duration_ns,
    COALESCE(SUM(total_cost_usd), 0)::numeric               AS total_cost_usd,
    (SELECT COUNT(*) FROM spans_20260615 sp
     WHERE sp.org_id = sqlc.arg(org_id)
       AND sp.kind = 'tool'
       AND (sqlc.narg(since_filter)::timestamptz IS NULL OR sp.started_at >= sqlc.narg(since_filter)::timestamptz)
       AND (sqlc.narg(until_filter)::timestamptz IS NULL OR sp.started_at < sqlc.narg(until_filter)::timestamptz)
    )::bigint                                               AS tool_calls
FROM matched;
