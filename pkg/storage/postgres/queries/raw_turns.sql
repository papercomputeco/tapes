-- name: InsertRawTurn :execrows
-- raw_turns is append-only and immutable: INSERT is the only write this
-- table ever sees. The ON CONFLICT arm matches the partial unique index
-- raw_turns_org_request_uq so a retried POST of the same captured turn
-- (same org, same extproc request_id) is a no-op rather than a
-- duplicate raw row. Writers without a request_id ('') bypass the
-- index and append unconditionally.
INSERT INTO raw_turns (
    org_id, source, provider, agent_name,
    harness_id, harness_session_id, request_id,
    raw_request, response, meta, session_envelope,
    raw_response, raw_response_encoding, raw_response_dropped
) VALUES (
    $1, $2, $3, $4,
    $5, $6, $7,
    $8, $9, $10, $11,
    $12, $13, $14
)
ON CONFLICT (org_id, request_id) WHERE request_id <> '' DO NOTHING;

-- name: ListRawTurns :many
-- Keyset-paginated scan in insertion order, for the re-runnable deriver.
-- Pass after_id = 0 to start from the beginning.
SELECT r.id, r.org_id, r.source, r.provider, r.agent_name,
       COALESCE(c.harness_id, r.harness_id) AS harness_id,
       COALESCE(c.harness_session_id, r.harness_session_id) AS harness_session_id,
       r.request_id, r.raw_request, r.response,
       (CASE WHEN c.id IS NULL THEN r.meta
             ELSE jsonb_set(r.meta, '{thread_id}', to_jsonb(c.thread_id), true)
        END)::jsonb AS meta,
       (CASE WHEN c.id IS NULL THEN r.session_envelope
             WHEN c.parent_harness_session_id IS NULL THEN
                  (COALESCE(r.session_envelope, '{}'::jsonb) - 'parent_harness_session_id') ||
                  jsonb_build_object(
                      'harness_id', c.harness_id,
                      'harness_session_id', c.harness_session_id
                  )
             ELSE COALESCE(r.session_envelope, '{}'::jsonb) || jsonb_build_object(
                      'harness_id', c.harness_id,
                      'harness_session_id', c.harness_session_id,
                      'parent_harness_session_id', c.parent_harness_session_id
                  )
        END)::jsonb AS session_envelope,
       r.received_at,
       r.raw_response, r.raw_response_encoding, r.raw_response_dropped
FROM raw_turns r
LEFT JOIN LATERAL (
    SELECT id, harness_id, harness_session_id, thread_id, parent_harness_session_id
    FROM raw_turn_attribution_corrections
    WHERE org_id = r.org_id AND raw_turn_id = r.id
    ORDER BY id DESC LIMIT 1
) c ON TRUE
WHERE r.id > sqlc.arg(after_id)
ORDER BY r.id
LIMIT sqlc.arg(page_size);

-- name: ListRawTurnsBySession :many
-- Every raw turn captured for one harness session, in insertion order.
SELECT r.id, r.org_id, r.source, r.provider, r.agent_name,
       COALESCE(c.harness_id, r.harness_id) AS harness_id,
       COALESCE(c.harness_session_id, r.harness_session_id) AS harness_session_id,
       r.request_id, r.raw_request, r.response,
       (CASE WHEN c.id IS NULL THEN r.meta
             ELSE jsonb_set(r.meta, '{thread_id}', to_jsonb(c.thread_id), true)
        END)::jsonb AS meta,
       (CASE WHEN c.id IS NULL THEN r.session_envelope
             WHEN c.parent_harness_session_id IS NULL THEN
                  (COALESCE(r.session_envelope, '{}'::jsonb) - 'parent_harness_session_id') ||
                  jsonb_build_object(
                      'harness_id', c.harness_id,
                      'harness_session_id', c.harness_session_id
                  )
             ELSE COALESCE(r.session_envelope, '{}'::jsonb) || jsonb_build_object(
                      'harness_id', c.harness_id,
                      'harness_session_id', c.harness_session_id,
                      'parent_harness_session_id', c.parent_harness_session_id
                  )
        END)::jsonb AS session_envelope,
       r.received_at,
       r.raw_response, r.raw_response_encoding, r.raw_response_dropped
FROM raw_turns r
LEFT JOIN LATERAL (
    SELECT id, harness_id, harness_session_id, thread_id, parent_harness_session_id
    FROM raw_turn_attribution_corrections
    WHERE org_id = r.org_id AND raw_turn_id = r.id
    ORDER BY id DESC LIMIT 1
) c ON TRUE
WHERE r.org_id = $1
  AND COALESCE((
      SELECT c2.harness_session_id FROM raw_turn_attribution_corrections c2
      WHERE c2.org_id = r.org_id AND c2.raw_turn_id = r.id
      ORDER BY c2.id DESC LIMIT 1
  ), r.harness_session_id) = $2
ORDER BY r.id;

-- name: CountRawTurns :one
SELECT COUNT(*) FROM raw_turns;

-- name: ListRawTurnHeadersBySession :many
-- Operator wire log: identity + sizes, no payloads. The raw layer is
-- the capture truth; this surfaces it without shipping the blobs.
SELECT r.id, r.org_id, r.source, r.provider, r.agent_name, r.request_id,
       r.received_at,
       (CASE WHEN c.id IS NULL THEN r.meta
             ELSE jsonb_set(r.meta, '{thread_id}', to_jsonb(c.thread_id), true)
        END)::jsonb AS meta,
       COALESCE(length(r.raw_request::text), 0)::bigint AS request_bytes,
       COALESCE(length(r.response::text), 0)::bigint AS response_bytes,
       COALESCE(octet_length(r.raw_response), 0)::bigint AS raw_response_bytes,
       r.raw_response_dropped
FROM raw_turns r
LEFT JOIN LATERAL (
    SELECT id, harness_id, harness_session_id, thread_id
    FROM raw_turn_attribution_corrections
    WHERE org_id = r.org_id AND raw_turn_id = r.id
    ORDER BY id DESC LIMIT 1
) c ON TRUE
WHERE r.org_id = $1
  AND COALESCE((
      SELECT c2.harness_id FROM raw_turn_attribution_corrections c2
      WHERE c2.org_id = r.org_id AND c2.raw_turn_id = r.id
      ORDER BY c2.id DESC LIMIT 1
  ), r.harness_id) = $2
  AND COALESCE((
      SELECT c2.harness_session_id FROM raw_turn_attribution_corrections c2
      WHERE c2.org_id = r.org_id AND c2.raw_turn_id = r.id
      ORDER BY c2.id DESC LIMIT 1
  ), r.harness_session_id) = $3
ORDER BY r.id ASC;

-- name: RawTurnFidelityByIDs :many
-- Provenance tier for a set of raw turns, for stamping the span projection.
--
-- The projection needs to know whether each row can be re-derived from stored
-- bytes, which is a fact about how the turn was captured — not something the
-- deriver can know, since it is a pure function of the rows it is handed and
-- must not read storage state. Resolving it here keeps that separation while
-- still letting the write stamp it.
-- COALESCE so the column types as a plain bool rather than a nullable one:
-- the predicate cannot actually be NULL, and a three-valued result would push
-- an "unknown" case into the caller that has no meaning here.
SELECT id,
       COALESCE(octet_length(raw_response) > 0, false)::boolean AS has_raw_response,
       raw_response_dropped
FROM raw_turns
WHERE id = ANY(sqlc.arg(ids)::bigint[]);

-- name: FindRawTurnIDsByPaperProxyRequestID :many
SELECT id
FROM raw_turns
WHERE org_id = sqlc.arg(org_id)
  AND session_envelope->'harness_metadata'->>'paperProxyRequestId' = sqlc.arg(paper_proxy_request_id)::text
ORDER BY id
LIMIT 2;

-- name: GetRawTurnAttributionForUpdate :one
-- Locks the immutable row as a serialization point for competing repairs.
SELECT r.id, r.org_id, r.session_envelope, r.received_at,
       COALESCE(c.harness_id, r.harness_id) AS harness_id,
       COALESCE(c.harness_session_id, r.harness_session_id) AS harness_session_id,
       COALESCE(c.thread_id, r.meta->>'thread_id', '') AS thread_id,
       (c.id IS NOT NULL)::boolean AS has_correction,
       COALESCE(c.parent_harness_session_id, '') AS corrected_parent_harness_session_id,
       COALESCE(r.session_envelope->>'parent_harness_session_id', '')::text AS raw_parent_harness_session_id
FROM raw_turns r
LEFT JOIN LATERAL (
    SELECT id, harness_id, harness_session_id, thread_id, parent_harness_session_id
    FROM raw_turn_attribution_corrections
    WHERE org_id = r.org_id AND raw_turn_id = r.id
    ORDER BY id DESC LIMIT 1
) c ON TRUE
WHERE r.org_id = sqlc.arg(org_id) AND r.id = sqlc.arg(raw_turn_id)
FOR UPDATE OF r;

-- name: InsertRawTurnAttributionCorrection :exec
INSERT INTO raw_turn_attribution_corrections (
    org_id, raw_turn_id, harness_id, harness_session_id, thread_id,
    parent_harness_session_id, reason
) VALUES (
    sqlc.arg(org_id), sqlc.arg(raw_turn_id), sqlc.arg(harness_id),
    sqlc.arg(harness_session_id), sqlc.arg(thread_id),
    sqlc.narg(parent_harness_session_id), sqlc.arg(reason)
);
