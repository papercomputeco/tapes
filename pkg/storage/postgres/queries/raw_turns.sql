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
    raw_request, response, meta, session_envelope
) VALUES (
    $1, $2, $3, $4,
    $5, $6, $7,
    $8, $9, $10, $11
)
ON CONFLICT (org_id, request_id) WHERE request_id <> '' DO NOTHING;

-- name: ListRawTurns :many
-- Keyset-paginated scan in insertion order, for the re-runnable deriver.
-- Pass after_id = 0 to start from the beginning.
SELECT id, org_id, source, provider, agent_name,
       harness_id, harness_session_id, request_id,
       raw_request, response, meta, session_envelope, received_at
FROM raw_turns
WHERE id > sqlc.arg(after_id)
ORDER BY id
LIMIT sqlc.arg(page_size);

-- name: ListRawTurnsBySession :many
-- Every raw turn captured for one harness session, in insertion order.
SELECT id, org_id, source, provider, agent_name,
       harness_id, harness_session_id, request_id,
       raw_request, response, meta, session_envelope, received_at
FROM raw_turns
WHERE org_id = $1
  AND harness_session_id = $2
ORDER BY id;

-- name: CountRawTurns :one
SELECT COUNT(*) FROM raw_turns;

-- name: ListRawTurnHeadersBySession :many
-- Operator wire log: identity + sizes, no payloads. The raw layer is
-- the capture truth; this surfaces it without shipping the blobs.
SELECT id, org_id, source, provider, agent_name, request_id,
       received_at, meta,
       COALESCE(length(raw_request::text), 0)::bigint AS request_bytes,
       COALESCE(length(response::text), 0)::bigint AS response_bytes
FROM raw_turns
WHERE org_id = $1 AND harness_id = $2 AND harness_session_id = $3
ORDER BY id ASC;
