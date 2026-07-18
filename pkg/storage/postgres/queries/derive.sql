-- name: SessionIDByHarnessKey :one
-- Resolve the sessions row for one raw turn's natural key. The deriver
-- only attributes to existing sessions; it never creates them.
SELECT id FROM sessions
WHERE org_id = $1
  AND harness_id = $2
  AND harness_session_id = $3;

-- name: ListRawTurnIndex :many
-- Lightweight scan for the deriver's ordering pass: identity and
-- timing only, no payloads. meta rides along because it carries the
-- original capture time for backfilled rows.
SELECT id, org_id, source, harness_id, harness_session_id, received_at, meta
FROM raw_turns
WHERE id > sqlc.arg(after_id)
ORDER BY id
LIMIT sqlc.arg(page_size);

-- name: GetRawTurn :one
SELECT id, org_id, source, provider, agent_name,
       harness_id, harness_session_id, request_id,
       raw_request, response, meta, session_envelope, received_at
FROM raw_turns
WHERE id = $1;
