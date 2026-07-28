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
-- The derive read. raw_response is selected ONLY for turns whose reduction is
-- missing its content blocks — the shape the deriver skips.
--
-- The column runs to the ingest cap, so selecting it unconditionally would pull
-- megabytes through every derive read to be discarded. Selecting it never is
-- what left a failed reduction unrecoverable: the bytes were stored and no read
-- path could reach them. This CASE is the whole difference, and it costs nothing
-- on the common path because a healthy reduction returns NULL here.
--
-- The test is deliberately coarse — "no content blocks" rather than a faithful
-- port of ingest's reducedResponseAbsent. It only decides whether to *fetch*;
-- the caller re-checks authoritatively before reducing, so a false positive
-- costs one wasted read and a false negative is impossible for the case that
-- matters (an empty reduction always lacks content).
SELECT id, org_id, source, provider, agent_name,
       harness_id, harness_session_id, request_id,
       raw_request, response, meta, session_envelope, received_at,
       CASE
           WHEN jsonb_typeof(response -> 'message' -> 'content') = 'array'
                AND jsonb_array_length(response -> 'message' -> 'content') > 0
           THEN NULL
           ELSE raw_response
       END::bytea AS raw_response,
       raw_response_encoding
FROM raw_turns
WHERE id = $1;
