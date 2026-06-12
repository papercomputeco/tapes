# Verify the proxy leg's capture history is readable from the API.
# Session rows are envelope-driven on this surface, and the bare proxy
# chat carries no session envelope — so history here means stems, not
# sessions. The sessions-list assertion lives in 06-derive-run.hurl,
# after the ingest leg has landed an envelope-bearing turn.

GET http://tapes-api:8081/v1/stems
HTTP 200
[Asserts]
jsonpath "$.items" count >= 1
