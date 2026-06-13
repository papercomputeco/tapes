# Verify that the proxy persisted conversation data into the Postgres-backed storage.
# The async worker pool needs a moment to flush, so the orchestration script
# should add a brief delay before running this file.
#
# /v1/stats reads the span layer on this backend, and the span
# projection is empty until the derive leg (06) runs — so the bare
# stats call asserts the pre-derive zero state. The node-layer numbers
# the proxy leg wrote are still reachable through the legacy fallback,
# which any per-node filter (here: the chat leg's model) forces;
# stem_count only exists on that path. The proxy path carries no
# session envelope, so it contributes stems and turns but no
# first-class session (PCC-565) — the ingest leg (05+) creates the
# session.
#
# With /v1/stems gone, this legacy-fallback read is also the e2e's
# proxy-leg persistence check: stem_count >= 1 proves the proxy's
# captured chain landed as Merkle leaves in Postgres.

GET http://tapes-api:8081/v1/stats
HTTP 200
[Asserts]
jsonpath "$.turn_count" == 0
jsonpath "$.session_count" == 0

GET http://tapes-api:8081/v1/stats?model=qwen3:0.6b
HTTP 200
[Asserts]
jsonpath "$.turn_count" >= 2
jsonpath "$.root_count" >= 1
jsonpath "$.stem_count" >= 1
