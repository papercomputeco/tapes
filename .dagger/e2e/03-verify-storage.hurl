# Verify that the proxy persisted conversation data into the Postgres-backed storage.
# The async worker pool needs a moment to flush, so the orchestration script
# should add a brief delay before running this file.

GET http://tapes-api:8081/dag/stats
HTTP 200
[Asserts]
jsonpath "$.total_nodes" >= 2
jsonpath "$.root_count" >= 1
jsonpath "$.leaf_count" >= 1
