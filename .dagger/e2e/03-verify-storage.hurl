# Verify that the proxy persisted conversation data into the Postgres-backed storage.
# The async worker pool needs a moment to flush, so the orchestration script
# should add a brief delay before running this file.
#
# session_count is session-grained (distinct sessions rows touched by the
# matching nodes — PCC-565). The proxy path carries no session envelope, so
# it contributes stems and turns but no first-class session; assert
# stem_count here. The ingest leg (05+) is the one that creates a session.

GET http://tapes-api:8081/v1/stats
HTTP 200
[Asserts]
jsonpath "$.turn_count" >= 2
jsonpath "$.root_count" >= 1
jsonpath "$.stem_count" >= 1
