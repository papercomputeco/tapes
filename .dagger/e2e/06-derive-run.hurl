# Re-derive the node + span projection from the raw layer. After this,
# the ingested turn exists as a span_turns/spans row set — the input
# the span embed pass selects from.

POST http://tapes-api:8081/v1/admin/derive/run
HTTP 200
[Asserts]
jsonpath "$.orgs.default.raw_turns" >= 1
jsonpath "$.orgs.default.parsed_turns" >= 1
jsonpath "$.orgs.default.upserted" >= 2

# The ingest leg carried a session envelope, so a first-class session
# row exists now (the proxy leg alone contributes none — see
# 04-history.hurl).

GET http://tapes-api:8081/v1/sessions
HTTP 200
[Asserts]
jsonpath "$.items" count >= 1
