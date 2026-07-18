# Re-derive the span projection from the raw layer. After this, the
# ingested turn exists as a span_turns/spans row set — the input the span
# embed pass selects from. raw_turns/parsed_turns confirm the derive read
# and parsed the ingested turn; the session/trace assertions below prove
# the projection actually landed.

POST http://tapes-api:8081/v1/admin/derive/run
HTTP 200
[Asserts]
jsonpath "$.orgs.default.raw_turns" >= 1
jsonpath "$.orgs.default.parsed_turns" >= 1

# The ingest leg carried a session envelope, so a first-class session
# row exists now (the proxy leg alone contributes none — see
# 03-verify-storage.hurl). The session's counters are the derive-time span
# fold, not the ingest counters: turn_count counts traces, and
# derived_model is the dominant spine model.

GET http://tapes-api:8081/v1/sessions
HTTP 200
[Captures]
session_id: jsonpath "$.items[0].id"
[Asserts]
jsonpath "$.items" count >= 1
jsonpath "$.items[0].total_input_tokens" == 25
jsonpath "$.items[0].total_output_tokens" == 30
jsonpath "$.items[0].turn_count" == 1
jsonpath "$.items[0].model" == "claude-sonnet-4-5"

# Span-layer stats are live post-derive: trace-grain sums, agent time =
# sum of trace durations, stem_count absent on this path.

GET http://tapes-api:8081/v1/stats
HTTP 200
[Asserts]
jsonpath "$.turn_count" >= 1
jsonpath "$.session_count" >= 1
jsonpath "$.input_tokens" >= 25
jsonpath "$.output_tokens" >= 30
jsonpath "$.stem_count" not exists

# Turn cards render from summaries: the derive-time folds carry both
# the prompt and the closing answer line.

GET http://tapes-api:8081/v1/traces?session_id={{session_id}}
HTTP 200
[Asserts]
jsonpath "$.items[0].user_prompt" contains "retry backoff"
jsonpath "$.items[0].response_preview" contains "max-poll-backoff"
