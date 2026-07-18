# Demo seeding round trip: the admin endpoint replays the bundled
# capture corpora through the normal ingest write path (raw turns +
# sessions) and derives the seeded sessions synchronously. Seeded data
# is real capture, so the whole raw → derive → span pipeline runs.

POST http://tapes-api:8081/v1/admin/seed/demo
HTTP 200
[Asserts]
jsonpath "$.sessions" == 2
jsonpath "$.raw_turns" == 216
jsonpath "$.raw_turns_inserted" == 216
jsonpath "$.raw_turns_deduped" == 0

# Re-seeding is a no-op: every replayed turn dedupes at the raw layer
# and the derive pass is idempotent.

POST http://tapes-api:8081/v1/admin/seed/demo
HTTP 200
[Asserts]
jsonpath "$.sessions" == 2
jsonpath "$.raw_turns_inserted" == 0
jsonpath "$.raw_turns_deduped" == 216

# The demo sessions are first-class session rows (newest by
# last_seen_at, so they lead the list) and carry the demo marker in
# harness_metadata.

GET http://tapes-api:8081/v1/sessions
HTTP 200
[Captures]
demo_session_id: jsonpath "$.items[0].id"
[Asserts]
jsonpath "$.items" count >= 3
jsonpath "$.items[0].harness_metadata.demo" == true
jsonpath "$.items[1].harness_metadata.demo" == true
jsonpath "$.items[0].rollup.turn_count" >= 1

# The seeded sessions browse exactly like captured ones: turn
# summaries from the span projection, with derive-time prompt and
# answer folds.

GET http://tapes-api:8081/v1/traces?session_id={{demo_session_id}}
HTTP 200
[Asserts]
jsonpath "$.items" count >= 1
jsonpath "$.items[0].trace_id" startsWith "trc_"
