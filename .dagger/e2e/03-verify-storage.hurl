# Verify the derived read model is empty after the proxy chat leg but
# before any ingest/derive has run.
#
# /v1/stats reads the span layer exclusively — the deriver is the single
# writer of session/trace totals, and the retired merkle node/stem layer
# no longer backs a legacy per-model fallback. So the proxy chat leg (02),
# which carries no session envelope and does not populate the derived
# surface, must leave stats at the zero state until the ingest (05) and
# derive (06) legs land real captured data.
#
# This is the pre-derive checkpoint: it proves the proxy leg does not leak
# into the derived read model, and it guards against the removed
# root_count / stem_count fields creeping back into the stats response.

GET http://tapes-api:8081/v1/stats
HTTP 200
[Asserts]
jsonpath "$.turn_count" == 0
jsonpath "$.session_count" == 0
jsonpath "$.root_count" not exists
jsonpath "$.stem_count" not exists
