# Verify that conversation history can be retrieved from the API.
# At least one conversation should exist after the chat request.

GET http://tapes-api:8081/dag/history
HTTP 200
[Asserts]
jsonpath "$.histories" count >= 1
