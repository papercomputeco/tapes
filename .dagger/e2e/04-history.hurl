# Verify that sessions can be listed from the API.
# At least one session should exist after the chat request.

GET http://tapes-api:8081/v1/sessions
HTTP 200
[Asserts]
jsonpath "$.items" count >= 1
