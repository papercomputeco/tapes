# Verify the API server is healthy and responding.

GET http://tapes-api:8081/ping
HTTP 200
`"pong"`
