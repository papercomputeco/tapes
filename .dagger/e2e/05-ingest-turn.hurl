# Ingest one completed turn through the sidecar path. This lands a raw
# turn (the derive input) plus a session row, so the derive -> embed ->
# span-search legs that follow operate on real captured data.
#
# stream + a non-empty tool set matter: the classifier's tell for a
# conversation-spine (call_kind=main) turn, and only main llm spans
# are embed candidates.

POST http://tapes-ingest:8082/v1/ingest
{
  "provider": "anthropic",
  "agent_name": "e2e",
  "request": {
    "model": "claude-sonnet-4-5",
    "max_tokens": 512,
    "stream": true,
    "tools": [
      {
        "name": "Bash",
        "description": "Run a shell command",
        "input_schema": {
          "type": "object",
          "properties": {
            "command": { "type": "string" }
          }
        }
      }
    ],
    "messages": [
      {
        "role": "user",
        "content": "How do I configure the retry backoff for the tapes derive worker?"
      }
    ]
  },
  "response": {
    "model": "claude-sonnet-4-5",
    "message": {
      "role": "assistant",
      "content": [
        {
          "type": "text",
          "text": "Use the --max-poll-backoff flag on the derive worker; poll failures back off exponentially up to that cap."
        }
      ]
    },
    "done": true,
    "stop_reason": "end_turn",
    "usage": {
      "prompt_tokens": 25,
      "completion_tokens": 30,
      "total_tokens": 55
    }
  },
  "meta": {
    "request_id": "req_e2e_embed_001",
    "model": "claude-sonnet-4-5"
  },
  "session": {
    "org_id": "",
    "auth_subject": "e2e",
    "harness_id": "e2e-harness",
    "harness_session_id": "e2e-session-embed"
  }
}
HTTP 202
[Asserts]
jsonpath "$.status" == "accepted"
