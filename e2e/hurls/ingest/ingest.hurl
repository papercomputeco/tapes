# Tapes ingest server e2e validation.
# Run against a live ingest server:
#   hurl --variable host=http://localhost:8090 e2e/hurls/ingest/ingest.hurl

# ──────────────────────────────────────────────
# 1. Health check
# ──────────────────────────────────────────────

GET {{host}}/ping
HTTP 200
[Asserts]
jsonpath "$.status" == "ok"

# ──────────────────────────────────────────────
# 2. Single turn — OpenAI format
# ──────────────────────────────────────────────

POST {{host}}/v1/ingest
Content-Type: application/json
{
  "provider": "openai",
  "agent_name": "e2e-test",
  "request": {
    "model": "gpt-4",
    "messages": [
      { "role": "user", "content": "What is content addressing?" }
    ]
  },
  "response": {
    "id": "chatcmpl-e2e-001",
    "object": "chat.completion",
    "model": "gpt-4",
    "choices": [
      {
        "index": 0,
        "message": { "role": "assistant", "content": "Content addressing is a method of storing and retrieving data based on the content itself, rather than its location." },
        "finish_reason": "stop"
      }
    ],
    "usage": {
      "prompt_tokens": 12,
      "completion_tokens": 25,
      "total_tokens": 37
    }
  }
}
HTTP 202
[Asserts]
jsonpath "$.status" == "accepted"

# ──────────────────────────────────────────────
# 3. Single turn — Anthropic format
# ──────────────────────────────────────────────

POST {{host}}/v1/ingest
Content-Type: application/json
{
  "provider": "anthropic",
  "agent_name": "e2e-test",
  "request": {
    "model": "claude-sonnet-4-20250514",
    "max_tokens": 1024,
    "system": "You are a helpful assistant.",
    "messages": [
      { "role": "user", "content": "Explain Merkle DAGs in one sentence." }
    ]
  },
  "response": {
    "id": "msg_e2e_002",
    "type": "message",
    "role": "assistant",
    "model": "claude-sonnet-4-20250514",
    "content": [
      { "type": "text", "text": "A Merkle DAG is a directed acyclic graph where each node is identified by a cryptographic hash of its content and links." }
    ],
    "stop_reason": "end_turn",
    "usage": {
      "input_tokens": 20,
      "output_tokens": 30
    }
  }
}
HTTP 202
[Asserts]
jsonpath "$.status" == "accepted"

# ──────────────────────────────────────────────
# 4. Single turn — Ollama format
# ──────────────────────────────────────────────

POST {{host}}/v1/ingest
Content-Type: application/json
{
  "provider": "ollama",
  "agent_name": "e2e-test",
  "request": {
    "model": "llama3",
    "messages": [
      { "role": "user", "content": "Hello" }
    ],
    "stream": false
  },
  "response": {
    "model": "llama3",
    "created_at": "2025-01-01T00:00:00Z",
    "message": { "role": "assistant", "content": "Hi there! How can I help you?" },
    "done": true,
    "done_reason": "stop",
    "prompt_eval_count": 5,
    "eval_count": 10,
    "total_duration": 500000000
  }
}
HTTP 202
[Asserts]
jsonpath "$.status" == "accepted"

# ──────────────────────────────────────────────
# 5. Batch ingest — mixed providers
# ──────────────────────────────────────────────

POST {{host}}/v1/ingest/batch
Content-Type: application/json
{
  "turns": [
    {
      "provider": "openai",
      "agent_name": "e2e-batch",
      "request": {
        "model": "gpt-4",
        "messages": [{ "role": "user", "content": "Batch turn 1" }]
      },
      "response": {
        "id": "chatcmpl-e2e-batch-1",
        "object": "chat.completion",
        "model": "gpt-4",
        "choices": [{ "index": 0, "message": { "role": "assistant", "content": "Batch response 1" }, "finish_reason": "stop" }],
        "usage": { "prompt_tokens": 5, "completion_tokens": 5, "total_tokens": 10 }
      }
    },
    {
      "provider": "ollama",
      "agent_name": "e2e-batch",
      "request": {
        "model": "llama3",
        "messages": [{ "role": "user", "content": "Batch turn 2" }],
        "stream": false
      },
      "response": {
        "model": "llama3",
        "created_at": "2025-01-01T00:00:00Z",
        "message": { "role": "assistant", "content": "Batch response 2" },
        "done": true
      }
    }
  ]
}
HTTP 202
[Asserts]
jsonpath "$.accepted" == 2
jsonpath "$.rejected" == 0

# ──────────────────────────────────────────────
# 6. Error — unsupported provider
# ──────────────────────────────────────────────

POST {{host}}/v1/ingest
Content-Type: application/json
{
  "provider": "unknown-provider",
  "request": { "model": "x", "messages": [] },
  "response": {}
}
HTTP 422
[Asserts]
jsonpath "$.error" contains "unsupported provider"

# ──────────────────────────────────────────────
# 7. Error — malformed JSON
# ──────────────────────────────────────────────

POST {{host}}/v1/ingest
Content-Type: application/json
`{not valid json}`
HTTP 400

# ──────────────────────────────────────────────
# 8. Batch partial failure
# ──────────────────────────────────────────────

POST {{host}}/v1/ingest/batch
Content-Type: application/json
{
  "turns": [
    {
      "provider": "openai",
      "request": {
        "model": "gpt-4",
        "messages": [{ "role": "user", "content": "Valid turn" }]
      },
      "response": {
        "id": "chatcmpl-e2e-partial",
        "object": "chat.completion",
        "model": "gpt-4",
        "choices": [{ "index": 0, "message": { "role": "assistant", "content": "OK" }, "finish_reason": "stop" }],
        "usage": { "prompt_tokens": 3, "completion_tokens": 1, "total_tokens": 4 }
      }
    },
    {
      "provider": "bad-provider",
      "request": {},
      "response": {}
    }
  ]
}
HTTP 202
[Asserts]
jsonpath "$.accepted" == 1
jsonpath "$.rejected" == 1
jsonpath "$.errors" count == 1
