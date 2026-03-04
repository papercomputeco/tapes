# Send a non-streaming chat request through the proxy to Ollama.
# This exercises the full proxy -> upstream -> storage pipeline.

POST http://tapes-proxy:8080/api/chat
{
  "model": "qwen3:0.6b",
  "messages": [
    {
      "role": "user",
      "content": "Say hello in exactly 3 words. Do not use any other words. /no_think"
    }
  ],
  "stream": false
}
HTTP 200
[Asserts]
jsonpath "$.message.role" == "assistant"
jsonpath "$.message.content" isString
