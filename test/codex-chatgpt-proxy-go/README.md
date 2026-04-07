# Codex ChatGPT Go Transport Probe

This is a standalone Go relay that mirrors the Python probe more closely than Tapes does.

It exists to answer one question:

- can a Go relay forward Codex traffic with minimal transformation and still reconstruct a turn?

## What It Does

- listens on `127.0.0.1:8765`
- accepts HTTP `POST /responses` and `POST /v1/responses`
- rewrites them to `https://chatgpt.com/backend-api/codex/responses`
- forwards the original body with minimal header stripping
- streams the upstream response back to Codex
- parses streamed events on the side and logs a reconstructed turn
- exposes `/ws` with Fiber websocket so a WS relay path can be tested in the same binary

## Header Handling

Upstream requests strip only:

- `Host`
- `Content-Length`
- `Connection`
- `Upgrade`
- `Accept-Encoding`

Client responses strip only:

- `Connection`
- `Transfer-Encoding`
- `Content-Encoding`
- `Content-Length`

That is intentionally close to the Python probe behavior.

## Run

```bash
cd /home/alex/tapes/test/codex-chatgpt-proxy-go
go run .
```

## Direct Codex Test

```bash
env -u OPENAI_API_KEY -u OPENAI_BASE_URL -u OPENAI_API_BASE \
  codex \
  -c 'model_provider="openai-custom"' \
  -c 'model_providers.openai-custom={name="OpenAI Custom",base_url="http://127.0.0.1:8765/v1",wire_api="responses"}'
```

## Good Run

You want to see:

- `http_request` on `/responses` or `/v1/responses`
- `http_response` with `status: 200`
- `http_turn_probe`
- `turn_reconstructed`

For websocket experiments, you want to see:

- `ws_upgrade`
- `turn_reconstructed`
