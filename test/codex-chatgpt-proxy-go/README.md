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

## Run Modes

There are two different ways to use this work:

- standalone Go probe flow
- Tapes integration flow

They are not the same test.

### Standalone Go Probe Flow

Use this when you want to test the raw relay itself, without Tapes in the loop.

Start the Go probe:

```bash
make run-codex-proxy
```

Then launch Codex directly against it:

```bash
make run-codex-direct
```

### Tapes Integration Flow

Use this when you want to test whether Tapes itself captures and stores Codex sessions.

This does **not** use the standalone Go probe.

Run:

```bash
make run-codex
```

Then inspect Tapes logs:

```bash
tail -n 120 /tmp/tapes-codex-oauth/start.log
```

and verify stored sessions in Deck:

```bash
./build/tapes deck
```

## Good Run For The Standalone Probe

You want to see:

- `http_request` on `/responses` or `/v1/responses`
- `http_response` with `status: 200`
- `http_turn_probe`
- `turn_reconstructed`

For websocket experiments, you want to see:

- `ws_upgrade`
- `turn_reconstructed`
