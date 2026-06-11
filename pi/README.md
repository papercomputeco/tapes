# Pi → Tapes span-context POC

This directory contains the Pi extension that closes the loop for the span-model POC.

## How it works

1. `tapes_span_extension.ts` registers a `tapes` Pi provider that uses OpenAI Chat Completions over `TAPES_PROXY_URL`.
2. On `session_start`, the extension selects that `tapes` provider by default, so a plain `pi -e ...` run traverses the Tapes proxy instead of Pi's configured default provider (for example `openai-codex`).
3. It creates trace/span IDs inside Pi:
   - one `trc_*` trace per user turn;
   - one `agent_*` root span for the turn;
   - one `llm_*` span for each provider request.
4. The extension patches `globalThis.fetch` and injects those IDs as `X-Pi-*` headers when the provider request is sent to the configured Tapes/Paper proxy. The local proxy also accepts `X-Tapes-*` aliases for direct tests.
5. The Tapes proxy reads those headers, strips them before forwarding upstream, and puts the IDs on `worker.Job.SpanContext`.
6. With the default local stack (`proxy.provider=ollama`, upstream `http://localhost:11434`), Tapes routes the OpenAI-compatible request to Ollama's `/v1` compatibility endpoint.
7. The Postgres span ingester stores the same IDs in `span_turns` and `spans`.
8. The API exposes the result via:
   - `GET /v1/traces`
   - `GET /v1/traces/:trace_id`

## Run

Start the local stack and server:

```bash
tapes local up
tapes serve --postgres "postgres://tapes:tapes@localhost:5432/tapes?sslmode=disable" --debug
```

Then launch Pi with a chat model that exists in your local Ollama instance:

```bash
TAPES_PROXY_URL=http://localhost:8080 \
TAPES_MODEL=qwen3-coder:30b \
  pi -e ./pi/tapes_span_extension.ts
```

If `TAPES_MODEL` is omitted, the extension uses `qwen3-coder:30b`. Pull it first if needed:

```bash
docker exec -it tapes-local-ollama ollama pull qwen3-coder:30b
```

If you intentionally want to keep Pi's currently selected provider/model and only override built-in provider URLs, set:

```bash
TAPES_PROXY_PRESERVE_MODEL=1
```

That mode will not route unsupported providers such as `openai-codex` through the local span POC.

If your Pi provider transport does not expose a URL matching `TAPES_PROXY_URL`, set this for the POC:

```bash
TAPES_SPAN_INJECT_ALL=1 pi -e ./pi/tapes_span_extension.ts
```

`TAPES_SPAN_INJECT_ALL=1` intentionally injects into every fetch made during an active provider request, so use it only for local testing.

## Headers

- `X-Pi-Trace-Id`
- `X-Pi-Turn-Id`
- `X-Pi-Root-Span-Id`
- `X-Pi-Llm-Span-Id`
- `X-Pi-Parent-Span-Id`
- `X-Tapes-Pi-Session-Id` (diagnostic only in this POC)

The proxy strips these from upstream requests.
