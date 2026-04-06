# Codex ChatGPT Transport Probe

This folder contains a standalone Python relay used to answer one question:

- can Codex model traffic be forced through a local proxy in a form we can record?

It is a reference adapter, not the final product path. It forwards requests as raw as possible, then reconstructs turns on the side for ingest/debugging.

## How It Works

The probe runs two listeners:

- HTTP on `127.0.0.1:8765`
- WebSocket on `127.0.0.1:8766`

For HTTP traffic it:

1. logs the incoming request
2. rewrites local Codex paths to ChatGPT backend paths
3. forwards the original body upstream with minimal header changes
4. logs the upstream response
5. parses streamed `/responses` events and reconstructs a best-effort final turn

For WebSocket traffic it:

1. logs the upgrade
2. relays frames upstream
3. records response events until a turn completes

The probe redacts bearer tokens and cookies before printing logs.

## Event Recording

The recorder is intentionally simple:

- it watches for `response.created`
- it accumulates text from `response.output_text.delta` and related events
- it stops on `response.done`, `response.completed`, or `response.failed`
- it reconstructs a provider-agnostic request/response pair

For HTTP `/responses`, it parses SSE event lines from the upstream body and feeds them through the same recorder.

This is enough to prove:

- the request reached the proxy
- the upstream stream succeeded
- a final turn can be reconstructed for storage

It is not trying to preserve every low-level transport detail. It is optimized for transport debugging and ingest viability.

## Setup

```bash
cd /home/alex/tapes/test/codex-chatgpt-proxy
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
python3 proxy_probe.py
```

If you want the probe to post reconstructed turns into Tapes ingest:

```bash
python3 proxy_probe.py --ingest-url http://127.0.0.1:8082/v1/ingest
```

## Run Codex Directly

This is the direct sanity check that exercises the model path:

```bash
env -u OPENAI_API_KEY -u OPENAI_BASE_URL -u OPENAI_API_BASE \
  codex \
  -c 'model_provider="openai-custom"' \
  -c 'model_providers.openai-custom={name="OpenAI Custom",base_url="http://127.0.0.1:8765/v1",wire_api="responses"}'
```


