# Prompt cassette

A minimal [Tapes](https://github.com/papercomputeco/tapes) cassette that returns
a session's first captured user prompt and advertises it as an MCP tool.

## Run

```sh
docker compose up --build
curl http://127.0.0.1:9999/openapi
```

Compose starts PostgreSQL, Tapes (read API on `8081`, ingest on `8082`), and the
cassette on `9999`. Tapes loads the cassette from `http://prompt:9999/openapi`.

The operation-level `x-tapes-mcp` declaration publishes
`prompt.get_prompt` through Tapes' streamable HTTP MCP endpoint at
`http://127.0.0.1:8081/v1/mcp`.

The cassette also works directly over HTTP:

```sh
curl -X POST http://127.0.0.1:9999/api/prompt/get \
  -H 'Content-Type: application/json' \
  -d '{"session_id":"00000000-0000-0000-0000-000000000001"}'
```

## Local Python with `uv`

```sh
uv venv
source .venv/bin/activate
uv sync
python main.py
```
