---
title: Installation and local setup
description: Install the server and the client, bootstrap the local PostgreSQL and Ollama dependencies, and start Tapes.
sidebar:
  order: 2
---

## Install a release

`tapes` is the server — it runs the services and owns the database:

```bash
curl -fsSL https://download.tapes.dev/install | bash
tapes version
```

[`tapesctl`](https://github.com/papercomputeco/tapesctl) is the client. It
captures sessions and reads them back, and is what you use day to day:

```bash
curl -sSfL https://download.tapes.dev/tapesctl/install | bash
tapesctl version
```

Each `version` call is an install smoke test: it proves the binary is on your
`PATH` and runs.

## Bootstrap local dependencies

Tapes uses PostgreSQL as its storage backend and pgvector for semantic search. The bundled bootstrap requires Docker and provisions:

- PostgreSQL with pgvector and pg_duckdb;
- Ollama for local embeddings. It reuses a running native server when available, or starts an Ollama container when Ollama is not installed. If native Ollama is installed but stopped, the command tells you to start it and pull the model.

```bash
tapes local up
tapes local status
```

The default PostgreSQL port is `5432`, Ollama port is `11434`, and embedding model is `embeddinggemma`. To force Ollama into Docker:

```bash
tapes local up --docker-ollama
```

The PostgreSQL data directory lives under the active `.tapes/` directory. Stopping containers preserves it:

```bash
tapes local down
```

Delete both containers and captured PostgreSQL data only when a reset is intended:

```bash
tapes local down --wipe
```

> `--wipe` permanently removes locally captured sessions.

## Start Tapes

```bash
tapes serve
```

Defaults are proxy `:8080`, read API `:8081`, private ingest API `:8082`, Ollama upstream `http://localhost:11434`, and background span embedding enabled. Verify the read API and configuration:

```bash
curl http://localhost:8081/ping
tapes status
```

The client defaults reads to this API and capture to the ingest service on
`http://localhost:8082`.

Seed representative capture data through the normal ingest and derive path:

```bash
tapesctl seed
tapesctl sessions list
```

Capture commands address the **ingest** port instead. Override its local default
with `--ingest-url` or `TAPES_INGEST_URL`. See [Agent integrations](./integrations.md).

## Run the complete Docker Compose stack

From the repository root, with Docker Engine and Docker Compose v2 installed:

```bash
make help
make validate-envoy
make up
```

`make up` runs `docker compose up --build` in the foreground. The default stack
now uses **stock Envoy v1.37.0**, not the Go proxy or Envoy AI Gateway. It builds
`tapes-extproc` for capture and runs `serve api`, `serve ingest`, and
`serve derive-worker` as separate services. PostgreSQL and the search, skills,
and export cassettes remain; Ollama is for embeddings/skills, **not capture**.
The first run downloads images and the `embeddinggemma` model into `~/.ollama`.
Stop any other local Tapes stack first to free its ports. Use `make down` to
stop this stack without deleting volumes; do not add `--volumes` to preserve data.

| Surface | Host URL | Container destination |
| --- | --- | --- |
| LLM forwarding | `http://localhost:8080` | `envoy:8080` |
| Read API | `http://localhost:8081` | `tapes.svc:8081` (alias of `tapes`) |
| Capture ingest | `http://localhost:8082` | `tapes-ingest:8082` |

The Envoy config bind mount uses `:ro,Z`: it stays read-only and receives a
private SELinux container label on enforcing hosts. If upgrading an already
created container, recreate Envoy (`docker compose up -d --force-recreate envoy`)
to apply the mount label; do not disable SELinux or use privileged mode. Avoid
running config validation concurrently with Envoy: private relabeling assigns
the file to the most recently created container.

All published ports bind host loopback. Extproc gRPC `50051` and metrics `9090`
are not published; Envoy admin `9901` listens only on its container's loopback.
This is an unauthenticated **local POC**, not a multi-tenant gateway. Do not
expose it publicly or send secrets in URL query strings.

### Provider routing and credentials

Only these POST routes are forwarded (an optional trailing slash and query
parameters are allowed):

| Local path | Upstream | SDK base URL |
| --- | --- | --- |
| `/v1/messages` | `https://api.anthropic.com/v1/messages` | Anthropic: `http://localhost:8080` |
| `/v1/responses` | `https://api.openai.com/v1/responses` | OpenAI: `http://localhost:8080/v1` |
| `/v1/chat/completions` | `https://api.openai.com/v1/chat/completions` | OpenAI: `http://localhost:8080/v1` |

Envoy preserves paths/query strings and rewrites upstream authority. DNS, TLS
SNI, CA verification, and the expected DNS certificate SAN are configured for
each provider. Unknown routes/methods return 404; there is no arbitrary upstream
selection, Ollama capture, token-count endpoint, WebSocket/Realtime support,
Codex subscription authentication, or legacy headless/proxy parity.

Credentials come from the **client request**: Anthropic `x-api-key` (plus
`anthropic-version`), or OpenAI `Authorization: Bearer ...`. Nothing reads
provider keys from Compose's environment, injects keys, or uses `tapes auth`.
Configure an SDK's base URL as above (or `ANTHROPIC_BASE_URL` /
`OPENAI_BASE_URL` if your client supports those variables). Provider billing
and authentication are unchanged. Envoy has no access log and runs at warn
level; do not enable header/body debug logging with real credentials.

### Session capture requires explicit headers

For this POC, send both `X-Tapes-Harness-Id` and
`X-Tapes-Harness-Session-Id` on **every** request. Reuse the session ID within
one conversation and choose a new ID for a new conversation. SDKs can supply
these via default headers. `X-Tapes-Session-Name` is optional (percent-encoded
UTF-8). Header-free requests still forward, but the adapter does not synthesize
a session envelope: do not expect a derived session in the read API.

Example (requires a real OpenAI API key; model availability depends on account):

```bash
export OPENAI_API_KEY='your-key'
curl -N http://localhost:8080/v1/chat/completions \
  -H "Authorization: Bearer $OPENAI_API_KEY" \
  -H 'Content-Type: application/json' \
  -H 'X-Tapes-Harness-Id: manual' \
  -H 'X-Tapes-Harness-Session-Id: envoy-demo-1' \
  -d '{"model":"gpt-4o-mini","stream":true,"messages":[{"role":"user","content":"Say hello"}]}'
```

Capture sees the session headers before removal. An independent Lua filter
strips **all** `X-Tapes-*` headers upstream even if extproc fails. Forged
`X-Paper-Auth-*` headers are removed before capture as well as upstream; there
is no trusted Paper identity in this local stack.

### Limitations and diagnostics

Both bodies use ordinary Envoy `STREAMED` mode, with mode overrides disabled.
Do not switch to `FULL_DUPLEX_STREAMED` or `observability_mode`: the existing
processor responds with per-chunk ACKs. SSE is delivered incrementally, but
capture accumulates the completed turn in memory. The route has no total
response timeout and a five-minute stream idle timeout. This is not a durable
capture queue: only supported, completed successful turns are captured; client
cancellation, size limits, processor/ingest failure, or saturation may lose
capture without failing the LLM response. A failed processor is fail-open,
with up to a one-second per-message wait before bypass; ingest/storage/API
availability does not gate Envoy startup. Startup requests can therefore be
forwarded before capture is ready.

```bash
curl -fsS http://localhost:8081/ping
curl -fsS http://localhost:8081/v1/sessions
# Service state and capture/derivation errors (no provider keys needed):
docker compose ps
docker compose logs --tail=100 envoy tapes-extproc tapes-ingest tapes-derive-worker
# Optional temporary diagnostic client sharing Envoy's network namespace:
docker run --rm --network "container:$(docker compose ps -q envoy)" \
  curlimages/curl:8.12.1 -fsS http://127.0.0.1:9901/stats
docker run --rm --network "container:$(docker compose ps -q envoy)" \
  curlimages/curl:8.12.1 -fsS http://tapes-extproc:9090/metrics
```

Allow roughly 20–30 seconds for the default derive debounce/poll after a turn
completes. If forwarding works but sessions are absent, check the two session
headers, extproc drop/dispatch metrics and logs, ingest availability at
`tapes-ingest:8082` (not its adapter default `8090`), and derive-worker logs.
A 401/403 usually means provider credentials; 404 means an unsupported local
route; 503 can indicate DNS, TLS, or upstream connectivity. Avoid sharing logs
containing user-controlled paths or capture content.

`make validate-envoy` checks Compose and runs the pinned Envoy's config validator
without starting the stack. `make smoke-envoy` additionally uses real Envoy,
the Go processor, and mock providers/ingest to check all three routes, capture
POSTs, incremental SSE, and fail-open header stripping **without credentials**.
It requires Go 1.26+ and local container host networking (Linux, or Docker
Desktop with host networking enabled), uses temporary resources, and does not
touch stack volumes. Its mock upstream is plaintext: it does not prove live
provider TLS/authentication, PostgreSQL persistence, or derivation.

Docker Desktop cannot give an Ollama container access to an Apple GPU, so
cassette embedding can continue consuming CPU after capture. The two standalone
recipes below are alternatives for embeddings; they retain their legacy
forwarding setup and do **not** inherit this default Envoy configuration.

## Use native Ollama

On macOS, [Docker Desktop container GPU support is limited to Windows with the
WSL2 backend](https://docs.docker.com/desktop/features/gpu/), while native
[Ollama accelerates Apple GPUs through Metal](https://docs.ollama.com/gpu#metal-apple-gpus).
After starting Ollama on the host, run the native recipe:

```bash
cd compose/native-ollama
docker compose up --build
```

This runs the rest of the stack in Docker, checks that host Ollama is reachable,
and pulls `embeddinggemma` before background embedding starts. The recipe's
`README.md` contains preflight checks and Linux host binding requirements.

## Use OpenAI for background embeddings

To avoid sustained local embedding work without running native Ollama, use the
OpenAI embeddings recipe:

```bash
cd compose/openai-embeddings
cp .env.example .env
# Set OPENAI_API_KEY in .env, then:
docker compose up --build
```

Only background span embeddings use OpenAI. Containerized Ollama remains
available for synchronous local work such as skill generation. The recipe uses
`text-embedding-3-large` shortened to 768 dimensions, preserving the default
pgvector schema. Changing models causes a one-time re-embedding pass and incurs
OpenAI API usage. The recipe's `README.md` covers key handling and operational
details.

Stop one recipe before switching to another because they share the `tapes`
Compose project name and host ports:

```bash
docker compose down
```

Without Compose, configure the server directly:

```bash
tapes auth openai
tapes config set embedding.provider openai
tapes serve
```

`OPENAI_API_KEY` may be used instead of `tapes auth openai`. The configured model and dimensions must match the provider's output.

For source builds and contributor dependencies, see [Local development](./development.md).
