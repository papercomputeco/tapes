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

## Bring in the sessions you already have

You do not have to start from an empty server. `tapesctl sync` walks the Claude
Code transcripts already on disk under `~/.claude/projects` and pushes the last
seven days of them to the ingest port:

```bash
tapesctl sync --ingest-url http://localhost:8082
tapesctl sessions list
```

It is safe to run again: the server keeps one copy per file version. Sessions
pushed this way derive in the background, so the list may take a moment to
fill in. Pass `--since-days 0` to sweep everything. See
[Capture](/docs/tapesctl/capture/#the-sweep) for what a transcript-only session
does and does not contain.

## Run the complete Docker Compose stack

The repository's default Compose file puts PostgreSQL, Ollama, Tapes, and the
bundled cassettes in containers. It is the simplest way to see the entire local
system working:

```bash
docker compose up --build
```

This convenience has a tradeoff: Docker Desktop cannot give an Ollama container
access to an Apple GPU, so background span embedding can continue consuming CPU
after capture has finished. The repository includes two standalone Compose
recipes when that matters.

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
