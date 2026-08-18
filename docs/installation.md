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

Point the client at the read API once, so the read commands need no flag:

```bash
tapesctl config set tapes-url http://localhost:8081
```

Seed representative capture data through the normal ingest and derive path:

```bash
tapesctl seed
tapesctl sessions list
```

Capture commands address the **ingest** port instead, and take it explicitly:
`tapesctl start claude --ingest-url http://localhost:8082`. See
[Agent integrations](./integrations.md).

## OpenAI embeddings

Local PostgreSQL is still required, but Ollama need not be used for embeddings:

```bash
tapes auth openai
tapes config set embedding.provider openai
tapes serve
```

`OPENAI_API_KEY` may be used instead of `tapes auth openai`. The configured model and dimensions must match the provider's output.

For source builds and contributor dependencies, see [Local development](./development.md).
