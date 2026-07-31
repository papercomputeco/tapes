# Installation and local setup

## Install a release

```bash
curl -fsSL https://download.tapes.dev/install | bash
tapes version
```

## Storage modes

`tapes serve` and `tapes start` use a local SQLite core database at
`.tapes/core.sqlite` by default. It is the Docker-free, single-player demo
mode: it captures raw turns and derives sessions, traces, and spans.

SQLite mode deliberately does not provide vector search, embeddings, skills,
or independent service processes. It is one combined `tapes serve`/`tapes start`
process on one host.

For multi-player or platform deployments, provide PostgreSQL explicitly:

```bash
tapes serve --postgres "$TAPES_STORAGE_POSTGRES_DSN"
```

PostgreSQL enables the separate services and pgvector-backed search.

## Start Tapes

```bash
tapes serve
```

Defaults are proxy `:8080`, read API `:8081`, and private ingest API `:8082`.
The default SQLite mode does not start embeddings; pass `--postgres` to enable
PostgreSQL-backed vector search. Verify the read API and configuration:

```bash
curl http://localhost:8081/ping
tapes status
```

Seed representative capture data through the normal ingest and derive path:

```bash
tapes seed --demo
tapes deck
```

## OpenAI embeddings

PostgreSQL is required for embeddings, but Ollama need not be used:

```bash
tapes auth openai
tapes config set embedding.provider openai
tapes serve
```

`OPENAI_API_KEY` may be used instead of `tapes auth openai`. The configured model and dimensions must match the provider's output.

For source builds and contributor dependencies, see [Local development](./development.md).
