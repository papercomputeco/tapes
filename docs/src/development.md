# Local development

The repository's supported workflow is Make-based. Run `make help` before invoking lower-level tools directly.

## Prerequisites

- Go 1.26 or newer;
- Docker for Dagger checks and the pinned PostgreSQL test service;
- Nix with flakes (recommended) or equivalent pinned tools;
- PostgreSQL with pgvector and pg_duckdb for runtime and DB-backed tests;
- Ollama when exercising default local embeddings.

The Nix development shell supplies Go, Dagger, sqlc, hurl, mdBook, and `GOEXPERIMENT=jsonv2`:

```bash
git clone https://github.com/papercomputeco/tapes.git
cd tapes
nix develop
make help
```

With direnv, `direnv allow` activates the same shell automatically.

## Build and run

```bash
make build-local
./build/tapes local up
./build/tapes serve
```

`make build-local` sets `CGO_ENABLED=0` and `GOEXPERIMENT=jsonv2` and writes `./build/tapes`. Install it to Go's binary directory with:

```bash
make install
```

## Tests and checks

Run the test suite through Dagger so DB-backed suites receive the pinned PostgreSQL service with pgvector and pg_duckdb:

```bash
make test
make check
make format
```

Do not start an arbitrary stock PostgreSQL container for tests; missing extensions can look like application failures.

## Documentation

```bash
make docs-build
make docs-serve
```

The mdBook source is `docs/src/`, configuration is `docs/book.toml`, and generated HTML is `docs/book/` (ignored by Git). `docs-serve` rebuilds and serves the book locally.

## OpenAPI contracts

Read and ingest routes register their OpenAPI descriptions through the `oasfiber` wrapper and each running server compiles its contract at `GET /openapi`. There is no generated contract to update or check in. From a checkout, `tapes dev openapi [api|ingest]` emits a contract with field prose when a consumer needs bytes on disk.
