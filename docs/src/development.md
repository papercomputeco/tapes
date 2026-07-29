# Local development

The repository's supported workflow is Make-based. Run `make help` before invoking lower-level tools directly.

## Prerequisites

- Go 1.26 or newer;
- Docker for Dagger checks and the pinned PostgreSQL test service;
- Nix with flakes (recommended) or equivalent pinned tools;
- PostgreSQL with pgvector and pg_duckdb for runtime and DB-backed tests;
- Ollama when exercising default local embeddings.

The Nix development shell supplies Go, Dagger, sqlc, swag, hurl, mdBook, `GOEXPERIMENT=jsonv2`, and the test PostgreSQL DSN:

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

DB-backed suites must use the pinned PostgreSQL image, which carries pgvector and pg_duckdb:

```bash
make test-db-up
make test-local
make test-db-down
```

Scope a local run with `PKG=./pkg/... make test-local`. CI-style operations run through Dagger:

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
make docs-check-links
```

The mdBook source is `docs/src/`, configuration is `docs/book.toml`, and generated HTML is `docs/book/` (ignored by Git). `docs-serve` rebuilds and serves the book locally.

## Generated contracts

`make openapi` regenerates both read and ingest contracts and their Swagger intermediates. Do not hand-edit generated files. Documentation changes should validate routes against `api/openapi.yaml`, `ingest/openapi.yaml`, and current command `--help` output.
