---
title: Local development
description: Prerequisites, the Make-based build and test workflow, where the docs live, and how the OpenAPI seals work.
sidebar:
  order: 15
---

The repository's supported workflow is Make-based. Run `make help` before invoking lower-level tools directly.

## Prerequisites

- Go 1.26 or newer;
- Docker for Dagger checks and the pinned PostgreSQL test service;
- Nix with flakes (recommended) or equivalent pinned tools;
- PostgreSQL with pgvector and pg_duckdb for runtime and DB-backed tests;
- Ollama when exercising default local embeddings.

The Nix development shell supplies Go, Dagger, sqlc, hurl, and `GOEXPERIMENT=jsonv2`:

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

The docs are flat Markdown files in `docs/`, one page per file, with no build
step: read them in the repository or on GitHub exactly as they are. A change to
user-visible behavior updates the relevant page in the same change.

Every page carries YAML frontmatter with a `title` and a one-sentence
`description`, plus a `sidebar.order` that fixes the reading order the file
names alone do not imply. Links between pages are ordinary relative links
(`./cli.md`), so they resolve both in the repository and on the published site.

## OpenAPI contracts

Read and ingest routes register their OpenAPI descriptions through the `oasfiber` wrapper and each running server compiles its contract at `GET /openapi`. There is no generated contract file to update or check in.

There is, however, a seal. `api/CONTRACT` and `ingest/CONTRACT` each hold a `sha256` fingerprint of that surface's compiled document with prose stripped out, and `api/openapi_seal_test.go` and `ingest/openapi_seal_test.go` recompile and compare. A change that moves a published contract fails until the new value — which the failure prints — is written into the file in the same change. Doc-comment edits are excluded by design; text declared inline on a route registration is not, and is the usual cause of a move that touches no structure.

From a checkout, `tapes dev openapi [api|ingest]` emits a contract with field prose when a consumer needs bytes on disk, and `make contracts` writes both into `./build/contracts`.
