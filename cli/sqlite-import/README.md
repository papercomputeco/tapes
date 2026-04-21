# `sqlite-import`

This is a small helper program for migrating a legacy `tapes.sqlite` DAG into a
local PostgreSQL-backed tapes runtime.

This is especially useful for `tapes.sqlite` databases bootstrapped with v0.4.0
and need to transition to v0.5.0.

This tool migrates legacy SQLite data into the local Postgres-backed tapes runtime:

- copies DAG rows from SQLite `nodes` into Postgres `nodes`
- copies legacy sqlite-vec embeddings from `vec_documents`/`vec_embeddings` into Postgres `tapes_embeddings`
- preserves existing node hashes
- preserves parent/child links
- preserves timestamps and usage metadata
- upserts safely, so it is fine to re-run

It is intended for local migration only.

## Requirements

- Go 1.25+
- CGO enabled
- system SQLite development libraries installed
- a local Postgres-backed tapes instance running

On this branch, `tapes local up` starts Postgres + Ollama. You still need to run the tapes server separately.

## Start tapes locally

From the repo root:

```bash
tapes local up
```

Then start the tapes server against the local Postgres instance:

```bash
tapes serve --postgres 'postgres://tapes:tapes@localhost:5432/tapes?sslmode=disable'
```

## Run the importer

From the repo root:

```bash
cd cli/sqlite-import
go run .
```

Default behavior:

- source SQLite DB: `~/.tapes/tapes.sqlite`
- target Postgres DSN: `postgres://tapes:tapes@localhost:5432/tapes?sslmode=disable`

### Common examples

Dry run:

```bash
go run . --dry-run
```

Custom SQLite path:

```bash
go run . --sqlite-path /path/to/tapes.sqlite
```

Custom Postgres DSN:

```bash
go run . --postgres-dsn 'postgres://tapes:tapes@localhost:5432/tapes?sslmode=disable'
```

Progress logging is always enabled:

```bash
go run . --batch-size 2000
```

## Notes

- The importer migrates both DAG rows in `nodes` and legacy sqlite-vec embeddings when present.
- Vector migration requires the local Postgres instance to already have a compatible `tapes_embeddings` table (for example `vector(768)` matching the legacy SQLite embedding dimension).
- If the legacy SQLite DB contains dangling parent references, those orphaned rows/subtrees are skipped and a warning is printed.
- If Postgres rejects specific legacy rows during import (for example malformed/unsupported JSON content in legacy `bucket`/`content` payloads), those rows are skipped with a warning and the import continues.
- If a skipped row has descendants, those descendants may also be skipped because their parent can no longer be inserted.
- Imported rows become immediately available to the API/deck/session views backed by Postgres.
- Imported vectors become immediately available to `tapes search`.
- Because the import is idempotent, you can re-run it if the process is interrupted.
