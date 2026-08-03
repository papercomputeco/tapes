# Contributing

## Quick start (recommended)

The Nix flake dev shell is the recommended way to develop tapes. It pins the
toolchain (Go 1.26, Dagger, `sqlc`, `hurl`) and exports
`GOEXPERIMENT=jsonv2`. `make test` provisions the test PostgreSQL service and
passes its DSN to DB-backed suites.

```bash
nix develop          # enter the dev shell
direnv allow         # or use direnv for automatic activation
make build-local
./build/tapes local
```

## Quick start (manual)

If you prefer not to use Nix, ensure you have the prerequisites below.

```bash
make build-local
./build/tapes local
```

## Contributing a PR

0. _BEFORE_ you create a PR, search for existing issues. If no issue exists,
   create an issue and signal that you'd like to work on it.
1. When submitting a pull request, _ALL_ titles must start with one of the
   following. CI rejects anything else, and because merges are squashed the PR
   title becomes the commit message on `main`.
  * `✨ feat:` - `:sparkles: feat:` - adds a new feature. Triggers a minor version bump (i.e., `0.1.0` --> `0.2.0`).
  * `🔧 fix:` - `:wrench: fix:` - fixes a bug. Triggers a patch bump (i.e., `0.0.1` --> `0.0.2`).
  * `🧹 chore:` - `:broom: chore:` - non-feature, non-bug code changes (i.e., CICD, tests, etc.). Patch bump.
  * `♻️ refactor:` - `:recycle: refactor:` - behaviour-preserving restructuring. Patch bump.
  * `🎨 design:` - `:art: design:` - design and presentation changes.
  * `📚 docs:` - `:books: docs:` - documentation only changes.
  * `✏️ RFD:` - `:pencil2: RFD:` - a request for discussion.

An optional scope goes in parentheses (`✨ feat(ingest): ...`). Mark a breaking
change with `!` before the colon — `✨ feat!:` or `🔧 fix(api)!:` — which
triggers a major version bump.

The validator is `ghcontrib` in the `papercomputeco/daggerverse` module (see
`.github/workflows/pr.yaml`); its `validPRTitleSpecs` is the authoritative list
if this one ever falls behind.

## Local demo data

Seeding is a client operation, so it lives in
[`tapesctl`](https://github.com/papercomputeco/tapesctl). Demo data is captured
and derived just like live sessions, so it shows up in `tapesctl sessions list`:

```bash
tapesctl seed --tapes-url http://localhost:8081
```

To reset demo data, use a fresh database behind the API server.

## Prerequisites checklist

- Go 1.26+
- Docker (required for `make format`, `make check`, and `make test` via Dagger)
- PostgreSQL with pgvector + pg_duckdb for local runtime work
- Optional: Ollama for embeddings when running `tapes serve`

## Common issues

- Merkle hashing (the internal content-addressed provenance layer) requires
  `GOEXPERIMENT=jsonv2`
  - `make build-local` sets this automatically
- `make format`/`make check`/`make test` require Docker for Dagger
- Demo seeding docs
  - Use `tapesctl seed --tapes-url http://localhost:8081` to seed demo sessions
  - Use a fresh Postgres database behind the API when reseeding

## Example commands

```bash
# Build local binaries
make build-local

# Start local dependencies
./build/tapes local

# Seed demo data through a running API (tapesctl), then browse it in the deck UI
tapesctl seed --tapes-url http://localhost:8081
tapesctl sessions list --tapes-url http://localhost:8081

# Run tests with the Postgres service provisioned by Dagger
make test

# Format code
make format
```
