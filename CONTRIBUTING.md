# Contributing

## Quick start (recommended)

The Nix flake dev shell is the recommended way to develop tapes. It pins
Go 1.25, GCC, Dagger, and configures all required environment variables
(`CGO_ENABLED`, `GOEXPERIMENT`). This avoids toolchain drift and CGO build
warnings on macOS by using Nix-provided GCC instead of Xcode's system clang.

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
1. When submitting a pull request, _ALL_ titles must be labeled with one of:
  * `⚠️ breaking:`- `:warning: feat:` - adds a breaking change. Triggers a major version bump (i.e., `1.0.0` --> `2.0.0`).
  * `✨ feat:` - `:sparkles: feat:` - adds a new feature. Triggers a minor version bump (i.e., `0.1.0` --> `0.2.0`).
  * `🔧 fix:` - `:wrench: fix:` - fixes a bug. Triggers a patch bump (i.e., `0.0.1` --> `0.0.2`).
  * `🧹 chore:` - `:broom: chore` - non-feature, non-bug code changes (i.e., CICD, tests, etc.). Does _NOT_ trigger a version change.
  * `📚 docs:` - `:books: docs:` - documentation only changes. Does _NOT_ trigger a version change.

## Local demo data

Seed demo sessions through the local API server (demo data is captured and
derived just like live sessions, so it shows up in `tapes deck`):

```bash
tapes seed --demo --api-target http://localhost:8081
```

To reset demo data, use a fresh database behind the API server.

## Prerequisites checklist

- Go 1.25+
- Docker (required for `make format`, `make check`, `make test` via Dagger)
- PostgreSQL with pgvector + pg_duckdb for local runtime work
- Optional: Ollama for embeddings when running `tapes serve`

## Common issues

- Merkle hashing (the internal content-addressed provenance layer) requires
  `GOEXPERIMENT=jsonv2`
  - `make build-local` sets this automatically
- `make format`/`make check`/`make test` require Docker for Dagger
- Demo seeding docs
  - Use `tapes seed --demo --api-target http://localhost:8081` to seed demo sessions
  - Use a fresh Postgres database behind the API when reseeding

## Example commands

```bash
# Build local binaries
make build-local

# Start local dependencies
./build/tapes local

# Seed demo data through a running API, then browse it in the deck UI
./build/tapes seed --demo --api-target http://localhost:8081
./build/tapes deck --api-target http://localhost:8081

# Run tests
make test

# Format code
make format
```
