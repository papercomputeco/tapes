# Contributing

## Quick start (Nix)

We encourage contributors to utilize the Nix flake which carries all development dependencies
and has automatic `direnv` support:
to enter the nix development shell, run `nix develop`.
Allow automatic environment loading with `direnv` via `direnv allow`.

## Quick start (manual)

```bash
make build-local
./build/tapes deck --demo
```

## Local demo data

Seed demo sessions for the deck UI without touching real data:

```bash
tapes deck -m
tapes deck --demo --sqlite ./tapes.demo.sqlite
```

To reset the demo database:

```bash
tapes deck -m -f
```

## Prerequisites checklist

- Go 1.25+
- CGO enabled and SQLite dev libraries (e.g., `libsqlite3`)
- Docker (required for `make format`, `make check`, `make unit-test` via Dagger)
- Optional: Ollama for embeddings when running `tapes serve`

## Common issues

- SQLite errors when building or running
  - Ensure SQLite dev libraries are installed and `CGO_ENABLED=1`
- Merkle hashing requires `GOEXPERIMENT=jsonv2`
  - `make build-local` sets this automatically
- `make format`/`make check`/`make unit-test` require Docker for Dagger
- Demo seeding docs
  - Use `tapes deck --demo` to seed demo sessions
  - Demo DB path defaults to `./tapes.demo.sqlite`
  - Reseed with `tapes deck --demo --overwrite`

## Example commands

```bash
# Build local binaries
make build-local

# Run the deck UI with demo data
./build/tapes deck --demo

# Reseed demo data
./build/tapes deck --demo --overwrite

# Run tests
make unit-test

# Format code
make format

# Run deck against a specific database
TAPES_SQLITE=./tapes.sqlite ./build/tapes deck
```
