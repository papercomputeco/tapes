# AGENTS.md

### Don't

- Do not write design documents or implementation plans to disk (no `docs/plans/` or similar).
  Discuss plans in conversation only.

### Do

- Always use the Ginkgo/Gomega testing frameworks
- Be careful adding anything to `Bucket` since that's the content addressing unit —
  changing it changes everything for the internal merkle/dedup layer.
- Always use `make` operations for development: use `make help` to understand
  the various operations available.
- Run tests with `make test`; the Dagger workflow provisions the Postgres
  service and passes its DSN to DB-backed suites.
- Run `make format` to format and organize imports using `goimports` and `golangci-lint`
- OpenAPI lives in `pkg/tapesoapi` (there is no swaggo and no annotation
  comments). A route is registered and described in the same call, through the
  `oasfiber` wrapper — see `api/openapi_routes.go` and `ingest/openapi.go`.
  There is **nothing to regenerate**: each server compiles its contract from
  those registrations and serves it at `GET /openapi`. No contract file is
  checked in, so changing a route or a payload changes the published document
  immediately and no document can be stale. Field prose comes from ordinary Go
  doc comments — do not put descriptions in struct tags. A running binary has no
  source tree, so the served document carries route and operation prose but not
  per-field prose; `tapes dev openapi [api|ingest]` compiles the fully
  documented version from a checkout when a consumer wants it.
- Follow idiomatic Go and prefer using the `func NewExampleStruct() *ExampleStruct`
  paradigm seen throughout.

### Project Overview

`tapes` is an agentic telemetry system for content-addressable LLM interactions.

Data flows in one direction: a transparent proxy intercepts LLM API calls and
appends them to an immutable `raw_turns` log; a pure, idempotent **deriver**
projects that log into the read model of **sessions → traces → spans** (re-derive
reproduces the projection and prunes anything no longer present). Derived IDs are
deterministic. The persisted merkle `nodes` table has been **dropped**; the
content-addressed merkle layer now lives only **in memory** (`merkle.ProjectContent`),
used at derive time for content identity and dedup. It is not persisted and not a
user-facing browsing surface.

The system is made up of:

- A transparent proxy for intercepting LLM API calls and appending them to the
  immutable `raw_turns` capture log.
- A derive worker (run via `tapes serve derive-worker`) that projects `raw_turns`
  into sessions/traces/spans.
- An API server for querying and exporting over the derived surface
  (`/v1/sessions`, `/v1/traces`, `/v1/stats`, `/v1/search/spans`,
  `/v1/sessions/{id}/raw_turns`).
- A bundled CLI (`tapes`) for running the proxy, API, and workers, and for the
  operator tooling around them.

CLI surface notes for agents:

- **`tapes` is the server; `tapesctl` is the client.** This binary runs the
  services, owns the database, and carries the operator tooling. Capturing a
  session and reading one back belong to `tapesctl`, a separate Rust CLI in its
  own repository.
- The client commands that used to live here have **moved to `tapesctl`**:
  `tapes start` → `tapesctl start`, `tapes export` → `tapesctl export`,
  `tapes seed` → `tapesctl seed`, `tapes sessions` → `tapesctl sessions list`,
  `tapes search` → `tapesctl search`, `tapes skill` → `tapesctl skill`.
  Do not re-add them here.
- `tapes chat` has been **removed**; tapes captures and derives, it does not host
  chat sessions.
- Span search stays **server-side** (`/v1/search/spans` and the embed worker);
  only the client command moved. Search and skill authoring are slated for
  extraction into cassettes.
- The deck TUI has been **removed with no CLI replacement** — the console owns
  the ROI/session-browsing surface.

**Language:** Go 1.26+
**Go Module:** `github.com/papercomputeco/tapes`

### Project Structure

- `api/` - REST API server for interfacing with `tapes` system
- `cli/` - Individual CLI targets
- `cmd/` - `spf13/cobra` commands: these are built to be modular in order to be bundled
  in various CLIs
- `pkg/` - Go packages. Use the `go doc` command to get the documentation on the
  packages public API. Ex: `go doc pkg/llm`
- `internal/` - Packages private to this module (not importable by consumers).
- `proxy/` - The `tapes` telemetry collector proxy
- `.dagger/` - Dagger CI/CD builds and utilities. Used through `make` targets.
- `.github/` - GitHub metadata and action workflows.
- `flake.nix` - The development Nix flake which bundles all necessary dependencies for development.

### Build System

The project uses a Makefile for all build and dev operations. Utilize `make help`
to see all available commands.

Build artifacts land in the `build/` directory.

### PR and Commit Conventions

See [CONTRIBUTING.md](CONTRIBUTING.md#contributing-a-pr) for the required PR
title format and allowed labels. Squash-merge commits inherit the PR title,
so the PR title **is** the commit message that lands on `main`.
