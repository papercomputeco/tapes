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
- Run `make format` to format and organize imports using `goimports` and `golangci-lint`
- Follow idiomatic Go and prefer using the `func NewExampleStruct() *ExampleStruct`
  paradigm seen throughout.

### Project Overview

`tapes` is an agentic telemetry system for content-addressable LLM interactions.

Data flows in one direction: a transparent proxy intercepts LLM API calls and
appends them to an immutable `raw_turns` log; a pure, idempotent **deriver**
projects that log into the read model of **sessions → traces → spans** (re-derive
reproduces the projection and prunes anything no longer present). Derived IDs are
deterministic. The merkle/content-addressed node layer is retained **internally**
for provenance and dedup — it is not a user-facing browsing surface.

The system is made up of:

- A transparent proxy for intercepting LLM API calls and appending them to the
  immutable `raw_turns` capture log.
- A derive worker (run via `tapes serve derive-worker`) that projects `raw_turns`
  into sessions/traces/spans.
- An API server for querying and exporting over the derived surface
  (`/v1/sessions`, `/v1/traces`, `/v1/stats`, `/v1/search/spans`,
  `/v1/sessions/{id}/raw_turns`).
- An all in one, bundled CLI for running the proxy, API, derive worker, and
  interfacing with the system.
- A deck TUI (`tapes deck`) — an ROI dashboard over sessions that drills into a
  single session's traces and spans.

CLI surface notes for agents:

- `tapes chat` has been **removed**; tapes captures and derives, it does not host
  chat sessions.
- `tapes checkout` is a conversation-**export** primitive: it reads the derived
  trace surface and renders a session (or a single trace) as Markdown or JSONL.
  It owns no state.
- `tapes search` is **span-only** — it queries the span projection
  (`/v1/search/spans`) and returns individual main-conversation LLM spans with
  their trace/turn context.
- `tapes deck` is built on the traces/span model (not the old stems/merkle TUI).

**Language:** Go 1.25+
**Go Module:** `github.com/papercomputeco/tapes`

### Project Structure

- `api/` - REST API server for interfacing with `tapes` system
- `cli/` - Individual CLI targets
- `cmd/` - `spf13/cobra` commands: these are built to be modular in order to be bundled
  in various CLIs
- `pkg/` - Go packages. Use the `go doc` command to get the documentation on the
  packages public API. Ex: `go doc pkg/llm`
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
