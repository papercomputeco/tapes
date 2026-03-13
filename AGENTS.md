# AGENTS.md

### Don't

- Do not write design documents or implementation plans to disk (no `docs/plans/` or similar).
  Discuss plans in conversation only.

### Do

- Always use the Ginkgo/Gomega testing frameworks
- Be careful adding anything to `Bucket` since that's the content addressing unit —
  changing that changes everything for the DAG.
- Always use `make` operations for development: use `make help` to understand
  the various operations available.
- Run `make format` to format and organize imports using `goimports` and `golangci-lint`
- Follow idiomatic Go and prefer using the `func NewExampleStruct() *ExampleStruct`
  paradigm seen throughout.

### Project Overview

`tapes` is an agentic telemetry system for content-addressable LLM interactions.

The system is made up of:

- A transparent proxy for intercepting LLM API calls and persisting conversation turns.
- An API server for managing, querying, and interacting with the system.
- An all in one, bundled CLI for easily running the proxy, API, and interfacing with the system.
- A TUI available via `tapes deck` for dynamically interfacing with the system.

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

PR titles are validated by CI (`ghcontrib check-pull-request`) and feed into
auto-generated release notes. Use this format:

```
<emoji> <type>: <Short description>
```

| Type    | Emoji | When to use                        |
|---------|-------|------------------------------------|
| `feat`  | ✨    | New functionality                  |
| `fix`   | 🐛    | Bug fix                            |
| `chore` | 🧹    | Maintenance, CI, deps, cleanup     |
| `perf`  | ⚡    | Performance improvement            |

Examples:
- `✨ feat: Add 24h time period to tapes deck`
- `🐛 fix: Correct streaming model loss`
- `🧹 chore: Add GPT Codex model pricing`

Squash-merge commits inherit the PR title, so the PR title **is** the commit
message that lands on `main`.
