# CLI reference

Run `tapes <command> --help` for the complete, version-matched flag list. The commands below are the normal user surface.

| Command | Use |
| --- | --- |
| `tapes init [--preset ...]` | Create a local `.tapes/` configuration directory. |
| `tapes local [up|status|down]` | Manage local PostgreSQL and Ollama dependencies. |
| `tapes serve` | Run proxy, read API, private ingest API, derive worker, and optional embed worker together. |
| `tapes start [agent]` | Start services, optionally launching Claude, OpenCode, or Codex. |
| `tapes status` | Show active config, provider/upstream, API reachability, and capture summary. |
| `tapes sessions` | List recent session IDs and summaries. |
| `tapes deck` | Browse the session ROI dashboard and drill into traces/spans. |
| `tapes search <query>` | Semantic search over main-conversation LLM spans. |
| `tapes export <session-id>` | Stream the API session projection as JSONL. |
| `tapes seed --demo` | Seed idempotent demo capture data through the normal write path. |
| `tapes skill generate|list|sync` | Create and distribute reusable agent skills. |
| `tapes auth` | Store OpenAI or Anthropic credentials in `.tapes/credentials.toml`. |
| `tapes config get|set|list` | Manage persistent scalar settings. |
| `tapes version` | Print version information. |

## Running services

The common local command is:

```bash
tapes serve
```

It accepts provider/upstream, PostgreSQL, listening, embedding, and project flags. Useful examples:

```bash
tapes serve --provider anthropic --upstream https://api.anthropic.com
tapes serve --api-web-ui
tapes serve --embed-spans=false
```

For split deployments, service subcommands are available:

```bash
tapes serve proxy
tapes serve api
tapes serve derive-worker
tapes serve embed-worker
tapes serve ingest
```

The last three are operator-oriented: the derive worker projects dirty sessions, the independent embed worker populates search vectors, and the private ingest sidecar receives completed turns from a trusted gateway. See [HTTP APIs](./apis.md) before exposing any endpoint.

## Launching agents

```bash
tapes start
tapes start claude
tapes start claude -- --dangerously-skip-permissions
tapes start opencode --provider anthropic --model claude-sonnet-4-5
tapes start codex
tapes start --logs
```

Arguments after `--` go directly to the agent. See [Agent integrations](./integrations.md).

## Listing, exporting, and piping

```bash
tapes sessions
tapes sessions --quiet | head -1
tapes export <session-id> --detail spans -o session.jsonl
tapes search "database migration" --top 10
tapes search "database migration" --quiet
```

`sessions` can connect to the API with `--api-target` or open PostgreSQL through a local in-process API with `--postgres`. `export` always calls a running read API. Full IDs and unambiguous short prefixes are accepted by export.

## Commands not intended as everyday workflow

`backfill` is for replaying existing capture artifacts into a deployment. `dev` contains developer maintenance utilities. Consult their `--help` only when operating those workflows.

Tapes no longer provides `chat` or `checkout` commands. It captures external agents; it does not host a chat client or expose history branching.
