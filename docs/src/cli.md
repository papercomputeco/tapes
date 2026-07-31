# CLI reference

`tapes` is the server. It runs the services, owns the database, and carries the operator tooling around them. Capturing a session and reading one back are client concerns and live in [`tapesctl`](https://github.com/papercomputeco/tapesctl) — see [The client CLI](#the-client-cli) below.

Run `tapes <command> --help` for the complete, version-matched flag list.

| Command | Use |
| --- | --- |
| `tapes init [--preset ...]` | Create a local `.tapes/` configuration directory. |
| `tapes local [up|status|down]` | Manage local PostgreSQL and Ollama dependencies. |
| `tapes serve` | Run proxy, read API, private ingest API, derive worker, and optional embed worker together. |
| `tapes status` | Show active config, provider/upstream, API reachability, and capture summary. |
| `tapes deck` | Browse the session ROI dashboard and drill into traces/spans. |
| `tapes search <query>` | Semantic search over main-conversation LLM spans. |
| `tapes skill generate|list` | Author reusable agent skills from session data. |
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

## The client CLI

Launching an agent under capture, listing sessions, exporting one, and seeding demo data are all client operations against a running server. They live in `tapesctl`:

```bash
curl -sSfL https://download.tapes.dev/tapesctl/install | bash
```

```bash
tapesctl start claude --tapes-url http://localhost:8081
tapesctl sessions list --tapes-url http://localhost:8081
tapesctl export <session-id> --detail spans -o session.jsonl
tapesctl seed --tapes-url http://localhost:8081
tapesctl skill sync <name> --claude
```

Every `tapesctl` command takes `--tapes-url`, falling back to `TAPES_URL`. Arguments after `--` go directly to the agent. See [Agent integrations](./integrations.md) and the [`tapesctl` README](https://github.com/papercomputeco/tapesctl) for the full surface.

## Searching

```bash
tapes search "database migration" --top 10
tapes search "database migration" --quiet
```

`search` calls a running read API; point it with `--api-target`.

## Commands not intended as everyday workflow

`backfill` is for replaying existing capture artifacts into a deployment. `dev` contains developer maintenance utilities. Consult their `--help` only when operating those workflows.

Tapes no longer provides `chat` or `checkout` commands. It captures external agents; it does not host a chat client or expose history branching.

The `start`, `export`, `seed`, `sessions`, and `skill sync` commands have moved to `tapesctl`; `tapes` keeps the server and the operator tooling.
