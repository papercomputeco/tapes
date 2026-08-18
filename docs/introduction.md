---
title: Tapes
description: What Tapes captures, and a quickstart that takes you from install to a real captured session.
sidebar:
  order: 1
---

Tapes is agent telemetry for LLM interactions. It transparently captures provider calls, keeps their original request and response data in an append-only log, and derives a browsable model of sessions, traces, and spans.

Use Tapes to inspect agent work, measure token use and cost, search previous work by meaning, export sessions, and turn successful sessions into reusable skills.

## Quickstart

Tapes ships as two binaries: `tapes`, the server, and
[`tapesctl`](https://github.com/papercomputeco/tapesctl), the client that
captures sessions and reads them back. Install both:

```bash
curl -fsSL https://download.tapes.dev/install | bash
curl -sSfL https://download.tapes.dev/tapesctl/install | bash
```

Docker is required for the bundled local dependencies. Start PostgreSQL with pgvector and Ollama with the default `embeddinggemma` embedding model:

```bash
tapes local up
```

`tapes local up` writes the resulting PostgreSQL, pgvector, and Ollama settings to the active `.tapes/config.toml`. If native Ollama is already installed, Tapes uses it rather than starting the Ollama container.

Start the all-in-one runtime in one terminal:

```bash
tapes serve
```

This runs the capture proxy on `http://localhost:8080`, read API on `http://localhost:8081`, private ingest API on `http://localhost:8082`, derive pipeline, and span-embedding pipeline.

### Two ports, two jobs

The client talks to two different servers, and mixing them up is the one mistake
that costs you a whole session:

| Port | Surface | Used by |
| --- | --- | --- |
| `8081` | read API | `sessions`, `traces`, `spans`, `search`, `export`, `seed` |
| `8082` | private ingest API | `start`, `capture`, `sync` |

`tapesctl` reads one configured server URL, so configure the read API — the one
most commands want — and pass the ingest URL explicitly when capturing:

```bash
tapesctl config set tapes-url http://localhost:8081
```

That writes `tapes-url` to `~/.tapes/config.toml`. A `--tapes-url` flag beats
`TAPES_API_URL`, which beats the configured value; with none of the three, commands
that need a server fail rather than guess a host.

### Seed and read

Seed data and browse it from another terminal:

```bash
tapesctl seed
tapesctl sessions list
```

Search individual spans:

```bash
tapesctl search "explain the retry logic"
```

### Capture real work

When ready to capture real work, launch a supported agent through the client,
pointed at the **ingest** port:

```bash
tapesctl start claude --ingest-url http://localhost:8082
# or
tapesctl start codex --ingest-url http://localhost:8082
```

`tapesctl start` launches the agent under a just-in-time capture proxy, routes its provider traffic through it, and ships the captured turns to the server.

On exit `start` prints the harness's own session id, which is not the id the
read commands take. Find the captured session with `tapesctl sessions list`
rather than pasting the printed id into `tapesctl sessions get`.

See [Agent integrations](./integrations.md) for the harnesses Tapes supports and how each one is captured.

## Next steps

- Understand the [`raw_turns` → sessions → traces → spans](./architecture.md) model.
- Review [configuration precedence and supported keys](./configuration.md).
- Learn how to [inspect and export](./data.md) captured work.
- Connect an agent to the [MCP search tool](./mcp.md).
