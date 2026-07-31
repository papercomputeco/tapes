# Tapes

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

This runs the capture proxy on `http://localhost:8080`, read API on `http://localhost:8081`, private ingest API on `http://localhost:8082`, derive pipeline, and span-embedding pipeline. Seed data and browse it from another terminal:

```bash
tapesctl seed --tapes-url http://localhost:8081
tapesctl sessions list --tapes-url http://localhost:8081
tapes deck
```

Search individual spans:

```bash
tapes search "explain the retry logic"
```

When ready to capture real work, launch a supported agent through the client:

```bash
tapesctl start claude --tapes-url http://localhost:8081
# or
tapesctl start opencode --tapes-url http://localhost:8081
```

`tapesctl start` launches the agent under a just-in-time capture proxy, routes its provider traffic through it, and ships the captured turns to the server. See [Agent integrations](./integrations.md) for manual and generic-proxy setups.

## Next steps

- Understand the [`raw_turns` → sessions → traces → spans](./architecture.md) model.
- Review [configuration precedence and supported keys](./configuration.md).
- Learn how to [inspect and export](./data.md) captured work.
- Connect an agent to the [MCP search tool](./mcp.md).
