# Tapes

Tapes is agent telemetry for LLM interactions. It transparently captures provider calls, keeps their original request and response data in an append-only log, and derives a browsable model of sessions, traces, and spans.

Use Tapes to inspect agent work, measure token use and cost, search previous work by meaning, export sessions, and turn successful sessions into reusable skills.

## Quickstart

Install the CLI:

```bash
curl -fsSL https://download.tapes.dev/install | bash
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
tapes seed --demo
tapes sessions
tapes deck
```

Search individual spans:

```bash
tapes search "explain the retry logic"
```

When ready to capture real work, launch a supported agent through Tapes:

```bash
tapes start claude
# or
tapes start opencode
```

`tapes start` chooses local ports, starts the necessary services, launches the agent with its provider routed through the proxy, and tags the captured session. See [Agent integrations](./integrations.md) for manual and generic-proxy setups.

## Next steps

- Understand the [`raw_turns` → sessions → traces → spans](./architecture.md) model.
- Review [configuration precedence and supported keys](./configuration.md).
- Learn how to [inspect and export](./data.md) captured work.
- Connect an agent to the [MCP search tool](./mcp.md).
