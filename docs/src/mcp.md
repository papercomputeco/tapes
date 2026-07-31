# MCP

The read API mounts a stateless, streamable HTTP Model Context Protocol endpoint at:

```text
http://localhost:8081/v1/mcp
```

Configure that URL as a **streamable HTTP** server in an MCP client. The transport supports `POST` for JSON-RPC invocation, `GET` for the stream, and `DELETE` for session termination semantics.

## Search tool

When span search and the embedder are configured, the server registers one tool:

| Field | Value |
| --- | --- |
| Name | `search` |
| Required input | `query` string |
| Optional input | `top_k` integer; defaults to `5` |

The tool embeds the query and uses the same span search implementation as `GET /v1/search/spans`. Its structured results contain `session_id`, `trace_id`, `span_id`, `score`, `user_prompt`, `snippet`, `model`, and `started_at` for each matched main-conversation LLM span.

Example tool arguments:

```json
{
  "query": "how was logging configured?",
  "top_k": 3
}
```

## Enable it locally

```bash
tapes local up
tapes serve
```

The bundled local setup configures PostgreSQL/pgvector and Ollama embeddings, and `tapes serve` embeds spans by default. Capture or seed data before searching:

```bash
tapes seed --demo
```

If search dependencies are not configured, the endpoint serves an MCP server with no tools. If storage is configured but the span embedding projection is not initialized, search reports an error until the embed worker populates it.

## Scope

Header-less MCP search uses the same nil-org tenant bucket as header-less HTTP search. The current MCP tool does not expose arbitrary session browsing, writes, checkout, or history branching. Use the [read API](./apis.md) for session and trace inspection.
