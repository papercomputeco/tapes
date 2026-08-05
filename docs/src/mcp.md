# MCP

The read API mounts a stateless, streamable HTTP Model Context Protocol endpoint at:

```text
http://localhost:8081/v1/mcp
```

Configure that URL as a **streamable HTTP** server in an MCP client. The transport supports `POST` for JSON-RPC invocation, `GET` for the stream, and `DELETE` for session termination semantics.

## Cassette tools

The MCP server aggregates tools advertised by installed cassettes. A cassette
marks an OpenAPI operation with `x-tapes-mcp`; Tapes validates its JSON-body
contract and publishes it under a cassette-qualified name such as
`summary.summarize_session`. `tools/list` reads the current cassette registry,
so successfully refreshed cassettes appear without restarting Tapes and removed
cassettes disappear.

Tool arguments are sent as the cassette operation's JSON request body. A
successful JSON object is returned as both MCP structured content and JSON text;
non-2xx responses and unavailable cassettes are tool errors. Calls use the same
admitted cassette origin and caller identity headers as the cassette HTTP proxy.
See [Cassettes](./cassettes.md#mcp-tool-advertisement) for the extension and its
initial POST-only constraints.

The transport is stateless, so it cannot push reliable out-of-band tool-list
change notifications. A client connected while the cassette fleet changes may
need to reconnect or issue `tools/list` again.

## Legacy core search tool

While search is being extracted to its own cassette, configuring span search
and the embedder still registers the core tool below:

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
tapesctl seed --tapes-url http://localhost:8081
```

If search dependencies are not configured, the legacy core search tool is
omitted; cassette tools remain available. If storage is configured but the span
embedding projection is not initialized, core search reports an error until the
embed worker populates it.

## Scope

Header-less core MCP search uses the same nil-org tenant bucket as header-less
HTTP search. Cassette tools can expose whatever behavior their admitted POST
operation implements; MCP annotations are descriptive hints and do not replace
gateway or cassette authorization.
