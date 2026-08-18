---
title: MCP
description: The read API's Model Context Protocol endpoint and the cassette tools it aggregates.
sidebar:
  order: 12
---

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

Semantic search over spans is a cassette tool: the search cassette
advertises `search.search`, which appears here like any other
cassette-advertised tool once that cassette is installed. Core registers no
tools of its own.

## Enable it locally

```bash
tapes local up
tapes serve
```

The bundled local setup configures PostgreSQL and Ollama. Capture or seed data
before browsing:

```bash
tapesctl seed --tapes-url http://localhost:8081
```

## Scope

Cassette tools can expose whatever behavior their admitted POST
operation implements; MCP annotations are descriptive hints and do not replace
gateway or cassette authorization.
