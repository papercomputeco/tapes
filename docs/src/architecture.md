# Architecture

Tapes separates immutable capture from derived, query-oriented data.

```text
agent or application
        |
        v
proxy :8080  --------------------> upstream LLM provider
        |
        | append completed request/response turn
        v
PostgreSQL raw_turns
        |
        | idempotent derivation
        v
sessions -> traces -> spans -----> pgvector span embeddings
        |                              |
        +----------> API :8081 <-------+
                         |
               CLI / Deck / MCP clients
```

## Capture: `raw_turns`

The transparent proxy supports Anthropic, OpenAI, and Ollama traffic. It forwards a request to the configured upstream and appends the completed interaction to PostgreSQL's immutable `raw_turns` capture log. The private ingest service is an alternative write path for an external gateway.

Capture is append-only. Replayed turns deduplicate by capture identity instead of rewriting prior capture.

A captured turn is stored twice over: `raw_response` holds the upstream bytes verbatim, and `response` holds the reduced turn an adapter produced from them. Keeping the bytes is what makes the reduction reproducible and auditable rather than authoritative — a future data-model change becomes a re-derive over existing rows instead of a re-capture. Reducing server-side from those bytes is the end state, reached through a proven ratchet: see [Proving the capture ratchet](./cli.md#proving-the-capture-ratchet).

## Derivation: sessions, traces, and spans

The deriver projects raw turns into the read model:

- **Session**: one harness or agent session, with folded model, token, cost, duration, status, and turn-count data.
- **Trace**: a turn in that session.
- **Span**: the work within a trace, including the main LLM conversation and attached tool activity. Span links preserve relationships where needed.

Derived IDs are deterministic. Re-deriving unchanged raw input reproduces the same projection, and the deriver prunes derived records no longer present in the projection. This makes derivation idempotent and safe to run repeatedly.

## Search

The embedding worker embeds **main-conversation LLM spans** into PostgreSQL using pgvector. Search is span-only: `/v1/search/spans`, `tapesctl search`, and the MCP `search` tool all return individual span hits with session, trace, and turn context. They do not search session objects or a conversation DAG.

## Content addressing

Merkle content addressing remains an internal, in-memory derivation mechanism for content identity and deduplication. Tapes does not persist a user-facing Merkle node graph, and there is no checkout, branching, or Merkle browsing workflow.

## Runtime forms

`tapes serve` is the convenient all-in-one runtime. It runs the proxy, read API, private ingest API, derive worker, and—by default—the embed worker in one process. Operators can instead run `tapes serve proxy`, `api`, `ingest`, `derive-worker`, or `embed-worker` as separate processes. In a split deployment, derivation and embedding are deliberately independent failure domains.
