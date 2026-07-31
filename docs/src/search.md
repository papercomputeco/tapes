# Search

Tapes performs semantic search over the embedded **span projection**. Each result is an individual main-conversation LLM span, with its session ID, trace ID, span ID, turn prompt, model, timestamp, similarity score, and text snippet.

Search does not return sessions as its search unit and does not search internal Merkle content.

## Local setup

The quickstart provides everything required:

```bash
tapes local up
tapes serve
```

`tapes local up` configures PostgreSQL/pgvector and the Ollama `embeddinggemma` model. `tapes serve` derives captures and embeds eligible spans in the background by default.

After capturing or seeding data:

```bash
tapesctl search "how was authentication fixed?"
tapesctl search "logging configuration" --top 10
```

Use quiet output to return unique session IDs in score order:

```bash
tapesctl search "Charm CLI patterns" --quiet --top 3
```

That output composes with skill generation:

```bash
tapesctl skill generate $(tapesctl search "Charm CLI" --quiet --top 1) \
  --name charm-patterns
```

## API

The equivalent read endpoint is:

```bash
curl --get http://localhost:8081/v1/search/spans \
  --data-urlencode 'query=how was authentication fixed?' \
  --data-urlencode 'top_k=5'
```

There is no `/v1/search` endpoint. See [HTTP APIs](./apis.md) for tenant headers and contracts.

## Separate workers

In a split deployment, run derivation and embedding independently:

```bash
tapes serve derive-worker --postgres "$TAPES_STORAGE_POSTGRES_DSN"
tapes serve embed-worker --postgres "$TAPES_STORAGE_POSTGRES_DSN"
tapes serve api --postgres "$TAPES_STORAGE_POSTGRES_DSN"
```

The embed worker runs a bounded pass at startup and periodically thereafter. Its embedding model and dimensions must match the pgvector table. Failures leave a span unembedded for a later retry rather than blocking derivation.

## Troubleshooting

1. Confirm the API is reachable with `tapes status`.
2. Confirm sessions and derived spans exist with `tapesctl sessions list` and `tapesctl sessions list`.
3. Confirm the embedding service is running; for Ollama, use `curl http://localhost:11434/api/tags`.
4. Confirm `embedding.model` and `embedding.dimensions` match.
5. In a split deployment, verify the embed worker is running. A configured but uninitialized search surface returns HTTP `503`.
