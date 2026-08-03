# HTTP APIs

Tapes publishes two separate contracts because reading derived telemetry and ingesting trusted captures have different trust models.

## Read API

The default read API listens on `:8081`. It serves health, derived data, search, skills, operator maintenance, MCP, and its own OpenAPI contract.

| Area | Routes |
| --- | --- |
| Health and contract | `GET /ping`, `GET /openapi`, `/swagger` |
| Sessions | `/v1/sessions`, `/v1/sessions/{id}`, `/v1/sessions/{id}/traces`, `/v1/sessions/{id}/raw_turns`, `/v1/sessions/{id}/export` |
| Traces and spans | `/v1/traces`, `/v1/traces/{trace_id}`, `/v1/traces/{trace_id}/spans/{span_id}` |
| Search and aggregates | `GET /v1/search/spans`, `GET /v1/stats` |
| Skills | `/v1/skills` and session skill routes |
| MCP | `/v1/mcp` |
| Operator actions | `/v1/admin/derive/run`, `/v1/admin/seed/demo`, `/v1/admin/raw-turns/attribution-repair` |
| Cassettes | `GET /v1/cassettes`, `GET /v1/cassettes/{name}/openapi.json`, `/v1/cassettes/{name}`, `/v1/cassettes/{name}/*` |

The authoritative parameters, schemas, and methods are compiled from route registrations and served by the running API at `GET /openapi`; no generated contract is checked in. The aggregate includes admitted cassette operations. See [Cassettes](./cassettes.md) for their manifest and proxy contract. Notable current behavior:

- session listing is cursor-paginated;
- session and trace/span paths use UUID IDs;
- session content is read through traces and spans;
- semantic search exists only at `/v1/search/spans`;
- raw turns remain available at `/v1/sessions/{id}/raw_turns`.

There is no `/v1/search`, `/v1/sessions/summary`, or hash-based session route.

### Attribution repair

`POST /v1/admin/raw-turns/attribution-repair` records an audited, append-only attribution correction for exactly one raw turn — selected by `raw_turn_id` or `paper_proxy_request_id` — without modifying `raw_turns`, then synchronously re-derives the previous and effective sessions.

A `200` is a completed repair. A `202` means the correction committed and is effective, but the synchronous projection rebuild did not finish: `projections_pending` names the stale sessions, which the derive worker converges on its own — the repair is recorded, so do not retry it.

`source_cleanup_pending` is independent of the status code and can accompany either. It discloses an emptied source-session row the cleanup step failed to delete: cosmetic, anchoring no effective turns, and — unlike `projections_pending` — retried by nothing. It does not resolve on its own.

## Private ingest API

The private ingest API defaults to `:8082` and serves its separate contract at `GET /openapi`. The all-in-one `tapes serve` stack starts it alongside the proxy and read API; `tapes serve ingest` runs it as a standalone sidecar. Its write routes are:

- `POST /v1/ingest` — append one completed conversation turn;
- `POST /v1/ingest/transcript` — append one harness transcript file or spawn-anchor row;
- `GET /ping` — health.

A transcript payload is the main session transcript, one subagent's transcript, or a Codex spawn-anchor row (a `sub_agent_activity` rollout record). `agent_id` and `tool_use_id` carry the subagent fork edge the deriver reconciles against the wire capture. The optional `kind` field qualifies Codex anchor rows: absent or empty means spawn evidence; `"interacted"` marks a re-entry record (`send_message`, `followup_task`) that is stored for future rendering and deliberately ignored by derivation. Rows deduplicate on a content hash of `records`, so re-uploading unchanged content is a no-op while a grown transcript appends a new version; the deriver reads the latest version per (session, agent, lifecycle kind), so an interacted row never supersedes a spawn anchor.

Run the standalone form only for sidecar/gateway capture:

```bash
tapes serve ingest --postgres "$TAPES_STORAGE_POSTGRES_DSN"
```

Request bodies are capped at roughly 14.67 MiB. A POST over the limit is rejected with `413` and the surface's standard JSON error envelope (`{"error": "..."}`), the same shape as every other rejection, so capture adapters can parse all failures uniformly. Each body-limit rejection is counted in `tapes_ingest_writes_total{provider="unknown",status="reject_oversize"}` (the body is never parsed, so the provider is unknown) and logged with the declared content length, the configured limit, and the request path.

The ingest server appends to immutable `raw_turns`; it does not provide the read API. Treat it as a private trusted write surface, not as a public application endpoint. Authentication, network policy, and gateway grants are deployment responsibilities.

## Provider proxy

The capture proxy defaults to `:8080`. It exposes provider-compatible request paths, not the Tapes read contract. Clients send LLM traffic to the proxy; they send inspection/search requests to `:8081`.

## CORS and exposure

Do not infer a production security boundary from local listen defaults or generated OpenAPI. Choose network exposure, TLS, authentication, tenant headers, and access control for the deployment environment. Tapes documentation intentionally does not prescribe a hosting redirect or public deployment topology.
