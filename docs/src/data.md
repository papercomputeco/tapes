# Inspecting and exporting data

## Check the active setup

```bash
tapes status
```

This reports the selected `.tapes/` directory, provider and upstream, read API reachability, and a brief capture summary.

## List sessions

Reading the data model is a client operation, so it lives in [`tapesctl`](https://github.com/papercomputeco/tapesctl):

```bash
tapesctl sessions list --tapes-url http://localhost:8081
tapesctl sessions list --limit 20
tapesctl sessions get <session-id>
tapesctl sessions traces <session-id>
```

Each prints the server's JSON verbatim, so it composes with `jq`. `--tapes-url` falls back to `TAPES_URL`. A running read API is required; start one with `tapes serve`.

## Browse interactively

```bash
tapesctl sessions list
tapesctl sessions get <session-id>
```

Deck shows session aggregates and drills into traces and spans. See [Deck](./deck.md).

## Export JSONL

`tapesctl export` is a thin client for `GET /v1/sessions/{id}/export`. It streams the API's projection rather than maintaining a separate renderer or state store.

```bash
tapesctl export <session-id> -o session.jsonl
tapesctl export <session-id> --detail traces
tapesctl export <session-id> --tapes-url http://localhost:8081
```

Detail modes:

- `spans` (default): trace records with full span trees and links;
- `traces`: turn headers without spans or links.

A running read API is required. For a multi-session export, the API also provides `GET /v1/sessions/export`; consult its current OpenAPI parameters.

## Inspect over HTTP

Common read operations include:

```bash
curl http://localhost:8081/v1/sessions
curl http://localhost:8081/v1/sessions/<session-uuid>
curl http://localhost:8081/v1/sessions/<session-uuid>/traces
curl http://localhost:8081/v1/sessions/<session-uuid>/raw_turns
curl http://localhost:8081/v1/traces/<trace-uuid>
curl http://localhost:8081/v1/traces/<trace-uuid>/spans/<span-uuid>
curl http://localhost:8081/v1/stats
```

Session IDs and trace/span IDs are UUIDs, not content hashes. `GET /v1/sessions/{id}` returns session metadata; conversation content is on the trace/span endpoints. Raw-turn retrieval preserves the original capture separately from the derived model.

Browse the live contract at `http://localhost:8081/swagger`, or fetch it from `http://localhost:8081/openapi`. See [HTTP APIs](./apis.md) for the surface and trust boundary.
