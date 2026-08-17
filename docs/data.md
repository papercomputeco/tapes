---
title: Inspecting and exporting data
description: Read captured sessions from the client, the built-in browser UI, an export bundle, or the HTTP read API directly.
sidebar:
  order: 10
---

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
tapesctl sessions raw-turns <session-id>
```

Each prints the server's JSON verbatim, so it composes with `jq`. `--tapes-url` falls back to `TAPES_URL`, and then to the value from `tapesctl config set tapes-url`. A running read API is required; start one with `tapes serve`.

The `<session-id>` these take is the Tapes session id from `sessions list`. It
is not the harness session id `tapesctl start` prints when it exits.

Session responses include `display_title`, the label clients should render. It
prefers a user rename, then a generated title, then the first human prompt
preview before falling back to harness identity. Prompt previews omit a leading
Codex App plugin invocation such as `[@visualize](plugin://visualize@openai-bundled)`;
the user's request remains as the human-facing title. Valid JSON object or array
previews are treated as tool payloads and skipped rather than shown as titles.
This classification uses the complete prompt, so JSON payloads remain skipped
when their displayed preview is truncated.

## Browse in a browser

The API binary can serve a small same-origin UI at `/`, in the style of
Prometheus's built-in one. It is off by default:

```bash
tapes serve --api-web-ui
```

Then open `http://localhost:8081/`. It lists sessions, browses a session's turns,
shows the aggregate stats, and can trigger a derive run or a demo seed — the same
`/v1/sessions`, `/v1/stats`, and `/v1/admin` responses the client uses. There is
no frontend build step and no external scripts; equally, there is no search and
no export, so use the client or the API for those.

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

With `-o`, the byte count is written to stderr rather than stdout, so redirecting stdout stays clean. Without it, the bundle goes to stdout.

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
