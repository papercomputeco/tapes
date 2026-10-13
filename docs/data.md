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
tapesctl sessions list --api-url http://localhost:8081
tapesctl sessions list --limit 20
tapesctl sessions get <session-id>
tapesctl sessions traces <session-id>
tapesctl sessions raw-turns <session-id>
```

`sessions list` and `sessions get` render a table and a record for the
terminal; `--json` on either prints the server's document verbatim. `traces`
and `raw-turns` print the document as it is, so all four compose with `jq`. `--api-url` falls back to `TAPES_API_URL`, and then to the value from `tapesctl config set api-url`. A running read API is required; start one with `tapes serve`.

The `<session-id>` these take is the Tapes session id from `sessions list`. It
is not the harness session id `tapesctl start` prints when it exits.

A session started by another session carries `parent_session_id` (the parent's Tapes session id) and `parent_harness_id` (the parent's harness). The two harnesses differ when one harness launched the other, such as a Codex session started from a Claude session.

Session responses include `display_title`, the label clients should render. It
prefers a user rename, then a generated title, then the first human prompt
preview before falling back to harness identity. Prompt previews omit a leading
Codex App plugin invocation such as `[@visualize](plugin://visualize@openai-bundled)`;
the user's request remains as the human-facing title. Valid JSON object or array
previews are treated as tool payloads and skipped rather than shown as titles.
This classification uses the complete prompt, so JSON payloads remain skipped
when their displayed preview is truncated.

### Sessions without a harness session id

A turn captured without a harness session id is grouped by its conversation:
`harness_session_id` is the first 16 hex characters of the hash of the
request's first message, which every turn of the conversation re-sends. A
conversation that goes quiet for longer than
[`sessions.idle_gap`](./configuration.md#idle-split) (30 minutes by default)
and then resumes starts a new session, whose `harness_session_id` is the
conversation's id, a dash, and the UTC second at which the turn that opened it
was captured: `be436c07b169c409-20261013T170000Z`. The first session of a
conversation keeps the bare id. A session id the harness supplies is never
split.

Turns are placed by when they were captured upstream, not by when they reach
Tapes, so a retried or backfilled turn lands where it belongs. A turn joins the
session whose captured turns it falls among, else the one whose last turn is at
most the gap before it, else the one whose first turn is at most the gap after
it; otherwise it starts a session. Placement is decided when a turn is captured
and never revised. A turn that is already stored keeps its session when it is
sent again. The one consequence of never revising: a late turn that would have
bridged two sessions in arrival order — within the gap of both — joins the
earlier one, and the two stay separate.

Each session derives, lists, exports, and deletes on its own, exactly like a
session with a harness-supplied id. An [attribution repair](./apis.md#attribution-repair) moves a
turn between sessions as for any other turn; it does not change where later
turns of the conversation are placed.

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

`tapesctl export` is a thin client for the export cassette's
`GET /v1/cassettes/export/sessions/{id}`. It streams the API's projection
rather than maintaining a separate renderer or state store. Core no longer
serves an export route of its own; a deployment without the export cassette
has no export endpoint.

```bash
tapesctl export <session-id> -o session.jsonl
tapesctl export <session-id> --detail traces
tapesctl export <session-id> --api-url http://localhost:8081
```

Detail modes:

- `spans` (default): trace records with full span trees and links;
- `traces`: turn headers without spans or links.

With `-o`, the byte count is written to stderr rather than stdout, so redirecting stdout stays clean. Without it, the bundle goes to stdout.

A running read API with the export cassette is required. For a multi-session export, the cassette also provides `GET /v1/cassettes/export/sessions`; consult its OpenAPI document for parameters.

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

The trace endpoints take `?payload=preview` for a bounded view of every
span's content. The deriver stores that preview next to the payload — the
spans table carries `input_preview` and `output_preview` beside `input` and
`output` — and a preview read selects only the preview columns, so it never
touches the payload. Rows derived before those columns existed report
`payload: "preview_pending"` until a backfill fills them in.
See [HTTP APIs](./apis.md#span-payload-modes).

Browse the live contract at `http://localhost:8081/swagger`, or fetch it from `http://localhost:8081/openapi`. See [HTTP APIs](./apis.md) for the surface and trust boundary.

## Payload compression on disk

The raw layer (`raw_turns.raw_request`, `raw_turns.response`) and the span
payloads and previews (`input`, `output`, `input_preview`, `output_preview`)
are JSONB, so Postgres compresses any value past its TOAST threshold. Those
columns are set to `lz4`, which compresses this JSON about as well as the
`pglz` default and decompresses several times faster; every read of a turn or
a span is a decompression, so that is where it shows. The setting is per
column — the cluster's `default_toast_compression` is untouched — and applies
to new rows only: values stored before it keep their `pglz` and read back
correctly until something rewrites them (a dump and restore does; `VACUUM
FULL` and `CLUSTER` copy compressed bytes as they are). Whether to rewrite
existing rows is an operator decision; nothing in Tapes does it for you.
`SELECT pg_column_compression(raw_request) FROM raw_turns WHERE id = ...`
shows which method a stored value carries (`NULL` for values small enough to
stay inline).

## Trace page order index

A trace's spans are read in `(seq, started_at, span_id)` order, and
`spans_20260615_page_order_idx` carries that key under `(org_id, trace_id)`,
so resuming a trace read from a cursor seeks straight to it instead of sorting
the whole trace, and with SSD planner costs (`random_page_cost` near 1) the
whole read streams from the index in order with no sort at all. The migration
builds it with a plain `CREATE INDEX`, which on a very large table blocks
writes for the length of the build; an operator who would rather not pay that
at deploy time can build it first by hand with `CREATE INDEX CONCURRENTLY`
under the same name, and the migration's `IF NOT EXISTS` then skips it.
