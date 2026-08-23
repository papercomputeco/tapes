---
title: Cassettes
description: The manifest, admission rules, and deployment responsibilities for a service that extends the Tapes read API.
sidebar:
  order: 6
---

> **Alpha/POC:** the current cassette contract is `cassette/v1alpha1`. It is
> suitable for experiments and integrations, but its manifest and runtime
> behavior are not yet a stable compatibility promise.

A cassette is an independently deployed HTTP service that extends the Tapes read
API. Tapes fetches the service's OpenAPI document, admits the manifest embedded
in that document, rewrites the cassette's paths into the Tapes namespace, and
reverse-proxies client requests to the cassette.

A cassette is not a plugin loaded into the Tapes process. It can use any
language or HTTP framework and does not have to import Tapes. The deployment,
not Tapes, starts it and supplies its credentials and configuration.

The complete runnable example is in
[`pkg/cassette/examples/hello-world`](https://github.com/papercomputeco/tapes/tree/main/pkg/cassette/examples/hello-world).
It includes an HTTP service, OpenAPI generation, `cassette.toml`, a container,
PostgreSQL provisioning, and a Compose deployment. A smaller
[`mcp-tool` example](https://github.com/papercomputeco/tapes/tree/main/pkg/cassette/examples/mcp-tool)
advertises one `ping` tool and returns `pong`.
[Running a cassette locally](./cassette-walkthrough.md) walks the hello-world
example end to end, including driving the discovered surface with `tapesctl`.

## What a cassette must provide

A cassette has three kinds of endpoint on its own listener:

1. a health anchor, `/ping` by default;
2. an OpenAPI anchor, `/openapi` by default; and
3. its API below a declared local prefix.

For a cassette named `summary` with the default `prefix_path = "api"`, its own
listener might serve:

```text
GET /ping
GET /openapi
GET /api/summary/reports
```

Tapes republishes only the cassette API:

```text
GET /v1/cassettes/summary/reports
```

The health and OpenAPI anchors describe the process itself. Do **not** include
those root paths as operations in the cassette OpenAPI document. Every path in
the document must be below the cassette's local API prefix, or Tapes refuses the
whole document.

The OpenAPI document must carry an `x-tapes-cassette` root extension containing
the manifest. Tapes uses the configured document URL to both fetch the contract
and determine the origin to which API requests are proxied.

## Minimum manifest

The current manifest kind is `cassette/v1alpha1`. The authored TOML form can be
as small as:

```toml
kind = "cassette/v1alpha1"

[cassette]
name = "summary"
version = "0.1.0"

[depends]
core = "v1"
```

Omitted API anchors default to:

```toml
[api]
health = "/ping"
openapi = "/openapi"
prefix_path = "api"
```

The same logical manifest is required in the OpenAPI document as JSON:

```json
{
  "openapi": "3.1.0",
  "info": {"title": "Summary cassette", "version": "0.1.0"},
  "x-tapes-cassette": {
    "kind": "cassette/v1alpha1",
    "cassette": {"name": "summary", "version": "0.1.0"},
    "depends": {"core": "v1"},
    "api": {
      "health": "/ping",
      "openapi": "/openapi",
      "prefix_path": "api"
    }
  },
  "paths": {
    "/api/summary/reports": {
      "get": {
        "operationId": "listReports",
        "responses": {"200": {"description": "Reports"}}
      }
    }
  }
}
```

Use any OpenAPI library that can add a root extension. The hello-world example
uses `pkg/tapesoapi`, but that package is a convenience rather than part of the
wire protocol.

## The two published forms

A cassette normally publishes the same declaration in two places:

- **`cassette.toml`** is read before the process starts by a registry,
  installer, or orchestrator. It describes the image, port, database access,
  and configuration that deployment tooling may need. Tapes does not read this
  file.
- **`x-tapes-cassette` in OpenAPI** is read from the running service by Tapes.
  This copy is required for admission.

They are two encodings of one schema, not independent manifests. For the same
installation identity, keep them in sync and test that they produce the same
canonical manifest digest. Defaults are applied before canonicalization, and
set-like fields are sorted, so an explicit default and an omitted default have
the same identity.

The Go parser is strict: duplicate keys, unknown fields, trailing JSON values,
and an unsupported `kind` are errors. Parsing applies defaults but does not run
semantic validation; callers of the package must also call `Validate`:

```go
package main

import (
    "fmt"
    "os"

    "github.com/papercomputeco/tapes/pkg/cassette"
    "github.com/papercomputeco/tapes/pkg/cassette/manifest"
)

func main() {
    declared, err := manifest.Load("cassette.toml")
    if err != nil {
        panic(err)
    }
    if err := declared.Validate([]cassette.ContractVersion{"v1"}); err != nil {
        panic(err)
    }
    digest, err := declared.Digest()
    if err != nil {
        panic(err)
    }
    fmt.Fprintln(os.Stdout, digest)
}
```

There is not yet a dedicated `tapes cassette validate` command.

## `cassette/v1alpha1` field reference

### Identity

| Field | Required | Rules and purpose |
| --- | --- | --- |
| `kind` | yes | Must be exactly `cassette/v1alpha1`. |
| `cassette.name` | yes | Two to 32 lowercase letters, digits, or interior dashes; must start with a letter and end with a letter or digit. `public`, `tapes`, and names beginning `pg_` are reserved. |
| `cassette.version` | yes | Non-empty release identifier. The alpha schema does not require semantic version syntax. |
| `cassette.display_name` | no | Human-readable name. |
| `cassette.description` | no | Human-readable summary. |
| `cassette.license` | no | License identifier or prose. |
| `cassette.homepage` | no | Absolute `http` or `https` URL. |
| `cassette.image` | no | Image reference for deployment tooling, without leading or trailing whitespace. If set, `port` is required. Tapes does not pull or run it. |
| `cassette.port` | no | Listener port from 1 through 65535. If set, `image` is required. |
| `x-source-digest` | no | Optional source provenance in `sha256:<64 lowercase hex characters>` form. Tapes checks the shape but does not fetch or verify a source artifact. |

The name is shared across several namespaces:

```text
public route     /v1/cassettes/<name>
Postgres schema  <name>
Postgres role    cassette_<name>
```

A valid name may contain a dash, so quote derived PostgreSQL identifiers rather
than interpolating them as bare SQL identifiers.

### Tapes dependency

```toml
[depends]
core = "v1"
views = ["sessions", "spans"]
```

`depends.core` names a major Tapes contract (`v1`, `v2`, and so on), not a Tapes
binary release. A running core admits the cassette only if it serves that
contract. The current default contract is `v1`.

Each `depends.views` entry must be a unique lowercase PostgreSQL identifier of
at most 63 bytes. `raw_turns` is explicitly forbidden: it is an internal capture
log, not a cassette contract view. The manifest derives requested grants as
`tapes_<core>.<view>`, for example `tapes_v1.spans`.

The `v1` contract publishes four views, created by core's migrations in the
`tapes_v1` schema:

| View                 | Fronts                                        |
| -------------------- | --------------------------------------------- |
| `tapes_v1.sessions`  | the sessions table                            |
| `tapes_v1.spans`     | the current span projection generation        |
| `tapes_v1.span_turns`| the current span-turn projection generation   |
| `tapes_v1.span_links`| the current span-link projection generation   |

The views are the stable names. The physical projection tables behind them are
date-versioned and rotate when a new projection generation lands; the views are
repointed in the same migration, so a cassette granted the views never notices.
Point queries and grants at `tapes_v1.*`, never at a physical table name.

This is a declaration only. Tapes does not check that a named view exists, apply
grants, create roles, or give the cassette a database credential. Deployment
tooling owns those actions.

### API anchors and path mapping

```toml
[api]
health = "/ping"
openapi = "/openapi"
prefix_path = "api"
```

`health` and `openapi` must be absolute paths without a host, query, fragment,
or `.`/`..` segment. The current POC records both anchors, but Tapes fetches the
exact OpenAPI URL configured by the operator and does not currently probe the
health anchor.

`prefix_path` is the path before the cassette name on the cassette's own
listener. Each slash-separated segment must begin with a lowercase letter or
digit; the rest may contain only lowercase letters, digits, dashes, or
underscores. Prefer slash-free outer edges, such as `api` or `extensions/v2`.
Tapes normalizes surrounding slashes. Set it to `/` to mount directly below the
name:

| `prefix_path` | Cassette-local path | Public path |
| --- | --- | --- |
| omitted or `api` | `/api/summary/reports` | `/v1/cassettes/summary/reports` |
| `extensions/v2` | `/extensions/v2/summary/reports` | `/v1/cassettes/summary/reports` |
| `/` | `/summary/reports` | `/v1/cassettes/summary/reports` |

Every documented OpenAPI path must be contained by the local path in the middle
column. Tapes rewrites that prefix in both the cached per-cassette document and
the aggregate document.

### Owned tables

Declare tables the cassette owns in its own schema:

```toml
[[tables]]
name = "daily_summary"
```

Names must be unique lowercase PostgreSQL identifiers of at most 63 bytes.
Discovery publishes the qualified name, such as `summary.daily_summary`.

Again, this is desired deployment state. The cassette owns its migrations;
Tapes does not create the schema or tables.

### Configuration schema

A manifest can describe values that the deployment supplies to the cassette:

```toml
[[config]]
key = "llm.model"
type = "string"
required = true
enum = ["claude", "other"]
description = "Model used to create summaries."

[[config]]
key = "batch_size"
type = "int"
default = 50
min = 1
max = 500

[[config]]
key = "llm.api_key"
type = "string"
required = true
secret = true
```

Keys consist of dotted lowercase snake-case segments. They must be unique both
as keys and after conversion to the conventional environment name:

```text
llm.model   -> CASSETTE_LLM_MODEL
batch_size  -> CASSETTE_BATCH_SIZE
```

Supported types are:

| Type | Default value rules | Extra constraints |
| --- | --- | --- |
| `string` | TOML/JSON string | `enum` is allowed only here, and its values must be unique. |
| `int` | Integer | Optional inclusive `min` and `max`; `min` must not exceed `max`. |
| `bool` | Boolean | — |
| `duration` | String accepted by Go's duration parser, such as `30s` or `5m` | — |
| `json` | A string whose contents are valid JSON | The manifest value is a string, not an inline TOML object. |

A secret setting cannot have a default. Tapes publishes this schema, never
runtime values, and does not inject environment variables. The `CASSETTE_...`
name is a convention for the deployment and cassette to implement.

The current discovery response projects each setting's key, type, required and
secret flags, default, and description. Constraints such as `enum`, `min`, and
`max` remain in the manifest but are not projected into discovery, so deployment
tooling that needs the full configuration schema should read the manifest.

## OpenAPI admission rules

Before publishing a cassette, Tapes:

1. fetches the configured URL with `GET`, a ten-second default timeout, and an
   8 MiB response limit; an initial or changed document must return HTTP 200,
   while a conditional refresh may return 304;
2. refuses redirects by default;
3. parses the OpenAPI document and its required root manifest extension;
4. validates the manifest against the contracts this core serves;
5. verifies that every path is below the declared local prefix;
6. rewrites paths to `/v1/cassettes/<name>`; and
7. compiles the rewritten document to ensure it can be published.

Every operation must declare at least one response. If an operation supplies an
`operationId`, it must be unique within that cassette; Tapes synthesizes an ID
for anonymous operations in the aggregate. Component names and operation IDs
are namespaced by cassette name in the aggregate `/openapi` document, so
independently authored cassettes can use the same local names. A cassette's own
cached document remains available at
`/v1/cassettes/<name>/openapi.json`.

The configured source must be a full `http` or `https` URL with a host and no
userinfo or fragment. Tapes uses only its origin (scheme, host, and port) as the
reverse-proxy target, so the service API must be reachable on the same origin as
the OpenAPI document. Do not change the manifest name served by an already
resolved source URL; the source is pinned to its first admitted identity.

Cassette requests receive `X-Tapes-Cassette: <name>` and standard forwarded
headers.

Request bodies are read whole; response bodies are streamed. A cassette may hold
a response open for as long as it needs to, and each write reaches the client as
the cassette makes it rather than when the response ends — so server-sent event
streams and other long-lived responses work through `/v1/cassettes/<name>`. A
response that declares a `Content-Length` keeps it and is framed as one sized
body; anything else is sent chunked.

Two consequences are worth knowing:

- **Ask for an event stream by name.** Send `Accept: text/event-stream` when
  reading one. Tapes compresses responses by default, and compressing a stream
  re-buffers it; that header is what tells Tapes to leave the stream alone, and
  it must come from the client because the decision is made before the cassette
  has answered.
- **Backpressure belongs to the client.** A client that reads slowly slows the
  cassette rather than accumulating in the Tapes process, so a cassette that
  streams should expect writes to block.

## MCP tool advertisement

A cassette can expose an operation through the Tapes MCP endpoint by adding
`x-tapes-mcp` to that operation:

```json
{
  "post": {
    "operationId": "summarizeSession",
    "summary": "Summarize a session",
    "x-tapes-mcp": {
      "name": "summarize_session",
      "annotations": {
        "readOnlyHint": true,
        "idempotentHint": true,
        "openWorldHint": false
      }
    },
    "requestBody": {
      "required": true,
      "content": {
        "application/json": {
          "schema": {
            "type": "object",
            "properties": {"session_id": {"type": "string"}},
            "required": ["session_id"]
          }
        }
      }
    },
    "responses": {"200": {
      "description": "Summary",
      "content": {"application/json": {"schema": {
        "type": "object",
        "properties": {"summary": {"type": "string"}}
      }}}
    }}
  }
}
```

For a cassette named `summary`, this registers the MCP tool
`summary.summarize_session`. The operation summary and description become the
tool title and description. Annotations use the MCP field names
`readOnlyHint`, `destructiveHint`, `idempotentHint`, and `openWorldHint`; they
are client hints, not authorization rules.

The initial bridge is deliberately narrow. An advertised operation must:

- be declared in an OpenAPI 3.1 document;
- use `POST` with no path, query, header, or cookie parameters;
- have an inline, required `application/json` request body whose schema resolves
  to an object; and
- return a JSON object on success; and
- advertise no more than 128 tools per cassette.

Put every tool argument in the JSON body. Local
`#/components/schemas/...` references are supported and are bundled into the
standalone JSON Schema published through MCP. Remote references and a
request-body `$ref` are not supported. A cassette needing other HTTP semantics
should expose a small JSON-body POST facade rather than relying on Tapes to act
as a general OpenAPI client.

Malformed advertised tools refuse the refreshed cassette document. Unknown
extension fields are ignored so newer declarations remain compatible with older
Tapes servers. If a later refresh fails, Tapes retains the previously admitted
document and tools just as it retains the cassette's stale HTTP surface. Tool calls use the admitted
cassette origin, forward the caller's end-to-end headers, set
`X-Tapes-Cassette`, refuse redirects, and return non-2xx responses as MCP tool
errors.

The tool declaration lives on the operation rather than inside
`x-tapes-cassette`, so adding or changing a tool changes the OpenAPI ETag but not
the cassette manifest digest. Admitting a cassette also trusts its operation and
schema prose: MCP clients may place that text directly in an agent's context.

## Run and register a cassette

Tapes does not start cassette processes. Start the cassette through your normal
process manager, then give the Tapes API server the exact URL of its OpenAPI
document:

```toml
# .tapes/config.toml
cassettes = ["http://127.0.0.1:9999/openapi"]
```

Equivalent CLI configuration is:

```bash
tapes serve --cassettes=http://127.0.0.1:9999/openapi
# or: tapes serve api --cassettes=http://127.0.0.1:9999/openapi
# or: TAPES_CASSETTES=http://127.0.0.1:9999/openapi tapes serve
```

Tapes retries unresolved sources during startup and refreshes documents every
30 seconds by default. Change that interval with `--cassette-refresh`. A
cassette being unavailable does not prevent Tapes from starting.

Inspect the installed surface with:

```bash
curl http://localhost:8081/v1/cassettes
curl http://localhost:8081/v1/cassettes/summary/openapi.json
curl http://localhost:8081/openapi
curl http://localhost:8081/v1/cassettes/summary/reports
```

Discovery reports admitted cassettes, their manifest digests, OpenAPI status,
and rejected source problems. After a successful admission, a later refresh
failure marks the cached document stale rather than deleting it. Removing the
source from configuration withdraws the cassette.

The `manifest_digest` in discovery identifies canonical manifest metadata. The
`ETag` on a cached cassette OpenAPI response identifies the complete republished
OpenAPI document; these digests answer different questions and need not match.

## Database and deployment responsibilities

For a manifest named `summary`, depending on `v1` views `sessions` and `spans`,
and declaring table `daily_summary`, the derived grant plan is:

```text
role        cassette_summary
own schema  summary
SELECT      tapes_v1.sessions
SELECT      tapes_v1.spans
owned table summary.daily_summary
```

The deployment should:

- create and manage the cassette role and credentials;
- grant only the declared contract views;
- allow or create the cassette's schema as appropriate;
- supply database and manifest-declared configuration values directly to the
  process; and
- let the cassette run its own schema migrations.

Tapes publishes the declaration but deliberately performs none of those steps.
It also does not pull `cassette.image`, expose `cassette.port`, or manage the
cassette lifecycle.

## Builder checklist

Before handing a cassette to an operator:

- serve a 200 health response at the declared health anchor;
- serve one valid OpenAPI JSON document with HTTP 200 at the declared OpenAPI
  anchor;
- embed the exact `cassette/v1alpha1` manifest at `x-tapes-cassette`;
- keep every OpenAPI operation under `/<prefix_path>/<name>` (or `/<name>` when
  `prefix_path = "/"`);
- make any supplied operation IDs unique and declare responses for every
  operation;
- validate both TOML and embedded copies, and compare their manifest digests
  for the same installation identity;
- keep deployment metadata, listener port, runtime name, and documented paths
  consistent;
- provision database access outside Tapes and run cassette-owned migrations;
- test direct health/OpenAPI access, discovery, the cached spec, the aggregate
  spec, and at least one proxied request; and
- describe a streaming endpoint with its real media type (`text/event-stream`),
  and document that its clients must send a matching `Accept` header.
