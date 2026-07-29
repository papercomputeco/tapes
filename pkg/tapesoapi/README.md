# tapesoapi

OpenAPI v3 parsing, aggregation, and compilation for tapes.

These are implementation notes: what the pieces are, and why they are shaped the
way they are. The API itself is documented in the Go doc comments — `go doc
./pkg/tapesoapi` — and this file does not repeat them.

## Why this package exists

tapes has to publish one description of an HTTP origin that is assembled from two
sources nothing off the shelf treats alike:

- **Live Fiber routes.** The read API and the ingest server describe themselves
  as they mount, in the same call that mounts them.
- **Documents fetched over HTTP.** A cassette publishes its own OpenAPI at its
  own `/openapi`; core fetches it, moves its paths onto the public surface, and
  reverse-proxies it.

A generator handles the first and a parser handles the second, and gluing a
generator's output to a parser's input means two models of the same thing that
disagree at the seams. Here both are one thing — a `Fragment`, a partial
contribution tagged with a `Provenance` — so a single merge → validate → render
pipeline serves both. `KindRoute`, `KindDocument`, `KindManual`, and
`KindReflect` are the four kinds in use; `Kind` is a string rather than an enum
so a caller can name its own.

## The contract is compiled, never stored

There is no checked-in contract file, no embedded one, and no generator step. Each
server compiles its document from its own route registrations and serves it at
`GET /openapi`. **There is nothing to regenerate**, which is the point: a
committed contract describes the routes as of the last time someone remembered
to regenerate it, and every check you add for that staleness is a check for a
problem this design does not have. Compiling per request is also the only way
`/openapi` can describe cassettes that were mounted at runtime, which no
build-time artifact could.

The cost is per-field prose. Field descriptions come from ordinary Go doc
comments (never from struct tags), and a deployed binary has no source tree to
read them out of. So a running server's document carries route and operation
prose but not per-field prose; `tapes dev openapi [api|ingest]` compiles the
fully documented version from a checkout for consumers that want it (it defaults
to `--docs-root .`; pass `--docs-root ''` for shapes only).
`pkg/tapesoapi/gosource` is the go/ast reader behind that, and it is
generator-only by design.

## Pipeline

```
Add* ──(all I/O)──▶ fragments ──▶ Compile ──▶ CompiledDoc
                                     │
       snapshot ▶ merge ▶ parser defaults ▶ version compat
              ▶ resolve refs ▶ render ▶ structure ▶ lint ▶ freeze
```

`Parser` is a mutable accumulator behind a mutex — the Fiber adapter contributes
a fragment per route registration, and nothing orders those against a concurrent
`Compile`. Everything that can fail slowly (reading a file, fetching a document,
parsing YAML) happens at `Add` time.

`Compile` is a pure function of the fragments held. It never reads a file or
opens a socket, so it is safe on a request path — which is exactly what
`/openapi` does. Compiling the same fragments twice is byte-identical, so
`Fingerprint()` (sha256 of the rendered JSON) is a usable ETag and generated
contracts are diffable in CI. Determinism survives concurrent registration
because merge order comes from `Provenance.sortKey()`, not from arrival order.

`Freeze` marks a parser as closed; further `Add`s return `ErrFrozen`. It is for
the case where a parser is handed out for reading after its owner is done
contributing.

## Versions: neutral model, versioned render

The IR stores the *union* of 3.0 and 3.1 semantics. `render()` is the only place
`target Version` is consulted, and three keywords carry nearly the whole
3.0-vs-3.1 story:

| Concept | 3.0 | 3.1 |
| --- | --- | --- |
| Nullability | `nullable: true` | `type: ["string", "null"]` |
| Exclusive bounds | `exclusiveMinimum: true` alongside `minimum` | numeric `exclusiveMinimum` |
| Single fixed value | `enum: [x]` | `const: x` |

Plus: webhooks and `info.license.identifier` render only under 3.1, and
`examples` (plural) only under 3.1.

`V30` is `3.0.3` and is the default. Not because 3.1 is unfinished — both render
— but because the Rust client generator tapes publishes for reads (progenitor)
only accepts 3.0.x. A 3.1-only construct reaching a 3.0 render is *lost*
information, so `checkVersionCompatibility` refuses it and names what would be
lost; `WithDowngradeLossy()` says to approximate instead. The cassette aggregate
passes that option, because approximating a cassette's 3.1 construct beats
refusing to describe a surface clients can already reach.

`ParseVersion` maps the whole 3.0.x line to `V30` and the whole 3.1.x line to
`V31`. Swagger 2.x is rejected with instructions rather than half-converted.

## Three kinds of check, kept separate

- **`structure.go` — spec MUST rules.** A path that does not start with `/`, an
  operation with no responses object, a `minLength` above `maxLength`, a security
  requirement naming an undeclared scheme. Violating one of these means the
  document is not OpenAPI.
- **`validate.go` — `LintRule`s.** Legal but unwise, and therefore switchable per
  call via `WithLint`: `OperationIDPresent`, `OperationIDUnique`,
  `ResponsesDeclared`, `NoOrphanComponents`. These exist because a document that
  omits an operationId or declares no responses is valid OpenAPI and useless to a
  code generator, and being generated from is the whole purpose of the published
  document.
- **`checkVersionCompatibility` — downgrade loss.** Not a defect in the document;
  a mismatch between the document and the requested target.

Structure runs before lint, so the message a caller sees describes the worse
problem first.

### Why our own validator instead of importing one

The long-form reasoning is at the top of `structure.go`; the summary is:

1. **The failing test would be the wrong test.** Round-tripping the rendered
   bytes through a third-party parser catches *rendering* failures, which render
   tests already pin. It misses the failure that actually happens: an ingested
   document, or a route description written by hand, that says something the spec
   forbids. Catching that well needs the provenance — "cassette `summary`",
   "route registered at `api/sessions_handlers.go:88`" — and provenance is
   present in the IR and gone from the bytes.
2. **A published contract should not be able to fail to build because someone
   else's validator changed its mind.** `/openapi` compiles on the request path.
3. Depending on a second OpenAPI library to check the first one is an
   anti-pattern even when the dependency is validation-only. A separate,
   heavier-weight conformance harness is a reasonable thing to want, and it would
   live outside tapes.

## Aggregation

Merging independently authored documents is where the interesting decisions are.
`ConflictPolicy` is `PolicyError` by default, and `ConflictError` collects *all*
conflicts rather than returning the first — someone aggregating a fleet wants the
list. `PolicyFirstWins` / `PolicyLastWins` are for callers that must keep
answering.

Per-ingest options rewrite a document into a shape that can coexist with others.
`applyDocOptions` applies them in a fixed order — **filter, namespace, name, then
prefix**:

- `WithComponentNamespace` — OpenAPI's component space is flat and two cassettes
  may each define a `Row`. Namespacing rewrites the definitions and every `$ref`
  that reaches them.
- `WithOperationIDPrefix` — the operation-level counterpart. Two cassettes may
  each have named an operation `read`, and an id must be unique across the
  document it appears in.
- `WithPathPrefix` — mounts a document's paths under a prefix.
- `WithoutInfo` / `WithoutServers` / `WithoutRootExtensions` — a cassette's
  `info`, `servers`, and `x-tapes-cassette` manifest describe the cassette, not
  the origin. Carrying its `servers` through would point clients at a listener
  they cannot reach.

Naming before prefixing is the one ordering a caller can observe: an id
synthesized for an anonymous operation comes from the path *as the document
declared it*. A caller that wants the mounted path in the id — because that is
the path a generated client calls — rewrites the paths before ingesting instead
of passing `WithPathPrefix`. The cassette runner does exactly that.

`SynthesizeOperationID` is exported so the Fiber adapter and the aggregate cannot
disagree about what one route is called (`GET /v1/sessions/{id}` →
`getV1SessionsId`).

## The reflector holds the component registry

`parser.Schema(SessionItem{})` reflects a Go type, claims a component name, and
returns a `$ref` to it. The definition lives in the **reflector**, not in any
fragment; `Compile` folds `reflector.Components()` in as a synthetic fragment at
the end.

The consequence matters for aggregation: compiling another parser's fragments
against a fresh reflector produces a document full of refs to schemas it does not
define. An aggregate has to *borrow* the source parser's reflector —
`WithSchemaReflector(base.Reflector())`. Borrowing is read-only; the registry
hands out clones under its own lock.

`Schema` also cannot fail. A type it cannot reflect degrades to
`&Schema{Description: "schema unavailable: …"}`, because the call site is a route
registration that has nowhere to put an error.

## `Document` is not the IR

`Document` (`document.go`) is a generic, order-preserving tree over a parsed
OpenAPI file, with `Extension`, `Paths`, `Version`, `RewritePrefix`, `Marshal`,
and `Fragment`. It is deliberately *not* the IR.

A cassette document is republished — tapes serves it back at
`/v1/cassettes/{name}/openapi.json` — and round-tripping it through the IR would
silently drop every field this package does not model, `x-tapes-cassette`
included. So republication is a tree rewrite, and only the merge path goes
through the IR. `Parse` rejects duplicate keys rather than letting the last one
win.

## Subpackages

- **`oasfiber`** — the Fiber adapter, so the core has no web-framework
  dependency. `Wrap` returns a `Router` that mounts and describes a route in one
  call; `openAPIPath` converts `:id` → `{id}`; `callerLocation` records the
  registration site for provenance. `Undocumented` chooses what an undescribed
  route means (`Stub`, `Skip`, `Fail`), errors are collected and checked once via
  `Err()`, and `Server` caches the compiled document by fingerprint with an
  `Invalidate` hook for runtime mounts.
- **`gosource`** — go/ast doc-comment extraction. Generator-only; never linked
  into a served path.
- **`v3.0` / `v3.1`** — embedded fixture documents (petstore, nullable-and-bounds,
  components-and-refs, vendor-extensions, discriminated-union, conflict pairs,
  webhooks-and-const). Each is described where it is declared, on the principle
  that a fixture no test explains is a fixture nobody dares change.

Tests are Ginkgo/Gomega throughout, per `AGENTS.md`.

## How tapes consumes it

- **`api`** — `NewOpenAPIParser` builds the parser every route registers into;
  `api/openapi_routes.go` is the registration surface. `GET /openapi` compiles
  and serves; `GET /swagger` serves a Scalar viewer pointed at `/openapi`, and
  nothing else serves a spec.
- **`ingest`** — the same pattern for the write surface, publishing its own
  contract at its own `/openapi`. It compiles once behind a `sync.Once`, because
  every input to that compile is compiled in; the API's aggregate cannot, because
  cassettes mount at runtime.
- **`api/cassetterunner`** — `Document` compiles core's fragments plus every
  fetched cassette document into the canonical origin contract: component
  namespace, operationId prefix, `PolicyLastWins`, borrowed reflector, structural
  validation, `WithLint(OperationIDPresent, OperationIDUnique,
  ResponsesDeclared)`, and `WithDowngradeLossy()`.

  Validating that aggregate opened a failure mode, and `publishable` closes it:
  `/openapi` is compiled as a contract rather than as a best-effort catalogue, so
  a single malformed cassette document could otherwise fail the whole endpoint and
  take every healthy cassette's surface down with it. Each document is therefore
  compiled *alone* at admission time — inside `republish`, after the path rewrite,
  so what passes is byte-for-byte what the aggregate merges — and a document that
  cannot be compiled is rejected against the source that served it.
- **`internal/openapicheck`** — asserts that everything served is described and
  nothing described is unserved. Now that the contract is compiled from the
  registrations, staleness is gone as a way for the two to disagree; what is left
  is routes mounted directly on the `*fiber.App` (metrics, the viewer, `/openapi`
  itself), and the check is what keeps that exemption list deliberate.
