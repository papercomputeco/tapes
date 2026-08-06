# Drop-reason fixtures — why a turn was not captured

L0-layer fixtures, sibling to `fixtures/envelope/`, `fixtures/thread/` and
`fixtures/content-encoding/`: small, synthetic, language-neutral JSON cases,
one per drop reason.

A capture path that declines to record a turn owes an answer to "why". The
gateway adapter has carried a closed enum of fourteen answers; the standalone
client carries none — it logs a sentence per site. Neither was specified
anywhere, so nothing decided the question that matters:

> Which of these answers are rules that any implementation of tapes capture
> must share, and which are artefacts of how one deployment moves bytes?

Both are real, and both must stay observable. Only the first is contract. This
corpus draws that line and writes it down, with **both** halves specified — the
transport reasons are here precisely so that "this one is not shared" is
recorded rather than assumed. A reason nobody classified is a reason the next
implementation guesses about.

## The line

**Capture policy (contract).** Whether a turn is capturable at all. These are
properties of the exchange — its method, its path, its status, its bytes — that
any implementation can observe and must agree on. Two capture paths that
disagree about one of them record different sessions from identical traffic,
which is the property the whole fixture corpus exists to protect.

**Transport / runtime (not contract).** How one deployment failed to move
bytes. Each of these names a component a different implementation need not
have: a dispatch queue, a downstream client connection, a remote ingest
endpoint, an ext_proc stream. A client that writes turns to a local file cannot
produce them, and requiring it to would specify one deployment's plumbing as
everyone's contract.

The test is not "does this feel like policy" but: **could a conformant
implementation with different plumbing produce this reason at all?** If no, it
is transport. If yes and it disagrees, capture fidelity differs.

| reason | class | why |
| --- | --- | --- |
| `upstream_status` | policy | a turn is a completed exchange; an error response is a record of one failing to happen |
| `non_turn_request` | policy | adjacent endpoints and non-POST methods on a turn path are not conversation |
| `request_decode` | policy | the shared decode policy refused the request body |
| `empty_response` | policy | a response that completed carrying nothing is not a turn |
| `unknown_provider` | policy | the set of shapes capture can read is a property of the build |
| `response_decode` | policy | the shared decode policy refused the response body |
| `reducer_error` | policy | the bytes were readable and the content was not a turn |
| `client_disconnect` | transport | requires a downstream connection to observe being closed |
| `upstream_no_response` | transport | requires observing a stream torn down before its response phase |
| `missing_status` | transport | an ext_proc message-ordering violation; unreachable where a status is read off a response |
| `sem_full` | transport | requires a bounded dispatch queue |
| `ingest_reject` | transport | requires a remote ingest endpoint that can refuse |
| `ingest_timeout` | transport | requires a remote ingest endpoint that can time out |
| `marshal_error` | transport | serialising to one deployment's wire envelope |

Three of these were close calls, and the cases record the argument rather than
only the verdict:

* **`empty_response` vs `upstream_no_response`** — both mean "no response
  bytes", and only one is contract. The difference is whether the response
  phase *completed*: a normally-completed response carrying nothing is a
  property of the exchange, which any implementation sees; a stream torn down
  before its response phase is a property of the connection, which only a
  streaming interception path can distinguish.
* **`missing_status`** — reads like policy ("don't capture a turn whose status
  you never saw") and is transport. It exists only because Envoy can send a
  response body before its headers, violating the ext_proc contract. An
  implementation that reads a status directly off a response cannot reach it.
* **`marshal_error`** — a failure to serialise the outbound envelope. It is a
  bug rather than a rule, and the envelope it fails on is one deployment's.

## What is specified, and what is only documented

The vocabulary — every reason, its class, the constant that carries it, and
what triggers it — is specified as data, one case per file, and is executable:
a consumer asserts its own reasons against `cases/*.json` and fails if either
side has a reason the other does not.

The **behaviour** behind each reason is a different matter, and only one of them
is expressible as cases today. `non_turn_request` is a pure function of
`(method, path)`, so `cases/non_turn_request.json` carries `examples` that any
implementation can run. The rest depend on bytes, streams, reducers or a live
upstream; a case for them would either restate prose as JSON or pin one
implementation's internals. Those cases carry `not_expressible` saying so.

This is the honest state, not the target: expressing more of them means giving
each implementation a pure classifier to point the cases at, which is a change
to the implementations first and to this corpus second — never the reverse.

## Precedence

A turn can satisfy several reasons at once. A `HEAD` probe that returned 500
with no body satisfies three, and two implementations that report different
reasons for it have given two different answers to the same question even
though both correctly declined to capture it.

The specified order, which is the order the reference adapter evaluates them:

```
upstream_status → non_turn_request → request_decode → empty_response
                → unknown_provider → response_decode → reducer_error
```

`upstream_status` is first because a non-success exchange is not examined
further. `reducer_error` is last because everything before it says the turn was
capturable in principle and only the content says otherwise.

Precedence is stated here and asserted nowhere: asserting it needs the same
pure classifier the behaviour cases need.

## Not in the contract

**`DefaultLargeTurnThreshold`** (4 MiB, `extproc/metrics.go`) is a policy-shaped
constant that is deliberately **not** contract. It gates a counter
(`tapes_extproc_turns_large_total`) and nothing else: turns above it are
captured, dispatched and stored exactly as turns below it are. Two
implementations that disagree about it produce identical rows and differ only in
one deployment's sizing dashboard, which is why it is also settable per
deployment. A threshold that *dropped* or *truncated* a turn would be contract;
this one is an observability knob wearing a policy's shape.

The bounds that ARE contract live where they bite: `MaxDecompressedBytes` in
`fixtures/content-encoding/`, and ingest's body limit in ingest.

## Case schema

Each `cases/*.json` file is one object, and its filename is the reason's
wire string.

| field | required | meaning |
| --- | --- | --- |
| `name` | yes | the reason's wire string — the metric label value and log field, verbatim (matches the filename) |
| `class` | yes | `policy` \| `transport` |
| `constant` | yes | the Go constant that carries it, so a rename on either side is caught |
| `summary` | yes | one line: what it means |
| `trigger` | yes | when it fires, in behavioral terms |
| `grounding` | yes | why it is in the class it is in — the argument, not the verdict |
| `examples` | no | executable cases, where the reason is a pure function of data (see below) |
| `not_expressible` | no | why this reason carries no examples yet. Required when `examples` is absent |
| `notes` | no | anything a consumer needs to know |

`examples` is an array of objects, each with a `description`, a `request`
(`method` and `path`), and an `expect` of `eligible` or `dropped`. `eligible`
means the turn passes THIS reason's gate, not that it is captured — a later
gate may still refuse it.

## Consumers

* `extproc/dropreason_corpus_test.go` — the authored-home gate: the oracle over
  the examples, the DIGEST seal, the vocabulary conformance check in both
  directions, and rule coverage. It lives in `extproc` rather than beside the
  vocabulary in `pkg/capture` because it is the one place both halves meet: the
  policy reasons the shared home owns and the transport reasons the adapter
  keeps.
* `pkg/capture/dropreason.go` — the Go home of the policy half
  (`capture.PolicyDropReasons`), which the gate asserts this corpus against.
* `tapesctl` — has no drop-reason vocabulary today. It drops turns for several
  of the policy reasons (`request_decode` when a body will not decode, an
  unparseable request that has no reason here at all) and reports them only as
  log prose. This corpus is the specification an implementation there should be
  wired to.

## Known divergences

The two capture paths do not currently agree on two policy reasons. They are
recorded here rather than fixed by this corpus, because the corpus's job is to
make disagreement visible and a silent repair would defeat it:

* **`upstream_status`** — the gateway adapter captures only a 200 response and
  drops everything else. The client records the upstream status in metadata and
  captures the turn regardless.
* **`non_turn_request`** — the gateway adapter gates on a known set of turn
  paths and on POST. The client has no path gate; anything with a JSON body is
  a capture candidate.

## `DIGEST`

Same sealing rule as the sibling corpora: for each `cases/*.json`, sorted by
base name, feed `"<basename>  <sha256-hex-of-file-bytes>\n"` into a SHA-256; the
digest is `"sha256:" + hex` of that hash. Consumers vendor `DIGEST` alongside
`cases/` and recompute it, so a stale or hand-edited copy fails in the
consumer's own CI.

## Adding a reason

1. Decide the class first, and write the argument into `grounding`. A reason
   that cannot be argued into a class is a reason that will be guessed about.
2. Add `cases/<wire-string>.json`; `name` must match the filename.
3. Add the constant — to `pkg/capture/dropreason.go` if it is policy, to the
   deployment if it is transport. The gate fails until both sides agree.
4. Give it `examples` if it is a pure function of data, or `not_expressible`
   saying why not.
5. Run `go test ./extproc/`, copy the new digest it prints into `DIGEST`, and
   commit both.
