# Wire recordings (L1)

Verbatim transport bytes for a single turn, replayable across every ingest
transport. Read `../manifest.json` for the full provenance and scrub record of each
set; this file covers layout and how to add another.

## Layout

One directory per capture set, one directory per turn inside it:

```
recordings/
  claude-20260727-019fa57e/     ← <harness>-<capture date>-<session id prefix>
    turn-<ns:020>-<seq:08>/
      request.json    request headers + base64 body
      response.sse    verbatim response bytes (gzip — see meta.content_encoding)
      meta.json       status / content type / encoding / timings / finalized_by
```

`response.sse` is stored exactly as it came off the wire, which means it is
**compressed**. Anything reading it must consult `meta.json`'s `content_encoding`
and decode first — scanning or reducing the raw bytes will silently find nothing.

## Sets

### `claude-20260727-019fa57e`

17 `POST /v1/messages` turns from one Claude Code session (`claude-sonnet-4-6`,
`claude-cli/2.1.219`), produced by the `generate-claude-fixture` skill: a fixed-order,
clean-room script run through a local clearing.

What it exercises: parallel `tool_use` batches and their `tool_result` round trips, a
long text-delta run, thinking blocks with signature deltas, one subagent sub-thread
(`x-claude-code-agent-id`), and four distinct tool-error results.

Two things it deliberately does **not** contain:

- **MCP tool definitions.** The session ran with no MCP servers. Tool results are
  echoed into every later request body, so one MCP call would spread real team,
  channel and issue names across a dozen turns. Body cleanliness was worth more than
  MCP coverage here; a test needing MCP request shapes wants a different fixture.
- **Sibling cancellation.** The generating skill assumes a failing tool call cancels
  the rest of its batch. It does not, in this harness and mode — all three ran and
  returned errors independently. The shape is absent; don't write a test expecting it.

Every bundle reduces `EMPTY_CONTENT` raw and `HEALTHY` decoded. That contrast is a
property of the whole set, not an accident of one turn.

## The admissibility bar

A recording is committable when it is **fixture-grade**: synthetic by construction —
captured in a clean room, credential- and PII-free in headers *and* bodies, and
reviewed by a human before it lands. `../manifest.json` states the bar; each set
records how it met it under `scrub_detail`.

Clean-room means the capturing session could not observe anything about the machine
it ran on: no MCP, no git, no web, no reads outside a scratch directory it created
itself, and a working directory that is not a repository (the working directory
travels in the envelope as `x-tapes-cwd`).

The recorder redacts credential *headers* automatically. It does **not** redact
bodies — the clean room is the only thing keeping those clean, which is why the
generating script is fixed rather than improvised.

## Adding a set

1. Run `skills/generate-claude-fixture` through a clearing. Do not substitute the
   `exercise-claude-harness-*` skills: they vary their inputs by design and read real
   Linear, Slack and git state, all of which is disqualifying here.
2. Review **every** bundle. Headers should show `<redacted:…>` for credentials;
   decode the bodies and the gzip response and read them for paths, names, and
   anything workspace-specific. `clearing debug` renders captured turns.
3. Commit under a new `<harness>-<date>-<session prefix>/` directory and add a
   manifest entry recording provenance, the scrub attestation, and any coverage
   deliberately given up.

Post-hoc scrubbing is not a substitute for a clean room. A narrow, same-length
substitution of a single harness-injected string is defensible and must be recorded in
the manifest; rewriting captured content is not, and silently changing byte lengths
breaks `content-length` and `body_peek_len`.

## Relationship to the L2 corpus

A corpus row can carry raw response bytes, so one L2 row is in principle a superset of
an L1 recording, making the two interconvertible. That only holds once rows actually
carry those bytes. The storage side exists — `raw_turns.raw_response` plus its encoding
and drop marker, and ingest will reduce a raw-only payload server-side — but **no
capture adapter populates it yet**, so today's corpus rows carry only the reduced
response. Until an adapter sends the raw bytes, an L1 recording holds strictly more
than the L2 row derived from it.
