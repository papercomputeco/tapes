<h1>
<p align="center">
  <img width="1280" height="640" alt="Tapes logo" src="https://github.com/user-attachments/assets/133642d6-88e6-4f96-be7a-e6e109a59e5d" />
  <br><code>tapes</code>
</h1>
</p>

<p align="center">
  Transparent agentic telemetry and instrumentation for content-addressable LLM interactions.
  <br />
  <a href="#about">About</a>
  ·
  <a href="https://tapes.dev/">Download</a>
  ·
  <a href="https://tapes.dev/docs/">Documentation</a>
  ·
  <a href="CONTRIBUTING.md">Contributing</a>
</p>

<p align="center">
  <img src="https://img.shields.io/github/stars/papercomputeco/tapes">

  ·
  <a target="_blank" href="https://github.com/papercomputeco/tapes/releases/latest">
    <img src="https://img.shields.io/github/v/release/papercomputeco/tapes?style=flat-square">
  </a>
  ·
  <a target="_blank" href="https://github.com/papercomputeco/tapes/actions/workflows/ci.yaml">
    <img src="https://img.shields.io/github/actions/workflow/status/papercomputeco/tapes/ci.yaml?style=flat-square">
  </a>
</p>

<p align="center">
  <a target="_blank" href="https://discord.gg/T6Y4XkmmV5">
    <img src="https://dcbadge.limes.pink/api/server/https://discord.gg/T6Y4XkmmV5">
  </a>
</p>

`tapes` is an Agentic telemetry system for content-addressable LLM interactions.
It provides durable storage of agent sessions, plug-and-play OpenTelemetry instrumentation,
and a derived sessions/traces/spans model for querying and exporting past agent work.

## About

Capture is **append-only**: every intercepted LLM interaction is persisted to an
immutable `raw_turns` log. A pure, idempotent **deriver** projects that log into
the read model — **sessions → traces → spans** (with span links) — and re-running
the deriver simply reproduces the same projection (re-derive prunes anything no
longer present down to 0). Derived IDs are deterministic, so the same raw input
always yields the same sessions, traces, and spans.

Reads happen over that derived surface: list and inspect sessions
(`/v1/sessions`, cursor-paginated, with model/token/cost/turn-count folds),
browse traces and spans (`/v1/traces`, `/v1/sessions/{id}/traces`), and aggregate
at span grain (`/v1/stats`). Span-grain semantic search is served by the search
cassette (`/v1/cassettes/search/spans`).
The original capture is always available verbatim via
`/v1/sessions/{id}/raw_turns`.

Content addressing (the merkle node layer) is retained **internally** for
provenance and dedup; it is not a user-facing browsing surface.

---

# 📼 Quickstart

Install `tapes`:

```bash
curl -fsSL https://download.tapes.dev/install | bash
```

`tapes` stores sessions in PostgreSQL. The quickest way to get a local
Postgres — plus Ollama as a local LLM upstream — is the bundled Docker
bootstrap (requires Docker):

```bash
tapes local up
```

`tapes local up` writes the Postgres + Ollama connection settings into your
`.tapes` config, so the commands below need no connection flags.

Then start Tapes. `tapes serve` runs the whole local pipeline together — the
proxy (capture), the API, and the derive worker (which projects captured turns
into sessions/traces/spans) — so anything you capture becomes browsable
automatically:

```bash
tapes serve
```

### Capturing and reading: `tapesctl`

`tapes` is the server. Capturing a session and reading one back are client
concerns, and they live in [`tapesctl`](https://github.com/papercomputeco/tapesctl):

```bash
curl -sSfL https://download.tapes.dev/tapesctl/install | bash
```

`tapesctl` defaults reads to `http://localhost:8081` and capture to
`http://localhost:8082`; flags, environment variables, and config override them.

Start with demo data so every command below has something to show — this path
works end to end before you wire up a real agent:

```bash
tapesctl seed --api-url http://localhost:8081
```

List captured sessions and their ids:

```bash
tapesctl sessions list --api-url http://localhost:8081
```

Export a captured session as JSONL — the API's session→traces→spans projection
verbatim. `tapesctl export` is a thin client of the export cassette's
`GET /v1/cassettes/export/sessions/{id}`, so it needs a running API serving
that cassette. The full span tree is included by default; pass
`--detail traces` for turn headers only:

```bash
tapesctl export <session-id> --api-url http://localhost:8081 -o session.jsonl
tapesctl export <session-id> --detail traces
```

**Ready for the real thing?** Clear the demo data and point your own agent at
the proxy:

```bash
tapes local down --wipe && tapes local up   # recreate the DB, clearing the demo
tapesctl start claude --ingest-url http://localhost:8082
```

`tapesctl start` launches the agent under a just-in-time capture proxy and ships
the turns to this server. Capture addresses the private ingest API on `8082`,
not the read API on `8081` — a capture pointed at the read port reports success
and stores nothing. `start` launches `claude`, `codex`, and `pi`; the Codex
desktop app launches itself and is captured with `tapesctl capture codex-app`.
See [Agent integrations](https://tapes.dev/docs/integrations/) for the full
matrix and the plugin each lane needs first.

## License

Dual-licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
- MIT license ([LICENSE-MIT](LICENSE-MIT))

at your option. Unless you explicitly state otherwise, any contribution
intentionally submitted for inclusion in the work by you, as defined in the
Apache-2.0 license, shall be dual licensed as above, without any additional
terms or conditions.
