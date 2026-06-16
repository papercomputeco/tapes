<h1>
<p align="center">
  <img src="./tapes.png" alt="Tapes Logo">
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
  <a href="https://tapes.dev/guides/">Documentation</a>
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
browse traces and spans (`/v1/traces`, `/v1/sessions/{id}/traces`), aggregate at
span grain (`/v1/stats`), and run span-grain semantic search (`/v1/search/spans`).
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

`tapes` stores sessions in PostgreSQL (with the `pgvector` extension) and uses an
embedding provider to power `tapes search`. The quickest way to get a local
Postgres — plus Ollama for embeddings — is the bundled Docker bootstrap (requires Docker):

```bash
tapes local up
```

For local embeddings, pull the default `embeddinggemma` model:

```bash
ollama pull embeddinggemma
```

Then start Tapes. `tapes serve` runs the whole local pipeline together — the
proxy (capture), the API, and the derive worker (which projects captured turns
into sessions/traces/spans) — so anything you capture becomes browsable
automatically, no extra steps:

```bash
tapes serve
```

Prefer OpenAI embeddings? Store an API key and switch the embedding provider
(`tapes local up` still provides the required Postgres; Ollama just goes unused):

```bash
tapes auth openai
tapes config set embedding.provider openai
tapes serve
```

You can also provide the key with `OPENAI_API_KEY` instead of `tapes auth openai`.
When OpenAI is selected without a key, Tapes fails at startup with an authentication
configuration error from the OpenAI embedder.

Capture a session by launching an agent through Tapes (it points the agent at the
local proxy for you), or send any LLM client at the proxy address:

```bash
tapes start claude
```

Just exploring? Seed the bundled demo sessions and skip straight to the deck:

```bash
tapes seed --demo
```

Search across captured spans (individual main-conversation LLM spans, with
their trace and turn context):

```bash
tapes search "What's the weather like in New York?"
```

Export a captured conversation as a transcript (Markdown by default, or JSONL):

```bash
tapes checkout <session-id> --format md -o session.md
```

Browse sessions and drill into a single session in the deck TUI:

```bash
tapes deck
```
