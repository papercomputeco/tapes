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

`tapes local up` pulls the default `embeddinggemma` model and writes the
Postgres + Ollama connection settings into your `.tapes` config, so the
commands below need no connection flags.

Then start Tapes. `tapes serve` runs the whole local pipeline together — the
proxy (capture), the API, and the derive worker (which projects captured turns
into sessions/traces/spans) — so anything you capture becomes browsable
automatically. Add `--embed-spans` so spans are embedded for `tapes search`:

```bash
tapes serve --embed-spans
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

List captured sessions and their ids:

```bash
tapes sessions
```

Browse sessions and drill into a single session in the deck TUI:

```bash
tapes deck
```

Export a captured conversation as a transcript (Markdown by default, or JSONL).
Pass a full session id or just its short prefix:

```bash
tapes checkout <session-id> --format md -o session.md
```

Search across captured spans (individual main-conversation LLM spans, with
their trace and turn context). This needs the embed pass — run `tapes serve`
with `--embed-spans` (above), or `tapes dev embed-spans` once:

```bash
tapes search "explain the retry logic"
```
