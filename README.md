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
  <a href="https://tapes.dev/docs">Documentation</a>
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
and deterministic replay of past agent messages.

---

# 📼 Quickstart

Install `tapes`:

```bash
curl -fsSL https://download.tapes.dev/install | bash
```

Run Ollama and the `tapes` services. By default, `tapes` targets embeddings on Ollama 
with the `embeddinggema:latest` model - pull this model with `ollama pull embeddinggema`:

```bash
ollama serve
```
```bash
tapes serve
```

Start a chat session:

```bash
tapes chat --model gemma3
```

Search conversation turns:

```bash
tapes search "What's the weather like in New York?"
```

Checkout a previous conversation state for context check-pointing and retry:

```bash
tapes checkout abc123xyz987
tapes chat
```
