---
title: Architecture
description: How Tapes separates the immutable capture log from the derived session, trace, and span read model.
sidebar:
  order: 5
---

Tapes separates immutable capture from derived, query-oriented data.

```text
agent or application
        |
        v
proxy :8080  --------------------> upstream LLM provider
        |
        | append completed request/response turn
        v
PostgreSQL raw_turns
        |
        | idempotent derivation
        v
sessions -> traces -> spans -----> pgvector span embeddings
        |                              |
        +----------> API :8081 <-------+
                         |
                 CLI / MCP clients
```

## Capture: `raw_turns`

The transparent proxy supports Anthropic, OpenAI, and Ollama traffic. It forwards a request to the configured upstream and appends the completed interaction to PostgreSQL's immutable `raw_turns` capture log. The private ingest service is an alternative write path for an external gateway.

Capture is append-only. Replayed turns deduplicate by capture identity instead of rewriting prior capture.

A captured turn is stored twice over: `raw_response` holds the upstream bytes verbatim, and `response` holds the reduced turn an adapter produced from them. Keeping the bytes is what makes the reduction reproducible and auditable rather than authoritative — a future data-model change becomes a re-derive over existing rows instead of a re-capture. Reducing server-side from those bytes is the end state, reached through a proven ratchet: see [Proving the capture ratchet](./cli.md#proving-the-capture-ratchet).

## Derivation: sessions, traces, and spans

The deriver projects raw turns into the read model:

- **Session**: one harness or agent session, with folded model, token, cost, duration, status, and turn-count data.
- **Trace**: a turn in that session.
- **Span**: the work within a trace, including the main LLM conversation and attached tool activity. Span links preserve relationships where needed.

Derived IDs are deterministic. Re-deriving unchanged raw input reproduces the same projection, and the deriver prunes derived records no longer present in the projection. This makes derivation idempotent and safe to run repeatedly.

A session with no successfully parsed main-conversation wire call gets a partial projection from the latest version of each uploaded transcript file. The fallback builds the `parentUuid` causal graph and selects exactly one active root-to-leaf branch: the newest valid `last-prompt` leaf and its later descendants win when present; otherwise main files prefer non-sidechain leaves, subagent files prefer sidechain leaves, and latest record position then UUID is the deterministic fallback. Alternate branches never merge or double-count usage. The selected branch preserves transcript messages, coalesces assistant fragments by message identity, attaches tools and subagents where transcript IDs provide evidence, and folds model, usage, stop reason, title, and timestamps through the same trace/span rollup path as wire capture. Transcript call, trace, and span IDs hash the harness session plus a tagged main-vs-agent thread and remain stable as files grow. Its trace source is `transcript`; individual spans inherit that provenance from the trace. Directly transcript-backed LLM spans reference the selected raw transcript row, while synthetic agent and tool spans remain unbacked.

Fallback is session-wide, not a patch over incomplete conversation capture. As soon as at least one wire row parses as a `main` conversation call, the complete conversation uses the wire projection and transcripts only reconcile causal structure. A later main wire capture therefore replaces and prunes the partial transcript projection instead of producing duplicate turns. Parsed shadow calls do not suppress fallback: title, permission, suggestion, and similar traffic can coexist with transcript conversation calls. Compaction alone also does not suppress fallback; when chronology places it first, the ordinary emitter can represent its seam into the transcript continuation. Malformed or reduction-empty wire rows do not suppress a valid fallback. Unsupported transcript record types remain available in `raw_turns` and are counted as explicit omissions in the derive report.

### Subagent threads

Raw turns carry a per-turn thread id when a harness runs subagents. Which of a harness's own request headers resolve to that id is harness-native knowledge, and its canonical home is the `tapes-harnesses` crate (`src/envelope.rs`); the language-neutral corpus in `fixtures/thread/` pins the same contract for every independent reader. Start at both when adding a harness or writing a second implementation.

The deriver keeps a session's threads inside that session's projection: each thread becomes an `agent` span named `subagent`, parented to the tool span that spawned it, with the thread's own LLM and tool spans nested beneath it and a `rejoin` link back to the spawning tool span. Claude Code and Codex both project this shape — Claude threads anchor through the `Task` tool, Codex threads through `spawn_agent`, recursively for nested spawns. For Codex it holds for the terminal CLI and the ChatGPT desktop app alike: the shape depends on the anchors a capture client uploads, not on how the harness was launched.

The spawn anchor — which tool call created which thread — is not on the wire for either harness. It arrives as transcript-source rows through the private ingest API: Claude's per-agent transcript names its spawning `tool_use_id`; Codex parent rollouts carry `sub_agent_activity` records joining the `spawn_agent` call id to the child thread id. Reconciliation joins those rows against the wire capture during derivation. A thread without a usable anchor degrades safely: its agent span parents to the trace root instead of a guessed tool call, and the derive report's reconcile block counts it (`codex_threads_unanchored`).

Codex subagents share the root session's harness session id, so a captured run is one session: only the root appears in the sessions list, its traces contain the child runs inline, and rollups fold child usage into the root. Re-entering an existing thread (`followup_task`, `send_message`) does not open a new agent span — the thread's whole conversation collapses under its first spawn anchor, matching Claude subagent semantics.

## Search

The embedding worker embeds **main-conversation LLM spans** into PostgreSQL using pgvector. Search is span-only: `/v1/search/spans`, `tapesctl search`, and the MCP `search` tool all return individual span hits with session, trace, and turn context. They do not search session objects or a conversation DAG.

## Content addressing

Merkle content addressing remains an internal, in-memory derivation mechanism for content identity and deduplication. Tapes does not persist a user-facing Merkle node graph, and there is no checkout, branching, or Merkle browsing workflow.

## Runtime forms

`tapes serve` is the convenient all-in-one runtime. It runs the proxy, read API, private ingest API, derive worker, and—by default—the embed worker in one process. Operators can instead run `tapes serve proxy`, `api`, `ingest`, `derive-worker`, or `embed-worker` as separate processes. In a split deployment, derivation and embedding are deliberately independent failure domains.
