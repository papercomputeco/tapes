# Agent Session Capture & Reconciled Conversation Tree — Design

**Status:** Draft for execution by a broadly-scoped implementation agent.
**Scope:** Local changes across `telemetry/tapes`, `telemetry/tapes-extproc`, and `platform/console`, verifiable in a `clearing`. Prod is the north star, not the deliverable of this doc.
**Working/prototyping surface (unchanged):** `tools/clearing/debug-viz.mjs` stays the fast local exploration UI. This doc is for the *larger* codebase changes.

---

## 0. Goal / North Star

1. This capability runs **off the developer's machine, in prod**, and the reconciled conversation view is displayed **in the Paper Compute console**.
2. **Capture as much data as possible** so the data model can be iterated on *without re-capturing*. Never destroy signal at capture; shape it at derive/read time.
3. **Hybrid source of truth** for the causal tree: the **wire** (tapes capture) is the complete *call inventory* (including offshoot calls and verdicts that never appear in a transcript); the **harness transcript + subagent metadata** is the authoritative *causal/fork skeleton* (`parentUuid`, `subagents/agent-<id>.meta.json`). The two are **joined by projected-content hash**.

**Decisions locked (do not re-litigate — these were settled with the owner):**
- **Full vertical, phased** — Phases 1→5 in order; each has a clearing-verifiable DoD.
- **Hybrid causal source** (per #3 above), joined by projected-content hash. Phaseable: wire first (Phases 1–2), transcript slots in (Phase 3) *because* the raw layer is source-agnostic.
- **Two layers** — immutable raw + recomputable derived. Never destructive-migrate; re-derive.
- **Extend, don't rebuild** — the graph-walk API (`/stems/:hash/graph`) and the `parent_session_id` fork edge already exist (§2c/§2d); add semantic typing on top.
- **Start at Phase 1** — it's the keystone. Prove the raw→re-derive round-trip (re-deriving nodes from `raw_turns` reproduces current nodes) before building Phase 2 on it.

## 1. Architecture: two layers + read + view

```
            ┌─────────────────────────────────────────────────────────────┐
 SOURCES    │  wire capture (Envoy→extproc)        harness transcript      │
            │     full request+response+meta        parentUuid, meta.json   │
            └───────────────┬─────────────────────────────┬───────────────┘
                            ▼                             ▼
 RAW LAYER     ┌──────────────────────────────────────────────────┐
 (immutable,   │  raw_turns: full envelope, source-agnostic,        │   ← Phase 1 (+3 for transcript source)
  append-only) │  append-only, never projected/sanitized-in-place   │
            └───────────────────────┬──────────────────────────────┘
                                    ▼   (re-runnable deriver)
 DERIVED LAYER ┌──────────────────────────────────────────────────┐
 (recomputable)│ nodes + edges + typing: node_kind, parent_tool_use │   ← Phase 2, reconciled in Phase 3
            │  _id (fork/rejoin), offshoot tags, projected hash    │
            └───────────────────────┬──────────────────────────────┘
                                    ▼
 READ LAYER    GET /v1/.../graph (typed reconciled tree)              ← Phase 4
                                    ▼
 VIEW LAYER    console session-detail → "Conversation Tree" tab        ← Phase 5
```

**Core principle:** the raw layer is **immutable and complete**; the derived layer is a **pure, re-runnable function of raw**. Every model change (new offshoot kind, different projection, new edge type) is a *re-derive over existing raw data*, not a re-capture or a destructive migration. This is what makes "iterate on the data model" cheap.

## 2. Current state (grounded — verified by research, with pointers)

### 2a. Capture path (prod)
```
Tenant harness ──POST /v1/messages (+x-tapes-* headers)──► Envoy AI Gateway
  └─ext_proc gRPC─► tapes-extproc (buffers req+resp, reduces SSE→ChatResponse)
       └─POST {TAPES_INGEST_URL}/v1/ingest (TurnEnvelope)─► tapes-ingest
            └─ handleIngest → processTurn → workerPool.Enqueue
                 └─ buildTurnChain → IngestTurn (Tx) → Postgres sessions+nodes
```
- extproc state machine: `telemetry/tapes-extproc/processor.go:183-292`; reduce at `:462-476`; dispatch `:547`; POST `telemetry/tapes-extproc/dispatcher.go:393`.
- ingest: `telemetry/tapes/ingest/ingest.go:134,228,399,437`; node build `telemetry/tapes/proxy/worker/pool.go:283-324`.

### 2b. What is captured then DISCARDED (the "capture everything" gap)
- **`meta` is a one-way drop.** extproc builds a rich `TurnMeta` (`dispatcher.go:175-187`) but only `request_id`/`content_type` serialize; the rest is `json:"-"` (Method, Path, Endpoint, **Model, ModelFamily, Stream**, ContentEncoding, **UpstreamStatus**, RequestBytes, ResponseBytes, **ElapsedSeconds**). And `TurnPayload` (`ingest.go:44-69`) has **no `Meta` field**, so even the serialized two are dropped at `BodyParser`.
- **`buildTurnChain` ignores parsed request params.** `ParseRequest` populates `System`, `MaxTokens`, `Temperature`, `Stream`, tool defs, and keeps `RawRequest` (`pkg/llm/request.go:8-35`, `pkg/llm/provider/anthropic/anthropic.go:110-121`), but `buildTurnChain` reads only `Req.Messages`, `Req.Model`, and the response (`pool.go:291-322`). **System prompt, max_tokens, stream, tool defs, raw request → never persisted.**
- **No raw envelope persists.** The full `TurnEnvelope`/`TurnPayload` lives only in extproc memory + the in-flight POST; only projected nodes hit the DB.
- extproc already forwards the **full raw request bytes** as `request` (`processor.go:526`), so `system`/`metadata`/`max_tokens`/tool-count are *already on the wire to ingest* — the loss is purely downstream. extproc needs **no new reads**. Only structural cap: 32 MiB gzip decompress (`processor.go:667`).

### 2c. Data model (extend, don't rebuild)
- **Fork edge exists + wired but unexercised:** `sessions.parent_session_id` (`migrations/1779329142_session_tracking.up.sql:37`), driven by envelope `ParentHarnessSessionID` (`pkg/sessions/ingest_envelope.go:49`), resolved at `pkg/storage/postgres/session_ingest.go:357-421` (placeholder-inserts parent if it lands later). Limits: **session-granularity** (not which tool_use forked), **same-harness only**, no rejoin marker, no real captures use it yet.
- **Edges that exist:** only two — `nodes.parent_hash` (hash chain; FK dropped when PK became `(org_id, hash)`) and `sessions.parent_session_id`.
- **MISSING:** `parent_tool_use_id` as a column/edge, `node_kind`/offshoot tag, fork-at-turn / rejoin-at-result markers. Raw `tool_use_id`/`tool_result_id` exist **inside `content` JSONB** (`pkg/llm/message.go:27,32`) but are not promoted to columns/edges.
- **`agent_name` is a routing label** (claude/codex/opencode) set by `resolveAgent` (`proxy/proxy.go:672`), **NOT** main-vs-subagent. Do not reuse it for fork typing.
- **Raw content DOES persist:** `Driver.Put` stores full `bucket` + `content` JSONB unprojected (`pkg/storage/postgres/postgres.go:96-103,117,120`). Projection happens **only transiently inside `computeHash`** (`pkg/merkle/node.go:108-124`; strips `HarnessTags` `pkg/merkle/projection.go:23-31`). So content-level offshoot signals are already recoverable; **the request envelope (system/params/stream) is the part that is not.**
- **Read-side graph walk EXISTS:** `GET /v1/stems/:hash/graph` → `graphResponseBuilder` (`api/graph_handlers.go:24-332`) already emits flat `Nodes[]`+`Links[]` with `depth`, `is_root`, `is_leaf`, `is_branch_point`, `Leaves[]`, `BranchPoints[]`, scope `root|branch|ancestry`, cycle detection. Recursive ancestry: `ancestry_chains_rows` (`migrations/1776787153_ancestry_func.up.sql`), `Driver.AncestryChains` (`postgres.go:412`). In-session DAG rebuild: `api/sessions_handlers.go:208-331`. **What's missing is semantic edge typing**, not the walk.

### 2d. Console (≈70% there)
- `platform/console` — React 19 + TanStack Start/Router/Query, shadcn/ui + Tailwind v4, Netlify. (`platform/console/AGENTS.md`).
- Existing: session list `src/routes/_app/index.tsx`; **session detail** `src/routes/_app/sessions/$sessionId.tsx` with a **branching-aware "Stems" picker** + "All nodes" toggle (`:141-163,214-227`); `src/components/session-detail/conversation-thread.tsx`; **reconciliation logic** `src/components/session-detail/turn-formatting.ts:25-63` (pairs tool_use↔tool_result across turns, parses harness tags).
- Data: server fns `src/lib/sessions.functions.ts` (`tapesFetch` → per-org tapes data-plane subdomain `https://${orgSlug}.${CLOUD}/${gateway}/tapes/v1/...`, `X-Paper-Auth` bearer); hooks `src/hooks/use-sessions.ts`; wire schemas `src/lib/sessions/schemas.ts`. **The console does not yet call `/stems/:hash/graph`** and has no `GraphResponse` schema.
- tapes ships a D3 force-graph of the graph endpoint in its own debug page `telemetry/tapes/api/web_ui.html:84-176` — a working render reference.

### 2e. Findings this design is built on (from prior measurement work, golden session `0ea3c2cc`)
- Exact projected-content match: **conversation nodes ~95%** join transcript turns; **wrapper/offshoot/marker nodes 0%** (cleanly the wire-only set).
- **Offshoot taxonomy** (all are separate API calls absent from the transcript, all Claude Code built-ins — NOT the `lapdog` plugin, which is pure DataDog telemetry): 2-stage **permission/security checks** (`<transcript>` request → `<block>` verdict; stage-1 block-biased can be overturned by stage-2 reasoned — the one `<block>yes` Slack-draft was overturned, 0 actions ultimately denied), **title-gen** (`{"title":…}`), **plan-name-gen** (`{"name":…}`), **typeahead/suggestion** (`[SUGGESTION MODE…]` in + suggested next input out), **web-content summarization**.
- **Fragmentation causes:** `<session>` and `<conversation>` (and `<new-diagnostics>`, `<task-notification>`, …) are **not in `HarnessTags`**, so they fork the projected chain; and `ProjectContent` strips tags from `text` blocks but **not from `tool_result.tool_output`**, so a `<system-reminder>` concatenated into a Bash result fragments the chain.
- **Structural drifts** (genuine turns that won't naively join): `ExitPlanMode` is sent on the wire with **empty input** (transcript has full `{plan, planFilePath}`); **WebSearch is server-side on the wire** (`server_tool_use`+`web_search_tool_result`) vs client-side `WebSearch` tool_use in the transcript.
- The **check↔judged-action** link has no `tool_use_id`; join via last-`<transcript>`-line action text + `created_at` + the check's own `parent_hash`; group stage1/stage2 by `(action, parent_hash)`.

### 2f. Phase-3 prototype (built & validated 2026-06 — reference implementation)
A working read-side reconciler exists in **`tools/clearing/debug-viz.mjs`** (the "Reconciled" tab; endpoints `/reconcile/sessions`, `/reconcile/data`; server fn `buildReconcile` + the `rc*` projection helpers). It fuses the transcript causal tree (`buildCausalTree`) with the wire DAG (full `content` pulled per session) and is the **reference to port into `pkg/derive`/`pkg/reconcile`**. Measured on `0ea3c2cc` against the live clearing:
- **Conversation-join rate 93%** (150 of 161 conversation nodes join the transcript by projected content). The other **62 wire-only nodes are genuine offshoots** (the value the wire adds), correctly 0%-joined.
- The **11 unmatched conversation nodes ("join-residual")** are exactly the `tool_output`-drift cases (a `<system-reminder>` concatenated into a Bash/tool result + `Read`/`ls` formatting). **Phase-2's `tool_output` strip + whitespace normalization lifts these into "joined"** — i.e. the residual quantifies the projection-fix payoff (93% → ~98% expected).
- **21 actions auto-judged, 20 attached** one-to-one to their tool call (incl. inside the 2 inlined subagents); the `BLOCK→ALLOW(reasoned)` two-stage overturn on the Slack draft renders as a badge on the judged call.
- Offshoot kinds confirmed live: `permission-check`/`verdict`, `title-gen`, `plan-name-gen`, `suggestion`, `web-summary`, `injected-context`, `session-start`.
- **Discovery affordance (carry into the Phase-2 deriver):** unmatched wire-only nodes split into `join-residual` (a real turn that drifted — linked by a known tool-call id or partial projected-block overlap; the `tool_output`-strip target) vs **`unknown`** (zero linkage/overlap → surfaced prominently for investigation). On `0ea3c2cc` `unknown`=0 after classification, so a genuinely-new side-call in a future session pops cleanly instead of hiding in the residual. **Offshoots are request→response pairs:** classify the distinctive half (e.g. `[SUGGESTION MODE]` input, `Perform a web search` request) and inherit the kind to the other half via `parent_hash` — that's how the prototype classifies the generic web-summary / suggestion *outputs*.

**Second deep session (session 3 / `c94d8e75`, 2026-06-09 — 212 nodes, 126 main turns, 7 subagents / 150 nested, 31 judged actions, 113 offshoots):** `unknown` = **0**. The classifier held on a much larger, more varied run (web-summary×6 paired, plan-name-gen×2, NotebookEdit, LSP `<new-diagnostics>`, cron, Monitor, 3-way fan-out) and surfaced **no new kinds** — strong robustness signal. Two carry-forwards:
- **Compaction still not observed.** ~6 large reads (full `Cargo.lock`, ~3400-line `pnpm-lock`, etc.) did *not* trip auto-compaction, so `offshoot:compaction` remains hypothesized — it needs a genuinely longer session, not just a few big reads.
- **Verdict→action attach was 24/31.** Of the 7 misses, 1 was a non-tool subagent-handback (correctly unattached); the other 6 were a *matcher gap*: the judged action was extracted as the **last `<transcript>` line**, which for multi-line actions is a content fragment (`}`, `println!(…)`, a mid-script Bash line) rather than the tool header. **Fix (applied in the prototype `d109fad`, and the recipe for the deriver):** extract the judged action as the last line that *begins with a tool name* (`Bash`/`Write`/`Edit`/`NotebookEdit`/`mcp__…`), not the last raw line. This lifted attach to **36/38** on session 3 (the 2 remaining are non-tool subagent handbacks).

**Caveat:** the prototype is a content-similarity reconciler, not the exact chain-hash. It proves the join + attach + fork/offshoot model; the Go port should use the real projected-content hash (and `parent_tool_use_id`) per Phases 1–2.

### 2g. Harness side-call taxonomy (the `node_kind` seed)

A "session" in the user's mind is one conversation; on the wire it is **many API calls of different kinds**. Only ~60–70% are the conversation. The rest are *shadow model calls* the harness fires on your behalf and *injected context* it prepends — none of which appear in the transcript. This table is the seed for the Phase-2 `node_kind` enum. Three rules of thumb:

- **Classification needs Phase 1.** Today `system`, `max_tokens`, `tool_count`, and `stream` are parsed then discarded (§2b). Once Phase 1 persists them, the "tells" below become a *definitive* classifier — not content-sniffing.
- **The shadow-call set is OPEN.** We've observed the rows marked ✓; ⚑ = observed in other investigations; **hyp** = expected but not yet seen. Drive varied sessions (compaction, deep nesting, more tools) to extend it — the deriver must be re-runnable so new kinds reclassify old raw data.
- **Bridge to PCC-622.** The shadow calls ARE the non-streaming auxiliary traffic behind the 504s. The security monitor is `max_tokens:64`, `tool_count:0`, non-streaming — the canonical case that hits the ext_proc 60s `messageTimeout` when judging a large action. A `node_kind` column turns the latency work into per-kind attribution: *which shadow-call types are slow/large/504-prone.*

**Disposition vocabulary:** `keep` (render as a turn — the spine) · `attach` (to the turn/tool it relates to) · `fold` (into session/turn metadata) · `drop` (ephemeral, exclude) · `strip` (remove from content before hashing) · `mark` (render as a mode boundary, exclude from hash) · `fix` (a defect, not a kind).

**Shadow model calls** — separate API requests, never in the transcript:

| `node_kind` | tells (system / max_tokens / tools / content) | disposition | 504-class |
|---|---|---|---|
| `offshoot:permission-check:stage1` ✓ | system=`"You are a security monitor…"`; trailing `"err on the side of blocking"`; `max_tokens≈64`; `tool_count=0`; `stream=false`; content `<transcript>…</transcript>` → `<block>yes\|no` | `attach` verdict to judged tool_use (incl. inside subagents); `fold` body | **yes** (canonical) |
| `offshoot:permission-check:stage2` ✓ | same system; trailing `"use <thinking>… explicit confirmation"`; `<thinking>` present in output | `attach`; record overturn (stage1 `BLOCK`→stage2 `ALLOW`) | yes |
| `offshoot:title-gen` ✓ | output `{"title":"…"}`; small `max_tokens`; `tool_count=0` | `fold` → `session.name` | likely |
| `offshoot:plan-name-gen` ✓ | output `{"name":"…"}`; small `max_tokens` | `fold` → plan metadata | likely |
| `offshoot:suggestion` ✓ | input `[SUGGESTION MODE…]`; short free-text "next user input" output | `drop` (ephemeral typeahead) | likely |
| `offshoot:web-summary` ✓ | content `"Web page content:"` / `"Perform a web search…"`; summary output | `attach` to the WebFetch/WebSearch turn | likely |
| `offshoot:probe` ⚑ | `max_tokens:1`; minimal body | `drop` (connectivity probe) | yes (seen in PCC-622) |
| `offshoot:compaction` ⚑hyp | request: full conversation + a "create a detailed summary of the conversation so far" instruction; response: the Claude Code **structured summary** — section headers like *Primary Request and Intent*, *Key Technical Concepts*, *Files and Code Sections*, *Errors and fixes*, *Pending Tasks*, *Current Work*, *Optional Next Step*. Large input, moderate `max_tokens`, `stream=false`. | `fold` → context-compaction boundary | likely — tell derived from the CC summary format; **not yet seen in tapes** (only in prod, not develop) — validate against a prod example |

**Injected context** — prefixed material, not calls; fragments the chain because it's hashed:

| `node_kind` | tell | disposition |
|---|---|---|
| `injected:system-reminder` ✓ | `<system-reminder>…</system-reminder>` block | `strip` — already in `HarnessTags` (works) |
| `injected:session-wrapper` ✓ | `<session>…</session>` around the opener | `strip` — **add `session` to `HarnessTags`** (fixes the opener fork) |
| `injected:claude-md` ✓ | `"# claudeMd … Contents of …/CLAUDE.md"` (inside a reminder) | `strip` / `fold` as session context |
| `injected:skills-list` ✓ | `"The following skills are available…"` | `mark` side-metadata; exclude from chain hash |
| `injected:mcp-instructions` ✓ | `"# MCP Server Instructions"` (role=system) | `mark` side-metadata; exclude from hash (drifts → seams) |
| `injected:new-diagnostics` ✓ | `<new-diagnostics>…` LSP output | `strip` — **add to `HarnessTags`** (volatile) |
| `injected:mode-banner` ✓ | `"Plan mode is active"` / `"Exited Plan Mode"` / `"[SYSTEM NOTIFICATION…]"` (role=system) | `mark` as mode boundary; exclude from hash |

**Conversation + drift** — the spine and the genuine turns that resist a naive join:

| `node_kind` | tell | disposition |
|---|---|---|
| `main` ✓ | normal user/assistant/thinking/tool_use/tool_result; full tool set; `stream=true`; high `max_tokens` | `keep` (the spine) |
| `main` (drift: ExitPlanMode) ✓ | wire `tool_input:{}` vs transcript `{plan, planFilePath}` | `keep`; trust transcript for the input |
| `main` (drift: server WebSearch) ✓ | `server_tool_use`+`web_search_tool_result` on wire vs client `WebSearch` in transcript | `keep`; map to the one client turn |

**Not a kind — a defect (`fix`):** a `<system-reminder>` concatenated *into* a `tool_result.tool_output` is a real `main` turn that fails the join only because `ProjectContent` strips reminders from `text` but not `tool_output`. This is the bulk of the prototype's 11 "join-residual." Fix = Phase 2's `tool_output` strip; do not model it as an offshoot.

---

## 3. Phases (each independently verifiable in a clearing)

### Phase 1 — Immutable raw-capture layer  *(foundational; unblocks everything)*
**Outcome:** every captured turn has a fully recoverable envelope; nodes become re-derivable from raw.

1. **Thread `meta` end-to-end.** Add a `Meta` field to `TurnPayload` (`ingest.go:44-69`) mirroring extproc's `TurnMeta`; un-`json:"-"` the fields we want on the wire in `dispatcher.go:175-187` (model, model_family, stream, upstream_status, elapsed_seconds, request/response bytes, content_encoding, request_id).
2. **Persist the full raw turn.** New append-only store at the ingest boundary (before/around `ParseRequest`, `ingest.go:228-265`): a `raw_turns` table (or blob + index row) holding `{org_id, harness_session_id, request_id, provider, agent_name, raw_request JSONB, response JSONB, meta JSONB, session_envelope JSONB, received_at}`. Immutable; this is the iteration substrate.
3. **Promote request params onto nodes** (cheap queryable copies; raw remains source of truth): `system` (store, still hash-excluded), `max_tokens`, `temperature`, `stream`, `tool_count`. Add columns to `nodes` and/or an assistant-turn-level metadata row.
4. **Make the raw envelope source-agnostic** so Phase 3's transcript source can write the same shape (a `source` discriminator: `wire` | `transcript`).

**Touch:** `tapes-extproc/dispatcher.go`, `tapes/ingest/ingest.go`, `tapes/proxy/worker/pool.go`, `tapes/pkg/storage/postgres/*`, `tapes/migrations/` (new `raw_turns` + node columns).
**DoD:** drive a clearing session; confirm each turn's full request (incl. `system`, `stream`, `max_tokens`) + meta (model, elapsed, upstream_status) is queryable; confirm a re-derive of nodes from `raw_turns` reproduces current nodes.

### Phase 2 — Derived typing & edges  *(recomputable pass over raw)*
**Outcome:** nodes carry semantic kind + fork edges; phantom roots collapse; offshoots are tagged. All re-runnable.

1. **Re-runnable deriver** (new pkg, e.g. `pkg/derive`) that reads `raw_turns` and (re)writes derived columns/edges idempotently.
2. **`node_kind`** classification from captured signal (now available post-Phase-1). Use **§2g (the harness side-call taxonomy)** as the authoritative enum + tells — it's the seed, grounded in observed traffic; keep the classifier re-runnable so newly-observed kinds reclassify old raw. The `system` prompt + `max_tokens` + `tool_count` + `stream` are the definitive discriminators; content shape (`<block>`, `{"title"}`, `{"name"}`, `[SUGGESTION MODE]`) is the fallback.
3. **`parent_tool_use_id` edges:** populate fork-at-creation (a subagent/offshoot node references the originating tool_use) and rejoin (the consuming tool_result). For permission verdicts, attach to the judged tool_use. **Refined by the Phase-3 prototype (§2f):**
   - There is **no `tool_use_id`** in the check; the join is **content-based and one-to-one** on the rendered action (the last `<transcript>` line): MCP tools match by full `mcp__…` tool-name prefix; Bash/Web/etc. match by command-body substring. Consume each verdict once. Group stage-1/stage-2 by `(action, parent_hash)`.
   - **The security monitor judges SUBAGENT actions too**, not just the main thread — so the attach pass **must recurse into subagent forks**, not walk the main chain only. (In `0ea3c2cc`, most judged actions were the Explore agent's `find`/`grep`/`Read`.)
   - Measured: **20/21 actions attached** one-to-one; the lone miss is a subagent *handback* event, not a tool call.
4. **Stop chaining injected-context** (MCP instructions, skills list, banners): mark `injected:*`, attach as side-metadata, exclude from the main chain (so they stop fragmenting).
5. **Projection fixes as DERIVED transforms (non-destructive):** extend `HarnessTags` (`projection.go:23-31`) with `session`, `conversation`, `new-diagnostics`, `task-notification`, `status`, `summary`, `transcript`, `event`, `tool-use-id`, `output-file`, `task-id`; and **apply `stripHarnessTags` to `tool_result.tool_output`**, not just `text`. Recompute derived/coalesced hashes from raw — do **not** mutate raw.

**Touch:** new `tapes/pkg/derive/`, `tapes/pkg/merkle/projection.go` + `node.go`, `tapes/migrations/` (`node_kind`, `parent_tool_use_id`, plus add columns to `ancestry_chains_rows` RETURNS TABLE + final SELECT and the gensqlc `InsertNode`/`AncestryChains` queries).
**DoD:** on session `0ea3c2cc`, phantom roots collapse to ~1 main root; the ~95% conversation join rises (tool_output-drift turns now match); every offshoot has a `node_kind`; verdicts attach to their judged tool_use; the whole pass re-runs idempotently from `raw_turns`.

### Phase 3 — Hybrid causal source (transcript ingestion + reconciliation)
**Outcome:** the true causal tree with subagent forks-at-creation / rejoin-at-result, plus wire offshoots attached.

1. **Transcript ingest path** feeding the *same* raw layer (`source: transcript`): accept the harness transcript (`parentUuid` causal tree) + `subagents/agent-<id>.meta.json` (`toolUseId→agentId` fork edges). In a clearing, the uploader is `clearing`/paperd pushing local `~/.claude/projects/.../*.jsonl`. *(Prod uploader from the sandbox = north-star follow-on; note, don't build here.)*
2. **Reconcile by projected-content hash** (the exact recipe: reassemble transcript by `message.id`, rename fields → `ContentBlock` shape, flatten tool_result arrays, apply `ProjectContent` to both sides, compare). Transcript supplies causal/fork skeleton; wire supplies offshoots.
3. **Exercise the plumbed fork edge:** populate `parent_session_id` (subagent → parent) from `meta.json`, and `parent_tool_use_id` at the Task tool_use (fork) and the rejoin tool_result.
4. Handle the two structural drifts explicitly (trust transcript for `ExitPlanMode` full input; map server-side ↔ client-side WebSearch).

**Touch:** new transcript-ingest endpoint in `tapes/ingest/`, new reconciler in `tapes/pkg/derive/` (or `pkg/reconcile/`), `clearing` uploader (`tools/clearing/clearing`).
**DoD:** a clearing session renders, end to end, a tree where the two subagents (Explore/Plan) fork at their Task tool_use and rejoin at the result, with permission verdicts shown on the judged tool calls.

### Phase 4 — Graph/read API for the reconciled tree
**Outcome:** one endpoint returns the typed, reconciled conversation tree.

1. Extend `GET /v1/stems/:hash/graph` (or add `GET /v1/sessions/:id/tree`) to emit typed nodes (`node_kind`), fork edges (`parent_tool_use_id`, rejoin), and offshoot annotations (verdicts as badges, web-summary as foldable), preserving the existing roots/leaves/branch-points.
2. Keep the response additive/back-compatible with the current `GraphResponse` so the existing tapes D3 page still works.

**Touch:** `tapes/api/graph_handlers.go`, swagger (`tapes/docs/`).
**DoD:** the endpoint returns the full reconciled tree for `0ea3c2cc`; verdicts/forks/offshoots are all expressed in the payload.

### Phase 5 — Console "Conversation Tree" view
**Outcome:** the reconciled tree is visible in the console for a real session.

1. Add `GraphResponse`/typed-node/edge **Zod schemas** to `src/lib/sessions/schemas.ts` (mirror Phase 4 payload).
2. Add `getSessionTree` server fn (`src/lib/sessions.functions.ts`, via `tapesFetch`) + `useSessionTree` hook (`src/hooks/use-sessions.ts`).
3. Build `src/components/session-detail/conversation-tree.tsx` (reference render: `tapes/api/web_ui.html` D3, or a React tree). Render fork-at-creation / rejoin, offshoot annotations (verdict shields, web-summary folds), reuse `turn-formatting.ts` for node previews.
4. Mount as a **new tab/sub-route on session-detail** (`src/routes/_app/sessions/$sessionId/...`).

**Touch:** `platform/console/src/lib/sessions/{schemas.ts,sessions.functions.ts}`, `src/hooks/use-sessions.ts`, `src/components/session-detail/conversation-tree.tsx`, `src/routes/_app/sessions/`.
**DoD:** in a clearing (with `TAPES_DEV_URL` pointed at the local tapes), the console session-detail shows the reconciled conversation tree.

---

## 4. Change-type taxonomy → phase mapping
(From the analysis; kept so the agent knows *why* each change is where it is.)

| Type | Mechanism | Phase | Source it draws on |
|---|---|---|---|
| **A. Projection** | extend `HarnessTags`; strip `tool_output` | 2 (as *derived* transform) | wire-as-captured |
| **B. Node construction / topology** | don't chain injected; `node_kind`; `parent_tool_use_id` fork/rejoin; verdict→tool_use | 2–3 | wire + transcript |
| **C. Capture augmentation** | persist `meta`, system/params/stream, raw envelope | 1 | wire (stop discarding) |
| **D. Local-data enrichment** | ingest transcript + subagent meta.json | 3 | transcript |
| **E. Sanitization / redaction** | scrub secrets/PII in raw; fold redundant offshoot bodies | cross-cutting | any |
| **F. Reconciliation / presentation** | graph API + console tree | 4–5 | both |

## 5. Cross-cutting requirements
- **Recomputability:** the Phase-2/3 deriver MUST be idempotent and re-runnable end-to-end from `raw_turns`. This is the property that makes data-model iteration cheap; treat any non-re-runnable step as a bug.
- **Sanitization (E):** raw layer carries secrets (CLAUDE.md, file bytes, tokens). Add a redaction policy pass for anything surfaced to read/console; never sanitize the immutable raw in place — redact on the derived/read side. Per-tenant access is already enforced by WorkOS + per-org subdomain.
- **No destructive migrations:** projection/coalescing changes recompute the derived layer; they never rewrite raw. Hash re-index cost is acceptable because derived is rebuildable.
- **Testing:** clearing-based; golden session `0ea3c2cc` as the fixture; `tools/clearing/debug-viz.mjs` remains the fast prototyping/inspection surface alongside the console.

## 6. Open decisions / risks (carry into execution)
- **Prod transcript upload** from the sandbox (Phase 3's prod-parity step) — mechanism TBD; out of scope here, but Phase 1's `source`-agnostic raw layer must not foreclose it.
- **Offshoot-inference reliability** depends on Phase 1 persisting the `system` field + `max_tokens` + tool_count. Verify the security-monitor system prompt is recoverable before building Phase-2 classification on it.
- **Stage1/stage2 grouping** must key on `(judged-action, parent_hash)`, not assume one check per action.
- **Structural drifts** (`ExitPlanMode` empty wire input; server- vs client-side WebSearch) need explicit handling in Phase 3, not a generic join.
- **`agent_name` is not fork provenance** — do not let any phase lean on it for main/sub/offshoot.

## 7. Execution notes for the implementing agent
- **Start from the working prototype.** `tools/clearing/debug-viz.mjs` (the "Reconciled" tab + `buildReconcile`) already implements the reconciliation end-to-end against live clearing data (§2f). Read it first — it's the reference for `node_kind` classification, the content-based verdict attach (recursing into subagents), and the offshoot taxonomy. The Go port reuses its logic with the exact projected-content hash.
- **Work in the existing `develop` grove/clearing — do NOT spin up a fresh one.** The golden fixtures (§8), this doc (tapes branch `agent-session-reconciliation-design`), and the reference prototype (forest branch `clearing-wire-trace-visualization`) are *already there*, so there is **no Phase-0 fixture generation** — the captured data exists. Bring the data plane up per `$FOREST_ROOT/tools/clearing/README.md`; use `clearing kubectl` / the guarded psql exec, not workstation-global `kubectl`; `clearing debug` serves the prototype. (A fresh grove would have an empty tapes DB and no transcripts — only do that for a clean slate, then regenerate fixtures by driving the `exercise-claude-harness-*` skills.)
- Research current code with `crumbly search` before each phase; the pointers above are a 2026-06 snapshot — verify line numbers.
- Commit per repo (`telemetry/tapes`, `telemetry/tapes-extproc`, `platform/console`), local only. **Pushing to `upstream`/GitHub and any PR is a per-push human-approved step — do not push without explicit approval.**
- Land phases in order; each phase has a clearing-verifiable DoD. Keep raw immutable and the deriver re-runnable throughout.

## 8. Build, test & verification (per repo)

**tapes (Go) — `telemetry/tapes`:**
- Build: **`GOEXPERIMENT=jsonv2 go build ./...`** — the `jsonv2` experiment is **required** (merkle hashing uses `encoding/json/v2`); a plain `go build` will fail. `make build-local` builds the CLI; `make help` lists all targets.
- Test: `make test` (runs `go test` in a Dagger env with Postgres/Ollama); `make e2e-test` for the e2e suite.
- **Codegen after schema/query changes:** `make generate` (`sqlc generate`). After API changes: `make swag` (regenerates `docs/swagger.*`). Migrations live in `migrations/` (paired `*.up.sql`/`*.down.sql`). For any new node column you want the graph walk to carry, update the **`ancestry_chains_rows`** SQL function (both its `RETURNS TABLE` and the final `SELECT`) and regenerate the sqlc `InsertNode`/`AncestryChains` queries.
- **Verify against the live clearing:** `make build-local-image IMAGE=tapes:dev`, reload it into the clearing (`clearing image …`; check `clearing image status --json`), then re-drive/re-query. This is how ingest/derive changes get tested against real captured data.

**extproc (Go) — `telemetry/tapes-extproc`:** `make help` / `make test`; `go build ./...`. Only **Phase 1** touches this (the `meta` widening).

**console (pnpm) — `platform/console`:** `pnpm install`; `pnpm build` (vite — also typechecks); `pnpm lint` (eslint); `pnpm exec vitest run` (unit — there is **no `test` script**); `pnpm test:e2e` (Playwright). Point `TAPES_DEV_URL` at the local clearing tapes to render Phase 5 against real data. Use **`pnpm`** (the repo pins `pnpm@10`), not npm.

**Golden fixtures (develop clearing — in both the tapes DB and `~/.claude/projects/-Users-jasonwc-workspace-paper-forest-groves-develop/`):**
- `0ea3c2cc` — `exercise-claude-harness-advanced` (~223 nodes): primary fixture.
- `c94d8e75` — `exercise-claude-harness-super-advanced` (212 nodes, 7 subagents, 38 judged actions, NotebookEdit / LSP-diagnostics / cron / Monitor): breadth fixture.
- **Compaction is NOT in develop** (prod-only) — validate `offshoot:compaction` against a prod example (§2g), don't block on it. To generate fresh fixtures, drive `exercise-claude-harness-super-advanced` through the clearing.

**Regression oracle (run after every phase):** the reconciler prototype is ground truth. After a derived-layer change, open `clearing debug` → the **Reconciled** tab (or hit `/reconcile/data`) on the golden sessions and confirm: the **conversation-join rate holds or improves** (≥93%, → ~98% once the Phase-2 `tool_output` strip lands), **`unknown` stays ~0** (a new non-zero `unknown` is *either* a real new kind to catalog *or* a regression — investigate which), and verdicts stay attached (~36/38 on session 3). A derived change that drops the join rate or spawns spurious unknowns is a regression.
