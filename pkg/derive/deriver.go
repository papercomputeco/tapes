package derive

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/papercomputeco/tapes/pkg/llm"
	"github.com/papercomputeco/tapes/pkg/llm/provider"
	"github.com/papercomputeco/tapes/pkg/merkle"
	"github.com/papercomputeco/tapes/pkg/storage"
)

// SessionKey is the natural key a raw turn attributes its nodes to.
type SessionKey struct {
	HarnessID        string
	HarnessSessionID string
}

// DerivedNode is one node of the rebuilt derived layer plus its
// attribution and provenance.
type DerivedNode struct {
	Node       *merkle.Node
	Session    SessionKey
	CapturedAt time.Time
}

// DerivedSet is the deriver's output for one org: the complete node
// set re-derived from the raw layer, ready to upsert.
type DerivedSet struct {
	// Nodes in capture order (chain order within a turn). Deduplicated
	// by hash — the first capturing call wins, mirroring ingest's
	// ON CONFLICT DO NOTHING semantics.
	Nodes []*DerivedNode

	// Sessions are the harness keys covered by the raw layer. The
	// store prunes stale derived rows only within these sessions.
	Sessions []SessionKey

	// SessionTitles carries the folded title-gen output per session
	// (latest call wins — the harness regenerates titles as the
	// session evolves).
	SessionTitles map[SessionKey]string

	// SpanSources retains one slim record per parsed raw turn so the
	// span emit stage (EmitSpans) can re-walk the capture in call
	// order after the attach/reconcile passes have stamped fork and
	// offshoot anchors. Holds only pointers to retained nodes plus
	// scalar call identity — never payload copies.
	SpanSources []*SpanSource

	// CodexThreads is the per-(session, thread) Codex spawn metadata
	// captured from child raw turns' session envelopes — the agent_path
	// fallback join key and the console-facing nickname (codex.go).
	CodexThreads map[SessionKey]map[string]CodexThreadMeta

	// SpawnLabels carries console-facing subagent labels per spawn
	// tool_use (keyed by toolKey), resolved by reconcileCodexSpawns and
	// folded into the spawning tool span's tool_input by EmitSpans.
	SpawnLabels map[string]SpawnLabel

	Report RederiveReport
}

// SpanSource is the per-call input to the span emit stage: the call's
// wire identity, its classified kind, and which chain positions this
// call captured first (the delta vs. re-sent history).
type SpanSource struct {
	RawTurnID  int64
	RequestID  string
	CapturedAt time.Time
	Kind       string
	ThreadID   string
	Session    SessionKey
	// Source is the capture source of the raw turn this call came from
	// ('wire' | 'transcript') — provenance carried onto the trace.
	Source string

	// Chain holds the retained node for every chain position of this
	// call (root → leaf; last is the response). New marks positions
	// first captured by THIS call.
	Chain []*DerivedNode
	New   []bool

	// Anchor is the tool_use id this call attaches to, recorded
	// per-call by the attach passes. Node stamps (ParentToolUseID)
	// cannot carry this: checks share deduped prefix nodes, and a
	// shared node holds only the last writer's edge.
	Anchor string

	// TurnIdentity optionally separates a transcript trace's stable user-turn
	// identity from its assistant-message call identity. Wire calls leave it
	// empty and retain their existing request-id based IDs.
	TurnIdentity string
}

// maxReportedMissing caps the per-report sample lists (parse failures,
// unattached actions) so a wholly broken pass doesn't produce a
// megabyte of strings.
const maxReportedMissing = 20

// RederiveReport summarizes one derive pass.
type RederiveReport struct {
	RawTurns      int            `json:"raw_turns"`
	ParsedTurns   int            `json:"parsed_turns"`
	RawOnlyTurns  int            `json:"raw_only_turns"`
	ParseFailures []string       `json:"parse_failures,omitempty"`
	Nodes         int            `json:"nodes"`
	CallKinds     map[string]int `json:"call_kinds"`
	NodeKinds     map[string]int `json:"node_kinds"`

	// Verdict attach: judged actions grouped across stages, and how
	// many attached one-to-one to a captured tool_use.
	JudgedActions    int `json:"judged_actions"`
	AttachedVerdicts int `json:"attached_verdicts"`

	// UnattachedActions samples judged actions that found no matching
	// tool_use (capped) — expected for non-tool events like subagent
	// handbacks; anything else is matcher signal worth reading.
	UnattachedActions []string `json:"unattached_actions,omitempty"`

	// WebSummaryAttached counts web-summary calls linked back to their
	// WebFetch/WebSearch tool_use.
	WebSummaryAttached int `json:"web_summary_attached"`

	// PlansAttached counts plan-name-gen calls linked to the
	// ExitPlanMode tool_use that accepted the plan.
	PlansAttached int `json:"plans_attached"`

	// Reconcile reports the transcript↔wire fusion when transcript
	// rows are present in the raw layer.
	Reconcile *ReconcileStats `json:"reconcile,omitempty"`

	// TranscriptProjection reports fallback coverage and explicit omissions.
	TranscriptProjection *TranscriptProjectionStats `json:"transcript_projection,omitempty"`
}

// rawMetaFields is the minimal meta decode the deriver needs: original
// capture time for chronology (captured_at is the completion instant
// outright; backfilled rows carry ts_request; live rows fall back to
// received_at).
type rawMetaFields struct {
	CapturedAt     string  `json:"captured_at"`
	TsRequest      string  `json:"ts_request"`
	ElapsedSeconds float64 `json:"elapsed_seconds"`
	ThreadID       string  `json:"thread_id"`
}

// maxDeriveElapsedSeconds mirrors ingest's bound on a plausible
// single-call duration; beyond it the elapsed value is treated as
// corrupt rather than folded into chronology.
const maxDeriveElapsedSeconds = 7 * 24 * 60 * 60

// threadIDFromMeta resolves the capture-side harness sub-thread id
// from a raw row's meta block.
func threadIDFromMeta(meta json.RawMessage) string {
	var m rawMetaFields
	if len(meta) == 0 || json.Unmarshal(meta, &m) != nil {
		return ""
	}
	return m.ThreadID
}

// CapturedAt resolves a raw record's original capture-side START
// instant, for span chronology: captured_at (a completion instant)
// rewound by the call's elapsed duration when the meta block carries
// both, else the adapter's request timestamp, otherwise the ingest
// receive time. The source precedence mirrors ingest's raw-only
// CreatedAt stamping — both fields ride one capture-side clock, offset
// from each other by exactly the call's duration.
func CapturedAt(rec *storage.RawTurnRecord) time.Time {
	var meta rawMetaFields
	if len(rec.Meta) > 0 {
		_ = json.Unmarshal(rec.Meta, &meta)
	}
	// captured_at is the COMPLETION instant, but this function's callers
	// want the turn's start (span StartedAt). It therefore only leads when
	// the elapsed duration can rewind it to an exact start; otherwise an
	// exact ts_request beats an approximation, and captured_at stands alone
	// only as the last capture-side stamp available — late by the call's
	// duration, still better than the ingest hop.
	capturedAt := time.Time{}
	if meta.CapturedAt != "" {
		if ts, err := time.Parse(time.RFC3339Nano, meta.CapturedAt); err == nil {
			capturedAt = ts
		}
	}
	if !capturedAt.IsZero() {
		if e := meta.ElapsedSeconds; e > 0 && e <= maxDeriveElapsedSeconds {
			return capturedAt.Add(-time.Duration(e * float64(time.Second)))
		}
	}
	if meta.TsRequest != "" {
		if ts, err := time.Parse(time.RFC3339Nano, meta.TsRequest); err == nil {
			return ts
		}
	}
	if !capturedAt.IsZero() {
		return capturedAt
	}
	return rec.ReceivedAt
}

// attachTurn is the slim per-turn record the cross-call attach passes
// operate on. The full parsed request/response and the duplicate-node
// chain copies are NOT retained — a chain re-contains the whole
// conversation history every turn, so holding every chain is O(N²) in
// content and OOMs a modestly-sized container. Only pointers to the
// retained (first-capture) node objects survive the turn.
type attachTurn struct {
	kind  string
	index int

	// threadID is the harness sub-thread that fired the call ("" =
	// main thread) — scopes verdict matching to the right thread.
	threadID string

	// judgedAction is the rendered action a permission check judges
	// (empty for non-check turns).
	judgedAction string

	// nodes are the retained DerivedNode objects this turn's chain
	// resolved to after dedup — the stamping targets for attach edges.
	nodes []*DerivedNode

	// source is the turn's SpanSource — the attach passes record the
	// per-call anchor on it.
	source *SpanSource
}

// Deriver streams raw turns (in capture order) into a deduplicated
// derived node set. Memory stays proportional to the UNIQUE content in
// the raw layer, not to the sum of every turn's re-sent history.
type Deriver struct {
	project   string
	providers map[string]provider.Provider

	byHash   map[string]*DerivedNode
	set      *DerivedSet
	sessions map[SessionKey]struct{}

	turns    []*attachTurn
	toolUses []*toolUseRef
	toolSeen map[string]struct{}

	// wireMainCalls is session-scoped and deliberately separate from
	// Report.CallKinds: transcript fallback calls also report as KindMain, but
	// only a successfully parsed wire conversation call in the SAME session is
	// authoritative enough to suppress fallback.
	wireMainCalls map[SessionKey]int

	// titlePriority makes source precedence explicit. Wire title-generation is
	// authoritative over transcript metadata; chronology resolves ties within a
	// source as calls are added in stable timestamp order.
	titlePriority map[SessionKey]int
}

// TranscriptTurn is one normalized transcript-backed LLM call. Its chain is
// constructed from the selected causal transcript path, while this envelope
// carries the call/thread identities and raw-row provenance that do not belong
// in content-addressed nodes.
type TranscriptTurn struct {
	Chain        []*merkle.Node
	Session      SessionKey
	RawTurnID    int64
	RequestID    string
	TurnIdentity string
	CapturedAt   time.Time
	ThreadID     string
	Anchor       string
}

type chainTurn struct {
	chain        []*merkle.Node
	session      SessionKey
	rawTurnID    int64
	requestID    string
	turnIdentity string
	capturedAt   time.Time
	threadID     string
	source       string
	anchor       string
	wire         bool
	judgedAction string
	title        string
	codexThread  *CodexThreadMeta
}

const (
	titlePriorityTranscript = 1
	titlePriorityWire       = 2
)

func (dv *Deriver) setSessionTitle(key SessionKey, title string, priority int) {
	if title == "" || priority < dv.titlePriority[key] {
		return
	}
	dv.set.SessionTitles[key] = title
	dv.titlePriority[key] = priority
}

// NewDeriver creates a streaming deriver. Feed turns with AddTurn in
// chronological order, then call Finish exactly once.
func NewDeriver(project string) (*Deriver, error) {
	providers := make(map[string]provider.Provider)
	for _, name := range provider.SupportedProviders() {
		prov, err := provider.New(name)
		if err != nil {
			return nil, fmt.Errorf("create provider %s: %w", name, err)
		}
		providers[name] = prov
	}
	set := &DerivedSet{SessionTitles: map[SessionKey]string{}}
	set.Report.CallKinds = map[string]int{}
	set.Report.NodeKinds = map[string]int{}
	return &Deriver{
		project:       project,
		providers:     providers,
		byHash:        map[string]*DerivedNode{},
		set:           set,
		sessions:      map[SessionKey]struct{}{},
		toolSeen:      map[string]struct{}{},
		wireMainCalls: map[SessionKey]int{},
		titlePriority: map[SessionKey]int{},
	}, nil
}

// ProbeTurn classifies one raw turn without changing derived output. It is
// used by transcript preparation's first pass to discover session-local wire
// main calls before retained wire and transcript calls are chronologically
// interleaved. The parsed chain is released immediately.
func (dv *Deriver) ProbeTurn(rec *storage.RawTurnRecord) {
	if dv == nil || rec == nil || rec.Source == storage.RawTurnSourceTranscript {
		return
	}
	chain, rawOnly, err := rederiveChain(dv.providers, rec, dv.project)
	if rawOnly || err != nil || len(chain) == 0 || chain[len(chain)-1].Kind != KindMain {
		return
	}
	key := SessionKey{HarnessID: rec.HarnessID, HarnessSessionID: rec.HarnessSessionID}
	dv.wireMainCalls[key]++
}

// AddTurn parses, classifies, and chains one raw turn, folding its
// nodes into the deduplicated set. The record is not retained.
func (dv *Deriver) AddTurn(rec *storage.RawTurnRecord) {
	dv.set.Report.RawTurns++

	chain, rawOnly, err := rederiveChain(dv.providers, rec, dv.project)
	if rawOnly {
		dv.set.Report.RawOnlyTurns++
		return
	}
	if err != nil {
		if len(dv.set.Report.ParseFailures) < maxReportedMissing {
			dv.set.Report.ParseFailures = append(dv.set.Report.ParseFailures,
				fmt.Sprintf("raw_turn id=%d request_id=%s: %v", rec.ID, rec.RequestID, err))
		}
		return
	}
	dv.set.Report.ParsedTurns++

	kind := chain[len(chain)-1].Kind
	var title string
	if kind == KindTitleGen {
		var resp llm.ChatResponse
		if json.Unmarshal(rec.Response, &resp) == nil {
			title = SessionTitle(kind, &resp)
		}
	}
	var action string
	if kind == KindCheckStage1 || kind == KindCheckStage2 {
		// The judged action needs the parsed request; extract it now,
		// before the request is released.
		if req, err := dv.providers[rec.Provider].ParseRequest(rec.RawRequest); err == nil {
			action = judgedAction(req)
		}
	}
	threadID := threadIDFromMeta(rec.Meta)
	var codexThread *CodexThreadMeta
	if rec.HarnessID == harnessCodex && threadID != "" {
		codexThread = codexThreadMetaFromEnvelope(rec.SessionEnvelope)
	}
	dv.addChainTurn(chainTurn{
		chain: chain,
		session: SessionKey{
			HarnessID:        rec.HarnessID,
			HarnessSessionID: rec.HarnessSessionID,
		},
		rawTurnID: rec.ID, requestID: rec.RequestID, capturedAt: CapturedAt(rec),
		threadID: threadID, source: rec.Source,
		wire:         rec.Source != storage.RawTurnSourceTranscript,
		judgedAction: action, title: title, codexThread: codexThread,
	})
}

// AddTranscriptTurn folds one normalized transcript call through the same
// chain seam as AddTurn. RawTurns/ParsedTurns continue to count immutable raw
// rows rather than synthetic calls; transcript call/node coverage is reported
// by CallKinds, NodeKinds, and TranscriptProjectionStats.
func (dv *Deriver) AddTranscriptTurn(turn TranscriptTurn) {
	if len(turn.Chain) == 0 {
		return
	}
	dv.addChainTurn(chainTurn{
		chain: turn.Chain, session: turn.Session, rawTurnID: turn.RawTurnID,
		requestID: turn.RequestID, turnIdentity: turn.TurnIdentity,
		capturedAt: turn.CapturedAt, threadID: turn.ThreadID,
		source: storage.RawTurnSourceTranscript, anchor: turn.Anchor,
	})
}

// addChainTurn is the single fold seam for parsed wire calls and normalized
// transcript calls. It owns session coverage, content-hash dedup, New flags,
// tool registration, attach-turn registration, report counts, and SpanSource
// creation, so every call reaches Finish through the same state machine.
func (dv *Deriver) addChainTurn(input chainTurn) {
	if len(input.chain) == 0 {
		return
	}
	kind := input.chain[len(input.chain)-1].Kind
	if input.wire && kind == KindMain {
		dv.wireMainCalls[input.session]++
	}
	dv.set.Report.CallKinds[kind]++

	if _, ok := dv.sessions[input.session]; !ok && input.session.HarnessSessionID != "" {
		dv.sessions[input.session] = struct{}{}
		dv.set.Sessions = append(dv.set.Sessions, input.session)
	}
	if input.title != "" {
		dv.setSessionTitle(input.session, input.title, titlePriorityWire)
	}

	turn := &attachTurn{
		kind: kind, index: len(dv.turns), threadID: input.threadID,
		judgedAction: input.judgedAction,
	}
	if input.codexThread != nil {
		if dv.set.CodexThreads == nil {
			dv.set.CodexThreads = map[SessionKey]map[string]CodexThreadMeta{}
		}
		byThread := dv.set.CodexThreads[input.session]
		if byThread == nil {
			byThread = map[string]CodexThreadMeta{}
			dv.set.CodexThreads[input.session] = byThread
		}
		// First capture wins, matching node dedup semantics.
		if _, ok := byThread[input.threadID]; !ok {
			byThread[input.threadID] = *input.codexThread
		}
	}
	source := &SpanSource{
		RawTurnID: input.rawTurnID, RequestID: input.requestID,
		CapturedAt: input.capturedAt, Kind: kind, ThreadID: input.threadID,
		Session: input.session, Source: input.source, Anchor: input.anchor,
		TurnIdentity: input.turnIdentity,
	}
	turn.source = source

	// A retained node keeps only the unique content first seen here, but
	// the strings it keeps are zero-copy sub-slices of this turn's raw
	// request/response buffer — so without intervention each retained node
	// pins its turn's whole multi-MB re-sent-history buffer, and the live
	// set grows to ~the re-sent history (O(N^2) on the wire) even though
	// unique content is tiny. CloneRetained reallocates those strings so
	// each raw buffer frees after its turn, bounding the live floor to
	// ~unique content. Cloning copies identical bytes, so node hashes and
	// the whole derived projection are unchanged.
	//
	// The chain shares one *RequestParams across all its nodes (one
	// Params() per call); clone its System once per call, keyed by pointer
	// identity, rather than re-cloning the system prompt for every node.
	var srcParams, clonedParams *llm.RequestParams
	for _, node := range input.chain {
		capturedAt := input.capturedAt
		if !node.CreatedAt.IsZero() {
			// Transcript normalization carries record-level chronology on each
			// node; wire TurnChain nodes are zero and inherit the call start.
			capturedAt = node.CreatedAt
		}
		retained, dup := dv.byHash[node.Hash]
		if !dup {
			if rp := node.Request; rp != srcParams {
				srcParams, clonedParams = rp, rp.Clone()
			}
			node.CloneRetained(clonedParams)
			node.CreatedAt = capturedAt
			retained = &DerivedNode{Node: node, Session: input.session, CapturedAt: capturedAt}
			dv.byHash[node.Hash] = retained
			dv.set.Nodes = append(dv.set.Nodes, retained)
			dv.set.Report.NodeKinds[node.Kind]++
		}
		turn.nodes = append(turn.nodes, retained)
		source.Chain = append(source.Chain, retained)
		source.New = append(source.New, !dup)

		// Tool-use registry for the attach passes, deduped by id so
		// the first (earliest) capture wins.
		for _, b := range node.Bucket.Content {
			if b.Type != "tool_use" && b.Type != "server_tool_use" {
				continue
			}
			if b.ToolUseID == "" {
				continue
			}
			if _, seen := dv.toolSeen[b.ToolUseID]; seen {
				continue
			}
			dv.toolSeen[b.ToolUseID] = struct{}{}
			dv.toolUses = append(dv.toolUses, &toolUseRef{
				id: b.ToolUseID, name: b.ToolName, threadID: turn.threadID,
				webTool: b.ToolName == "WebFetch" || b.ToolName == "WebSearch" || b.ToolName == "web_search" || b.ToolName == "web_fetch",
				atTurn:  turn.index, rendered: renderToolUse(b.ToolName, b.ToolInput),
			})
		}
	}

	dv.turns = append(dv.turns, turn)
	dv.set.SpanSources = append(dv.set.SpanSources, source)
}

// Finish runs the cross-call attach passes and returns the completed
// set. The deriver must not be reused afterwards.
func (dv *Deriver) Finish() *DerivedSet {
	attachVerdicts(dv.turns, dv.toolUses, &dv.set.Report)
	attachWebSummaries(dv.turns, dv.toolUses, &dv.set.Report)
	attachPlans(dv.turns, dv.toolUses, &dv.set.Report)
	dv.set.Report.Nodes = len(dv.set.Nodes)
	return dv.set
}

// BuildDerivedSet derives a complete node set from an in-memory slice
// of raw turns, in the order given. Convenience wrapper around the
// streaming Deriver for tests and small batches; callers with a real
// store should stream records in capture order instead.
func BuildDerivedSet(rawTurns []storage.RawTurnRecord, project string) (*DerivedSet, error) {
	dv, err := NewDeriver(project)
	if err != nil {
		return nil, err
	}
	for i := range rawTurns {
		dv.AddTurn(&rawTurns[i])
	}
	return dv.Finish(), nil
}
