package derive

import (
	"encoding/json"
	"fmt"
	"time"

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

	Report RederiveReport
}

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

	// Upserted/Pruned are filled by the store after the write pass.
	Upserted int `json:"upserted"`
	Pruned   int `json:"pruned"`

	// Reconcile reports the transcript↔wire fusion when transcript
	// rows are present in the raw layer.
	Reconcile *ReconcileStats `json:"reconcile,omitempty"`
}

// rawMetaFields is the minimal meta decode the deriver needs: original
// capture time for chronology (backfilled rows carry ts_request; live
// rows fall back to received_at).
type rawMetaFields struct {
	TsRequest string `json:"ts_request"`
	ThreadID  string `json:"thread_id"`
}

// threadIDFromMeta resolves the capture-side harness sub-thread id
// from a raw row's meta block.
func threadIDFromMeta(meta json.RawMessage) string {
	var m rawMetaFields
	if len(meta) == 0 || json.Unmarshal(meta, &m) != nil {
		return ""
	}
	return m.ThreadID
}

// CapturedAt resolves a raw record's original capture time: the
// adapter's request timestamp when the meta block carries one,
// otherwise the ingest receive time.
func CapturedAt(rec *storage.RawTurnRecord) time.Time {
	var meta rawMetaFields
	if len(rec.Meta) > 0 {
		_ = json.Unmarshal(rec.Meta, &meta)
	}
	if meta.TsRequest != "" {
		if ts, err := time.Parse(time.RFC3339Nano, meta.TsRequest); err == nil {
			return ts
		}
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
	set := &DerivedSet{}
	set.Report.CallKinds = map[string]int{}
	set.Report.NodeKinds = map[string]int{}
	return &Deriver{
		project:   project,
		providers: providers,
		byHash:    map[string]*DerivedNode{},
		set:       set,
		sessions:  map[SessionKey]struct{}{},
		toolSeen:  map[string]struct{}{},
	}, nil
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
	dv.set.Report.CallKinds[kind]++
	capturedAt := CapturedAt(rec)

	key := SessionKey{HarnessID: rec.HarnessID, HarnessSessionID: rec.HarnessSessionID}
	if _, ok := dv.sessions[key]; !ok && key.HarnessSessionID != "" {
		dv.sessions[key] = struct{}{}
		dv.set.Sessions = append(dv.set.Sessions, key)
	}

	turn := &attachTurn{kind: kind, index: len(dv.turns), threadID: threadIDFromMeta(rec.Meta)}

	for _, node := range chain {
		retained, dup := dv.byHash[node.Hash]
		if !dup {
			node.CreatedAt = capturedAt
			retained = &DerivedNode{Node: node, Session: key, CapturedAt: capturedAt}
			dv.byHash[node.Hash] = retained
			dv.set.Nodes = append(dv.set.Nodes, retained)
			dv.set.Report.NodeKinds[node.Kind]++
		}
		turn.nodes = append(turn.nodes, retained)

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
				id:       b.ToolUseID,
				name:     b.ToolName,
				threadID: turn.threadID,
				webTool:  b.ToolName == "WebFetch" || b.ToolName == "WebSearch" || b.ToolName == "web_search" || b.ToolName == "web_fetch",
				atTurn:   turn.index,
				rendered: renderToolUse(b.ToolName, b.ToolInput),
			})
		}
	}

	if kind == KindCheckStage1 || kind == KindCheckStage2 {
		// The judged action needs the parsed request; extract it now,
		// before the request is released.
		if req, err := dv.providers[rec.Provider].ParseRequest(rec.RawRequest); err == nil {
			turn.judgedAction = judgedAction(req)
		}
	}

	dv.turns = append(dv.turns, turn)
}

// Finish runs the cross-call attach passes and returns the completed
// set. The deriver must not be reused afterwards.
func (dv *Deriver) Finish() *DerivedSet {
	attachVerdicts(dv.turns, dv.toolUses, &dv.set.Report)
	attachWebSummaries(dv.turns, dv.toolUses, &dv.set.Report)
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
