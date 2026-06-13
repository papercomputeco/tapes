package derive

import (
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/papercomputeco/tapes/pkg/llm"
	"github.com/papercomputeco/tapes/pkg/merkle"
	"github.com/papercomputeco/tapes/pkg/sessions"
)

// Span emit — the RFD 00007 projection. EmitSpans re-walks the capture
// (via DerivedSet.SpanSources) and re-expresses it as traces, spans,
// and span links. Like everything in this package it is a pure,
// re-runnable function of the raw layer: every id minted here is a
// deterministic function of wire identity (request_id, tool_use_id,
// thread_id), so re-deriving the same raw layer yields byte-identical
// span rows.
//
// Run it AFTER Finish and ReconcileTranscripts: the attach passes stamp
// offshoot anchors (ParentToolUseID on shadow-call nodes) and the
// transcript reconciler stamps fork anchors (ParentToolUseID on thread
// roots), and span placement follows those stamps.

// Span kinds — the RFD 00007 vocabulary (step is reserved, unused).
const (
	SpanKindAgent = "agent"
	SpanKindLLM   = "llm"
	SpanKindTool  = "tool"
	SpanKindEvent = "event"
)

// Content-block discriminators the emitter switches on (pkg/llm
// ContentBlock.Type values and message roles).
const (
	roleUser           = "user"
	blockText          = "text"
	blockToolUse       = "tool_use"
	blockServerToolUse = "server_tool_use"
	blockToolResult    = "tool_result"
)

// Span link kinds — dataflow edges between spans. Containment is the
// parent_span_id tree; links carry causality that containment cannot.
const (
	LinkEmits          = "emits"           // llm output -> tool input
	LinkFeeds          = "feeds"           // tool output -> llm input
	LinkRejoin         = "rejoin"          // subagent agent -> Task tool
	LinkVerdict        = "verdict"         // shadow llm -> judged tool
	LinkCompactionSeam = "compaction-seam" // compaction llm -> next trace's first llm
)

// SpanSet is the emit stage's output for one derive pass: traces in
// capture order plus the cross-trace links between them.
type SpanSet struct {
	Turns []*SpanTurn

	// Links holds CROSS-trace links only (compaction seams, the odd
	// interrupted tool feed); intra-trace links live on their turn.
	Links []*SpanLink

	Report SpanReport
}

// SpanTurn is one user-visible turn: a trace. Everything the harness
// did inside the turn — subagent runs and shadow calls included —
// lives here.
type SpanTurn struct {
	TraceID string
	Session SessionKey

	// UserPrompt is the text of the genuine user prompt that opened
	// the turn ("" for synthetic openers).
	UserPrompt string

	// ResponsePreview is the text the closing conversation-spine llm
	// call answered with, truncated for the turn card — the response
	// counterpart of UserPrompt, folded at derive time so collapsed
	// renderings never need span payloads.
	ResponsePreview string

	// Synthetic marks traces not opened by a human prompt
	// ("post-compaction" for compaction continuations).
	Synthetic string

	StartedAt time.Time
	EndedAt   time.Time

	// Token totals summed over every llm span in the trace — shadow
	// calls included, because the turn really spent them. The Main*
	// pair counts only call_kind=main spans, so shadow spend is the
	// difference and both numbers are visible to the read layer.
	TotalInputTokens    int64
	TotalOutputTokens   int64
	MainInputTokens     int64
	MainOutputTokens    int64
	CacheReadTokens     int64
	CacheCreationTokens int64

	// TotalCostUSD prices every llm span's usage at derive time, so a
	// re-derive reprices history when the table updates.
	TotalCostUSD float64

	Spans []*Span
	Links []*SpanLink
}

// Span is one observed unit of work. Payloads are delta-only: an llm
// span's Input carries the content blocks NEW to that call, never the
// re-sent history; tool results live solely on their tool span's
// Output. RawTurnID references the capturing raw row — provenance by
// reference, not copy.
type Span struct {
	SpanID       string
	ParentSpanID string
	Kind         string
	Name         string
	Status       string
	StartedAt    time.Time
	DurationNS   int64

	// Seq is the emit ordinal within the trace — presentation order.
	// started_at cannot order spans inside one llm call (a parallel
	// tool batch shares a single instant), so readers sort by seq.
	Seq int64

	// Input: llm — fresh request content blocks; tool — the tool_use
	// input rendered as a single tool_use block.
	Input []llm.ContentBlock
	// Output: llm — response content blocks; tool — the tool_result
	// block, once it arrives.
	Output []llm.ContentBlock

	// CallKind is the §2g taxonomy verbatim ("main", "offshoot:…",
	// "injected:…") on llm and event spans.
	CallKind string
	ThreadID string
	Model    string

	StopReason string
	Usage      *llm.Usage

	// RawTurnID is the raw row whose call produced this span (0 for
	// tool/agent spans, which are assembled across calls).
	RawTurnID int64
	// NodeHash joins the span back to the merkle layer node that
	// carries the same content ("" for agent spans).
	NodeHash string
}

// SpanLink is a dataflow edge. From/To trace ids differ on cross-trace
// causality (compaction seams) — single-trace link keys cannot
// represent those.
type SpanLink struct {
	FromTraceID string
	FromSpanID  string
	FromIO      string
	ToTraceID   string
	ToSpanID    string
	ToIO        string
	Kind        string
}

// SpanReport summarizes one emit pass for gates and operators.
type SpanReport struct {
	Traces       int            `json:"traces"`
	Spans        int            `json:"spans"`
	SpanKinds    map[string]int `json:"span_kinds"`
	CallKinds    map[string]int `json:"call_kinds"`
	LinkKinds    map[string]int `json:"link_kinds"`
	Synthetic    int            `json:"synthetic_traces"`
	OrphanShadow int            `json:"orphan_shadow_calls"`
}

// spanEmitter carries the walk state across calls.
type spanEmitter struct {
	set *SpanSet

	curTrace  map[SessionKey]*SpanTurn
	timeline  map[SessionKey][]*SpanTurn
	toolSpans map[string]*Span
	toolTurn  map[string]*SpanTurn
	agentSpan map[string]*Span // session|thread -> subagent agent span
	agentTurn map[string]*SpanTurn
	seam      map[SessionKey]*seamSource
}

type seamSource struct {
	turn *SpanTurn
	span *Span
}

// EmitSpans projects a finished, reconciled DerivedSet into the span
// model. Pure; safe to call repeatedly.
//
// The walk is phased because wire order races structure: a permission
// check completes before the main call whose tool_use it judges
// finishes streaming, and a subagent's first call can land before its
// Task tool_use is captured. The spine pass builds every trace and
// tool span; threads then anchor to completed Task spans; shadow calls
// anchor last, when every candidate tool span exists. Capture order is
// preserved within each phase, and span ordering inside a trace is by
// StartedAt, so phasing never reorders time.
func EmitSpans(set *DerivedSet) *SpanSet {
	em := &spanEmitter{
		set: &SpanSet{Report: SpanReport{
			SpanKinds: map[string]int{},
			CallKinds: map[string]int{},
			LinkKinds: map[string]int{},
		}},
		curTrace:  map[SessionKey]*SpanTurn{},
		timeline:  map[SessionKey][]*SpanTurn{},
		toolSpans: map[string]*Span{},
		toolTurn:  map[string]*SpanTurn{},
		agentSpan: map[string]*Span{},
		agentTurn: map[string]*SpanTurn{},
		seam:      map[SessionKey]*seamSource{},
	}
	var threadCalls, shadowCalls []*SpanSource
	for _, src := range set.SpanSources {
		if len(src.Chain) == 0 {
			continue
		}
		switch {
		case src.Kind == KindCompaction && src.ThreadID != "":
			// #27 structural guard: a compaction is a main-thread session
			// event — the harness summarizes the user's conversation, not
			// a subagent's. A subagent call classified compaction is a
			// false positive (the classifier's response tell tripped on a
			// subagent that READ classify.go and quoted the summary
			// header), so route it as the ordinary thread call it is and
			// never arm a seam from it. ClassifyCall types from the
			// request/response alone and does not see thread_id; this is
			// the first stage that does.
			threadCalls = append(threadCalls, src)
		case src.Kind == KindMain && src.ThreadID == "":
			em.mainCall(src)
		case src.Kind == KindMain:
			threadCalls = append(threadCalls, src)
		case src.Kind == KindCompaction:
			em.compactionCall(src)
		default:
			shadowCalls = append(shadowCalls, src)
		}
	}
	for _, src := range threadCalls {
		em.threadCall(src)
	}
	for _, src := range shadowCalls {
		em.shadowCall(src)
	}
	em.finish()
	return em.set
}

// mainCall handles one conversation-spine API call: open a trace when
// the call carries a fresh genuine prompt, emit the llm span and its
// tool spans, fill tool results that arrived with the request.
func (em *spanEmitter) mainCall(src *SpanSource) {
	turn := em.curTrace[src.Session]
	prompt := freshGenuinePrompt(src)
	if turn == nil || prompt != nil {
		turn = em.openTrace(src, prompt)
	}
	em.emitConversation(src, turn, turn.Spans[0])
}

// threadCall handles a subagent's API call: ensure the thread's agent
// span exists (parented to the spawning Task tool span), then emit
// into the host trace under it.
func (em *spanEmitter) threadCall(src *SpanSource) {
	key := src.Session.HarnessID + "|" + src.Session.HarnessSessionID + "|" + src.ThreadID
	agent := em.agentSpan[key]
	turn := em.agentTurn[key]
	if agent == nil {
		taskID := threadAnchor(src)
		task := em.toolSpans[taskID]
		if task != nil {
			turn = em.toolTurn[taskID]
		} else {
			turn = em.ensureTrace(src)
		}
		agent = &Span{
			SpanID:    "agent_" + src.ThreadID,
			Kind:      SpanKindAgent,
			Name:      "subagent",
			Status:    "ok",
			StartedAt: src.CapturedAt,
			ThreadID:  src.ThreadID,
		}
		if task != nil {
			agent.ParentSpanID = task.SpanID
			em.link(turn, &SpanLink{
				FromTraceID: turn.TraceID, FromSpanID: agent.SpanID, FromIO: "output",
				ToTraceID: turn.TraceID, ToSpanID: task.SpanID, ToIO: "output",
				Kind: LinkRejoin,
			})
		} else {
			agent.ParentSpanID = turn.Spans[0].SpanID
		}
		em.addSpan(turn, agent)
		em.agentSpan[key] = agent
		em.agentTurn[key] = turn
	}
	em.emitConversation(src, turn, agent)
	// the agent span runs until its last observed call
	if end := src.CapturedAt; end.After(agent.StartedAt) {
		agent.DurationNS = end.Sub(agent.StartedAt).Nanoseconds()
	}
}

// compactionCall emits the compaction llm span into the current trace
// and arms the seam: the next trace this session opens is the
// continuation the summary seeds.
func (em *spanEmitter) compactionCall(src *SpanSource) {
	turn := em.ensureTrace(src)
	span := em.llmSpan(src, turn.Spans[0].SpanID, nil)
	em.addSpan(turn, span)
	em.seam[src.Session] = &seamSource{turn: turn, span: span}
}

// shadowCall emits an offshoot llm span under the tool span it judges
// (ParentToolUseID stamped by the attach passes); unanchored shadows
// (title-gen, suggestions — session-level by nature) land in the trace
// that was live when they fired.
func (em *spanEmitter) shadowCall(src *SpanSource) {
	// per-call anchor from the attach passes — NOT the node stamps:
	// checks share deduped prefix nodes, so a node's ParentToolUseID
	// only carries the last writer's edge and fans every check into
	// one tool.
	tool := em.toolSpans[src.Anchor]
	var turn *SpanTurn
	var parent string
	if tool != nil {
		turn = em.toolTurn[src.Anchor]
		parent = tool.SpanID
	} else {
		turn = em.traceAt(src)
		parent = turn.Spans[0].SpanID
		em.set.Report.OrphanShadow++
	}
	span := em.llmSpan(src, parent, freshInput(src))
	em.addSpan(turn, span)
	if tool != nil {
		em.link(turn, &SpanLink{
			FromTraceID: turn.TraceID, FromSpanID: span.SpanID, FromIO: "output",
			ToTraceID: em.toolTurn[src.Anchor].TraceID, ToSpanID: tool.SpanID, ToIO: "verdict",
			Kind: LinkVerdict,
		})
	}
}

// emitConversation is the shared main/thread call body: fill tool
// results delivered with this request, emit the llm span (delta input
// only), then open tool spans for the response's tool_use blocks.
func (em *spanEmitter) emitConversation(src *SpanSource, turn *SpanTurn, parent *Span) {
	var feeds []*Span
	for i, dn := range src.Chain[:len(src.Chain)-1] {
		if !src.New[i] {
			continue
		}
		node := dn.Node
		if strings.HasPrefix(node.Kind, "injected:") {
			em.eventSpan(turn, parent, node.Kind, node.Hash, src.CapturedAt, node.Bucket.Content)
			continue
		}
		if node.Bucket.Role == "system" {
			// mid-spine system-role inserts (task reminders, CLAUDE.md
			// re-injections, post-compaction replays) are harness
			// context, not user input — same family as injected:*,
			// surfaced as events so they keep rendering as chips. The
			// classifier types them "main" today; the emit stage gives
			// them an injected:* call_kind so span call-kind counts
			// keep meaning "llm calls" for main. Candidate for a real
			// classifier kind (injected:replay) later.
			em.eventSpan(turn, parent, KindInjectedSystemInsert, node.Hash, src.CapturedAt, node.Bucket.Content)
			continue
		}
		if node.Bucket.Role != roleUser {
			continue
		}
		for _, b := range node.Bucket.Content {
			if b.Type != blockToolResult {
				continue
			}
			if ts := em.fillToolResult(&b, src.CapturedAt); ts != nil {
				feeds = append(feeds, ts)
			}
		}
	}

	span := em.llmSpan(src, parent.SpanID, freshInput(src))
	em.addSpan(turn, span)
	for _, ts := range feeds {
		em.link(turn, &SpanLink{
			FromTraceID: em.toolTurn[toolID(ts)].TraceID, FromSpanID: ts.SpanID, FromIO: "output",
			ToTraceID: turn.TraceID, ToSpanID: span.SpanID, ToIO: "input",
			Kind: LinkFeeds,
		})
	}

	resp := src.Chain[len(src.Chain)-1].Node
	for _, b := range resp.Bucket.Content {
		if (b.Type != blockToolUse && b.Type != blockServerToolUse) || b.ToolUseID == "" {
			continue
		}
		if existing := em.toolSpans[b.ToolUseID]; existing != nil {
			// branch siblings re-emit the same tool_use: the first
			// emitter owns the span, later ones only link to it.
			em.link(turn, &SpanLink{
				FromTraceID: turn.TraceID, FromSpanID: span.SpanID, FromIO: "output",
				ToTraceID: em.toolTurn[b.ToolUseID].TraceID, ToSpanID: existing.SpanID, ToIO: "input",
				Kind: LinkEmits,
			})
			continue
		}
		name := b.ToolName
		if name == "" {
			name = "tool"
		}
		ts := &Span{
			SpanID:       b.ToolUseID,
			ParentSpanID: parent.SpanID,
			Kind:         SpanKindTool,
			Name:         name,
			Status:       "ok",
			StartedAt:    src.CapturedAt,
			ThreadID:     src.ThreadID,
			Input:        []llm.ContentBlock{b},
		}
		em.addSpan(turn, ts)
		em.toolSpans[b.ToolUseID] = ts
		em.toolTurn[b.ToolUseID] = turn
		em.link(turn, &SpanLink{
			FromTraceID: turn.TraceID, FromSpanID: span.SpanID, FromIO: "output",
			ToTraceID: turn.TraceID, ToSpanID: ts.SpanID, ToIO: "input",
			Kind: LinkEmits,
		})
	}
}

// fillToolResult stores a tool_result block on its tool span and
// returns the span when this result is fresh dataflow.
func (em *spanEmitter) fillToolResult(b *llm.ContentBlock, at time.Time) *Span {
	id := b.ToolResultID
	if id == "" {
		id = b.ToolUseID
	}
	ts := em.toolSpans[id]
	if ts == nil {
		return nil
	}
	if len(ts.Output) > 0 {
		return nil // first result wins; replays don't re-feed
	}
	ts.Output = []llm.ContentBlock{*b}
	if b.IsError {
		ts.Status = "error"
	}
	if d := at.Sub(ts.StartedAt); d > 0 {
		ts.DurationNS = d.Nanoseconds()
	}
	return ts
}

func (em *spanEmitter) openTrace(src *SpanSource, prompt *DerivedNode) *SpanTurn {
	turn := &SpanTurn{
		TraceID:   "trc_" + callIdentity(src),
		Session:   src.Session,
		StartedAt: src.CapturedAt,
		EndedAt:   src.CapturedAt,
	}
	if prompt != nil {
		turn.UserPrompt = promptText(prompt.Node)
	}
	root := &Span{
		SpanID:    "agent_main_" + callIdentity(src),
		Kind:      SpanKindAgent,
		Name:      "main",
		Status:    "ok",
		StartedAt: src.CapturedAt,
	}
	em.addSpan(turn, root)
	em.set.Turns = append(em.set.Turns, turn)
	em.curTrace[src.Session] = turn
	em.timeline[src.Session] = append(em.timeline[src.Session], turn)

	if s := em.seam[src.Session]; s != nil {
		turn.Synthetic = "post-compaction"
		em.set.Report.Synthetic++
		// seam closes on the opening llm span, which is emitted right
		// after openTrace returns; link to its deterministic id now.
		em.link(turn, &SpanLink{
			FromTraceID: s.turn.TraceID, FromSpanID: s.span.SpanID, FromIO: "output",
			ToTraceID: turn.TraceID, ToSpanID: "llm_" + callIdentity(src), ToIO: "input",
			Kind: LinkCompactionSeam,
		})
		delete(em.seam, src.Session)
	}
	return turn
}

// ensureTrace returns the session's open trace, synthesizing one when
// a shadow call arrives before any spine call (possible at session
// edges).
func (em *spanEmitter) ensureTrace(src *SpanSource) *SpanTurn {
	if turn := em.curTrace[src.Session]; turn != nil {
		return turn
	}
	turn := em.openTrace(src, nil)
	turn.Synthetic = "shadow-opener"
	em.set.Report.Synthetic++
	return turn
}

// traceAt returns the session trace live at the call's capture time —
// calls that raced the session's first spine call clamp into the first
// trace rather than opening a junk one.
func (em *spanEmitter) traceAt(src *SpanSource) *SpanTurn {
	turns := em.timeline[src.Session]
	if len(turns) == 0 {
		return em.ensureTrace(src)
	}
	live := turns[0]
	for _, t := range turns[1:] {
		if t.StartedAt.After(src.CapturedAt) {
			break
		}
		live = t
	}
	return live
}

func (em *spanEmitter) llmSpan(src *SpanSource, parentID string, input []llm.ContentBlock) *Span {
	resp := src.Chain[len(src.Chain)-1].Node
	span := &Span{
		SpanID:       "llm_" + callIdentity(src),
		ParentSpanID: parentID,
		Kind:         SpanKindLLM,
		Name:         resp.Bucket.Model,
		Status:       "ok",
		StartedAt:    src.CapturedAt,
		Input:        input,
		Output:       resp.Bucket.Content,
		CallKind:     spanCallKind(src),
		ThreadID:     src.ThreadID,
		Model:        resp.Bucket.Model,
		StopReason:   resp.StopReason,
		Usage:        resp.Usage,
		RawTurnID:    src.RawTurnID,
		NodeHash:     resp.Hash,
	}
	if span.Name == "" {
		span.Name = "llm"
	}
	if resp.Usage != nil {
		span.DurationNS = resp.Usage.TotalDurationNs
	}
	return span
}

func (em *spanEmitter) eventSpan(turn *SpanTurn, parent *Span, kind, hash string, at time.Time, content []llm.ContentBlock) {
	em.addSpan(turn, &Span{
		SpanID:       "evt_" + hash[:16],
		ParentSpanID: parent.SpanID,
		Kind:         SpanKindEvent,
		Name:         kind,
		Status:       "ok",
		StartedAt:    at,
		CallKind:     kind,
		Output:       content,
		NodeHash:     hash,
	})
}

func (em *spanEmitter) addSpan(turn *SpanTurn, s *Span) {
	turn.Spans = append(turn.Spans, s)
	em.set.Report.Spans++
	em.set.Report.SpanKinds[s.Kind]++
	if s.CallKind != "" {
		em.set.Report.CallKinds[s.CallKind]++
	}
	if end := s.StartedAt.Add(time.Duration(s.DurationNS)); end.After(turn.EndedAt) {
		turn.EndedAt = end
	}
}

func (em *spanEmitter) link(host *SpanTurn, l *SpanLink) {
	em.set.Report.LinkKinds[l.Kind]++
	if l.FromTraceID == l.ToTraceID {
		host.Links = append(host.Links, l)
		return
	}
	em.set.Links = append(em.set.Links, l)
}

func (em *spanEmitter) finish() {
	// Default rates; override plumbing (sessions.LoadPricing) can ride
	// the worker config later — the projection reprices on re-derive
	// either way.
	pricing := sessions.DefaultPricing()
	em.set.Report.Traces = len(em.set.Turns)
	for _, turn := range em.set.Turns {
		root := turn.Spans[0]
		// phases append out of time order within a trace; StartedAt is
		// the presentation order (root agent span stays first). No
		// same-instant tie-break on purpose: the stable sort preserves
		// walk order — an llm call's tool spans in block order — which
		// a span_id comparison would scramble (parallel tool batches
		// share one timestamp and provider ids are random).
		sort.SliceStable(turn.Spans[1:], func(i, j int) bool {
			a, b := turn.Spans[i+1], turn.Spans[j+1]
			return a.StartedAt.Before(b.StartedAt)
		})
		// Seq freezes this order for storage: readers ORDER BY seq, so
		// presentation order survives the round trip.
		for i, s := range turn.Spans {
			s.Seq = int64(i)
		}
		for _, s := range turn.Spans {
			if end := s.StartedAt.Add(time.Duration(s.DurationNS)); end.After(turn.EndedAt) {
				turn.EndedAt = end
			}
			if s.Kind == SpanKindLLM && s.Usage != nil {
				turn.TotalInputTokens += int64(s.Usage.PromptTokens)
				turn.TotalOutputTokens += int64(s.Usage.CompletionTokens)
				turn.CacheReadTokens += int64(s.Usage.CacheReadInputTokens)
				turn.CacheCreationTokens += int64(s.Usage.CacheCreationInputTokens)
				if s.CallKind == KindMain {
					turn.MainInputTokens += int64(s.Usage.PromptTokens)
					turn.MainOutputTokens += int64(s.Usage.CompletionTokens)
				}
				if price, ok := sessions.PricingForModel(pricing, s.Model); ok {
					_, _, total := sessions.CostForTokensWithCache(price,
						int64(s.Usage.PromptTokens), int64(s.Usage.CompletionTokens),
						int64(s.Usage.CacheCreationInputTokens), int64(s.Usage.CacheReadInputTokens))
					turn.TotalCostUSD += total
				}
			}
		}
		if root.Kind == SpanKindAgent {
			root.DurationNS = turn.EndedAt.Sub(turn.StartedAt).Nanoseconds()
		}
		turn.ResponsePreview = responsePreview(turn)
	}
}

// responsePreview folds the closing conversation-spine llm call's text
// output — the answer line on collapsed turn cards, mirroring
// UserPrompt. Subagent and shadow calls never carry the turn's answer,
// so only spine spans (call_kind main, no thread) qualify; a turn that
// ends on tool_use or was interrupted simply previews the last text
// the spine produced.
func responsePreview(turn *SpanTurn) string {
	for i := len(turn.Spans) - 1; i >= 0; i-- {
		s := turn.Spans[i]
		if s.Kind != SpanKindLLM || s.CallKind != KindMain || s.ThreadID != "" {
			continue
		}
		if text := joinTextBlocks(s.Output, true); text != "" {
			return text
		}
	}
	return ""
}

// spanCallKind is the call kind an llm span carries. It is src.Kind
// verbatim except for the #27 structural guard: a compaction is a
// main-thread session event, so a subagent call (ThreadID != "") can
// never be one. Such a call is routed as an ordinary thread call by
// EmitSpans; here its kind is normalized to main so its span never
// reads as a compaction either.
func spanCallKind(src *SpanSource) string {
	if src.Kind == KindCompaction && src.ThreadID != "" {
		return KindMain
	}
	return src.Kind
}

// callIdentity mints the deterministic id suffix for one call: the
// wire request_id when the capture has one, else the response node
// hash — both pure functions of the raw layer.
func callIdentity(src *SpanSource) string {
	if src.RequestID != "" {
		return src.RequestID
	}
	return src.Chain[len(src.Chain)-1].Node.Hash[:16]
}

// freshGenuinePrompt returns the user node first captured by this call
// that reads as a human prompt: no tool_result blocks, not injected
// context. Re-sent history is never fresh, so retries don't reopen
// traces.
func freshGenuinePrompt(src *SpanSource) *DerivedNode {
	for i, dn := range src.Chain[:len(src.Chain)-1] {
		if !src.New[i] {
			continue
		}
		node := dn.Node
		if node.Bucket.Role != roleUser || strings.HasPrefix(node.Kind, "injected:") {
			continue
		}
		genuine := true
		for _, b := range node.Bucket.Content {
			if b.Type == blockToolResult {
				genuine = false
				break
			}
		}
		if genuine {
			return dn
		}
	}
	return nil
}

// freshInput collects the delta request content: blocks of user nodes
// first captured by this call, minus tool_results (those live on tool
// spans) and injected context (event spans).
func freshInput(src *SpanSource) []llm.ContentBlock {
	var out []llm.ContentBlock
	for i, dn := range src.Chain[:len(src.Chain)-1] {
		if !src.New[i] {
			continue
		}
		node := dn.Node
		if node.Bucket.Role != roleUser || strings.HasPrefix(node.Kind, "injected:") {
			continue
		}
		for _, b := range node.Bucket.Content {
			if b.Type == blockToolResult {
				continue
			}
			out = append(out, b)
		}
	}
	return out
}

// threadAnchor resolves the Task tool_use a thread forked from: the
// reconciler stamps it on the thread's root node.
func threadAnchor(src *SpanSource) string {
	for _, dn := range src.Chain {
		if dn.Node.ParentHash == nil && dn.Node.ParentToolUseID != "" {
			return dn.Node.ParentToolUseID
		}
	}
	return ""
}

// toolID recovers a tool span's tool_use id (its span id, by minting).
func toolID(ts *Span) string {
	return ts.SpanID
}

// maxPreviewText bounds the trace header's prompt and response
// renderings; the full content stays on the llm spans' payloads.
const maxPreviewText = 280

// promptText renders a prompt node's text blocks for the trace header,
// truncated for display. Harnesses prepend injected context (e.g. Claude
// Code's claudeMd) as <system-reminder> text blocks ahead of the human
// prompt; those would eat the whole preview budget, so blocks that open
// with the marker only render when the turn carries nothing else.
func promptText(node *merkle.Node) string {
	text := joinTextBlocks(node.Bucket.Content, false)
	if text == "" {
		text = joinTextBlocks(node.Bucket.Content, true)
	}
	return text
}

const systemReminderMarker = "<system-reminder>"

func joinTextBlocks(blocks []llm.ContentBlock, includeInjected bool) string {
	var sb strings.Builder
	for _, b := range blocks {
		if b.Type != blockText || b.Text == "" {
			continue
		}
		if !includeInjected && strings.HasPrefix(strings.TrimSpace(b.Text), systemReminderMarker) {
			continue
		}
		if sb.Len() > 0 {
			sb.WriteString("\n")
		}
		sb.WriteString(b.Text)
		if sb.Len() >= maxPreviewText {
			break
		}
	}
	text := sb.String()
	if len(text) > maxPreviewText {
		// Truncate on a rune boundary: a byte slice can split a
		// multi-byte rune, and Postgres rejects the resulting invalid
		// UTF-8 when the preview lands in span_turns (22021).
		cut := maxPreviewText
		for cut > 0 && !utf8.RuneStart(text[cut]) {
			cut--
		}
		text = text[:cut]
	}
	return text
}
