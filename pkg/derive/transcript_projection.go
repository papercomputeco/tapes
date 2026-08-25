package derive

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/papercomputeco/tapes/pkg/llm"
	"github.com/papercomputeco/tapes/pkg/storage"
)

// TranscriptProjectionStats describes the deliberately partial transcript
// fallback. OmittedTypes makes forward schema skew visible: unsupported records
// remain immutable in raw_turns, but do not poison records that can be projected.
type TranscriptProjectionStats struct {
	Files            int            `json:"files"`
	Records          int            `json:"records"`
	ProjectedRecords int            `json:"projected_records"`
	OmittedRecords   int            `json:"omitted_records"`
	OmittedTypes     map[string]int `json:"omitted_types,omitempty"`
	SuppressedByWire bool           `json:"suppressed_by_wire,omitempty"`
}

// transcriptUsage is Claude's on-disk usage vocabulary. Assistant fragments
// repeat the whole message usage, so projection folds one max-valued snapshot
// per message identity instead of summing fragments.
type transcriptUsage struct {
	InputTokens              int `json:"input_tokens"`
	OutputTokens             int `json:"output_tokens"`
	CacheCreationInputTokens int `json:"cache_creation_input_tokens"`
	CacheReadInputTokens     int `json:"cache_read_input_tokens"`
}

// PreparedTranscriptProjection is the normalized, storage-independent
// transcript fallback for one derive pass. Preparing is deliberately separate
// from adding calls: callers can merge Turns with retained wire calls by
// CapturedAt before either source reaches the shared addChainTurn seam.
type PreparedTranscriptProjection struct {
	Turns   []TranscriptTurn
	Stats   *TranscriptProjectionStats
	titles  map[SessionKey]string
	files   int
	applied bool
}

// PrepareTranscriptFallback normalizes selected transcript files without
// mutating dv. dv is a lightweight probe whose session-scoped wire-main map
// decides suppression. Shadow and compaction calls are complementary; only a
// usable KindMain wire call suppresses transcript fallback for its own session.
func PrepareTranscriptFallback(dv *Deriver, files []*TranscriptFile) *PreparedTranscriptProjection {
	prepared := &PreparedTranscriptProjection{
		Stats:  &TranscriptProjectionStats{OmittedTypes: map[string]int{}},
		titles: map[SessionKey]string{},
	}
	if dv == nil {
		return prepared
	}
	for _, file := range files {
		if file == nil {
			continue
		}
		prepared.files++
		prepared.Stats.Files++
		prepared.Stats.Records += len(file.records)
	}

	// Main first, then subagents by stable harness identity. Final call order is
	// chronological, but deterministic file order resolves equal record times.
	ordered := append([]*TranscriptFile(nil), files...)
	sort.SliceStable(ordered, func(i, j int) bool {
		if ordered[i] == nil || ordered[j] == nil {
			return ordered[j] == nil
		}
		if ordered[i].Session != ordered[j].Session {
			if ordered[i].Session.HarnessID != ordered[j].Session.HarnessID {
				return ordered[i].Session.HarnessID < ordered[j].Session.HarnessID
			}
			return ordered[i].Session.HarnessSessionID < ordered[j].Session.HarnessSessionID
		}
		if (ordered[i].AgentID == "") != (ordered[j].AgentID == "") {
			return ordered[i].AgentID == ""
		}
		return ordered[i].AgentID < ordered[j].AgentID
	})

	toolOwners := transcriptToolOwners(ordered)
	for _, file := range ordered {
		if file == nil || !file.SpawnEvidence() {
			continue
		}
		if dv.wireMainCalls[file.Session] > 0 {
			prepared.Stats.SuppressedByWire = true
			continue
		}
		anchor := ""
		if file.ToolUseID != "" {
			owner := toolOwners[transcriptRawToolKey{Session: file.Session, ToolUseID: file.ToolUseID}]
			if owner == nil {
				// Claude subagent metadata normally points at a Task on the main
				// transcript. Keep that deterministic degrade when the spawning
				// record is absent from this upload.
				main := *file
				main.AgentID = ""
				owner = &main
			}
			anchor = transcriptToolIdentity(owner, file.ToolUseID)
		}
		turns, title := projectTranscriptFile(dv.project, file, prepared.Stats, anchor)
		prepared.Turns = append(prepared.Turns, turns...)
		if title != "" {
			prepared.titles[file.Session] = title
		}
	}
	sort.SliceStable(prepared.Turns, func(i, j int) bool {
		return prepared.Turns[i].CapturedAt.Before(prepared.Turns[j].CapturedAt)
	})
	return prepared
}

// ApplyTranscriptProjection accounts immutable transcript files exactly once
// and installs source-prioritized transcript titles. Synthetic assistant calls
// are intentionally absent from RawTurns and ParsedTurns accounting.
func (dv *Deriver) ApplyTranscriptProjection(prepared *PreparedTranscriptProjection) {
	if dv == nil || prepared == nil || prepared.applied {
		return
	}
	prepared.applied = true
	dv.set.Report.RawTurns += prepared.files
	dv.set.Report.ParsedTurns += prepared.files
	dv.set.Report.TranscriptProjection = prepared.Stats
	for key, title := range prepared.titles {
		dv.setSessionTitle(key, title, titlePriorityTranscript)
	}
}

// ProjectTranscriptFallback is the transcript-only convenience path. Mixed
// wire/transcript callers should use PrepareTranscriptFallback, interleave its
// Turns chronologically with wire calls, then ApplyTranscriptProjection.
func ProjectTranscriptFallback(dv *Deriver, files []*TranscriptFile) *TranscriptProjectionStats {
	prepared := PrepareTranscriptFallback(dv, files)
	dv.ApplyTranscriptProjection(prepared)
	for _, turn := range prepared.Turns {
		dv.AddTranscriptTurn(turn)
	}
	return prepared.Stats
}

// BuildDerivedSetWithTranscriptFallback is the pure mixed-source constructor.
// It probes wire classification first, suppresses fallback per session, then
// sends every retained wire/transcript call through addChainTurn in stable
// capture-start order. Wire wins exact timestamp ties, matching title/source
// priority without allowing input slice order to poison shared-hash metadata.
func BuildDerivedSetWithTranscriptFallback(rawTurns []storage.RawTurnRecord, files []*TranscriptFile, project string) (*DerivedSet, *TranscriptProjectionStats, error) {
	probe, err := NewDeriver(project)
	if err != nil {
		return nil, nil, err
	}
	for i := range rawTurns {
		probe.ProbeTurn(&rawTurns[i])
	}
	prepared := PrepareTranscriptFallback(probe, files)

	dv, err := NewDeriver(project)
	if err != nil {
		return nil, nil, err
	}
	dv.ApplyTranscriptProjection(prepared)
	type orderedCall struct {
		at       time.Time
		wire     *storage.RawTurnRecord
		fallback *TranscriptTurn
		order    int
	}
	calls := make([]orderedCall, 0, len(rawTurns)+len(prepared.Turns))
	for i := range rawTurns {
		calls = append(calls, orderedCall{at: CapturedAt(&rawTurns[i]), wire: &rawTurns[i], order: i})
	}
	for i := range prepared.Turns {
		calls = append(calls, orderedCall{at: prepared.Turns[i].CapturedAt, fallback: &prepared.Turns[i], order: i})
	}
	sort.SliceStable(calls, func(i, j int) bool {
		if !calls[i].at.Equal(calls[j].at) {
			return calls[i].at.Before(calls[j].at)
		}
		if (calls[i].wire != nil) != (calls[j].wire != nil) {
			return calls[i].wire != nil
		}
		return calls[i].order < calls[j].order
	})
	for _, call := range calls {
		if call.wire != nil {
			dv.AddTurn(call.wire)
		} else {
			dv.AddTranscriptTurn(*call.fallback)
		}
	}
	return dv.Finish(), prepared.Stats, nil
}

type assistantGroup struct {
	identity   string
	model      string
	stopReason string
	content    []llm.ContentBlock
	usage      transcriptUsage
	startedAt  time.Time
	endedAt    time.Time
	records    int
}

// activeTranscriptPath resolves Claude's append-only causal graph to one active
// root-to-leaf path. The newest usable last-prompt leaf is authoritative and a
// later descendant is allowed for an upload taken while the next turn is in
// flight. Without that evidence, the latest causal leaf wins; main files prefer
// non-sidechain leaves and subagent files prefer sidechain leaves. Record index,
// then UUID, is the deterministic fallback. Alternate branches never merge.
func activeTranscriptPath(file *TranscriptFile) map[int]struct{} {
	active := map[int]struct{}{}
	if file == nil {
		return active
	}

	byUUID := map[string]int{}
	children := map[int][]int{}
	for i := range file.records {
		r := &file.records[i]
		if !r.decoded || r.UUID == "" {
			continue
		}
		// First occurrence is stable if a malformed producer repeats an UUID in
		// a later grown version.
		if _, exists := byUUID[r.UUID]; !exists {
			byUUID[r.UUID] = i
		}
	}
	for uuid, i := range byUUID {
		parentUUID := file.records[i].ParentUUID
		if parent, ok := byUUID[parentUUID]; ok && uuid != parentUUID {
			children[parent] = append(children[parent], i)
		}
	}

	evidence := -1
	for i := range file.records {
		r := &file.records[i]
		if r.decoded && r.Type == "last-prompt" && r.LeafUUID != "" {
			if leaf, ok := byUUID[r.LeafUUID]; ok {
				evidence = leaf
			}
		}
	}

	candidates := make([]int, 0)
	if evidence >= 0 {
		descendants := map[int]struct{}{evidence: {}}
		queue := []int{evidence}
		for len(queue) > 0 {
			parent := queue[0]
			queue = queue[1:]
			for _, child := range children[parent] {
				if _, seen := descendants[child]; seen {
					continue
				}
				descendants[child] = struct{}{}
				queue = append(queue, child)
			}
		}
		for i := range descendants {
			leaf := true
			for _, child := range children[i] {
				if _, ok := descendants[child]; ok {
					leaf = false
					break
				}
			}
			if leaf {
				candidates = append(candidates, i)
			}
		}
	} else {
		for _, i := range byUUID {
			if len(children[i]) == 0 {
				candidates = append(candidates, i)
			}
		}
	}
	if len(candidates) == 0 {
		return active
	}

	wantSidechain := file.AgentID != ""
	preferred := candidates[:0]
	for _, i := range candidates {
		if file.records[i].IsSidechain == wantSidechain {
			preferred = append(preferred, i)
		}
	}
	if len(preferred) > 0 {
		candidates = preferred
	}
	sort.Slice(candidates, func(i, j int) bool {
		a, b := candidates[i], candidates[j]
		if a != b {
			return a > b
		}
		return file.records[a].UUID > file.records[b].UUID
	})
	leaf := candidates[0]

	seen := map[int]struct{}{}
	for at := leaf; at >= 0; {
		if _, cycle := seen[at]; cycle {
			break
		}
		seen[at] = struct{}{}
		active[at] = struct{}{}
		parent, ok := byUUID[file.records[at].ParentUUID]
		if !ok {
			break
		}
		at = parent
	}
	return active
}

type transcriptRawToolKey struct {
	Session   SessionKey
	ToolUseID string
}

// transcriptToolOwners resolves raw spawn anchors before tool ids are scoped to
// their transcript threads. A main-thread owner wins a duplicate raw id because
// Claude child metadata points at the parent/main Task tool; otherwise stable
// file order provides a deterministic degrade for ambiguous historical data.
func transcriptToolOwners(files []*TranscriptFile) map[transcriptRawToolKey]*TranscriptFile {
	owners := map[transcriptRawToolKey]*TranscriptFile{}
	for _, file := range files {
		if file == nil || !file.SpawnEvidence() {
			continue
		}
		active := activeTranscriptPath(file)
		for i := range file.records {
			if _, ok := active[i]; !ok {
				continue
			}
			r := &file.records[i]
			if !r.decoded || (r.Type != roleAssistant && r.Message.Role != roleAssistant) {
				continue
			}
			blocks, ok := transcriptBlocksForProjection(r.Message.Content)
			if !ok {
				continue
			}
			for _, block := range blocks {
				if (block.Type != blockToolUse && block.Type != blockServerToolUse) || block.ToolUseID == "" {
					continue
				}
				key := transcriptRawToolKey{Session: file.Session, ToolUseID: block.ToolUseID}
				owner := owners[key]
				if owner == nil || (owner.AgentID != "" && file.AgentID == "") {
					owners[key] = file
				}
			}
		}
	}
	return owners
}

type transcriptMessage struct {
	message    llm.Message
	kind       string
	capturedAt time.Time
}

func projectTranscriptFile(project string, file *TranscriptFile, stats *TranscriptProjectionStats, anchor string) ([]TranscriptTurn, string) {
	active := activeTranscriptPath(file)
	groups := map[string]*assistantGroup{}
	firstAssistant := map[string]int{}
	validAssistant := map[int]struct{}{}
	for i := range file.records {
		if _, ok := active[i]; !ok {
			continue
		}
		r := &file.records[i]
		if !r.decoded || (r.Type != roleAssistant && r.Message.Role != roleAssistant) {
			continue
		}
		blocks, ok := transcriptBlocksForProjection(r.Message.Content)
		if !ok {
			continue
		}
		blocks = namespaceTranscriptToolIDs(file, blocks)
		validAssistant[i] = struct{}{}
		id := assistantIdentity(r)
		group := groups[id]
		if group == nil {
			group = &assistantGroup{identity: id}
			groups[id] = group
			firstAssistant[id] = i
		}
		group.records++
		group.content = appendAssistantBlocks(group.content, blocks)
		if r.Message.Model != "" {
			group.model = r.Message.Model
		}
		if r.Message.StopReason != "" {
			group.stopReason = r.Message.StopReason
		}
		group.usage = maxTranscriptUsage(group.usage, r.Message.Usage)
		at := transcriptRecordTime(r, file.receivedAt)
		if group.startedAt.IsZero() || at.Before(group.startedAt) {
			group.startedAt = at
		}
		if group.endedAt.IsZero() || at.After(group.endedAt) {
			group.endedAt = at
		}
	}

	// history is the canonical normalized active conversation. Unlike a
	// transcript delta, every later call re-sends prior user/context/tool-result
	// messages AND each coalesced assistant response. Shared Merkle hash dedup can
	// therefore determine New exactly as it does for wire calls.
	var history []transcriptMessage
	var pending []transcriptMessage
	var turns []TranscriptTurn
	var turnIdentity string
	var titleValue string
	var titlePriority int
	for i := range file.records {
		r := &file.records[i]
		if !r.decoded {
			stats.omit("malformed:" + recordType(r))
			continue
		}

		_, causalActive := active[i]
		if r.UUID != "" && !causalActive {
			stats.omit("inactive-branch")
			continue
		}

		switch {
		case causalActive && (r.Type == roleAssistant || r.Message.Role == roleAssistant):
			if _, ok := validAssistant[i]; !ok {
				stats.omit("malformed:" + recordType(r))
				continue
			}
			id := assistantIdentity(r)
			if firstAssistant[id] != i {
				continue
			}
			group := groups[id]
			stats.ProjectedRecords += group.records
			turns = append(turns, normalizeTranscriptTurn(project, file, history, pending, group, turnIdentity, anchor))
			assistantAt := group.startedAt
			if assistantAt.IsZero() {
				assistantAt = file.receivedAt
			}
			history = append(history, transcriptMessage{
				message: llm.Message{Role: roleAssistant, Content: group.content},
				kind:    KindMain, capturedAt: assistantAt,
			})
			pending = nil
			turnIdentity = ""

		case causalActive && (r.Type == roleUser || r.Message.Role == roleUser || r.Message.Role == roleTool):
			blocks, ok := transcriptBlocksForProjection(r.Message.Content)
			if !ok {
				stats.omit("malformed:" + recordType(r))
				continue
			}
			if len(blocks) == 0 {
				stats.omit(recordType(r))
				continue
			}
			blocks = namespaceTranscriptToolIDs(file, blocks)
			kind := KindMain
			role := r.Message.Role
			if role == "" {
				role = roleUser
			}
			if injected := ClassifyInjected(llm.Message{Role: role, Content: blocks}); injected != "" || r.IsMeta {
				if injected == "" {
					injected = KindInjectedSystemInsert
				}
				kind = injected
			}
			message := transcriptMessage{
				message: llm.Message{Role: role, Content: blocks}, kind: kind,
				capturedAt: transcriptRecordTime(r, file.receivedAt),
			}
			history = append(history, message)
			pending = append(pending, message)
			if turnIdentity == "" && kind == KindMain && !hasToolResult(blocks) {
				turnIdentity = transcriptProjectionIdentity(file, "turn", transcriptRecordIdentity(r))
			}
			stats.ProjectedRecords++

		case causalActive && (r.Type == roleSystem || r.Message.Role == roleSystem || r.Type == "attachment"):
			if text := transcriptContextText(r); text != "" {
				message := transcriptMessage{
					message: llm.Message{Role: roleSystem, Content: []llm.ContentBlock{{Type: blockText, Text: text}}},
					kind:    KindInjectedSystemInsert, capturedAt: transcriptRecordTime(r, file.receivedAt),
				}
				history = append(history, message)
				pending = append(pending, message)
				stats.ProjectedRecords++
			} else {
				stats.omit(recordType(r))
			}

		case r.UUID == "" && r.Type == "last-prompt":
			// last-prompt is structural branch-selection evidence consumed by
			// activeTranscriptPath. It creates no span, but it is supported data
			// rather than an unknown omission.
			if r.LeafUUID == "" {
				stats.omit("malformed:last-prompt")
			} else {
				stats.ProjectedRecords++
			}

		case r.UUID == "" && (r.Type == "ai-title" || r.Type == "custom-title" || r.Type == "agent-name" || r.Type == "summary"):
			title, priority := transcriptTitle(r)
			if file.AgentID == "" && title != "" {
				if priority >= titlePriority {
					titleValue, titlePriority = title, priority
				}
				stats.ProjectedRecords++
			} else {
				stats.omit(recordType(r))
			}

		default:
			stats.omit(recordType(r))
		}
	}

	// Context and user records become observable only as input to a real
	// assistant response. An upload can end while the next call is in flight;
	// retaining that trailing context would mint a phantom LLM call, so leave it
	// raw for a later grown transcript instead.
	return turns, titleValue
}

// normalizeTranscriptTurn supplies the canonical full active history to the
// shared turnChain constructor. pending identifies only the fresh records since
// the prior assistant and therefore the call's true input start; history also
// includes all earlier normalized messages for parent/hash parity with wire.
func normalizeTranscriptTurn(project string, file *TranscriptFile, history, pending []transcriptMessage, group *assistantGroup, turnIdentity, anchor string) TranscriptTurn {
	responseAt := group.startedAt
	if responseAt.IsZero() {
		responseAt = file.receivedAt
	}
	capturedAt := earliestTranscriptTime(pending, responseAt)
	endedAt := group.endedAt
	if endedAt.IsZero() || endedAt.Before(responseAt) {
		endedAt = responseAt
	}
	usage := &llm.Usage{
		PromptTokens:             group.usage.InputTokens,
		CompletionTokens:         group.usage.OutputTokens,
		CacheCreationInputTokens: group.usage.CacheCreationInputTokens,
		CacheReadInputTokens:     group.usage.CacheReadInputTokens,
	}
	if endedAt.After(capturedAt) {
		usage.TotalDurationNs = endedAt.Sub(capturedAt).Nanoseconds()
	}
	if *usage == (llm.Usage{}) {
		usage = nil
	}

	messages := make([]llm.Message, len(history))
	kinds := make([]string, len(history))
	recordTimes := make([]time.Time, 0, len(history)+1)
	for i := range history {
		messages[i] = history[i].message
		kinds[i] = history[i].kind
		recordTimes = append(recordTimes, history[i].capturedAt)
	}
	// The response node retains its own record timestamp. Call/span chronology
	// uses capturedAt above and duration reaches the final coalesced fragment.
	recordTimes = append(recordTimes, responseAt)
	chain := turnChain(CallContext{
		Provider: storage.RawTurnSourceTranscript, AgentName: file.Session.HarnessID,
		ThreadID: file.AgentID, Project: project,
	}, &llm.ChatRequest{Messages: messages}, &llm.ChatResponse{
		Model: group.model, Message: llm.Message{Role: roleAssistant, Content: group.content},
		StopReason: group.stopReason, Usage: usage,
	}, chainOptions{
		kind: KindMain, messageKinds: kinds, capturedAt: recordTimes,
	})

	return TranscriptTurn{
		Chain: chain, Session: file.Session, RawTurnID: file.RawTurnID,
		RequestID:    transcriptProjectionIdentity(file, "call", group.identity),
		TurnIdentity: turnIdentity, CapturedAt: capturedAt, ThreadID: file.AgentID,
		Anchor: anchor,
	}
}

func earliestTranscriptTime(messages []transcriptMessage, fallback time.Time) time.Time {
	earliest := time.Time{}
	for _, message := range messages {
		if message.capturedAt.IsZero() {
			continue
		}
		if earliest.IsZero() || message.capturedAt.Before(earliest) {
			earliest = message.capturedAt
		}
	}
	if earliest.IsZero() {
		return fallback
	}
	return earliest
}

func assistantIdentity(r *transcriptRecord) string {
	if r.Message.ID != "" {
		return r.Message.ID
	}
	return transcriptRecordIdentity(r)
}

func transcriptRecordIdentity(r *transcriptRecord) string {
	if r.UUID != "" {
		return r.UUID
	}
	if r.Message.ID != "" {
		return r.Message.ID
	}
	data, err := json.Marshal(struct {
		Type      string          `json:"type"`
		Timestamp string          `json:"timestamp"`
		Role      string          `json:"role"`
		Content   json.RawMessage `json:"content"`
	}{r.Type, r.Timestamp, r.Message.Role, r.Message.Content})
	if err != nil {
		data = []byte(r.Type + "\x00" + r.Timestamp + "\x00" + r.Message.Role)
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:8])
}

// transcriptProjectionIdentity namespaces provider-local and occasionally
// missing transcript identities by the harness session and a tagged main-vs-
// agent thread. The raw row id is deliberately absent, so IDs remain stable as
// an append-only file grows and a newer content version supersedes it.
func transcriptProjectionIdentity(file *TranscriptFile, domain, local string) string {
	identity := struct {
		Version          int    `json:"version"`
		HarnessID        string `json:"harness_id"`
		HarnessSessionID string `json:"harness_session_id"`
		Domain           string `json:"domain"`
		Main             bool   `json:"main"`
		AgentID          string `json:"agent_id,omitempty"`
		Local            string `json:"local"`
	}{
		Version: 1, HarnessID: file.Session.HarnessID,
		HarnessSessionID: file.Session.HarnessSessionID, Domain: domain,
		Main: file.AgentID == "", AgentID: file.AgentID, Local: local,
	}
	data, err := json.Marshal(identity)
	if err != nil {
		// json.Marshal cannot fail for this scalar-only struct shape.
		panic(fmt.Sprintf("encode transcript projection identity: %v", err))
	}
	sum := sha256.Sum256(data)
	return "tx" + domain + "_" + hex.EncodeToString(sum[:16])
}

func transcriptRecordTime(r *transcriptRecord, fallback time.Time) time.Time {
	if r.Timestamp != "" {
		if at, err := time.Parse(time.RFC3339Nano, r.Timestamp); err == nil {
			return at
		}
	}
	return fallback
}

func maxTranscriptUsage(a, b transcriptUsage) transcriptUsage {
	if b.InputTokens > a.InputTokens {
		a.InputTokens = b.InputTokens
	}
	if b.OutputTokens > a.OutputTokens {
		a.OutputTokens = b.OutputTokens
	}
	if b.CacheCreationInputTokens > a.CacheCreationInputTokens {
		a.CacheCreationInputTokens = b.CacheCreationInputTokens
	}
	if b.CacheReadInputTokens > a.CacheReadInputTokens {
		a.CacheReadInputTokens = b.CacheReadInputTokens
	}
	return a
}

func appendAssistantBlocks(dst, blocks []llm.ContentBlock) []llm.ContentBlock {
	toolIDs := map[string]struct{}{}
	for _, b := range dst {
		if b.ToolUseID != "" {
			toolIDs[b.ToolUseID] = struct{}{}
		}
	}
	for _, b := range blocks {
		if b.ToolUseID != "" {
			if _, exists := toolIDs[b.ToolUseID]; exists {
				continue
			}
			toolIDs[b.ToolUseID] = struct{}{}
		}
		dst = append(dst, b)
	}
	return dst
}

// transcriptBlocksForProjection distinguishes a legitimate empty/null content
// value from a known message whose content has an unsupported shape. The latter
// is an explicit malformed-record omission and must never mint an empty span.
func transcriptBlocksForProjection(content json.RawMessage) ([]llm.ContentBlock, bool) {
	if len(content) == 0 || string(content) == "null" {
		return nil, true
	}
	var text string
	if json.Unmarshal(content, &text) == nil {
		return []llm.ContentBlock{{Type: blockTypeText, Text: text}}, true
	}
	var blocks []transcriptBlock
	if json.Unmarshal(content, &blocks) != nil {
		return nil, false
	}
	return transcriptBlocks(content), true
}

// namespaceTranscriptToolIDs scopes transcript-native tool identities to the
// thread before the blocks enter the shared span emitter. Harnesses may reuse a
// raw id in the main transcript and one or more subagent files; both tool uses
// and their results must therefore carry the same thread-local projected id.
func namespaceTranscriptToolIDs(file *TranscriptFile, blocks []llm.ContentBlock) []llm.ContentBlock {
	for i := range blocks {
		switch blocks[i].Type {
		case blockToolUse, blockServerToolUse:
			if blocks[i].ToolUseID != "" {
				blocks[i].ToolUseID = transcriptToolIdentity(file, blocks[i].ToolUseID)
			}
		case blockToolResult:
			if blocks[i].ToolResultID != "" {
				blocks[i].ToolResultID = transcriptToolIdentity(file, blocks[i].ToolResultID)
			}
			if blocks[i].ToolUseID != "" {
				blocks[i].ToolUseID = transcriptToolIdentity(file, blocks[i].ToolUseID)
			}
		}
	}
	return blocks
}

func transcriptToolIdentity(file *TranscriptFile, rawID string) string {
	return transcriptProjectionIdentity(file, "tool", rawID)
}

func hasToolResult(blocks []llm.ContentBlock) bool {
	for _, block := range blocks {
		if block.Type == blockToolResult {
			return true
		}
	}
	return false
}

func transcriptContextText(r *transcriptRecord) string {
	for _, raw := range []json.RawMessage{r.Content, r.Attachment, r.HookAdditionalContext} {
		if len(raw) == 0 {
			continue
		}
		var text string
		if json.Unmarshal(raw, &text) == nil {
			if strings.TrimSpace(text) != "" {
				return text
			}
			continue
		}
		var value any
		if json.Unmarshal(raw, &value) == nil && transcriptContextEmpty(value) {
			continue
		}
		return string(raw)
	}
	return ""
}

func transcriptContextEmpty(value any) bool {
	switch value := value.(type) {
	case nil:
		return true
	case string:
		return strings.TrimSpace(value) == ""
	case []any:
		for _, element := range value {
			if !transcriptContextEmpty(element) {
				return false
			}
		}
		return true
	case map[string]any:
		for _, element := range value {
			if !transcriptContextEmpty(element) {
				return false
			}
		}
		return true
	default:
		return false
	}
}

func transcriptTitle(r *transcriptRecord) (string, int) {
	candidates := []struct {
		value    string
		priority int
	}{
		{r.CustomTitle, 4},
		{r.AITitle, 3},
		{r.Summary, 2},
		{r.AgentName, 1},
	}
	for _, candidate := range candidates {
		if title := strings.TrimSpace(candidate.value); title != "" {
			return truncateTranscriptTitle(title), candidate.priority
		}
	}
	return "", 0
}

func truncateTranscriptTitle(title string) string {
	if len(title) <= maxFoldedTitleLen {
		return title
	}
	cut := maxFoldedTitleLen
	for cut > 0 && !utf8.RuneStart(title[cut]) {
		cut--
	}
	return title[:cut]
}

func recordType(r *transcriptRecord) string {
	if r.Type != "" {
		return r.Type
	}
	if r.Message.Role != "" {
		return "message:" + r.Message.Role
	}
	return "unknown"
}

func (s *TranscriptProjectionStats) omit(kind string) {
	s.OmittedRecords++
	if s.OmittedTypes == nil {
		s.OmittedTypes = map[string]int{}
	}
	s.OmittedTypes[kind]++
}
