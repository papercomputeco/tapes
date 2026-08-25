package derive

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"encoding/json/jsontext"
	"strings"
	"time"

	"github.com/papercomputeco/tapes/pkg/llm"
	"github.com/papercomputeco/tapes/pkg/merkle"
	"github.com/papercomputeco/tapes/pkg/storage"
)

// blockTypeText is the content-block type for plain text blocks, shared
// by the transcript parsers below.
const blockTypeText = "text"

// TranscriptFile is one parsed harness transcript: the main session
// file or one subagent's. ToolUseID (from the harness's subagent
// meta.json) is the Task tool_use that forked the agent — the causal
// fork edge reconciliation attaches to the wire capture.
type TranscriptFile struct {
	Session     SessionKey
	AgentID     string // "" for the main transcript
	AgentType   string
	Description string
	ToolUseID   string

	// RawTurnID identifies the selected immutable transcript version. It is
	// provenance for directly transcript-backed LLM spans only; projected IDs
	// never depend on it, so appending a grown file does not move existing IDs.
	RawTurnID int64

	// Kind qualifies Codex sub_agent_activity anchor rows: "" or
	// "started" is spawn evidence (join input); "interacted" is a
	// re-entry record (send_message / followup_task) banked for future
	// rendering and INERT in derivation — it must never anchor a
	// thread, override a started anchor, or join a chain. Populated
	// from row meta (paperd stamps kind:"interacted" on upload), with
	// a record-content fallback for rows minted by an ingest build
	// that predates the meta field.
	Kind string

	// signatures are the projected-content signatures of every block
	// in the transcript — the join key against wire-derived nodes.
	signatures map[string]struct{}

	// records retain the decoded transcript for fallback projection. They are
	// immutable derivation input from the selected raw row and are not exposed.
	records    []transcriptRecord
	receivedAt time.Time
}

// SpawnEvidence reports whether this file may participate in the
// spawn/fork joins. Only started (or legacy unmarked) rows qualify;
// interacted rows — and any future lifecycle kind — are ignored by
// derivation by design. Exported for the storage layer's derive-read
// version selection, which must not let an interacted row shadow a
// started anchor when both share an agent_id (the row's kind can be
// recoverable only from record content, so meta alone cannot decide).
func (f *TranscriptFile) SpawnEvidence() bool {
	return f.Kind == "" || f.Kind == subAgentKindStarted
}

// transcriptRecord is the subset of a harness transcript line the
// reconciler reads. Type/Payload exist only to recognize Codex
// sub_agent_activity anchor records (see subAgentActivityKind);
// Claude transcript lines never populate them.
type transcriptRecord struct {
	UUID        string `json:"uuid"`
	ParentUUID  string `json:"parentUuid"`
	LeafUUID    string `json:"leafUuid"`
	Type        string `json:"type"`
	Timestamp   string `json:"timestamp"`
	IsSidechain bool   `json:"isSidechain"`
	IsMeta      bool   `json:"isMeta"`

	// decoded is set only after the entire JSON object decodes. Go's JSON
	// decoder may populate fields before reporting a later type error; keeping
	// this explicit bit prevents those partial values from becoming spans.
	decoded bool

	AITitle     string `json:"aiTitle"`
	AgentName   string `json:"agentName"`
	Summary     string `json:"summary"`
	CustomTitle string `json:"customTitle"`

	Subtype               string          `json:"subtype"`
	Content               json.RawMessage `json:"content"`
	HookAdditionalContext json.RawMessage `json:"hookAdditionalContext"`
	Attachment            json.RawMessage `json:"attachment"`

	Payload struct {
		Type string `json:"type"`
		Kind string `json:"kind"`
	} `json:"payload"`
	Message struct {
		ID         string          `json:"id"`
		Role       string          `json:"role"`
		Model      string          `json:"model"`
		Content    json.RawMessage `json:"content"`
		StopReason string          `json:"stop_reason"`
		Usage      transcriptUsage `json:"usage"`
	} `json:"message"`
}

// subAgentActivityKind returns the sub_agent_activity lifecycle kind a
// record carries, or "" for anything that is not such a record.
func subAgentActivityKind(r *transcriptRecord) string {
	if r.Type != "event_msg" || r.Payload.Type != "sub_agent_activity" {
		return ""
	}
	return r.Payload.Kind
}

// transcriptBlock is a harness-side content block. Field names differ
// from the wire ContentBlock shape (name vs tool_name, id vs
// tool_use_id, …); toContentBlock renames them.
type transcriptBlock struct {
	Type      string          `json:"type"`
	Text      string          `json:"text"`
	Thinking  string          `json:"thinking"`
	Name      string          `json:"name"`
	ID        string          `json:"id"`
	Input     map[string]any  `json:"input"`
	ToolUseID string          `json:"tool_use_id"`
	Content   json.RawMessage `json:"content"`
	IsError   bool            `json:"is_error"`
}

// transcriptMetaFields mirrors the meta block transcript ingest writes.
type transcriptMetaFields struct {
	Transcript  bool   `json:"transcript"`
	AgentID     string `json:"agent_id"`
	AgentType   string `json:"agent_type"`
	Description string `json:"description"`
	ToolUseID   string `json:"tool_use_id"`
	Kind        string `json:"kind"`
}

// IsTranscriptMeta reports whether a raw row's meta marks it as a
// transcript file.
func IsTranscriptMeta(meta json.RawMessage) bool {
	var m transcriptMetaFields
	if len(meta) == 0 || json.Unmarshal(meta, &m) != nil {
		return false
	}
	return m.Transcript
}

// ParseTranscriptFile decodes one transcript raw row into the
// reconciler's working shape, building its projected-content signature
// set.
func ParseTranscriptFile(rec *storage.RawTurnRecord) (*TranscriptFile, error) {
	var m transcriptMetaFields
	if len(rec.Meta) > 0 {
		if err := json.Unmarshal(rec.Meta, &m); err != nil {
			return nil, err
		}
	}
	var rawRecords []json.RawMessage
	if err := json.Unmarshal(rec.RawRequest, &rawRecords); err != nil {
		return nil, err
	}
	records := make([]transcriptRecord, 0, len(rawRecords))
	for _, raw := range rawRecords {
		// Decode into a temporary and publish it only on complete success. The
		// standard decoder can otherwise leave a known record half-populated
		// when a later field has the wrong shape.
		var decoded transcriptRecord
		if err := json.Unmarshal(raw, &decoded); err == nil {
			decoded.decoded = true
			records = append(records, decoded)
			continue
		}

		// Retain only an omission label, never any partially decoded payload.
		// The verbatim object remains in raw_turns for a future parser.
		var head struct {
			Type string `json:"type"`
		}
		_ = json.Unmarshal(raw, &head)
		if head.Type == "" {
			head.Type = "unknown"
		}
		records = append(records, transcriptRecord{Type: head.Type})
	}

	file := &TranscriptFile{
		Session:     SessionKey{HarnessID: rec.HarnessID, HarnessSessionID: rec.HarnessSessionID},
		AgentID:     m.AgentID,
		AgentType:   m.AgentType,
		Description: m.Description,
		ToolUseID:   m.ToolUseID,
		Kind:        m.Kind,
		RawTurnID:   rec.ID,
		signatures:  map[string]struct{}{},
		records:     records,
		receivedAt:  rec.ReceivedAt,
	}
	for _, r := range records {
		if !r.decoded {
			continue
		}
		// Version-skew guard: an ingest build older than the meta kind
		// field drops paperd's kind marker, and an interacted row would
		// then masquerade as spawn evidence. The record content itself
		// is authoritative — an anchor row holds the verbatim
		// sub_agent_activity line — so recover the kind from it when
		// the meta carries none.
		if file.Kind == "" {
			file.Kind = subAgentActivityKind(&r)
		}
		for _, block := range transcriptBlocks(r.Message.Content) {
			if sig := blockSignature(block); sig != "" {
				file.signatures[sig] = struct{}{}
			}
		}
	}
	return file, nil
}

// transcriptBlocks converts a transcript message's content (string or
// block array) into wire-shaped ContentBlocks — the §3.2 recipe:
// rename name→tool_name, id→tool_use_id, tool_use_id→tool_result_id,
// flatten tool_result content arrays into tool_output.
func transcriptBlocks(content json.RawMessage) []llm.ContentBlock {
	if len(content) == 0 {
		return nil
	}
	var asText string
	if err := json.Unmarshal(content, &asText); err == nil {
		return []llm.ContentBlock{{Type: blockTypeText, Text: asText}}
	}
	var raw []transcriptBlock
	if err := json.Unmarshal(content, &raw); err != nil {
		return nil
	}
	out := make([]llm.ContentBlock, 0, len(raw))
	for _, b := range raw {
		cb := llm.ContentBlock{Type: b.Type}
		switch b.Type {
		case blockTypeText, "":
			cb.Type = blockTypeText
			cb.Text = b.Text
		case "thinking":
			cb.Thinking = b.Thinking
		case "tool_use", "server_tool_use":
			cb.ToolUseID = b.ID
			cb.ToolName = b.Name
			cb.ToolInput = b.Input
		case "tool_result":
			cb.ToolResultID = b.ToolUseID
			cb.ToolOutput = flattenToolResult(b.Content)
			cb.IsError = b.IsError
		case "image":
			// presence only; bytes don't participate in signatures
		default:
			cb.Text = b.Text
		}
		out = append(out, cb)
	}
	return out
}

// flattenToolResult collapses a transcript tool_result's content
// (string or array of text parts) into the wire's tool_output string.
func flattenToolResult(content json.RawMessage) string {
	if len(content) == 0 {
		return ""
	}
	var asText string
	if err := json.Unmarshal(content, &asText); err == nil {
		return asText
	}
	var parts []struct {
		Type string `json:"type"`
		Text string `json:"text"`
	}
	if err := json.Unmarshal(content, &parts); err != nil {
		return ""
	}
	var sb []string
	for _, p := range parts {
		if p.Type == blockTypeText {
			sb = append(sb, p.Text)
		}
	}
	return strings.Join(sb, "\n")
}

// blockSignature canonicalizes ONE projected content block into a
// stable join key. Both sides of the reconciliation — wire-derived
// node blocks and transcript blocks — pass through merkle's
// ProjectContent, so the same logical block signs identically
// regardless of source. Thinking blocks return "" (the harness omits
// signatures on re-send; presence carries no join value).
func blockSignature(block llm.ContentBlock) string {
	projected := merkle.ProjectContent([]llm.ContentBlock{block})
	if len(projected) == 0 {
		return ""
	}
	p := projected[0]
	if p.Type == "thinking" {
		return ""
	}
	// Tool ids are harness-stable across both sources and already part
	// of the block; marshal the whole projected block canonically.
	data, err := json.Marshal(p)
	if err != nil {
		return ""
	}
	v := jsontext.Value(data)
	if err := v.Canonicalize(); err != nil {
		return ""
	}
	sum := sha256.Sum256(v)
	return hex.EncodeToString(sum[:])
}
