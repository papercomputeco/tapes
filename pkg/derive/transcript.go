package derive

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"encoding/json/jsontext"
	"strings"

	"github.com/papercomputeco/tapes/pkg/llm"
	"github.com/papercomputeco/tapes/pkg/merkle"
	"github.com/papercomputeco/tapes/pkg/storage"
)

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

	// signatures are the projected-content signatures of every block
	// in the transcript — the join key against wire-derived nodes.
	signatures map[string]struct{}
}

// transcriptRecord is the subset of a harness transcript line the
// reconciler reads.
type transcriptRecord struct {
	UUID       string `json:"uuid"`
	ParentUUID string `json:"parentUuid"`
	Message    struct {
		Role    string          `json:"role"`
		Content json.RawMessage `json:"content"`
	} `json:"message"`
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
	var records []transcriptRecord
	if err := json.Unmarshal(rec.RawRequest, &records); err != nil {
		return nil, err
	}

	file := &TranscriptFile{
		Session:     SessionKey{HarnessID: rec.HarnessID, HarnessSessionID: rec.HarnessSessionID},
		AgentID:     m.AgentID,
		AgentType:   m.AgentType,
		Description: m.Description,
		ToolUseID:   m.ToolUseID,
		signatures:  map[string]struct{}{},
	}
	for _, r := range records {
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
		return []llm.ContentBlock{{Type: "text", Text: asText}}
	}
	var raw []transcriptBlock
	if err := json.Unmarshal(content, &raw); err != nil {
		return nil
	}
	out := make([]llm.ContentBlock, 0, len(raw))
	for _, b := range raw {
		cb := llm.ContentBlock{Type: b.Type}
		switch b.Type {
		case "text", "":
			cb.Type = "text"
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
		if p.Type == "text" {
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
