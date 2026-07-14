package derive

import (
	"encoding/json"
	"strings"

	"github.com/papercomputeco/tapes/pkg/llm"
)

type claudeTranscriptRecord struct {
	UUID       string `json:"uuid"`
	ParentUUID string `json:"parentUuid"`
	Message    struct {
		Role    string          `json:"role"`
		Content json.RawMessage `json:"content"`
	} `json:"message"`
}

type claudeTranscriptBlock struct {
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

func parseClaudeTranscript(raw json.RawMessage, file *TranscriptFile) error {
	var records []claudeTranscriptRecord
	if err := json.Unmarshal(raw, &records); err != nil {
		return err
	}
	for _, record := range records {
		for _, block := range claudeTranscriptBlocks(record.Message.Content) {
			if sig := blockSignature(block); sig != "" {
				file.signatures[sig] = struct{}{}
			}
		}
	}
	return nil
}

func claudeTranscriptBlocks(content json.RawMessage) []llm.ContentBlock {
	if len(content) == 0 {
		return nil
	}
	var asText string
	if err := json.Unmarshal(content, &asText); err == nil {
		return []llm.ContentBlock{{Type: blockText, Text: asText}}
	}
	var raw []claudeTranscriptBlock
	if err := json.Unmarshal(content, &raw); err != nil {
		return nil
	}
	out := make([]llm.ContentBlock, 0, len(raw))
	for _, block := range raw {
		canonical := llm.ContentBlock{Type: block.Type}
		switch block.Type {
		case blockText, "":
			canonical.Type = blockText
			canonical.Text = block.Text
		case blockThinking:
			canonical.Thinking = block.Thinking
		case blockToolUse, blockServerToolUse:
			canonical.ToolUseID = block.ID
			canonical.ToolName = block.Name
			canonical.ToolInput = block.Input
		case blockToolResult:
			canonical.ToolResultID = block.ToolUseID
			canonical.ToolOutput = flattenClaudeToolResult(block.Content)
			canonical.IsError = block.IsError
		case "image":
			// Presence only; bytes do not participate in signatures.
		default:
			canonical.Text = block.Text
		}
		out = append(out, canonical)
	}
	return out
}

func flattenClaudeToolResult(content json.RawMessage) string {
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
	var text []string
	for _, part := range parts {
		if part.Type == blockText {
			text = append(text, part.Text)
		}
	}
	return strings.Join(text, "\n")
}
