package derive

import (
	"encoding/json"

	"github.com/papercomputeco/tapes/pkg/llm"
)

type piTranscriptRecord struct {
	Type    string `json:"type"`
	Message struct {
		Role       string          `json:"role"`
		Content    json.RawMessage `json:"content"`
		ToolCallID string          `json:"toolCallId"`
		ToolName   string          `json:"toolName"`
		IsError    bool            `json:"isError"`
	} `json:"message"`
}

type piTranscriptBlock struct {
	Type      string         `json:"type"`
	Text      string         `json:"text"`
	Thinking  string         `json:"thinking"`
	ID        string         `json:"id"`
	Name      string         `json:"name"`
	Arguments map[string]any `json:"arguments"`
}

// parsePiTranscript reads pi's append-only session tree. Pi stores canonical
// user/assistant content in message records and tool results as their own
// toolResult messages, independent of the provider used for the wire call.
func parsePiTranscript(raw json.RawMessage, file *TranscriptFile) error {
	var records []piTranscriptRecord
	if err := json.Unmarshal(raw, &records); err != nil {
		return err
	}
	for _, record := range records {
		if record.Type != "message" {
			continue
		}
		if record.Message.Role == "toolResult" {
			block := llm.ContentBlock{
				Type:         blockToolResult,
				ToolResultID: record.Message.ToolCallID,
				ToolOutput:   flattenClaudeToolResult(record.Message.Content),
				IsError:      record.Message.IsError,
			}
			if sig := blockSignature(block); sig != "" {
				file.signatures[sig] = struct{}{}
			}
			continue
		}
		var blocks []piTranscriptBlock
		if json.Unmarshal(record.Message.Content, &blocks) != nil {
			continue
		}
		for _, block := range blocks {
			canonical := llm.ContentBlock{Type: block.Type}
			switch block.Type {
			case "text":
				canonical.Text = block.Text
			case blockThinking:
				canonical.Thinking = block.Thinking
			case "toolCall":
				canonical.Type = blockToolUse
				canonical.ToolUseID = block.ID
				canonical.ToolName = block.Name
				canonical.ToolInput = block.Arguments
			default:
				continue
			}
			if sig := blockSignature(canonical); sig != "" {
				file.signatures[sig] = struct{}{}
			}
		}
	}
	return nil
}
