package derive

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/papercomputeco/tapes/pkg/llm"
)

const (
	harnessCursor     = "cursor"
	cursorUnknownType = "unknown"
	cursorJSONNull    = "null"
)

type cursorToolPayload struct {
	name   string
	args   map[string]any
	result json.RawMessage
}

func (f *TranscriptFile) isCursor() bool {
	return f != nil && f.Session.HarnessID == harnessCursor
}

func projectCursorTranscriptFile(project string, file *TranscriptFile, stats *TranscriptProjectionStats, anchor string) []TranscriptTurn {
	var history []transcriptMessage
	var pending []transcriptMessage
	var turns []TranscriptTurn
	var turnIdentity string
	var model string
	var pendingAssistant *assistantGroup
	var pendingResults []transcriptMessage
	assistantRecords := 0
	initSeen := false
	startedTools := map[string]struct{}{}
	completedTools := map[string]struct{}{}

	flushAssistant := func() {
		if pendingAssistant == nil {
			return
		}
		turns = append(turns, normalizeTranscriptTurn(
			project, file, history, pending, pendingAssistant, turnIdentity, anchor,
		))
		history = append(history, transcriptMessage{
			message: llm.Message{Role: roleAssistant, Content: pendingAssistant.content},
			kind:    KindMain, capturedAt: file.receivedAt,
		})
		pending = nil
		turnIdentity = ""
		for _, result := range pendingResults {
			history = append(history, result)
			pending = append(pending, result)
		}
		pendingResults = nil
		pendingAssistant = nil
	}
	enqueueToolResult := func(message transcriptMessage) {
		if pendingAssistant == nil {
			history = append(history, message)
			pending = append(pending, message)
			return
		}
		pendingResults = append(pendingResults, message)
	}

	for i := range file.records {
		record := &file.records[i]
		if !record.decoded {
			stats.omit("malformed:cursor:" + cursorRecordType(record))
			continue
		}

		switch record.Type {
		case "system":
			if record.Subtype != "init" {
				stats.omit("cursor:system:" + cursorSubtype(record))
				continue
			}
			if initSeen {
				stats.omit("cursor:duplicate-init")
				continue
			}
			initSeen = true
			if record.Model != "" {
				model = record.Model
			}
			stats.ProjectedRecords++

		case roleUser:
			flushAssistant()
			blocks, ok := cursorMessageBlocks(record.Message.Content)
			if !ok || record.Message.Role != roleUser {
				stats.omit("malformed:cursor:user")
				continue
			}
			if len(blocks) == 0 {
				stats.omit("cursor:user")
				continue
			}
			message := transcriptMessage{
				message: llm.Message{Role: roleUser, Content: blocks},
				kind:    KindMain, capturedAt: file.receivedAt,
			}
			history = append(history, message)
			pending = append(pending, message)
			if turnIdentity == "" {
				turnIdentity = transcriptProjectionIdentity(file, "turn", cursorOrdinalIdentity(i))
			}
			stats.ProjectedRecords++

		case roleAssistant:
			blocks, ok := cursorMessageBlocks(record.Message.Content)
			if !ok || record.Message.Role != roleAssistant {
				stats.omit("malformed:cursor:assistant")
				continue
			}
			if len(blocks) == 0 {
				stats.omit("cursor:assistant")
				continue
			}
			flushAssistant()
			pendingAssistant = &assistantGroup{
				identity: cursorOrdinalIdentity(i),
				model:    model, content: blocks,
				startedAt: file.receivedAt, endedAt: file.receivedAt,
			}
			assistantRecords++
			stats.ProjectedRecords++

		case "tool_call":
			payload, ok := decodeCursorToolPayload(record.ToolCall)
			if !ok || record.CallID == "" {
				stats.omit("malformed:cursor:tool_call:" + cursorSubtype(record))
				continue
			}
			namespacedID := transcriptToolIdentity(file, record.CallID)
			switch record.Subtype {
			case "started":
				// Cursor often calls a tool before any assistant text.
				if pendingAssistant == nil {
					pendingAssistant = &assistantGroup{
						identity:  cursorOrdinalIdentity(i),
						model:     model,
						startedAt: file.receivedAt, endedAt: file.receivedAt,
					}
				}
				pendingAssistant.content = append(pendingAssistant.content, llm.ContentBlock{
					Type: blockToolUse, ToolUseID: namespacedID,
					ToolName: payload.name, ToolInput: payload.args,
				})
				startedTools[record.CallID] = struct{}{}
				stats.ProjectedRecords++

			case "completed":
				if _, done := completedTools[record.CallID]; done {
					stats.omit("cursor:duplicate-tool-completed")
					continue
				}
				output, ok := cursorToolResult(payload.result)
				if !ok {
					stats.omit("malformed:cursor:tool_call:completed")
					continue
				}
				_, started := startedTools[record.CallID]
				if !started {
					if pendingAssistant == nil {
						stats.omit("cursor:orphan-tool-completed")
						continue
					}
					// Cursor's completed event repeats the args from started.
					pendingAssistant.content = append(pendingAssistant.content, llm.ContentBlock{
						Type: blockToolUse, ToolUseID: namespacedID,
						ToolName: payload.name, ToolInput: payload.args,
					})
				}
				enqueueToolResult(transcriptMessage{
					message: llm.Message{Role: roleTool, Content: []llm.ContentBlock{{
						Type: blockToolResult, ToolResultID: namespacedID,
						ToolOutput: output, IsError: cursorToolResultIsError(payload.result),
					}}},
					kind: KindMain, capturedAt: file.receivedAt,
				})
				delete(startedTools, record.CallID)
				completedTools[record.CallID] = struct{}{}
				stats.ProjectedRecords++

			default:
				stats.omit("cursor:tool_call:" + cursorSubtype(record))
			}

		case "result":
			result, ok := cursorTerminalResult(record.Result)
			if !ok {
				stats.omit("malformed:cursor:result")
				continue
			}
			flushAssistant()
			if record.IsError {
				stats.omit("cursor:result:error")
				continue
			}
			// result.result concatenates the assistant text from the run.
			if assistantRecords == 0 && result != "" {
				pendingAssistant = &assistantGroup{
					identity:  cursorOrdinalIdentity(i),
					model:     model,
					content:   []llm.ContentBlock{{Type: blockText, Text: result}},
					startedAt: file.receivedAt, endedAt: file.receivedAt,
				}
				flushAssistant()
			}
			for _, message := range pending {
				if message.message.Role == roleTool {
					stats.omit("cursor:trailing-tool-result")
				}
			}
			stats.ProjectedRecords++

		default:
			stats.omit("cursor:" + cursorRecordType(record))
		}
	}
	flushAssistant()
	return turns
}

func cursorMessageBlocks(content json.RawMessage) ([]llm.ContentBlock, bool) {
	if len(content) == 0 || string(content) == cursorJSONNull {
		return nil, false
	}
	var text string
	if json.Unmarshal(content, &text) == nil {
		return []llm.ContentBlock{{Type: blockText, Text: text}}, true
	}
	var raw []struct {
		Type string `json:"type"`
		Text string `json:"text"`
	}
	if json.Unmarshal(content, &raw) != nil {
		return nil, false
	}
	blocks := make([]llm.ContentBlock, 0, len(raw))
	for _, block := range raw {
		if block.Type == blockText {
			blocks = append(blocks, llm.ContentBlock{Type: blockText, Text: block.Text})
		}
	}
	return blocks, true
}

func decodeCursorToolPayload(raw json.RawMessage) (cursorToolPayload, bool) {
	var tools map[string]json.RawMessage
	if len(raw) == 0 || json.Unmarshal(raw, &tools) != nil {
		return cursorToolPayload{}, false
	}
	name, tool, ok := cursorToolEntry(tools)
	if !ok {
		return cursorToolPayload{}, false
	}
	if name == "function" {
		var body struct {
			Name      string          `json:"name"`
			Arguments string          `json:"arguments"`
			Result    json.RawMessage `json:"result"`
		}
		if json.Unmarshal(tool, &body) != nil || body.Name == "" {
			return cursorToolPayload{}, false
		}
		var args map[string]any
		if json.Unmarshal([]byte(body.Arguments), &args) != nil && body.Arguments != "" {
			args = map[string]any{"arguments": body.Arguments}
		}
		return cursorToolPayload{name: body.Name, args: args, result: body.Result}, true
	}
	var body struct {
		Args   map[string]any  `json:"args"`
		Result json.RawMessage `json:"result"`
	}
	if json.Unmarshal(tool, &body) != nil {
		return cursorToolPayload{}, false
	}
	return cursorToolPayload{name: name, args: body.Args, result: body.Result}, true
}

// Cursor's docs allow added fields, so tool_call may contain keys other than the tool.
func cursorToolEntry(tools map[string]json.RawMessage) (string, json.RawMessage, bool) {
	var name string
	if len(tools) == 1 {
		for candidate := range tools {
			name = candidate
		}
		return name, tools[name], true
	}
	for candidate := range tools {
		if candidate != "function" && !strings.HasSuffix(candidate, "ToolCall") {
			continue
		}
		if name != "" {
			return "", nil, false
		}
		name = candidate
	}
	if name == "" {
		return "", nil, false
	}
	return name, tools[name], true
}

func cursorToolResult(raw json.RawMessage) (string, bool) {
	if len(raw) == 0 || string(raw) == cursorJSONNull {
		return "", false
	}
	var text string
	if json.Unmarshal(raw, &text) == nil {
		return text, true
	}
	var value any
	if json.Unmarshal(raw, &value) != nil {
		return "", false
	}
	canonical, err := json.Marshal(value)
	if err != nil {
		return "", false
	}
	return string(canonical), true
}

func cursorToolResultIsError(raw json.RawMessage) bool {
	var result map[string]json.RawMessage
	if json.Unmarshal(raw, &result) != nil {
		return false
	}
	for _, key := range []string{"error", "failure"} {
		if value, ok := result[key]; ok && len(value) > 0 && string(value) != cursorJSONNull {
			return true
		}
	}
	return false
}

func cursorTerminalResult(raw json.RawMessage) (string, bool) {
	if len(raw) == 0 || string(raw) == cursorJSONNull {
		return "", true
	}
	var result string
	if json.Unmarshal(raw, &result) != nil {
		return "", false
	}
	return result, true
}

func cursorOrdinalIdentity(ordinal int) string {
	return fmt.Sprintf("cursor-record:%d", ordinal)
}

func cursorRecordType(record *transcriptRecord) string {
	if record.Type == "" {
		return cursorUnknownType
	}
	return record.Type
}

func cursorSubtype(record *transcriptRecord) string {
	if record.Subtype == "" {
		return cursorUnknownType
	}
	return record.Subtype
}
