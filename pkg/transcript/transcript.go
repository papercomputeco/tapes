// Package transcript renders a session's span projection as one readable
// conversation: the user's words and the assistant's replies, in the order
// they happened, with the harness's own traffic left out.
//
// The projection already carries everything a transcript needs. An llm
// span's Input holds only the content blocks NEW to that call (never the
// re-sent history) and its Output is the model's reply, so walking the
// conversation-spine llm calls in presentation order yields each message
// exactly once. Tool results are not on the llm span that consumed them;
// they live on the tool span the deriver opened for each tool_use, which
// the deriver emits right after the call that requested it. A Walker
// folds a run of those tool spans into one user row, the same place the
// results sit on the wire.
//
// What is left out is what the deriver classified as not conversation:
// offshoot calls (permission checks, title generation, suggestions),
// injected context (event spans), and — unless asked for — subagent
// threads. Harness tags (<system-reminder> and friends) are stripped from
// user text with the same projection the trace header's prompt uses.
package transcript

import (
	"encoding/json"
	"strings"
	"time"

	"github.com/papercomputeco/tapes/pkg/llm"
	"github.com/papercomputeco/tapes/pkg/merkle"
	"github.com/papercomputeco/tapes/pkg/storage"
)

// Row roles.
const (
	RoleUser      = "user"
	RoleAssistant = "assistant"
)

// Span kinds and call kinds the walker reads. They mirror pkg/derive's
// emitted values; the transcript reads stored rows, not the deriver.
const (
	spanKindLLM  = "llm"
	spanKindTool = "tool"
	callKindMain = "main"

	blockText          = "text"
	blockToolUse       = "tool_use"
	blockServerToolUse = "server_tool_use"
	blockToolResult    = "tool_result"
)

// Row is one message of the transcript.
type Row struct {
	// Seq is the presentation ordinal, within its trace, of the span the
	// row was read from. A tool-results row takes the seq of its first
	// tool span.
	Seq     int64  `json:"seq"`
	TraceID string `json:"trace_id"`
	SpanID  string `json:"span_id"`
	// ThreadID names the subagent thread the message belongs to; absent
	// for the root conversation.
	ThreadID  string    `json:"thread_id,omitempty"`
	StartedAt time.Time `json:"started_at"`
	// Role is "user" or "assistant".
	Role string `json:"role"`
	// Text is the message's prose. For a user row it is the genuine user
	// text with harness tags stripped; a row that only carries tool
	// results has empty text. For an assistant row it is the reply's text
	// blocks (thinking is not included).
	Text string `json:"text"`
	// ToolCalls are the tools the assistant asked for, on assistant rows.
	ToolCalls []ToolCall `json:"tool_calls,omitempty"`
	// ToolResults are the results handed back to the model, on user rows.
	// An assistant row carries the inline results of server-side tools
	// (web_search_tool_result and the like), which arrive in the reply
	// itself rather than in a later request.
	ToolResults []ToolResult `json:"tool_results,omitempty"`
}

// ToolCall is one tool_use block of an assistant reply.
type ToolCall struct {
	ID    string         `json:"id"`
	Name  string         `json:"name"`
	Input map[string]any `json:"input"`
}

// ToolResult is one tool_result handed back to the model.
type ToolResult struct {
	ToolUseID string `json:"tool_use_id"`
	Text      string `json:"text"`
	IsError   bool   `json:"is_error"`
}

// Options selects which spans contribute.
type Options struct {
	// Subagents includes subagent threads; false keeps the root thread
	// (the spans whose thread_id is empty).
	Subagents bool
}

// Position is where a walk resumes within one trace: after the span key
// After (zero: from the trace's first span), and, when SkipUserOf names a
// span, without re-serving that span's user row — the row a page ended on
// when the same span's assistant row did not fit.
type Position struct {
	After      storage.SpanCursor
	SkipUserOf string
}

// Emitted is a row together with the position just past it, which is
// what a pager mints its cursor from.
type Emitted struct {
	Row  Row
	Next Position
}

// Walker turns one trace's spans, fed in presentation order, into rows.
// It holds at most one pending tool-results row, so its memory is bounded
// by the run of tool spans one call opened, not by the trace.
type Walker struct {
	opts       Options
	skipUserOf string
	last       storage.SpanCursor
	pending    *Row
	pendingAt  storage.SpanCursor
}

// NewWalker starts a walk of one trace from pos. Feed it the spans the
// store yields after pos.After.
func NewWalker(opts Options, pos Position) *Walker {
	return &Walker{opts: opts, skipUserOf: pos.SkipUserOf, last: pos.After}
}

// Contributes reports whether sp is part of the transcript under opts:
// a conversation llm call (call_kind main, or empty on rows derived before
// the taxonomy) or a tool span, on the root thread unless subagents are
// included.
func Contributes(sp storage.SpanRecord, opts Options) bool {
	if !opts.Subagents && sp.ThreadID != "" {
		return false
	}
	switch sp.Kind {
	case spanKindLLM:
		return sp.CallKind == callKindMain || sp.CallKind == ""
	case spanKindTool:
		return true
	default:
		return false
	}
}

// Feed consumes the next span and returns the rows it completes, each with
// the position just past it.
func (w *Walker) Feed(sp storage.SpanRecord) []Emitted {
	key := storage.SpanCursor{TraceID: sp.TraceID, Seq: sp.Seq, StartedAt: sp.StartedAt, SpanID: sp.SpanID}
	var out []Emitted

	contributes := Contributes(sp, w.opts)
	// A run of tool results closes at the first span that does not extend
	// it. Spans that are not part of the transcript at all (event spans,
	// offshoot calls, another thread's work) do not close it: the deriver
	// can emit them between two tool spans of one call.
	if w.pending != nil && contributes && (sp.Kind != spanKindTool || sp.ThreadID != w.pending.ThreadID) {
		out = append(out, w.flush()...)
	}
	defer func() { w.last = key }()

	if !contributes {
		return out
	}

	if sp.Kind == spanKindTool {
		results := toolResults(sp.Output)
		if len(results) == 0 {
			return out
		}
		if w.pending == nil {
			w.pending = &Row{
				Seq: sp.Seq, TraceID: sp.TraceID, SpanID: sp.SpanID, ThreadID: sp.ThreadID,
				StartedAt: sp.StartedAt, Role: RoleUser,
			}
		}
		w.pending.ToolResults = append(w.pending.ToolResults, results...)
		w.pendingAt = key
		return out
	}

	input := decodeBlocks(sp.Input)
	if w.skipUserOf != sp.SpanID {
		user := Row{
			Seq: sp.Seq, TraceID: sp.TraceID, SpanID: sp.SpanID, ThreadID: sp.ThreadID,
			StartedAt: sp.StartedAt, Role: RoleUser,
			Text:        UserText(input),
			ToolResults: toolResults(sp.Input),
		}
		if user.Text != "" || len(user.ToolResults) > 0 {
			out = append(out, Emitted{Row: user, Next: Position{After: w.last, SkipUserOf: sp.SpanID}})
		}
	}
	w.skipUserOf = ""

	output := decodeBlocks(sp.Output)
	assistant := Row{
		Seq: sp.Seq, TraceID: sp.TraceID, SpanID: sp.SpanID, ThreadID: sp.ThreadID,
		StartedAt: sp.StartedAt, Role: RoleAssistant,
		Text:        AssistantText(output),
		ToolCalls:   toolCalls(output),
		ToolResults: serverToolResults(output),
	}
	if assistant.Text != "" || len(assistant.ToolCalls) > 0 || len(assistant.ToolResults) > 0 {
		out = append(out, Emitted{Row: assistant, Next: Position{After: key}})
	}
	return out
}

// Flush returns the row still pending at the end of the trace.
func (w *Walker) Flush() []Emitted { return w.flush() }

func (w *Walker) flush() []Emitted {
	if w.pending == nil {
		return nil
	}
	row := *w.pending
	w.pending = nil
	return []Emitted{{Row: row, Next: Position{After: w.pendingAt}}}
}

// UserText renders the genuine user text of a call's fresh input: each
// text block projected for humans (harness tags stripped, the human words
// inside content-bearing wrappers such as <command-args> kept), blocks
// that project to nothing dropped, the rest joined by newlines.
func UserText(blocks []llm.ContentBlock) string {
	var parts []string
	for _, b := range blocks {
		if b.Type != blockText || b.Text == "" {
			continue
		}
		if prose := merkle.PreviewText(b.Text); prose != "" {
			parts = append(parts, prose)
		}
	}
	return strings.Join(parts, "\n")
}

// AssistantText joins a reply's text blocks verbatim; assistant output
// carries no harness scaffolding.
func AssistantText(blocks []llm.ContentBlock) string {
	var parts []string
	for _, b := range blocks {
		if b.Type == blockText && b.Text != "" {
			parts = append(parts, b.Text)
		}
	}
	return strings.Join(parts, "\n")
}

func toolCalls(blocks []llm.ContentBlock) []ToolCall {
	var calls []ToolCall
	for _, b := range blocks {
		if b.Type != blockToolUse && b.Type != blockServerToolUse {
			continue
		}
		input := b.ToolInput
		if input == nil {
			input = map[string]any{}
		}
		calls = append(calls, ToolCall{ID: b.ToolUseID, Name: b.ToolName, Input: input})
	}
	return calls
}

// toolResults reads the tool_result blocks of a raw content-block array.
func toolResults(raw json.RawMessage) []ToolResult {
	var results []ToolResult
	for _, b := range decodeBlocks(raw) {
		if b.Type != blockToolResult {
			continue
		}
		results = append(results, ToolResult{ToolUseID: b.ToolResultID, Text: b.ToolOutput, IsError: b.IsError})
	}
	return results
}

// serverToolResults reads the inline result blocks a server-side tool
// returns in the reply itself (web_search_tool_result, and any other
// `*_tool_result` block linked to its server_tool_use). The payload is
// kept as its captured JSON text; a result whose payload is an error
// object (`"type": "..._error"`) is marked as an error.
func serverToolResults(blocks []llm.ContentBlock) []ToolResult {
	var results []ToolResult
	for _, b := range blocks {
		if b.Type == blockToolResult || !strings.HasSuffix(b.Type, "_"+blockToolResult) || b.ToolResultID == "" {
			continue
		}
		text := b.ToolOutput
		if text == "" && len(b.Content) > 0 {
			text = string(b.Content)
		}
		var shape struct {
			Type string `json:"type"`
		}
		isErr := b.IsError || (json.Unmarshal(b.Content, &shape) == nil && strings.HasSuffix(shape.Type, "_error"))
		results = append(results, ToolResult{ToolUseID: b.ToolResultID, Text: text, IsError: isErr})
	}
	return results
}

// decodeBlocks reads a stored content-block array. A row whose payload is
// not one (empty, null, or malformed) contributes no content rather than
// failing the whole transcript.
func decodeBlocks(raw json.RawMessage) []llm.ContentBlock {
	if len(raw) == 0 {
		return nil
	}
	var blocks []llm.ContentBlock
	if err := json.Unmarshal(raw, &blocks); err != nil {
		return nil
	}
	return blocks
}

// Matches reports whether row passes the role and substring filters: role
// is "user", "assistant", or "" / "all"; a user filter keeps only rows with
// genuine user text. q is matched case-insensitively against the row text.
func Matches(row Row, role, q string) bool {
	switch role {
	case RoleUser:
		if row.Role != RoleUser || row.Text == "" {
			return false
		}
	case RoleAssistant:
		if row.Role != RoleAssistant {
			return false
		}
	}
	if q == "" {
		return true
	}
	return strings.Contains(strings.ToLower(row.Text), strings.ToLower(q))
}
