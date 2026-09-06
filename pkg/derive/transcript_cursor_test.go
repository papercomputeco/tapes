package derive_test

import (
	"encoding/json"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/derive"
	"github.com/papercomputeco/tapes/pkg/storage"
)

var _ = Describe("Cursor CLI transcript fallback", func() {
	const cursorSessionID = "c6b62c6f-7ead-4fd6-9922-e952131177ff"
	receivedAt := time.Date(2026, 8, 28, 4, 5, 6, 0, time.UTC)

	parseCursorFile := func(rawTurnID int64, records []map[string]any) *derive.TranscriptFile {
		raw, err := json.Marshal(records)
		Expect(err).NotTo(HaveOccurred())
		file, err := derive.ParseTranscriptFile(&storage.RawTurnRecord{
			ID: rawTurnID, Source: storage.RawTurnSourceTranscript,
			HarnessID: "cursor", HarnessSessionID: cursorSessionID,
			RawRequest: raw, Meta: json.RawMessage(`{"transcript":true}`),
			ReceivedAt: receivedAt,
		})
		Expect(err).NotTo(HaveOccurred())
		return file
	}

	officialStream := func() []map[string]any {
		return []map[string]any{
			{
				"type": "system", "subtype": "init", "apiKeySource": "login",
				"cwd": "/Users/user/project", "session_id": cursorSessionID,
				"model": "Claude 4 Sonnet", "permissionMode": "default",
			},
			{
				"type": "user", "session_id": cursorSessionID,
				"message": map[string]any{"role": "user", "content": []map[string]any{{"type": "text", "text": "Read README.md"}}},
			},
			{
				"type": "assistant", "session_id": cursorSessionID,
				"message": map[string]any{"role": "assistant", "content": []map[string]any{{"type": "text", "text": "Working"}}},
			},
			{
				"type": "tool_call", "subtype": "started", "call_id": "toolu_read_1", "session_id": cursorSessionID,
				"tool_call": map[string]any{"readToolCall": map[string]any{"args": map[string]any{"path": "README.md"}}},
			},
			{
				"type": "tool_call", "subtype": "completed", "call_id": "toolu_read_1", "session_id": cursorSessionID,
				"tool_call": map[string]any{"readToolCall": map[string]any{
					"args":   map[string]any{"path": "README.md"},
					"result": map[string]any{"success": map[string]any{"content": "# Project", "isEmpty": false}},
				}},
			},
			{
				"type": "assistant", "session_id": cursorSessionID,
				"message": map[string]any{"role": "assistant", "content": []map[string]any{{"type": "text", "text": "Working"}}},
			},
			{
				"type": "result", "subtype": "success", "duration_ms": 5234,
				"duration_api_ms": 5234, "is_error": false, "result": "WorkingWorking",
				"session_id": cursorSessionID, "request_id": "request-1",
			},
			{"type": "future_event", "session_id": cursorSessionID, "payload": map[string]any{"kept": "raw"}},
		}
	}

	It("projects the documented non-partial stream without duplicating its terminal aggregate", func() {
		records := officialStream()
		set, stats, err := deriveWithTranscriptFallback(nil, parseCursorFile(41, records))
		Expect(err).NotTo(HaveOccurred())
		spans := derive.EmitSpans(set)

		Expect(stats.Records).To(Equal(8))
		Expect(stats.ProjectedRecords).To(Equal(7))
		Expect(stats.OmittedTypes).To(HaveKeyWithValue("cursor:future_event", 1))
		Expect(spans.Turns).To(HaveLen(1))
		turn := spans.Turns[0]
		Expect(turn.Source).To(Equal(storage.RawTurnSourceTranscript))
		Expect(turn.UserPrompt).To(Equal("Read README.md"))
		Expect(turn.ResponsePreview).To(Equal("Working"))
		Expect(turn.StartedAt).To(Equal(receivedAt))
		Expect(turn.EndedAt).To(Equal(receivedAt))
		Expect(turn.TotalInputTokens).To(BeZero())
		Expect(turn.TotalOutputTokens).To(BeZero())
		Expect(turn.TotalCostUSD).To(BeZero())

		var llmIDs []string
		var tool *derive.Span
		for _, span := range turn.Spans {
			switch span.Kind {
			case derive.SpanKindLLM:
				llmIDs = append(llmIDs, span.SpanID)
				Expect(span.Model).To(Equal("Claude 4 Sonnet"))
				Expect(span.Usage).To(BeNil())
				Expect(span.StartedAt).To(Equal(receivedAt))
			case derive.SpanKindTool:
				tool = span
			}
		}
		Expect(llmIDs).To(HaveLen(2), "the result aggregate must not become a third assistant message")
		Expect(llmIDs[0]).NotTo(Equal(llmIDs[1]), "equal assistant text at different record ordinals is still two calls")
		Expect(tool).NotTo(BeNil())
		Expect(tool.Name).To(Equal("readToolCall"))
		Expect(tool.Input).To(ConsistOf(HaveField("ToolInput", HaveKeyWithValue("path", "README.md"))))
		Expect(tool.Output).To(ConsistOf(And(
			HaveField("Type", "tool_result"),
			HaveField("IsError", false),
			HaveField("ToolOutput", ContainSubstring(`"content":"# Project"`)),
		)))
		Expect(turn.Links).To(And(
			ContainElement(HaveField("Kind", derive.LinkEmits)),
			ContainElement(HaveField("Kind", derive.LinkFeeds)),
		))

		baseIDs := traceAndSpanIDs(spans)
		grownRecords := append(append([]map[string]any(nil), records...),
			map[string]any{
				"type": "user", "session_id": cursorSessionID,
				"message": map[string]any{"role": "user", "content": []map[string]any{{"type": "text", "text": "One more thing"}}},
			},
			map[string]any{
				"type": "assistant", "session_id": cursorSessionID,
				"message": map[string]any{"role": "assistant", "content": []map[string]any{{"type": "text", "text": "Done"}}},
			},
		)
		grownSet, _, err := deriveWithTranscriptFallback(nil, parseCursorFile(99, grownRecords))
		Expect(err).NotTo(HaveOccurred())
		grownIDs := traceAndSpanIDs(derive.EmitSpans(grownSet))
		Expect(len(grownIDs)).To(BeNumerically(">", len(baseIDs)),
			"a grown snapshot must add the new turn's ids")
		for _, id := range baseIDs {
			Expect(grownIDs).To(ContainElement(id), "a grown snapshot must retain projected id %s", id)
		}
	})

	It("keeps an assistant event that carries model_call_id, as the live build emits before a tool call", func() {
		records := []map[string]any{
			{"type": "system", "subtype": "init", "model": "cursor-model", "session_id": cursorSessionID},
			{
				"type": "user", "session_id": cursorSessionID,
				"message": map[string]any{"role": "user", "content": []map[string]any{{"type": "text", "text": "Keep going"}}},
			},
			{
				"type": "tool_call", "subtype": "started", "call_id": 42,
				"tool_call": map[string]any{"readToolCall": map[string]any{"args": map[string]any{"path": "bad"}}},
			},
			{
				"type": "assistant", "timestamp_ms": 1787890000000, "model_call_id": "live-call",
				"message": map[string]any{"role": "assistant", "content": []map[string]any{{"type": "text", "text": "first segment"}}},
			},
			{
				"type": "assistant", "session_id": cursorSessionID,
				"message":      map[string]any{"role": "assistant", "content": []map[string]any{{"type": "text", "text": "survives"}}},
				"future_field": map[string]any{"still": "retained raw"},
			},
			{
				"type": "result", "subtype": "success", "is_error": false,
				"result": "first segmentsurvives", "session_id": cursorSessionID,
			},
		}

		set, stats, err := deriveWithTranscriptFallback(nil, parseCursorFile(42, records))
		Expect(err).NotTo(HaveOccurred())
		spans := derive.EmitSpans(set)

		Expect(stats.Records).To(Equal(6))
		Expect(stats.ProjectedRecords).To(Equal(5))
		Expect(stats.OmittedRecords).To(Equal(1))
		Expect(stats.OmittedTypes).To(HaveKeyWithValue("malformed:cursor:tool_call", 1))
		var texts []string
		for _, turn := range spans.Turns {
			for _, span := range turn.Spans {
				if span.Kind == derive.SpanKindLLM {
					for _, block := range span.Output {
						texts = append(texts, block.Text)
					}
				}
			}
		}
		Expect(texts).To(Equal([]string{"first segment", "survives"}))
	})

	It("falls back to the terminal aggregate only when no assistant event survived, deterministically", func() {
		records := []map[string]any{
			{"type": "system", "subtype": "init", "model": "cursor-model", "session_id": cursorSessionID},
			{
				"type": "user", "session_id": cursorSessionID,
				"message": map[string]any{"role": "user", "content": []map[string]any{{"type": "text", "text": "Just answer"}}},
			},
			{"type": "result", "subtype": "success", "is_error": false, "result": "aggregate answer", "session_id": cursorSessionID},
		}

		set, _, err := deriveWithTranscriptFallback(nil, parseCursorFile(50, records))
		Expect(err).NotTo(HaveOccurred())
		spans := derive.EmitSpans(set)
		Expect(spans.Turns).To(HaveLen(1))
		Expect(spans.Turns[0].ResponsePreview).To(Equal("aggregate answer"))

		again, _, err := deriveWithTranscriptFallback(nil, parseCursorFile(51, records))
		Expect(err).NotTo(HaveOccurred())
		Expect(traceAndSpanIDs(derive.EmitSpans(again))).To(
			Equal(traceAndSpanIDs(spans)),
			"re-deriving identical content must mint identical ids",
		)
	})

	It("degrades tool and content edge cases without losing a delivered result", func() {
		records := []map[string]any{
			{"type": "system", "subtype": "init", "model": "cursor-model", "session_id": cursorSessionID},
			{
				"type": "user", "session_id": cursorSessionID,
				"message": map[string]any{"role": "user", "content": "plain string prompt"},
			},
			{
				"type": "assistant", "session_id": cursorSessionID,
				"message": map[string]any{"role": "assistant", "content": []map[string]any{{"type": "text", "text": "starting"}}},
			},
			{
				"type": "tool_call", "subtype": "started", "call_id": "tool_1", "session_id": cursorSessionID,
				"tool_call": map[string]any{"shellToolCall": map[string]any{"args": map[string]any{"command": "ls"}}},
			},
			{
				"type": "user", "session_id": cursorSessionID,
				"message": map[string]any{"role": "user", "content": []map[string]any{{"type": "text", "text": "hurry up"}}},
			},
			{
				"type": "tool_call", "subtype": "completed", "call_id": "tool_1", "session_id": cursorSessionID,
				"tool_call": map[string]any{"shellToolCall": map[string]any{
					"args": map[string]any{"command": "ls"}, "result": map[string]any{"error": "command rejected"},
				}},
			},
			{
				"type": "tool_call", "subtype": "completed", "call_id": "tool_orphan", "session_id": cursorSessionID,
				"tool_call": map[string]any{"shellToolCall": map[string]any{"args": map[string]any{}, "result": "late"}},
			},
			{
				"type": "assistant", "session_id": cursorSessionID,
				"message": map[string]any{"role": "assistant", "content": []map[string]any{{"type": "text", "text": "done"}}},
			},
			{"type": "result", "subtype": "success", "is_error": false, "result": "startingdone", "session_id": cursorSessionID},
		}

		set, stats, err := deriveWithTranscriptFallback(nil, parseCursorFile(52, records))
		Expect(err).NotTo(HaveOccurred())
		spans := derive.EmitSpans(set)

		Expect(stats.OmittedTypes).To(HaveKeyWithValue("cursor:orphan-tool-completed", 1))
		Expect(spans.Turns[0].UserPrompt).To(Equal("plain string prompt"))
		var tool *derive.Span
		for _, turn := range spans.Turns {
			for _, span := range turn.Spans {
				if span.Kind == derive.SpanKindTool {
					tool = span
				}
			}
		}
		Expect(tool).NotTo(BeNil())
		Expect(tool.Name).To(Equal("shellToolCall"))
		Expect(tool.Output).To(ConsistOf(And(
			HaveField("IsError", true),
			HaveField("ToolOutput", ContainSubstring("command rejected")),
		)), "a result delivered after an interjection flush must still reach its tool span")
	})

	It("decodes documented function-shape tools, refuses duplicate completions, and counts trailing results", func() {
		records := []map[string]any{
			{"type": "system", "subtype": "init", "model": "cursor-model", "session_id": cursorSessionID},
			{
				"type": "user", "session_id": cursorSessionID,
				"message": map[string]any{"role": "user", "content": []map[string]any{{"type": "text", "text": "Browse"}}},
			},
			{
				"type": "assistant", "session_id": cursorSessionID,
				"message": map[string]any{"role": "assistant", "content": []map[string]any{{"type": "text", "text": "Opening"}}},
			},
			{
				"type": "tool_call", "subtype": "started", "call_id": "fn_1", "session_id": cursorSessionID,
				"tool_call": map[string]any{"function": map[string]any{"name": "browser", "arguments": `{"url":"https://example.com"}`}},
			},
			{
				"type": "tool_call", "subtype": "completed", "call_id": "fn_1", "session_id": cursorSessionID,
				"tool_call": map[string]any{"function": map[string]any{"name": "browser", "arguments": `{"url":"https://example.com"}`, "result": "page loaded"}},
			},
			{
				"type": "tool_call", "subtype": "completed", "call_id": "fn_1", "session_id": cursorSessionID,
				"tool_call": map[string]any{"function": map[string]any{"name": "browser", "arguments": "{}", "result": "again"}},
			},
			{"type": "result", "subtype": "success", "is_error": false, "result": "Opening", "session_id": cursorSessionID},
		}

		set, stats, err := deriveWithTranscriptFallback(nil, parseCursorFile(53, records))
		Expect(err).NotTo(HaveOccurred())
		spans := derive.EmitSpans(set)

		Expect(stats.OmittedTypes).To(And(
			HaveKeyWithValue("cursor:duplicate-tool-completed", 1),
			HaveKeyWithValue("cursor:trailing-tool-result", 1),
		))
		var tool *derive.Span
		for _, turn := range spans.Turns {
			for _, span := range turn.Spans {
				if span.Kind == derive.SpanKindTool {
					Expect(tool).To(BeNil(), "a duplicate completion must not mint a second tool span")
					tool = span
				}
			}
		}
		Expect(tool).NotTo(BeNil())
		Expect(tool.Name).To(Equal("browser"), "function-shape tools carry their real name")
		Expect(tool.Input).To(ConsistOf(HaveField("ToolInput", HaveKeyWithValue("url", "https://example.com"))))
	})

	It("keeps a tool call that arrives before any assistant text", func() {
		records := []map[string]any{
			{"type": "system", "subtype": "init", "model": "cursor-model", "session_id": cursorSessionID},
			{
				"type": "user", "session_id": cursorSessionID,
				"message": map[string]any{"role": "user", "content": []map[string]any{{"type": "text", "text": "Read README.md and reply with the secret word only."}}},
			},
			{"type": "thinking", "subtype": "delta", "text": "Reading", "session_id": cursorSessionID, "timestamp_ms": 1787867456285},
			{
				"type": "tool_call", "subtype": "started", "call_id": "call-1", "session_id": cursorSessionID,
				"tool_call": map[string]any{"readToolCall": map[string]any{"args": map[string]any{"path": "README.md"}}, "hookAdditionalContexts": []any{}, "toolCallId": "call-1"},
			},
			{
				"type": "tool_call", "subtype": "completed", "call_id": "call-1", "session_id": cursorSessionID,
				"tool_call": map[string]any{"readToolCall": map[string]any{
					"args":   map[string]any{"path": "README.md"},
					"result": map[string]any{"success": map[string]any{"content": "The secret word is pelican.", "isEmpty": false}},
				}, "hookAdditionalContexts": []any{}, "toolCallId": "call-1"},
			},
			{
				"type": "assistant", "session_id": cursorSessionID,
				"message": map[string]any{"role": "assistant", "content": []map[string]any{{"type": "text", "text": "pelican"}}},
			},
			{"type": "result", "subtype": "success", "is_error": false, "result": "pelican", "session_id": cursorSessionID},
		}

		set, stats, err := deriveWithTranscriptFallback(nil, parseCursorFile(55, records))
		Expect(err).NotTo(HaveOccurred())
		spans := derive.EmitSpans(set)

		Expect(stats.OmittedTypes).NotTo(HaveKey("cursor:orphan-tool-started"))
		var llmCount int
		var tool *derive.Span
		for _, turn := range spans.Turns {
			for _, span := range turn.Spans {
				switch span.Kind {
				case derive.SpanKindLLM:
					llmCount++
				case derive.SpanKindTool:
					tool = span
				}
			}
		}
		Expect(llmCount).To(Equal(2), "the tool-only call and the final answer are two calls")
		Expect(tool).NotTo(BeNil(), "a tool call before any assistant text must still become a span")
		Expect(tool.Name).To(Equal("readToolCall"))
		Expect(tool.Input).To(ConsistOf(HaveField("ToolInput", HaveKeyWithValue("path", "README.md"))))
		Expect(tool.Output).To(ConsistOf(HaveField("ToolOutput", ContainSubstring("pelican"))))
		Expect(spans.Turns[len(spans.Turns)-1].ResponsePreview).To(Equal("pelican"))
	})

	It("counts an error result as an omission instead of an answer", func() {
		records := []map[string]any{
			{"type": "system", "subtype": "init", "model": "cursor-model", "session_id": cursorSessionID},
			{
				"type": "user", "session_id": cursorSessionID,
				"message": map[string]any{"role": "user", "content": []map[string]any{{"type": "text", "text": "Do the thing"}}},
			},
			{"type": "system", "subtype": "init", "model": "other-model", "session_id": cursorSessionID},
			{"type": "result", "subtype": "error", "is_error": true, "result": "Workspace Trust Required", "session_id": cursorSessionID},
		}

		set, stats, err := deriveWithTranscriptFallback(nil, parseCursorFile(54, records))
		Expect(err).NotTo(HaveOccurred())
		Expect(stats.OmittedTypes).To(And(
			HaveKeyWithValue("cursor:result:error", 1),
			HaveKeyWithValue("cursor:duplicate-init", 1),
		))
		Expect(derive.EmitSpans(set).Turns).To(BeEmpty(), "an error text must not become the assistant's answer")
	})

	It("projects a live ask-mode stream, counting its thinking events as visible omissions", func() {
		records := []map[string]any{
			{
				"type": "system", "subtype": "init", "apiKeySource": "login",
				"cwd": "/Users/user/project", "session_id": cursorSessionID,
				"model": "cursor-model-fast", "permissionMode": "default",
			},
			{
				"type": "user", "session_id": cursorSessionID,
				"message": map[string]any{"role": "user", "content": []map[string]any{{"type": "text", "text": "Reply with exactly the single word: pong"}}},
			},
			{"type": "thinking", "subtype": "delta", "text": "The user requested a", "session_id": cursorSessionID, "timestamp_ms": 1787867456285},
			{"type": "thinking", "subtype": "delta", "text": " single word.", "session_id": cursorSessionID, "timestamp_ms": 1787867456391},
			{"type": "thinking", "subtype": "completed", "session_id": cursorSessionID, "timestamp_ms": 1787867456492},
			{
				"type": "assistant", "session_id": cursorSessionID,
				"message": map[string]any{"role": "assistant", "content": []map[string]any{{"type": "text", "text": "pong"}}},
			},
			{
				"type": "result", "subtype": "success", "duration_ms": 6700, "duration_api_ms": 6700,
				"is_error": false, "result": "pong", "session_id": cursorSessionID,
				"request_id": "request-2",
				"usage":      map[string]any{"inputTokens": 23234, "outputTokens": 38},
			},
		}

		set, stats, err := deriveWithTranscriptFallback(nil, parseCursorFile(43, records))
		Expect(err).NotTo(HaveOccurred())
		spans := derive.EmitSpans(set)

		Expect(stats.Records).To(Equal(7))
		Expect(stats.ProjectedRecords).To(Equal(4))
		Expect(stats.OmittedTypes).To(HaveKeyWithValue("cursor:thinking", 3))
		Expect(spans.Turns).To(HaveLen(1))
		Expect(spans.Turns[0].UserPrompt).To(Equal("Reply with exactly the single word: pong"))
		Expect(spans.Turns[0].ResponsePreview).To(Equal("pong"))
		Expect(spans.Turns[0].Spans).To(ContainElement(And(
			HaveField("Kind", derive.SpanKindLLM),
			HaveField("Model", "cursor-model-fast"),
		)))
	})
})
