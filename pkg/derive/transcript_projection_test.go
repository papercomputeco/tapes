package derive_test

import (
	"encoding/json"
	"sort"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/derive"
	"github.com/papercomputeco/tapes/pkg/llm"
	"github.com/papercomputeco/tapes/pkg/storage"
)

var _ = Describe("transcript fallback projection", func() {
	parseFileFor := func(id int64, harnessID, sessionID, agentID, toolUseID string, records any) *derive.TranscriptFile {
		raw, err := json.Marshal(records)
		Expect(err).NotTo(HaveOccurred())
		meta, err := json.Marshal(map[string]any{
			"transcript": true, "agent_id": agentID, "agent_type": "researcher",
			"description": "investigate", "tool_use_id": toolUseID,
		})
		Expect(err).NotTo(HaveOccurred())
		file, err := derive.ParseTranscriptFile(&storage.RawTurnRecord{
			ID: id, Source: storage.RawTurnSourceTranscript, HarnessID: harnessID,
			HarnessSessionID: sessionID, RawRequest: raw, Meta: meta,
			ReceivedAt: time.Date(2026, 8, 24, 12, 0, 0, 0, time.UTC),
		})
		Expect(err).NotTo(HaveOccurred())
		return file
	}
	parseFile := func(id int64, agentID, toolUseID string, records any) *derive.TranscriptFile {
		return parseFileFor(id, "claude", "session-1", agentID, toolUseID, records)
	}

	It("projects a main transcript with coalesced assistants, tools, accounting, title, and stable identities", func() {
		records := []map[string]any{
			{"type": "ai-title", "aiTitle": "Transcript fallback"},
			{"type": "user", "uuid": "user-1", "timestamp": "2026-08-24T10:00:00Z", "message": map[string]any{"role": "user", "content": "inspect the tree"}},
			{"type": "system", "uuid": "context-1", "parentUuid": "user-1", "timestamp": "2026-08-24T10:00:00.500Z", "subtype": "hook", "content": "project context"},
			{"type": "assistant", "uuid": "frag-1", "parentUuid": "context-1", "timestamp": "2026-08-24T10:00:01Z", "message": map[string]any{"id": "msg-1", "role": "assistant", "model": "claude-sonnet-4-6", "content": []map[string]any{{"type": "text", "text": "I'll inspect it."}}, "stop_reason": "tool_use", "usage": map[string]any{"input_tokens": 12, "output_tokens": 7, "cache_read_input_tokens": 3}}},
			{"type": "assistant", "uuid": "frag-2", "parentUuid": "frag-1", "timestamp": "2026-08-24T10:00:02Z", "message": map[string]any{"id": "msg-1", "role": "assistant", "model": "claude-sonnet-4-6", "content": []map[string]any{{"type": "tool_use", "id": "tool-1", "name": "Bash", "input": map[string]any{"command": "find ."}}}, "stop_reason": "tool_use", "usage": map[string]any{"input_tokens": 12, "output_tokens": 7, "cache_read_input_tokens": 3}}},
			{"type": "user", "uuid": "result-1", "parentUuid": "frag-2", "timestamp": "2026-08-24T10:00:03Z", "message": map[string]any{"role": "user", "content": []map[string]any{{"type": "tool_result", "tool_use_id": "tool-1", "content": "a.go\nb.go", "is_error": true}}}},
			{"type": "assistant", "uuid": "answer-1", "parentUuid": "result-1", "timestamp": "2026-08-24T10:00:04Z", "message": map[string]any{"id": "msg-2", "role": "assistant", "model": "claude-sonnet-4-6", "content": []map[string]any{{"type": "text", "text": "There are two files."}}, "stop_reason": "end_turn", "usage": map[string]any{"input_tokens": 20, "output_tokens": 5}}},
			{"type": "queue-operation", "operation": "enqueue"},
		}

		file := parseFile(41, "", "", records)
		set, stats, err := deriveWithTranscriptFallback(nil, file)
		Expect(err).NotTo(HaveOccurred())
		spans := derive.EmitSpans(set)

		Expect(stats.OmittedTypes).To(HaveKeyWithValue("queue-operation", 1))
		Expect(set.SessionTitles).To(HaveKeyWithValue(derive.SessionKey{HarnessID: "claude", HarnessSessionID: "session-1"}, "Transcript fallback"))
		Expect(spans.Turns).To(HaveLen(1))
		turn := spans.Turns[0]
		Expect(turn.Source).To(Equal(storage.RawTurnSourceTranscript))
		Expect(turn.UserPrompt).To(Equal("inspect the tree"))
		Expect(turn.ResponsePreview).To(Equal("There are two files."))
		Expect(turn.TotalInputTokens).To(Equal(int64(32)), "repeated usage on assistant fragments must count once")
		Expect(turn.TotalOutputTokens).To(Equal(int64(12)))
		Expect(turn.TraceID).To(HavePrefix("trc_txturn_"))
		Expect(turn.StartedAt).To(Equal(time.Date(2026, 8, 24, 10, 0, 0, 0, time.UTC)))
		Expect(turn.EndedAt).To(Equal(time.Date(2026, 8, 24, 10, 0, 4, 0, time.UTC)))

		var llms, tools, events int
		for _, span := range turn.Spans {
			switch span.Kind {
			case derive.SpanKindLLM:
				llms++
				Expect(span.RawTurnID).To(Equal(int64(41)))
				Expect(span.Model).To(Equal("claude-sonnet-4-6"))
				Expect(span.StopReason).NotTo(BeEmpty())
				if span.Output[0].Type == "text" && span.Output[0].Text == "There are two files." {
					Expect(span.StartedAt).To(Equal(time.Date(2026, 8, 24, 10, 0, 3, 0, time.UTC)))
					Expect(span.DurationNS).To(Equal(int64(time.Second)))
				} else {
					Expect(span.StartedAt).To(Equal(turn.StartedAt))
					Expect(span.DurationNS).To(Equal(int64(2 * time.Second)))
				}
			case derive.SpanKindTool:
				tools++
				Expect(span.RawTurnID).To(BeZero())
				Expect(span.Status).To(Equal("error"))
				Expect(span.Output).To(HaveLen(1))
				Expect(span.Output[0].ToolOutput).To(Equal("a.go\nb.go"))
			case derive.SpanKindEvent:
				events++
				Expect(span.Input).To(ContainElement(HaveField("Text", "project context")))
			}
		}
		Expect(llms).To(Equal(2))
		Expect(tools).To(Equal(1))
		Expect(events).To(Equal(1))

		// A later raw row for the grown file must not enter projected IDs.
		grown := parseFile(99, "", "", records)
		again, _, err := deriveWithTranscriptFallback(nil, grown)
		Expect(err).NotTo(HaveOccurred())
		againSpans := derive.EmitSpans(again)
		Expect(traceAndSpanIDs(againSpans)).To(Equal(traceAndSpanIDs(spans)))
	})

	It("starts the trace root, root agent, and LLM at context before the prompt", func() {
		callAt := time.Date(2026, 8, 24, 10, 0, 0, 0, time.UTC)
		records := []map[string]any{
			{"type": "system", "uuid": "context", "timestamp": callAt.Format(time.RFC3339Nano), "content": "early context"},
			{"type": "user", "uuid": "prompt", "parentUuid": "context", "timestamp": "2026-08-24T10:00:01Z", "message": map[string]any{"role": "user", "content": "later prompt"}},
			{"type": "assistant", "uuid": "answer", "parentUuid": "prompt", "timestamp": "2026-08-24T10:00:02Z", "message": map[string]any{"id": "message", "role": "assistant", "content": "answer"}},
		}

		set, _, err := deriveWithTranscriptFallback(nil, parseFile(1, "", "", records))
		Expect(err).NotTo(HaveOccurred())
		spans := derive.EmitSpans(set)
		Expect(spans.Turns).To(HaveLen(1))
		turn := spans.Turns[0]
		Expect(turn.UserPrompt).To(Equal("later prompt"))
		Expect(turn.StartedAt).To(Equal(callAt))
		Expect(turn.Spans[0]).To(And(
			HaveField("Kind", derive.SpanKindAgent),
			HaveField("StartedAt", callAt),
		))
		Expect(turn.Spans).To(ContainElement(And(
			HaveField("Kind", derive.SpanKindLLM),
			HaveField("CallKind", derive.KindMain),
			HaveField("StartedAt", callAt),
		)))
		Expect(turn.Spans).To(ContainElement(And(
			HaveField("Kind", derive.SpanKindEvent),
			HaveField("StartedAt", callAt),
		)))
		Expect(turn.EndedAt).To(Equal(time.Date(2026, 8, 24, 10, 0, 2, 0, time.UTC)))
	})

	It("does not mint a call for trailing context and ignores semantically empty context", func() {
		records := []map[string]any{
			{"type": "user", "uuid": "user", "timestamp": "2026-08-24T10:00:00Z", "message": map[string]any{"role": "user", "content": "hello"}},
			{"type": "system", "uuid": "empty-array", "parentUuid": "user", "content": []any{}},
			{"type": "attachment", "uuid": "empty-object", "parentUuid": "empty-array", "attachment": map[string]any{}},
			{"type": "system", "uuid": "semantically-empty", "parentUuid": "empty-object", "hookAdditionalContext": []any{map[string]any{}}},
			{"type": "system", "uuid": "real-context", "parentUuid": "semantically-empty", "content": "context for the real call"},
			{"type": "assistant", "uuid": "answer", "parentUuid": "real-context", "timestamp": "2026-08-24T10:00:01Z", "message": map[string]any{"id": "message-1", "role": "assistant", "content": "hello back", "stop_reason": "end_turn"}},
			{"type": "system", "uuid": "trailing-context", "parentUuid": "answer", "timestamp": "2026-08-24T10:00:02Z", "content": "context for a call that has not happened"},
		}

		set, _, err := deriveWithTranscriptFallback(nil, parseFile(1, "", "", records))
		Expect(err).NotTo(HaveOccurred())
		spans := derive.EmitSpans(set)

		Expect(spans.Turns).To(HaveLen(1))
		Expect(spans.Turns[0].ResponsePreview).To(Equal("hello back"))
		Expect(spans.Turns[0].Spans).To(ConsistOf(
			HaveField("Kind", derive.SpanKindAgent),
			HaveField("Kind", derive.SpanKindLLM),
			And(
				HaveField("Kind", derive.SpanKindEvent),
				HaveField("Input", ConsistOf(HaveField("Text", "context for the real call"))),
			),
		))
	})

	It("nests subagent work under its spawning tool and emits a rejoin", func() {
		main := parseFile(1, "", "", []map[string]any{
			{"type": "user", "uuid": "main-user", "timestamp": "2026-08-24T10:00:00Z", "message": map[string]any{"role": "user", "content": "delegate"}},
			{"type": "assistant", "uuid": "main-assistant", "parentUuid": "main-user", "timestamp": "2026-08-24T10:00:01Z", "message": map[string]any{"id": "main-msg", "role": "assistant", "model": "claude", "content": []map[string]any{{"type": "tool_use", "id": "task-1", "name": "Task", "input": map[string]any{"description": "investigate"}}}, "stop_reason": "tool_use"}},
		})
		child := parseFile(2, "agent-a", "task-1", []map[string]any{
			{"type": "user", "uuid": "child-user", "timestamp": "2026-08-24T10:00:02Z", "message": map[string]any{"role": "user", "content": "do research"}},
			{"type": "assistant", "uuid": "child-assistant", "parentUuid": "child-user", "timestamp": "2026-08-24T10:00:03Z", "message": map[string]any{"id": "child-msg", "role": "assistant", "model": "claude-haiku", "content": []map[string]any{{"type": "text", "text": "done"}}, "stop_reason": "end_turn"}},
		})
		set, _, err := deriveWithTranscriptFallback(nil, main, child)
		Expect(err).NotTo(HaveOccurred())
		spans := derive.EmitSpans(set)
		Expect(spans.Turns).To(HaveLen(1))

		var agent, task *derive.Span
		for _, span := range spans.Turns[0].Spans {
			if span.Kind == derive.SpanKindAgent && span.ThreadID == "agent-a" {
				agent = span
			}
			if span.Kind == derive.SpanKindTool && span.Name == "Task" {
				task = span
			}
		}
		Expect(agent).NotTo(BeNil())
		Expect(task).NotTo(BeNil())
		Expect(agent.ParentSpanID).To(Equal(task.SpanID))
		Expect(spans.Turns[0].Links).To(ContainElement(HaveField("Kind", derive.LinkRejoin)))
	})

	It("namespaces reused tool ids by transcript thread and preserves the parent spawn anchor", func() {
		main := parseFile(1, "", "", []map[string]any{
			{"type": "user", "uuid": "main-user", "timestamp": "2026-08-24T10:00:00Z", "message": map[string]any{"role": "user", "content": "delegate"}},
			{"type": "assistant", "uuid": "main-assistant", "parentUuid": "main-user", "timestamp": "2026-08-24T10:00:01Z", "message": map[string]any{"id": "main-message", "role": "assistant", "content": []map[string]any{{"type": "tool_use", "id": "reused-tool-id", "name": "Task", "input": map[string]any{"description": "investigate"}}}, "stop_reason": "tool_use"}},
		})
		child := parseFile(2, "agent-a", "reused-tool-id", []map[string]any{
			{"type": "user", "uuid": "child-user", "timestamp": "2026-08-24T10:00:02Z", "message": map[string]any{"role": "user", "content": "inspect"}},
			{"type": "assistant", "uuid": "child-tool", "parentUuid": "child-user", "timestamp": "2026-08-24T10:00:03Z", "message": map[string]any{"id": "child-message-1", "role": "assistant", "content": []map[string]any{{"type": "tool_use", "id": "reused-tool-id", "name": "Bash", "input": map[string]any{"command": "pwd"}}}, "stop_reason": "tool_use"}},
			{"type": "user", "uuid": "child-result", "parentUuid": "child-tool", "timestamp": "2026-08-24T10:00:04Z", "message": map[string]any{"role": "user", "content": []map[string]any{{"type": "tool_result", "tool_use_id": "reused-tool-id", "content": "/work"}}}},
			{"type": "assistant", "uuid": "child-answer", "parentUuid": "child-result", "timestamp": "2026-08-24T10:00:05Z", "message": map[string]any{"id": "child-message-2", "role": "assistant", "content": "done", "stop_reason": "end_turn"}},
		})

		set, _, err := deriveWithTranscriptFallback(nil, main, child)
		Expect(err).NotTo(HaveOccurred())
		spans := derive.EmitSpans(set)
		Expect(spans.Turns).To(HaveLen(1))

		var task, bash, agent *derive.Span
		for _, span := range spans.Turns[0].Spans {
			switch {
			case span.Kind == derive.SpanKindTool && span.Name == "Task":
				task = span
			case span.Kind == derive.SpanKindTool && span.Name == "Bash":
				bash = span
			case span.Kind == derive.SpanKindAgent && span.ThreadID == "agent-a":
				agent = span
			}
		}
		Expect(task).NotTo(BeNil())
		Expect(bash).NotTo(BeNil())
		Expect(agent).NotTo(BeNil())
		Expect(task.SpanID).NotTo(Equal(bash.SpanID))
		Expect(task.Input).To(ConsistOf(HaveField("ToolUseID", task.SpanID)))
		Expect(task.Output).To(BeEmpty())
		Expect(bash.Input).To(ConsistOf(HaveField("ToolUseID", bash.SpanID)))
		Expect(bash.Output).To(ConsistOf(And(
			HaveField("ToolResultID", bash.SpanID),
			HaveField("ToolOutput", "/work"),
		)))
		Expect(agent.ParentSpanID).To(Equal(task.SpanID))
	})

	It("selects one causal branch from last-prompt evidence and excludes alternate usage", func() {
		records := []map[string]any{
			{"type": "user", "uuid": "root", "timestamp": "2026-08-24T10:00:00Z", "message": map[string]any{"role": "user", "content": "choose one"}},
			{"type": "assistant", "uuid": "chosen-tool", "parentUuid": "root", "timestamp": "2026-08-24T10:00:01Z", "message": map[string]any{"id": "chosen-1", "role": "assistant", "content": []map[string]any{{"type": "tool_use", "id": "chosen-tool-id", "name": "Bash", "input": map[string]any{"command": "true"}}}, "usage": map[string]any{"input_tokens": 10, "output_tokens": 2}}},
			{"type": "user", "uuid": "chosen-result", "parentUuid": "chosen-tool", "timestamp": "2026-08-24T10:00:02Z", "message": map[string]any{"role": "user", "content": []map[string]any{{"type": "tool_result", "tool_use_id": "chosen-tool-id", "content": "ok"}}}},
			{"type": "assistant", "uuid": "chosen-answer", "parentUuid": "chosen-result", "timestamp": "2026-08-24T10:00:03Z", "message": map[string]any{"id": "chosen-2", "role": "assistant", "content": "selected", "usage": map[string]any{"input_tokens": 20, "output_tokens": 3}}},
			{"type": "assistant", "uuid": "alternate", "parentUuid": "root", "timestamp": "2026-08-24T10:00:04Z", "isSidechain": true, "message": map[string]any{"id": "alternate-message", "role": "assistant", "content": "must not appear", "usage": map[string]any{"input_tokens": 999, "output_tokens": 999}}},
			{"type": "last-prompt", "leafUuid": "chosen-answer", "lastPrompt": "choose one"},
		}
		set, stats, err := deriveWithTranscriptFallback(nil, parseFile(1, "", "", records))
		Expect(err).NotTo(HaveOccurred())
		spans := derive.EmitSpans(set)

		Expect(stats.Records).To(Equal(6))
		Expect(stats.ProjectedRecords).To(Equal(5), "last-prompt is processed structural evidence")
		Expect(stats.OmittedRecords).To(Equal(1))
		Expect(stats.OmittedTypes).To(Equal(map[string]int{"inactive-branch": 1}))
		Expect(spans.Turns).To(HaveLen(1))
		Expect(spans.Turns[0].ResponsePreview).To(Equal("selected"))
		Expect(spans.Turns[0].TotalInputTokens).To(Equal(int64(30)))
		Expect(spans.Turns[0].TotalOutputTokens).To(Equal(int64(5)))
		Expect(spans.Turns[0].ToolCalls).To(Equal(1))
		for _, span := range spans.Turns[0].Spans {
			Expect(span.Output).NotTo(ContainElement(HaveField("Text", "must not appear")))
		}
	})

	It("uses sidechain class and latest record as the deterministic no-evidence fallback", func() {
		records := []map[string]any{
			{"type": "user", "uuid": "root", "message": map[string]any{"role": "user", "content": "root"}},
			{"type": "assistant", "uuid": "main-leaf", "parentUuid": "root", "message": map[string]any{"id": "main", "role": "assistant", "content": "main branch"}},
			{"type": "assistant", "uuid": "later-sidechain", "parentUuid": "root", "isSidechain": true, "message": map[string]any{"id": "side", "role": "assistant", "content": "side branch"}},
		}
		set, _, err := deriveWithTranscriptFallback(nil, parseFile(1, "", "", records))
		Expect(err).NotTo(HaveOccurred())
		spans := derive.EmitSpans(set)
		Expect(spans.Turns).To(HaveLen(1))
		Expect(spans.Turns[0].ResponsePreview).To(Equal("main branch"))
	})

	It("omits a malformed known record without projecting its partial fields", func() {
		records := []map[string]any{
			{"type": "user", "uuid": "root", "message": map[string]any{"role": "user", "content": "valid"}},
			{"type": "assistant", "uuid": "answer", "parentUuid": "root", "message": map[string]any{"id": "valid-answer", "role": "assistant", "content": "valid answer"}},
			// usage has the wrong shape. encoding/json fills type/message before
			// returning the error; projection must retain none of those fields.
			{"type": "assistant", "uuid": "malformed", "parentUuid": "answer", "message": map[string]any{"id": "must-not-project", "role": "assistant", "content": "partial", "usage": "wrong"}},
		}
		set, stats, err := deriveWithTranscriptFallback(nil, parseFile(1, "", "", records))
		Expect(err).NotTo(HaveOccurred())
		spans := derive.EmitSpans(set)
		Expect(stats.OmittedTypes).To(HaveKeyWithValue("malformed:assistant", 1))
		Expect(spans.Turns).To(HaveLen(1))
		Expect(spans.Turns[0].ResponsePreview).To(Equal("valid answer"))
		Expect(spans.Turns[0].Spans).To(ConsistOf(
			HaveField("Kind", derive.SpanKindAgent),
			HaveField("Kind", derive.SpanKindLLM),
		))
	})

	It("namespaces transcript trace and span identities by session and tagged agent", func() {
		records := []map[string]any{
			{"type": "user", "uuid": "same-user", "message": map[string]any{"role": "user", "content": "same"}},
			{"type": "assistant", "uuid": "same-answer", "parentUuid": "same-user", "message": map[string]any{"id": "same-message", "role": "assistant", "content": "same"}},
		}
		project := func(files ...*derive.TranscriptFile) *derive.SpanSet {
			set, _, err := deriveWithTranscriptFallback(nil, files...)
			Expect(err).NotTo(HaveOccurred())
			return derive.EmitSpans(set)
		}

		one := project(parseFileFor(1, "claude", "session-one", "", "", records))
		two := project(parseFileFor(2, "claude", "session-two", "", "", records))
		otherHarness := project(parseFileFor(3, "codex", "session-one", "", "", records))
		Expect(sharedIDs(one, two)).To(BeEmpty())
		Expect(sharedIDs(one, otherHarness)).To(BeEmpty())

		main := parseFileFor(4, "claude", "session-agents", "", "", records)
		agentNamedMain := parseFileFor(5, "claude", "session-agents", "main", "", records)
		agentOther := parseFileFor(6, "claude", "session-agents", "other", "", records)
		combined := project(main, agentNamedMain, agentOther)
		llmByThread := map[string]string{}
		for _, turn := range combined.Turns {
			for _, span := range turn.Spans {
				if span.Kind == derive.SpanKindLLM {
					llmByThread[span.ThreadID] = span.SpanID
				}
			}
		}
		Expect(llmByThread).To(HaveLen(3))
		Expect(llmByThread[""]).NotTo(Equal(llmByThread["main"]))
		Expect(llmByThread["main"]).NotTo(Equal(llmByThread["other"]))
	})

	It("matches the observable shape of an equivalent linear wire conversation", func() {
		wire := []storage.RawTurnRecord{
			{
				ID: 11, Source: storage.RawTurnSourceWire, Provider: "anthropic", AgentName: "claude",
				HarnessID: "claude", HarnessSessionID: "session-1", RequestID: "wire-call-1",
				RawRequest: json.RawMessage(`{"model":"claude-sonnet-4-6","stream":true,"max_tokens":4096,"tools":[{"name":"Bash"}],"messages":[{"role":"user","content":"inspect"}]}`),
				Response:   json.RawMessage(`{"model":"claude-sonnet-4-6","message":{"role":"assistant","content":[{"type":"tool_use","tool_use_id":"tool-1","tool_name":"Bash","tool_input":{"command":"pwd"}}]},"stop_reason":"tool_use","usage":{"prompt_tokens":4,"completion_tokens":2}}`),
			},
			{
				ID: 12, Source: storage.RawTurnSourceWire, Provider: "anthropic", AgentName: "claude",
				HarnessID: "claude", HarnessSessionID: "session-1", RequestID: "wire-call-2",
				RawRequest: json.RawMessage(`{"model":"claude-sonnet-4-6","stream":true,"max_tokens":4096,"tools":[{"name":"Bash"}],"messages":[{"role":"user","content":"inspect"},{"role":"assistant","content":[{"type":"tool_use","id":"tool-1","name":"Bash","input":{"command":"pwd"}}]},{"role":"user","content":[{"type":"tool_result","tool_use_id":"tool-1","content":"/work"}]}]}`),
				Response:   json.RawMessage(`{"model":"claude-sonnet-4-6","message":{"role":"assistant","content":[{"type":"text","text":"done"}]},"stop_reason":"end_turn","usage":{"prompt_tokens":8,"completion_tokens":1}}`),
			},
		}
		transcript := parseFile(21, "", "", []map[string]any{
			{"type": "user", "uuid": "user-1", "timestamp": "2026-08-24T10:00:00Z", "message": map[string]any{"role": "user", "content": "inspect"}},
			{"type": "assistant", "uuid": "assistant-1", "parentUuid": "user-1", "timestamp": "2026-08-24T10:00:01Z", "message": map[string]any{"id": "message-1", "role": "assistant", "model": "claude-sonnet-4-6", "content": []map[string]any{{"type": "tool_use", "id": "tool-1", "name": "Bash", "input": map[string]any{"command": "pwd"}}}, "stop_reason": "tool_use", "usage": map[string]any{"input_tokens": 4, "output_tokens": 2}}},
			{"type": "user", "uuid": "result-1", "parentUuid": "assistant-1", "timestamp": "2026-08-24T10:00:02Z", "message": map[string]any{"role": "user", "content": []map[string]any{{"type": "tool_result", "tool_use_id": "tool-1", "content": "/work"}}}},
			{"type": "assistant", "uuid": "assistant-2", "parentUuid": "result-1", "timestamp": "2026-08-24T10:00:03Z", "message": map[string]any{"id": "message-2", "role": "assistant", "model": "claude-sonnet-4-6", "content": "done", "stop_reason": "end_turn", "usage": map[string]any{"input_tokens": 8, "output_tokens": 1}}},
		})

		wireSet, _, err := deriveWithTranscriptFallback(wire)
		Expect(err).NotTo(HaveOccurred())
		transcriptSet, _, err := deriveWithTranscriptFallback(nil, transcript)
		Expect(err).NotTo(HaveOccurred())
		Expect(transcriptSet.Nodes).To(HaveLen(len(wireSet.Nodes)))
		Expect(transcriptSet.Report.CallKinds).To(Equal(wireSet.Report.CallKinds))
		Expect(conversationShape(derive.EmitSpans(transcriptSet))).To(Equal(conversationShape(derive.EmitSpans(wireSet))))

		Expect(wireSet.SpanSources).To(HaveLen(2))
		Expect(transcriptSet.SpanSources).To(HaveLen(2))
		Expect(wireSet.SpanSources[0].New).To(Equal([]bool{true, true}))
		Expect(wireSet.SpanSources[1].New).To(Equal([]bool{false, false, true, true}))
		Expect(transcriptSet.SpanSources[0].New).To(Equal(wireSet.SpanSources[0].New))
		Expect(transcriptSet.SpanSources[1].New).To(Equal(wireSet.SpanSources[1].New))
		Expect(transcriptSet.SpanSources[0].Chain[0].Node.Hash).To(Equal(wireSet.SpanSources[0].Chain[0].Node.Hash),
			"the provider/model-free root hash must match wire")
		for _, set := range []*derive.DerivedSet{wireSet, transcriptSet} {
			for _, source := range set.SpanSources {
				for i := 1; i < len(source.Chain); i++ {
					Expect(source.Chain[i].Node.ParentHash).NotTo(BeNil())
					Expect(*source.Chain[i].Node.ParentHash).To(Equal(source.Chain[i-1].Node.Hash))
				}
			}
		}
	})

	It("keeps transcript conversation content when the only wire call is shadow traffic", func() {
		shadow := storage.RawTurnRecord{
			ID: 31, Source: storage.RawTurnSourceWire, Provider: "anthropic", AgentName: "claude",
			HarnessID: "claude", HarnessSessionID: "session-1", RequestID: "title-call",
			RawRequest: json.RawMessage(`{"system":"Return JSON matching {\"title\":\"string\"}","max_tokens":32,"messages":[{"role":"user","content":"transcript prompt"}]}`),
			Response:   json.RawMessage(`{"model":"claude-haiku","message":{"role":"assistant","content":[{"type":"text","text":"{\"title\":\"Wire title\"}"}]},"stop_reason":"end_turn"}`),
		}
		file := parseFile(32, "", "", []map[string]any{
			{"type": "user", "uuid": "transcript-user", "message": map[string]any{"role": "user", "content": "transcript prompt"}},
			{"type": "assistant", "uuid": "transcript-answer", "parentUuid": "transcript-user", "message": map[string]any{"id": "transcript-message", "role": "assistant", "content": "transcript answer"}},
		})

		set, stats, err := deriveWithTranscriptFallback([]storage.RawTurnRecord{shadow}, file)
		Expect(err).NotTo(HaveOccurred())
		Expect(stats.SuppressedByWire).To(BeFalse())
		Expect(set.Report.CallKinds).To(And(
			HaveKeyWithValue(derive.KindMain, 1),
			HaveKeyWithValue(derive.KindTitleGen, 1),
		))
		spans := derive.EmitSpans(set)
		Expect(spans.Turns).To(HaveLen(1))
		Expect(spans.Turns[0].Source).To(Equal(storage.RawTurnSourceTranscript))
		Expect(spans.Turns[0].UserPrompt).To(Equal("transcript prompt"))
		Expect(spans.Turns[0].ResponsePreview).To(Equal("transcript answer"))
	})

	It("keeps transcript fallback beside compaction-only wire and represents the seam", func() {
		compaction := storage.RawTurnRecord{
			ID: 41, Source: storage.RawTurnSourceWire, Provider: "anthropic", AgentName: "claude",
			HarnessID: "claude", HarnessSessionID: "session-1", RequestID: "compact-call",
			RawRequest: json.RawMessage(`{"messages":[{"role":"user","content":"Create a summary of the conversation so far"}]}`),
			Response:   json.RawMessage(`{"model":"claude","message":{"role":"assistant","content":[{"type":"text","text":"Primary Request and Intent: continue\\nKey Technical Concepts: derive"}]},"stop_reason":"end_turn"}`),
			Meta:       json.RawMessage(`{"ts_request":"2026-08-24T10:00:02Z"}`),
		}
		file := parseFile(42, "", "", []map[string]any{
			{"type": "user", "uuid": "user-after-compact", "timestamp": "2026-08-24T10:00:03Z", "message": map[string]any{"role": "user", "content": "continue"}},
			{"type": "assistant", "uuid": "answer-after-compact", "parentUuid": "user-after-compact", "timestamp": "2026-08-24T10:00:04Z", "message": map[string]any{"id": "message-after-compact", "role": "assistant", "content": "continued"}},
		})

		set, stats, err := deriveWithTranscriptFallback([]storage.RawTurnRecord{compaction}, file)
		Expect(err).NotTo(HaveOccurred())
		Expect(stats.SuppressedByWire).To(BeFalse())
		Expect(set.Report.CallKinds).To(And(
			HaveKeyWithValue(derive.KindMain, 1),
			HaveKeyWithValue(derive.KindCompaction, 1),
		))
		spans := derive.EmitSpans(set)
		Expect(spans.Turns).To(HaveLen(2))
		Expect(spans.Turns[1].Source).To(Equal(storage.RawTurnSourceTranscript))
		Expect(spans.Turns[1].Synthetic).To(Equal("post-compaction"))
		Expect(spans.Links).To(ContainElement(HaveField("Kind", derive.LinkCompactionSeam)))
	})

	It("does not mix transcript content into a usable wire projection", func() {
		wire := storage.RawTurnRecord{
			ID: 7, Source: storage.RawTurnSourceWire, Provider: "anthropic", AgentName: "claude",
			HarnessID: "claude", HarnessSessionID: "session-1", RequestID: "wire-1",
			RawRequest: json.RawMessage(`{"model":"claude","stream":true,"max_tokens":4096,"tools":[{"name":"Bash"}],"messages":[{"role":"user","content":"wire prompt"}]}`),
			Response:   json.RawMessage(`{"model":"claude","message":{"role":"assistant","content":[{"type":"text","text":"wire answer"}]},"stop_reason":"end_turn"}`),
		}
		file := parseFile(8, "", "", []map[string]any{
			{"type": "user", "uuid": "transcript-user", "message": map[string]any{"role": "user", "content": "transcript prompt"}},
			{"type": "assistant", "uuid": "transcript-answer", "message": map[string]any{"id": "transcript-msg", "role": "assistant", "content": "transcript answer"}},
		})
		set, stats, err := deriveWithTranscriptFallback([]storage.RawTurnRecord{wire}, file)
		Expect(err).NotTo(HaveOccurred())
		Expect(stats.SuppressedByWire).To(BeTrue())
		spans := derive.EmitSpans(set)
		Expect(spans.Turns).To(HaveLen(1))
		Expect(spans.Turns[0].Source).To(Equal(storage.RawTurnSourceWire))
		Expect(spans.Turns[0].UserPrompt).To(Equal("wire prompt"))
	})

	It("interleaves shared hashes by time and gives wire title generation explicit priority", func() {
		wireAt := time.Date(2026, 8, 24, 9, 59, 0, 0, time.UTC)
		title := storage.RawTurnRecord{
			ID: 50, Source: storage.RawTurnSourceWire, Provider: "anthropic", AgentName: "claude",
			HarnessID: "claude", HarnessSessionID: "session-1", RequestID: "title-before-transcript",
			RawRequest: json.RawMessage(`{"system":"Return JSON matching {\"title\":\"string\"}","messages":[{"role":"user","content":"shared prompt"}]}`),
			Response:   json.RawMessage(`{"model":"claude-haiku","message":{"role":"assistant","content":[{"type":"text","text":"{\"title\":\"Wire title\"}"}]}}`),
			Meta:       json.RawMessage(`{"ts_request":"2026-08-24T09:59:00Z"}`),
		}
		file := parseFile(51, "", "", []map[string]any{
			{"type": "custom-title", "customTitle": "Transcript title"},
			{"type": "user", "uuid": "shared-user", "timestamp": "2026-08-24T10:00:00Z", "message": map[string]any{"role": "user", "content": "shared prompt"}},
			{"type": "assistant", "uuid": "shared-answer", "parentUuid": "shared-user", "timestamp": "2026-08-24T10:00:01Z", "message": map[string]any{"id": "shared-message", "role": "assistant", "content": "real answer"}},
		})

		set, _, err := deriveWithTranscriptFallback([]storage.RawTurnRecord{title}, file)
		Expect(err).NotTo(HaveOccurred())
		key := derive.SessionKey{HarnessID: "claude", HarnessSessionID: "session-1"}
		Expect(set.SessionTitles).To(HaveKeyWithValue(key, "Wire title"))
		Expect(set.SpanSources).To(HaveLen(2))
		Expect(set.SpanSources[0].Kind).To(Equal(derive.KindTitleGen))
		Expect(set.SpanSources[1].Source).To(Equal(storage.RawTurnSourceTranscript))
		wireRoot := set.SpanSources[0].Chain[0]
		transcriptRoot := set.SpanSources[1].Chain[0]
		Expect(transcriptRoot.Node.Hash).To(Equal(wireRoot.Node.Hash))
		Expect(transcriptRoot).To(BeIdenticalTo(wireRoot), "the chronologically first call owns shared-hash metadata")
		Expect(wireRoot.Node.Bucket.Provider).To(Equal("anthropic"))
		Expect(wireRoot.CapturedAt).To(Equal(wireAt))
		Expect(set.SpanSources[1].New[0]).To(BeFalse())
		turns := derive.EmitSpans(set).Turns
		Expect(turns).To(HaveLen(1))
		Expect(turns[0].UserPrompt).To(Equal("shared prompt"), "turn identity opens the trace without falsifying New")
		transcriptAt := time.Date(2026, 8, 24, 10, 0, 0, 0, time.UTC)
		Expect(turns[0].StartedAt).To(Equal(transcriptAt), "the earlier shadow's retained prompt timestamp must not replace the call start")
		Expect(turns[0].Spans[0]).To(And(
			HaveField("Kind", derive.SpanKindAgent),
			HaveField("StartedAt", transcriptAt),
		))
		Expect(turns[0].Spans).To(ContainElement(And(
			HaveField("Kind", derive.SpanKindLLM),
			HaveField("CallKind", derive.KindMain),
			HaveField("StartedAt", transcriptAt),
		)))
	})

	It("orders transcript tools before later plan and web shadow attachments", func() {
		file := parseFile(60, "", "", []map[string]any{
			{"type": "user", "uuid": "tool-user", "timestamp": "2026-08-24T10:00:00Z", "message": map[string]any{"role": "user", "content": "use plan and web"}},
			{"type": "assistant", "uuid": "tool-answer", "parentUuid": "tool-user", "timestamp": "2026-08-24T10:00:01Z", "message": map[string]any{
				"id": "tool-message", "role": "assistant", "content": []map[string]any{
					{"type": "tool_use", "id": "plan-tool", "name": "ExitPlanMode", "input": map[string]any{"plan": "ship it"}},
					{"type": "tool_use", "id": "web-tool", "name": "WebFetch", "input": map[string]any{"url": "https://example.test"}},
				},
			}},
		})
		plan := storage.RawTurnRecord{
			ID: 61, Source: storage.RawTurnSourceWire, Provider: "anthropic", AgentName: "claude",
			HarnessID: "claude", HarnessSessionID: "session-1", RequestID: "plan-name",
			RawRequest: json.RawMessage(`{"system":"name this plan","messages":[{"role":"user","content":"<conversation>ship it</conversation>"}]}`),
			Response:   json.RawMessage(`{"model":"claude-haiku","message":{"role":"assistant","content":[{"type":"text","text":"{\"name\":\"Ship it\"}"}]}}`),
			Meta:       json.RawMessage(`{"ts_request":"2026-08-24T10:00:02Z"}`),
		}
		web := storage.RawTurnRecord{
			ID: 62, Source: storage.RawTurnSourceWire, Provider: "anthropic", AgentName: "claude",
			HarnessID: "claude", HarnessSessionID: "session-1", RequestID: "web-summary",
			RawRequest: json.RawMessage(`{"messages":[{"role":"user","content":"Web page content: example"}]}`),
			Response:   json.RawMessage(`{"model":"claude-haiku","message":{"role":"assistant","content":[{"type":"text","text":"summary"}]}}`),
			Meta:       json.RawMessage(`{"ts_request":"2026-08-24T10:00:03Z"}`),
		}

		set, _, err := deriveWithTranscriptFallback([]storage.RawTurnRecord{web, plan}, file)
		Expect(err).NotTo(HaveOccurred())
		Expect(set.Report.PlansAttached).To(Equal(1))
		Expect(set.Report.WebSummaryAttached).To(Equal(1))
		spans := derive.EmitSpans(set)
		var attachedKinds []string
		for _, span := range spans.Turns[0].Spans {
			if span.Kind == derive.SpanKindLLM && span.ParentSpanID != spans.Turns[0].Spans[0].SpanID {
				attachedKinds = append(attachedKinds, span.CallKind)
			}
		}
		Expect(attachedKinds).To(ConsistOf(derive.KindPlanNameGen, derive.KindWebSummary))
	})

	It("reports transcript-only fallback structure without joining or stamping it against itself", func() {
		main := parseFile(65, "", "", []map[string]any{
			{"type": "user", "uuid": "main-user", "message": map[string]any{"role": "user", "content": "delegate"}},
			{"type": "assistant", "uuid": "main-answer", "parentUuid": "main-user", "message": map[string]any{"id": "main-message", "role": "assistant", "content": []map[string]any{{"type": "tool_use", "id": "task-1", "name": "Task", "input": map[string]any{"description": "research"}}}}},
		})
		child := parseFile(66, "agent-1", "task-1", []map[string]any{
			{"type": "user", "uuid": "child-user", "message": map[string]any{"role": "user", "content": "research"}},
			{"type": "assistant", "uuid": "child-answer", "parentUuid": "child-user", "message": map[string]any{"id": "child-message", "role": "assistant", "content": "done"}},
		})

		set, _, err := deriveWithTranscriptFallback(nil, main, child)
		Expect(err).NotTo(HaveOccurred())
		set.Report.Reconcile = derive.ReconcileTranscripts(set, []*derive.TranscriptFile{main, child})
		Expect(set.Report.Reconcile).To(Equal(&derive.ReconcileStats{
			TranscriptFiles: 2,
			SubagentForks:   1,
		}))
		for _, node := range set.Nodes {
			Expect(node.Node.ParentToolUseID).To(BeEmpty())
		}
	})

	It("reports wire-to-transcript joins only when wire main suppresses fallback", func() {
		wire := storage.RawTurnRecord{
			ID: 67, Source: storage.RawTurnSourceWire, Provider: "anthropic", AgentName: "claude",
			HarnessID: "claude", HarnessSessionID: "session-1", RequestID: "wire-main",
			RawRequest: json.RawMessage(`{"model":"claude","stream":true,"max_tokens":4096,"tools":[{"name":"Bash"}],"messages":[{"role":"user","content":"wire prompt"}]}`),
			Response:   json.RawMessage(`{"model":"claude","message":{"role":"assistant","content":[{"type":"text","text":"wire answer"}]}}`),
		}
		file := parseFile(68, "", "", []map[string]any{
			{"type": "user", "uuid": "wire-user", "message": map[string]any{"role": "user", "content": "wire prompt"}},
			{"type": "assistant", "uuid": "wire-answer", "parentUuid": "wire-user", "message": map[string]any{"id": "wire-message", "role": "assistant", "content": "wire answer"}},
		})

		set, projection, err := deriveWithTranscriptFallback([]storage.RawTurnRecord{wire}, file)
		Expect(err).NotTo(HaveOccurred())
		Expect(projection.SuppressedByWire).To(BeTrue())
		set.Report.Reconcile = derive.ReconcileTranscripts(set, []*derive.TranscriptFile{file})
		Expect(set.Report.Reconcile.TranscriptFiles).To(Equal(1))
		Expect(set.Report.Reconcile.MainChainsJoined).To(Equal(1))
		Expect(set.Report.Reconcile.ConversationJoined).To(BeNumerically(">", 0))
		Expect(set.Report.Reconcile.ConversationTotal).To(BeNumerically(">", 0))
		Expect(set.SpanSources).To(ConsistOf(HaveField("Source", storage.RawTurnSourceWire)))
	})

	It("accounts selected transcript files once without counting synthetic calls", func() {
		projected := parseFile(70, "", "", []map[string]any{
			{"type": "user", "uuid": "u", "message": map[string]any{"role": "user", "content": "hello"}},
			{"type": "assistant", "uuid": "a", "parentUuid": "u", "message": map[string]any{"id": "m", "role": "assistant", "content": "hi"}},
		})
		unsupported := parseFile(71, "agent-unsupported", "", []map[string]any{{"type": "future-record"}})
		unsupported.Kind = "interacted"
		probe, err := derive.NewDeriver("")
		Expect(err).NotTo(HaveOccurred())
		prepared := derive.PrepareTranscriptFallback(probe, []*derive.TranscriptFile{projected, unsupported})
		dv, err := derive.NewDeriver("")
		Expect(err).NotTo(HaveOccurred())
		dv.ApplyTranscriptProjection(prepared)
		dv.ApplyTranscriptProjection(prepared)
		for _, turn := range prepared.Turns {
			dv.AddTranscriptTurn(turn)
		}
		set := dv.Finish()
		Expect(set.Report.RawTurns).To(Equal(2))
		Expect(set.Report.ParsedTurns).To(Equal(2))
		Expect(set.Report.RawOnlyTurns).To(BeZero())
		Expect(set.Report.CallKinds).To(HaveKeyWithValue(derive.KindMain, 1),
			"the synthetic assistant call is projection coverage, not a parsed raw row")
	})

	It("suppresses transcript fallback only in the session that has wire main", func() {
		wire := storage.RawTurnRecord{
			ID: 80, Source: storage.RawTurnSourceWire, Provider: "anthropic", AgentName: "claude",
			HarnessID: "claude", HarnessSessionID: "wire-session", RequestID: "wire-main",
			RawRequest: json.RawMessage(`{"stream":true,"max_tokens":4096,"tools":[{"name":"Bash"}],"messages":[{"role":"user","content":"wire"}]}`),
			Response:   json.RawMessage(`{"model":"claude","message":{"role":"assistant","content":[{"type":"text","text":"wire answer"}]}}`),
		}
		wireFile := parseFileFor(81, "claude", "wire-session", "", "", []map[string]any{
			{"type": "user", "uuid": "wu", "message": map[string]any{"role": "user", "content": "must suppress"}},
			{"type": "assistant", "uuid": "wa", "parentUuid": "wu", "message": map[string]any{"id": "wm", "role": "assistant", "content": "must suppress"}},
		})
		fallbackFile := parseFileFor(82, "claude", "fallback-session", "", "", []map[string]any{
			{"type": "user", "uuid": "fu", "message": map[string]any{"role": "user", "content": "must project"}},
			{"type": "assistant", "uuid": "fa", "parentUuid": "fu", "message": map[string]any{"id": "fm", "role": "assistant", "content": "projected"}},
		})

		set, stats, err := deriveWithTranscriptFallback([]storage.RawTurnRecord{wire}, wireFile, fallbackFile)
		Expect(err).NotTo(HaveOccurred())
		Expect(stats.SuppressedByWire).To(BeTrue())
		Expect(set.Report.RawTurns).To(Equal(3))
		Expect(set.Report.ParsedTurns).To(Equal(3))
		spans := derive.EmitSpans(set)
		Expect(spans.Turns).To(HaveLen(2))
		sources := map[derive.SessionKey]string{}
		for _, turn := range spans.Turns {
			sources[turn.Session] = turn.Source
		}
		Expect(sources).To(Equal(map[derive.SessionKey]string{
			{HarnessID: "claude", HarnessSessionID: "wire-session"}:     storage.RawTurnSourceWire,
			{HarnessID: "claude", HarnessSessionID: "fallback-session"}: storage.RawTurnSourceTranscript,
		}))
	})
})

var _ = Describe("transcript-only live corpus fallback", func() {
	projectCorpus := func(includeSubagents bool) (*derive.DerivedSet, *derive.SpanSet) {
		_, rows := loadCorpus(corpusPath("corpus-cb9a87e5.jsonl.gz"))
		files := make([]*derive.TranscriptFile, 0, len(rows))
		for i := range rows {
			file, err := derive.ParseTranscriptFile(&rows[i])
			Expect(err).NotTo(HaveOccurred())
			if includeSubagents || file.AgentID == "" {
				files = append(files, file)
			}
		}
		set, _, err := deriveWithTranscriptFallback(nil, files...)
		Expect(err).NotTo(HaveOccurred())
		return set, derive.EmitSpans(set)
	}

	It("pins the real main transcript's turns, spans, tools, usage, and idempotence", func() {
		set, spans := projectCorpus(false)
		Expect(set.Report.TranscriptProjection.Files).To(Equal(1))
		Expect(set.Report.TranscriptProjection.Records).To(Equal(230))
		Expect(spans.Turns).To(HaveLen(3))
		Expect(spans.Report.SpanKinds).To(Equal(map[string]int{
			derive.SpanKindAgent: 3,
			derive.SpanKindEvent: 16,
			derive.SpanKindLLM:   18,
			derive.SpanKindTool:  53,
		}))
		Expect(totalTranscriptUsage(spans)).To(Equal([2]int64{7879, 14593}))
		Expect(transcriptToolCounts(spans)).To(And(
			HaveKeyWithValue("Agent", 2),
			HaveKeyWithValue("Bash", 5),
			HaveKeyWithValue("TaskCreate", 9),
		))
		for _, turn := range spans.Turns {
			Expect(turn.Source).To(Equal(storage.RawTurnSourceTranscript))
		}

		_, again := projectCorpus(false)
		Expect(traceAndSpanIDs(again)).To(Equal(traceAndSpanIDs(spans)))
	})

	It("pins the real main plus subagent structure and idempotence", func() {
		set, spans := projectCorpus(true)
		Expect(set.Report.TranscriptProjection.Files).To(Equal(3))
		Expect(set.Report.TranscriptProjection.Records).To(Equal(352))
		Expect(spans.Turns).To(HaveLen(3))
		Expect(spans.Report.SpanKinds).To(Equal(map[string]int{
			derive.SpanKindAgent: 5,
			derive.SpanKindEvent: 18,
			derive.SpanKindLLM:   56,
			derive.SpanKindTool:  92,
		}))
		Expect(totalTranscriptUsage(spans)).To(Equal([2]int64{17217, 23221}))
		Expect(transcriptToolCounts(spans)).To(And(
			HaveKeyWithValue("Bash", 28),
			HaveKeyWithValue("Read", 17),
			HaveKeyWithValue("TaskUpdate", 17),
		))

		var subagents, rejoins int
		spanIDs := map[string]struct{}{}
		for _, turn := range spans.Turns {
			for _, span := range turn.Spans {
				spanIDs[span.SpanID] = struct{}{}
				if span.Kind == derive.SpanKindAgent && span.ThreadID != "" {
					subagents++
					Expect(span.ParentSpanID).To(HavePrefix("txtool_"))
				}
			}
			for _, link := range turn.Links {
				if link.Kind == derive.LinkRejoin {
					rejoins++
					Expect(spanIDs).To(HaveKey(link.FromSpanID))
				}
			}
		}
		Expect(subagents).To(Equal(2))
		Expect(rejoins).To(Equal(2))

		_, again := projectCorpus(true)
		Expect(traceAndSpanIDs(again)).To(Equal(traceAndSpanIDs(spans)))
	})
})

type projectedConversationShape struct {
	UserPrompt          string
	ResponsePreview     string
	TotalInputTokens    int64
	TotalOutputTokens   int64
	MainInputTokens     int64
	MainOutputTokens    int64
	CacheReadTokens     int64
	CacheCreationTokens int64
	ToolCalls           int
	Spans               []projectedSpanShape
	LinkKinds           []string
}

type projectedSpanShape struct {
	Kind       string
	Name       string
	Status     string
	CallKind   string
	ThreadID   string
	Model      string
	StopReason string
	Usage      any
	Input      any
	Output     any
}

func conversationShape(set *derive.SpanSet) []projectedConversationShape {
	out := make([]projectedConversationShape, 0, len(set.Turns))
	for _, turn := range set.Turns {
		shape := projectedConversationShape{
			UserPrompt: turn.UserPrompt, ResponsePreview: turn.ResponsePreview,
			TotalInputTokens: turn.TotalInputTokens, TotalOutputTokens: turn.TotalOutputTokens,
			MainInputTokens: turn.MainInputTokens, MainOutputTokens: turn.MainOutputTokens,
			CacheReadTokens: turn.CacheReadTokens, CacheCreationTokens: turn.CacheCreationTokens,
			ToolCalls: turn.ToolCalls,
		}
		for _, span := range turn.Spans {
			shape.Spans = append(shape.Spans, projectedSpanShape{
				Kind: span.Kind, Name: span.Name, Status: span.Status,
				CallKind: span.CallKind, ThreadID: span.ThreadID, Model: span.Model,
				StopReason: span.StopReason, Usage: normalizedProjectionUsage(span.Usage),
				Input: normalizedProjectionBlocks(span.Input), Output: normalizedProjectionBlocks(span.Output),
			})
		}
		for _, link := range turn.Links {
			shape.LinkKinds = append(shape.LinkKinds, link.Kind)
		}
		for _, link := range set.Links {
			if link.FromTraceID == turn.TraceID || link.ToTraceID == turn.TraceID {
				shape.LinkKinds = append(shape.LinkKinds, link.Kind)
			}
		}
		sort.Strings(shape.LinkKinds)
		out = append(out, shape)
	}
	return out
}

func normalizedProjectionUsage(usage *llm.Usage) *llm.Usage {
	if usage == nil {
		return nil
	}
	out := *usage
	out.TotalDurationNs = 0
	out.PromptDurationNs = 0
	return &out
}

func normalizedProjectionBlocks(blocks []llm.ContentBlock) []llm.ContentBlock {
	out := append([]llm.ContentBlock(nil), blocks...)
	for i := range out {
		out[i].ToolUseID = ""
		out[i].ToolResultID = ""
	}
	return out
}

func deriveWithTranscriptFallback(rawTurns []storage.RawTurnRecord, files ...*derive.TranscriptFile) (*derive.DerivedSet, *derive.TranscriptProjectionStats, error) {
	return derive.BuildDerivedSetWithTranscriptFallback(rawTurns, files, "")
}

func totalTranscriptUsage(set *derive.SpanSet) [2]int64 {
	var total [2]int64
	for _, turn := range set.Turns {
		total[0] += turn.TotalInputTokens
		total[1] += turn.TotalOutputTokens
	}
	return total
}

func transcriptToolCounts(set *derive.SpanSet) map[string]int {
	out := map[string]int{}
	for _, turn := range set.Turns {
		for _, span := range turn.Spans {
			if span.Kind == derive.SpanKindTool {
				out[span.Name]++
			}
		}
	}
	return out
}

func sharedIDs(a, b *derive.SpanSet) []string {
	seen := map[string]struct{}{}
	for _, id := range traceAndSpanIDs(a) {
		seen[id] = struct{}{}
	}
	var shared []string
	for _, id := range traceAndSpanIDs(b) {
		if _, ok := seen[id]; ok {
			shared = append(shared, id)
		}
	}
	return shared
}

func traceAndSpanIDs(set *derive.SpanSet) []string {
	count := len(set.Turns)
	for _, turn := range set.Turns {
		count += len(turn.Spans)
	}
	out := make([]string, 0, count)
	for _, turn := range set.Turns {
		out = append(out, turn.TraceID)
		for _, span := range turn.Spans {
			out = append(out, span.SpanID)
		}
	}
	return out
}
