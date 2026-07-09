package export_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/export"
	"github.com/papercomputeco/tapes/pkg/llm"
	"github.com/papercomputeco/tapes/pkg/skill"
)

// fakeQuerier implements skill.Querier for export tests, mirroring the
// pattern used across cmd/tapes/checkout and pkg/skill tests.
type fakeQuerier struct {
	summaries map[string][]skill.TraceSummary
	traces    map[string]*skill.Trace
}

func (f *fakeQuerier) TraceSummaries(_ context.Context, sessionID string) ([]skill.TraceSummary, error) {
	turns, ok := f.summaries[sessionID]
	if !ok {
		return nil, fmt.Errorf("session %s not found", sessionID)
	}
	return turns, nil
}

func (f *fakeQuerier) Trace(_ context.Context, traceID string) (*skill.Trace, error) {
	trace, ok := f.traces[traceID]
	if !ok {
		return nil, fmt.Errorf("trace %s not found", traceID)
	}
	return trace, nil
}

func mainSpan(seq int64, text string) skill.Span {
	return skill.Span{Kind: "llm", CallKind: "main", Seq: seq, Output: []llm.ContentBlock{{Type: "text", Text: text}}}
}

func toolSpan(seq int64, name string) skill.Span {
	return skill.Span{Kind: "tool", Name: name, Seq: seq}
}

var _ = Describe("Turn", func() {
	It("carries the exact exportTurn field shape with matching json tags", func() {
		// T-2: omitempty regression — a zero-value Turn must serialize with
		// only the always-present fields; omitempty fields must vanish.
		turn := export.Turn{
			TraceID:   "t1",
			SessionID: "s1",
			// UserPrompt, Response, StartedAt intentionally left zero.
			TotalInputTokens:  0,
			TotalOutputTokens: 0,
			MainInputTokens:   0,
			MainOutputTokens:  0,
		}
		raw, err := json.Marshal(turn)
		Expect(err).NotTo(HaveOccurred())

		var m map[string]any
		Expect(json.Unmarshal(raw, &m)).To(Succeed())

		// Always present, even at zero value.
		Expect(m).To(HaveKey("trace_id"))
		Expect(m).To(HaveKey("session_id"))
		Expect(m).To(HaveKey("total_input_tokens"))
		Expect(m).To(HaveKey("total_output_tokens"))
		Expect(m).To(HaveKey("main_input_tokens"))
		Expect(m).To(HaveKey("main_output_tokens"))

		// omitempty fields must be absent when zero.
		Expect(m).NotTo(HaveKey("user_prompt"))
		Expect(m).NotTo(HaveKey("response"))
		Expect(m).NotTo(HaveKey("started_at"))
	})

	It("includes omitempty fields when non-zero", func() {
		turn := export.Turn{
			TraceID:    "t1",
			SessionID:  "s1",
			UserPrompt: "hi",
			Response:   "hello",
			StartedAt:  "2026-06-01T09:00:00Z",
		}
		raw, err := json.Marshal(turn)
		Expect(err).NotTo(HaveOccurred())

		var m map[string]any
		Expect(json.Unmarshal(raw, &m)).To(Succeed())
		Expect(m["user_prompt"]).To(Equal("hi"))
		Expect(m["response"]).To(Equal("hello"))
		Expect(m["started_at"]).To(Equal("2026-06-01T09:00:00Z"))
	})
})

var _ = Describe("SessionJSONL", func() {
	var (
		ctx     context.Context
		querier *fakeQuerier
	)

	BeforeEach(func() {
		ctx = context.Background()
		base := time.Date(2026, 6, 1, 9, 0, 0, 0, time.UTC)
		querier = &fakeQuerier{
			summaries: map[string][]skill.TraceSummary{
				"session-1": {
					{
						TraceID: "t1", UserPrompt: "Fix the infinite loop", StartedAt: base,
						TotalInputTokens: 100, TotalOutputTokens: 40, MainInputTokens: 80, MainOutputTokens: 30,
					},
					{
						TraceID: "t2", UserPrompt: "Thanks", StartedAt: base.Add(time.Minute),
						TotalInputTokens: 50, TotalOutputTokens: 10,
					},
					// synthetic turn — must be filtered out.
					{TraceID: "t3", Synthetic: "compaction", UserPrompt: "compacted context", StartedAt: base.Add(2 * time.Minute)},
				},
			},
			traces: map[string]*skill.Trace{
				"t1": {TraceID: "t1", Spans: []skill.Span{
					mainSpan(1, "Let me check the dependency array."),
					toolSpan(2, "Read"),
					mainSpan(3, "Fixed the object reference."),
				}},
				"t2": {TraceID: "t2", Spans: []skill.Span{mainSpan(1, "Glad it helped.")}},
			},
		}
	})

	It("writes one JSON object per non-synthetic turn to the given writer", func() {
		var b strings.Builder
		err := export.SessionJSONL(ctx, querier, "session-1", &b)
		Expect(err).NotTo(HaveOccurred())

		lines := nonEmptyLines(b.String())
		Expect(lines).To(HaveLen(2))

		var first map[string]any
		Expect(json.Unmarshal([]byte(lines[0]), &first)).To(Succeed())
		Expect(first["trace_id"]).To(Equal("t1"))
		Expect(first["session_id"]).To(Equal("session-1"))
		Expect(first["user_prompt"]).To(Equal("Fix the infinite loop"))
		Expect(first["response"]).To(ContainSubstring("[assistant] Let me check the dependency array."))
		Expect(first["response"]).To(ContainSubstring("[tools] Read"))
		Expect(first["response"]).NotTo(ContainSubstring("[user]"))
		Expect(first["total_input_tokens"]).To(BeNumerically("==", 100))
		Expect(first["main_output_tokens"]).To(BeNumerically("==", 30))

		var second map[string]any
		Expect(json.Unmarshal([]byte(lines[1]), &second)).To(Succeed())
		Expect(second["trace_id"]).To(Equal("t2"))
	})

	It("supports the trace filter transcript option", func() {
		var b strings.Builder
		err := export.SessionJSONL(ctx, querier, "session-1", &b, skill.WithTraceFilter("t1"))
		Expect(err).NotTo(HaveOccurred())

		lines := nonEmptyLines(b.String())
		Expect(lines).To(HaveLen(1))
		var rec map[string]any
		Expect(json.Unmarshal([]byte(lines[0]), &rec)).To(Succeed())
		Expect(rec["trace_id"]).To(Equal("t1"))
	})

	It("propagates a session lookup error", func() {
		var b strings.Builder
		err := export.SessionJSONL(ctx, querier, "missing", &b)
		Expect(err).To(HaveOccurred())
	})

	It("streams to an io.Writer that is not a *strings.Builder", func() {
		// Regression guard for the exportJSONL -> SessionJSONL generalization:
		// the function must accept any io.Writer, not just *strings.Builder.
		pr, pw := io.Pipe()
		done := make(chan error, 1)
		go func() {
			done <- export.SessionJSONL(ctx, querier, "session-1", pw)
			pw.Close()
		}()

		data, readErr := io.ReadAll(pr)
		Expect(readErr).NotTo(HaveOccurred())
		Expect(<-done).NotTo(HaveOccurred())

		lines := nonEmptyLines(string(data))
		Expect(lines).To(HaveLen(2))
	})
})

func nonEmptyLines(s string) []string {
	var out []string
	for line := range strings.SplitSeq(s, "\n") {
		if strings.TrimSpace(line) != "" {
			out = append(out, line)
		}
	}
	return out
}
