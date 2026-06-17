package checkoutcmder_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	checkoutcmder "github.com/papercomputeco/tapes/cmd/tapes/checkout"
	"github.com/papercomputeco/tapes/pkg/llm"
	"github.com/papercomputeco/tapes/pkg/skill"
)

// fakeQuerier implements skill.Querier for export tests, mirroring the
// generator_test.go pattern.
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

var _ = Describe("NewCheckoutCmd", func() {
	It("uses the session-id argument form", func() {
		cmd := checkoutcmder.NewCheckoutCmd()
		Expect(cmd.Use).To(Equal("checkout <session-id>"))
	})

	It("describes export, not chat/replay/stems", func() {
		cmd := checkoutcmder.NewCheckoutCmd()
		Expect(cmd.Long).To(ContainSubstring("Export"))
		Expect(cmd.Long).NotTo(ContainSubstring("chat"))
		Expect(cmd.Long).NotTo(ContainSubstring("replay"))
		Expect(cmd.Long).NotTo(ContainSubstring("stem"))
	})

	It("requires exactly one argument", func() {
		cmd := checkoutcmder.NewCheckoutCmd()
		Expect(cmd.Args(cmd, []string{})).To(HaveOccurred())
		Expect(cmd.Args(cmd, []string{"session-1"})).NotTo(HaveOccurred())
		Expect(cmd.Args(cmd, []string{"a", "b"})).To(HaveOccurred())
	})

	It("exposes the export flags", func() {
		cmd := checkoutcmder.NewCheckoutCmd()
		Expect(cmd.Flags().Lookup("trace")).NotTo(BeNil())
		Expect(cmd.Flags().Lookup("format")).NotTo(BeNil())
		Expect(cmd.Flags().Lookup("output")).NotTo(BeNil())
		Expect(cmd.Flags().Lookup("format").DefValue).To(Equal("md"))
	})
})

var _ = Describe("Export", func() {
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
					// synthetic turn — must be filtered out of both formats.
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

	Context("markdown format", func() {
		It("renders the whole session as a [user]/[assistant]/[tools] transcript", func() {
			out, err := checkoutcmder.Export(ctx, querier, checkoutcmder.ExportOptions{
				SessionID: "session-1", Format: "md", IncludeSpans: true,
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(out).To(ContainSubstring("[user] Fix the infinite loop"))
			Expect(out).To(ContainSubstring("[assistant] Let me check the dependency array."))
			Expect(out).To(ContainSubstring("[tools] Read"))
			Expect(out).To(ContainSubstring("[assistant] Fixed the object reference."))
			Expect(out).To(ContainSubstring("[user] Thanks"))
			Expect(out).To(ContainSubstring("[assistant] Glad it helped."))
			// Synthetic turn excluded.
			Expect(out).NotTo(ContainSubstring("compacted context"))
		})

		It("drops the [tools] span detail with IncludeSpans=false", func() {
			out, err := checkoutcmder.Export(ctx, querier, checkoutcmder.ExportOptions{
				SessionID: "session-1", Format: "md", IncludeSpans: false,
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(out).To(ContainSubstring("[user] Fix the infinite loop"))
			Expect(out).To(ContainSubstring("[assistant] Let me check the dependency array."))
			Expect(out).NotTo(ContainSubstring("[tools]"))
		})

		It("defaults to markdown when format is empty", func() {
			out, err := checkoutcmder.Export(ctx, querier, checkoutcmder.ExportOptions{SessionID: "session-1"})
			Expect(err).NotTo(HaveOccurred())
			Expect(out).To(ContainSubstring("[user] Fix the infinite loop"))
		})

		It("exports a single turn with --trace", func() {
			out, err := checkoutcmder.Export(ctx, querier, checkoutcmder.ExportOptions{
				SessionID: "session-1", TraceID: "t2", Format: "md",
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(out).To(ContainSubstring("[user] Thanks"))
			Expect(out).To(ContainSubstring("[assistant] Glad it helped."))
			Expect(out).NotTo(ContainSubstring("Fix the infinite loop"))
		})

		It("errors when --trace names an unknown turn", func() {
			_, err := checkoutcmder.Export(ctx, querier, checkoutcmder.ExportOptions{
				SessionID: "session-1", TraceID: "nope", Format: "md",
			})
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("nope"))
		})
	})

	Context("jsonl format", func() {
		It("emits one JSON object per non-synthetic turn", func() {
			out, err := checkoutcmder.Export(ctx, querier, checkoutcmder.ExportOptions{
				SessionID: "session-1", Format: "jsonl",
			})
			Expect(err).NotTo(HaveOccurred())

			lines := nonEmptyLines(out)
			Expect(lines).To(HaveLen(2))

			var first map[string]any
			Expect(json.Unmarshal([]byte(lines[0]), &first)).To(Succeed())
			Expect(first["trace_id"]).To(Equal("t1"))
			Expect(first["session_id"]).To(Equal("session-1"))
			Expect(first["user_prompt"]).To(Equal("Fix the infinite loop"))
			Expect(first["response"]).To(ContainSubstring("[assistant] Let me check the dependency array."))
			Expect(first["response"]).To(ContainSubstring("[tools] Read"))
			// The user line is dropped from the response body (it has its own field).
			Expect(first["response"]).NotTo(ContainSubstring("[user]"))
			Expect(first["total_input_tokens"]).To(BeNumerically("==", 100))
			Expect(first["main_output_tokens"]).To(BeNumerically("==", 30))

			var second map[string]any
			Expect(json.Unmarshal([]byte(lines[1]), &second)).To(Succeed())
			Expect(second["trace_id"]).To(Equal("t2"))
		})

		It("emits a single object with --trace", func() {
			out, err := checkoutcmder.Export(ctx, querier, checkoutcmder.ExportOptions{
				SessionID: "session-1", TraceID: "t1", Format: "jsonl",
			})
			Expect(err).NotTo(HaveOccurred())
			lines := nonEmptyLines(out)
			Expect(lines).To(HaveLen(1))
			var rec map[string]any
			Expect(json.Unmarshal([]byte(lines[0]), &rec)).To(Succeed())
			Expect(rec["trace_id"]).To(Equal("t1"))
		})

		It("nests the span tree under each turn with IncludeSpans=true", func() {
			out, err := checkoutcmder.Export(ctx, querier, checkoutcmder.ExportOptions{
				SessionID: "session-1", Format: "jsonl", IncludeSpans: true,
			})
			Expect(err).NotTo(HaveOccurred())
			lines := nonEmptyLines(out)
			Expect(lines).NotTo(BeEmpty())

			var first map[string]any
			Expect(json.Unmarshal([]byte(lines[0]), &first)).To(Succeed())
			spans, ok := first["spans"].([]any)
			Expect(ok).To(BeTrue())
			Expect(spans).NotTo(BeEmpty())
			span0, ok := spans[0].(map[string]any)
			Expect(ok).To(BeTrue())
			Expect(span0).To(HaveKey("span_id"))
			Expect(span0).To(HaveKey("kind"))
		})

		It("omits the spans field with IncludeSpans=false", func() {
			out, err := checkoutcmder.Export(ctx, querier, checkoutcmder.ExportOptions{
				SessionID: "session-1", Format: "jsonl", IncludeSpans: false,
			})
			Expect(err).NotTo(HaveOccurred())
			var first map[string]any
			Expect(json.Unmarshal([]byte(nonEmptyLines(out)[0]), &first)).To(Succeed())
			Expect(first).NotTo(HaveKey("spans"))
		})
	})

	It("rejects an unknown format", func() {
		_, err := checkoutcmder.Export(ctx, querier, checkoutcmder.ExportOptions{
			SessionID: "session-1", Format: "yaml",
		})
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("invalid format"))
	})

	It("errors on an unknown session", func() {
		_, err := checkoutcmder.Export(ctx, querier, checkoutcmder.ExportOptions{
			SessionID: "missing", Format: "md",
		})
		Expect(err).To(HaveOccurred())
	})
})

// The -o file-write path is exercised here against the rendered output,
// since Export returns the string the command then writes.
var _ = Describe("writing export output", func() {
	It("writes rendered content to a file path", func() {
		ctx := context.Background()
		querier := &fakeQuerier{
			summaries: map[string][]skill.TraceSummary{
				"session-1": {{TraceID: "t1", UserPrompt: "hi", StartedAt: time.Now()}},
			},
			traces: map[string]*skill.Trace{
				"t1": {TraceID: "t1", Spans: []skill.Span{mainSpan(1, "hello")}},
			},
		}

		out, err := checkoutcmder.Export(ctx, querier, checkoutcmder.ExportOptions{SessionID: "session-1", Format: "md"})
		Expect(err).NotTo(HaveOccurred())

		path := filepath.Join(GinkgoT().TempDir(), "export.md")
		Expect(os.WriteFile(path, []byte(out), 0o644)).To(Succeed())

		data, err := os.ReadFile(path)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(data)).To(ContainSubstring("[assistant] hello"))
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
