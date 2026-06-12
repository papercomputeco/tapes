package skill_test

import (
	"context"
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/llm"
	"github.com/papercomputeco/tapes/pkg/skill"
)

// mockQuerier implements skill.Querier for testing.
type mockQuerier struct {
	summaries map[string][]skill.TraceSummary
	traces    map[string]*skill.Trace
}

func (m *mockQuerier) TraceSummaries(_ context.Context, sessionID string) ([]skill.TraceSummary, error) {
	turns, ok := m.summaries[sessionID]
	if !ok {
		return nil, fmt.Errorf("session %s not found", sessionID)
	}
	return turns, nil
}

func (m *mockQuerier) Trace(_ context.Context, traceID string) (*skill.Trace, error) {
	trace, ok := m.traces[traceID]
	if !ok {
		return nil, fmt.Errorf("trace %s not found", traceID)
	}
	return trace, nil
}

func mainSpan(seq int64, text string) skill.Span {
	return skill.Span{
		Kind:     "llm",
		CallKind: "main",
		Seq:      seq,
		Output:   []llm.ContentBlock{{Type: "text", Text: text}},
	}
}

func toolSpan(seq int64, name string) skill.Span {
	return skill.Span{Kind: "tool", Name: name, Seq: seq}
}

const validSkillJSON = `{
	"description": "Debug React hooks infinite loops and stale closure issues. Use when debugging useEffect, useMemo, or useCallback problems.",
	"tags": ["react", "hooks", "debugging"],
	"content": "## Debug React Hooks\n\n1. Identify the problematic hook\n2. Check the dependency array\n3. Look for object reference issues\n4. Verify cleanup functions"
}`

var _ = Describe("Generator", func() {
	It("generates a skill from a single session", func() {
		querier := &mockQuerier{
			summaries: map[string][]skill.TraceSummary{
				"abc123": {
					{TraceID: "t1", UserPrompt: "My useEffect keeps running in an infinite loop", StartedAt: time.Now()},
					{TraceID: "t2", UserPrompt: "That fixed it, thanks!", StartedAt: time.Now()},
				},
			},
			traces: map[string]*skill.Trace{
				"t1": {TraceID: "t1", Spans: []skill.Span{
					mainSpan(1, "Let me check the dependency array."),
					toolSpan(2, "Read"),
					mainSpan(3, "The issue is that you're creating a new object reference on each render."),
				}},
				"t2": {TraceID: "t2", Spans: []skill.Span{
					mainSpan(1, "Glad that resolved it."),
				}},
			},
		}

		var capturedPrompt string
		mockLLM := func(_ context.Context, prompt string) (string, error) {
			capturedPrompt = prompt
			return validSkillJSON, nil
		}

		gen := skill.NewGenerator(querier, mockLLM)
		sk, err := gen.Generate(context.Background(), []string{"abc123"}, "debug-react-hooks", "workflow", nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(sk.Name).To(Equal("debug-react-hooks"))
		Expect(sk.Type).To(Equal("workflow"))
		Expect(sk.Version).To(Equal("0.1.0"))
		Expect(sk.Description).To(ContainSubstring("React hooks"))
		Expect(sk.Tags).To(ContainElement("react"))
		Expect(sk.Content).To(ContainSubstring("dependency array"))
		Expect(sk.Sessions).To(Equal([]string{"abc123"}))
		Expect(sk.CreatedAt).NotTo(BeZero())

		// The transcript carries turn-grain prompt/response pairs plus
		// the tool summary between assistant responses.
		Expect(capturedPrompt).To(ContainSubstring("[user] My useEffect keeps running in an infinite loop"))
		Expect(capturedPrompt).To(ContainSubstring("[assistant] Let me check the dependency array."))
		Expect(capturedPrompt).To(ContainSubstring("[tools] Read"))
		Expect(capturedPrompt).To(ContainSubstring("[assistant] The issue is that you're creating a new object reference on each render."))
	})

	It("excludes offshoot and injected spans and subagent threads from the transcript", func() {
		querier := &mockQuerier{
			summaries: map[string][]skill.TraceSummary{
				"abc123": {{TraceID: "t1", UserPrompt: "Fix the bug", StartedAt: time.Now()}},
			},
			traces: map[string]*skill.Trace{
				"t1": {TraceID: "t1", Spans: []skill.Span{
					{Kind: "llm", CallKind: "offshoot:title-gen", Seq: 1,
						Output: []llm.ContentBlock{{Type: "text", Text: "shadow title text"}}},
					{Kind: "event", CallKind: "injected:claude-md", Seq: 2,
						Output: []llm.ContentBlock{{Type: "text", Text: "injected context text"}}},
					{Kind: "llm", CallKind: "main", ThreadID: "subagent-1", Seq: 3,
						Output: []llm.ContentBlock{{Type: "text", Text: "subagent thread text"}}},
					mainSpan(4, "Fixed the bug in the handler."),
				}},
			},
		}

		var capturedPrompt string
		mockLLM := func(_ context.Context, prompt string) (string, error) {
			capturedPrompt = prompt
			return validSkillJSON, nil
		}

		gen := skill.NewGenerator(querier, mockLLM)
		_, err := gen.Generate(context.Background(), []string{"abc123"}, "fix-bug", "workflow", nil)
		Expect(err).NotTo(HaveOccurred())

		Expect(capturedPrompt).To(ContainSubstring("Fixed the bug in the handler."))
		Expect(capturedPrompt).NotTo(ContainSubstring("shadow title text"))
		Expect(capturedPrompt).NotTo(ContainSubstring("injected context text"))
		Expect(capturedPrompt).NotTo(ContainSubstring("subagent thread text"))
	})

	It("skips synthetic turns and falls back to the response preview", func() {
		querier := &mockQuerier{
			summaries: map[string][]skill.TraceSummary{
				"abc123": {
					{TraceID: "t1", UserPrompt: "Real question", ResponsePreview: "Preview answer", StartedAt: time.Now()},
					{TraceID: "t2", Synthetic: "compaction", UserPrompt: "compacted context", StartedAt: time.Now()},
				},
			},
			traces: map[string]*skill.Trace{
				// t1 has no main-span text, so the preview stands in.
				"t1": {TraceID: "t1", Spans: []skill.Span{toolSpan(1, "Bash")}},
			},
		}

		var capturedPrompt string
		mockLLM := func(_ context.Context, prompt string) (string, error) {
			capturedPrompt = prompt
			return validSkillJSON, nil
		}

		gen := skill.NewGenerator(querier, mockLLM)
		_, err := gen.Generate(context.Background(), []string{"abc123"}, "preview-fallback", "workflow", nil)
		Expect(err).NotTo(HaveOccurred())

		Expect(capturedPrompt).To(ContainSubstring("[assistant] Preview answer"))
		Expect(capturedPrompt).NotTo(ContainSubstring("compacted context"))
	})

	It("generates a skill from multiple sessions", func() {
		querier := &mockQuerier{
			summaries: map[string][]skill.TraceSummary{
				"abc123": {{TraceID: "t1", UserPrompt: "Fix the API endpoint", StartedAt: time.Now()}},
				"def456": {{TraceID: "t2", UserPrompt: "Add validation to the API", StartedAt: time.Now()}},
			},
			traces: map[string]*skill.Trace{
				"t1": {TraceID: "t1", Spans: []skill.Span{mainSpan(1, "Endpoint fixed.")}},
				"t2": {TraceID: "t2", Spans: []skill.Span{mainSpan(1, "Validation added.")}},
			},
		}

		mockLLM := func(_ context.Context, _ string) (string, error) {
			return `{
				"description": "API design patterns.",
				"tags": ["api"],
				"content": "## API Patterns\n\n1. Validate inputs\n2. Handle errors"
			}`, nil
		}

		gen := skill.NewGenerator(querier, mockLLM)
		sk, err := gen.Generate(context.Background(), []string{"abc123", "def456"}, "api-patterns", "domain-knowledge", nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(sk.Sessions).To(Equal([]string{"abc123", "def456"}))
		Expect(sk.Type).To(Equal("domain-knowledge"))
	})

	It("handles LLM response wrapped in markdown code blocks", func() {
		querier := &mockQuerier{
			summaries: map[string][]skill.TraceSummary{
				"abc123": {{TraceID: "t1", UserPrompt: "Test", StartedAt: time.Now()}},
			},
			traces: map[string]*skill.Trace{
				"t1": {TraceID: "t1", Spans: []skill.Span{mainSpan(1, "Done.")}},
			},
		}

		mockLLM := func(_ context.Context, _ string) (string, error) {
			return "```json\n" + `{
				"description": "Test skill",
				"tags": [],
				"content": "## Test\n\n1. Do the thing"
			}` + "\n```", nil
		}

		gen := skill.NewGenerator(querier, mockLLM)
		sk, err := gen.Generate(context.Background(), []string{"abc123"}, "test-skill", "workflow", nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(sk.Description).To(Equal("Test skill"))
	})

	It("rejects invalid skill types", func() {
		querier := &mockQuerier{}
		mockLLM := func(_ context.Context, _ string) (string, error) { return "", nil }

		gen := skill.NewGenerator(querier, mockLLM)
		_, err := gen.Generate(context.Background(), []string{"abc123"}, "test", "invalid-type", nil)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("invalid skill type"))
	})

	It("rejects empty session ID lists", func() {
		querier := &mockQuerier{}
		mockLLM := func(_ context.Context, _ string) (string, error) { return "", nil }

		gen := skill.NewGenerator(querier, mockLLM)
		_, err := gen.Generate(context.Background(), []string{}, "test", "workflow", nil)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("at least one session"))
	})

	Context("--since/--until filtering", func() {
		var (
			querier *mockQuerier
			mockLLM skill.LLMCallFunc
			base    time.Time
		)

		BeforeEach(func() {
			base = time.Date(2026, 2, 17, 10, 0, 0, 0, time.UTC)

			querier = &mockQuerier{
				summaries: map[string][]skill.TraceSummary{
					"abc123": {
						{TraceID: "t1", UserPrompt: "morning message", StartedAt: base},
						{TraceID: "t2", UserPrompt: "afternoon message", StartedAt: base.Add(4 * time.Hour)},
						{TraceID: "t3", UserPrompt: "evening message", StartedAt: base.Add(10 * time.Hour)},
					},
				},
				traces: map[string]*skill.Trace{
					"t1": {TraceID: "t1", Spans: []skill.Span{mainSpan(1, "morning reply")}},
					"t2": {TraceID: "t2", Spans: []skill.Span{mainSpan(1, "afternoon reply")}},
					"t3": {TraceID: "t3", Spans: []skill.Span{mainSpan(1, "evening reply")}},
				},
			}

			mockLLM = func(_ context.Context, _ string) (string, error) {
				return `{
					"description": "Filtered skill",
					"tags": ["test"],
					"content": "## Filtered\n\n1. Step one"
				}`, nil
			}
		})

		It("filters turns with --since", func() {
			since := base.Add(3 * time.Hour)
			opts := &skill.GenerateOptions{Since: &since}

			var capturedPrompt string
			captureLLM := func(ctx context.Context, prompt string) (string, error) {
				capturedPrompt = prompt
				return mockLLM(ctx, prompt)
			}

			gen := skill.NewGenerator(querier, captureLLM)
			sk, err := gen.Generate(context.Background(), []string{"abc123"}, "filtered", "workflow", opts)
			Expect(err).NotTo(HaveOccurred())
			Expect(sk).NotTo(BeNil())

			// The prompt should contain afternoon and evening but not morning
			Expect(capturedPrompt).To(ContainSubstring("afternoon message"))
			Expect(capturedPrompt).To(ContainSubstring("evening message"))
			Expect(capturedPrompt).NotTo(ContainSubstring("morning message"))
		})

		It("filters turns with --until", func() {
			until := base.Add(3 * time.Hour)
			opts := &skill.GenerateOptions{Until: &until}

			var capturedPrompt string
			captureLLM := func(ctx context.Context, prompt string) (string, error) {
				capturedPrompt = prompt
				return mockLLM(ctx, prompt)
			}

			gen := skill.NewGenerator(querier, captureLLM)
			sk, err := gen.Generate(context.Background(), []string{"abc123"}, "filtered", "workflow", opts)
			Expect(err).NotTo(HaveOccurred())
			Expect(sk).NotTo(BeNil())

			// The prompt should contain morning but not afternoon or evening
			Expect(capturedPrompt).To(ContainSubstring("morning message"))
			Expect(capturedPrompt).NotTo(ContainSubstring("afternoon message"))
			Expect(capturedPrompt).NotTo(ContainSubstring("evening message"))
		})

		It("filters turns with both --since and --until", func() {
			since := base.Add(3 * time.Hour)
			until := base.Add(5 * time.Hour)
			opts := &skill.GenerateOptions{Since: &since, Until: &until}

			var capturedPrompt string
			captureLLM := func(ctx context.Context, prompt string) (string, error) {
				capturedPrompt = prompt
				return mockLLM(ctx, prompt)
			}

			gen := skill.NewGenerator(querier, captureLLM)
			sk, err := gen.Generate(context.Background(), []string{"abc123"}, "filtered", "workflow", opts)
			Expect(err).NotTo(HaveOccurred())
			Expect(sk).NotTo(BeNil())

			// Only afternoon turns should be included
			Expect(capturedPrompt).NotTo(ContainSubstring("morning message"))
			Expect(capturedPrompt).To(ContainSubstring("afternoon message"))
			Expect(capturedPrompt).NotTo(ContainSubstring("evening message"))
		})

		It("returns an error when all turns are filtered out", func() {
			since := base.Add(24 * time.Hour) // future — filters everything
			opts := &skill.GenerateOptions{Since: &since}

			gen := skill.NewGenerator(querier, mockLLM)
			_, err := gen.Generate(context.Background(), []string{"abc123"}, "empty", "workflow", opts)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("no turns"))
		})

		It("passes nil opts through without filtering", func() {
			var capturedPrompt string
			captureLLM := func(ctx context.Context, prompt string) (string, error) {
				capturedPrompt = prompt
				return mockLLM(ctx, prompt)
			}

			gen := skill.NewGenerator(querier, captureLLM)
			sk, err := gen.Generate(context.Background(), []string{"abc123"}, "all-messages", "workflow", nil)
			Expect(err).NotTo(HaveOccurred())
			Expect(sk).NotTo(BeNil())

			// All turns should be present
			Expect(capturedPrompt).To(ContainSubstring("morning message"))
			Expect(capturedPrompt).To(ContainSubstring("afternoon message"))
			Expect(capturedPrompt).To(ContainSubstring("evening message"))
		})
	})
})
