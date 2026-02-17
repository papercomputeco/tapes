package skill_test

import (
	"context"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/deck"
	"github.com/papercomputeco/tapes/pkg/skill"
)

// mockQuerier implements deck.Querier for testing.
type mockQuerier struct {
	details map[string]*deck.SessionDetail
}

func (m *mockQuerier) Overview(_ context.Context, _ deck.Filters) (*deck.Overview, error) {
	return &deck.Overview{}, nil
}

func (m *mockQuerier) SessionDetail(_ context.Context, id string) (*deck.SessionDetail, error) {
	return m.details[id], nil
}

func (m *mockQuerier) AnalyticsOverview(_ context.Context, _ deck.Filters) (*deck.AnalyticsOverview, error) {
	return &deck.AnalyticsOverview{ProviderBreakdown: map[string]int{}}, nil
}

func (m *mockQuerier) SessionAnalytics(_ context.Context, _ string) (*deck.SessionAnalytics, error) {
	return &deck.SessionAnalytics{}, nil
}

var _ = Describe("Generator", func() {
	It("generates a skill from a single conversation hash", func() {
		querier := &mockQuerier{
			details: map[string]*deck.SessionDetail{
				"abc123": {
					Summary: deck.SessionSummary{
						ID:    "abc123",
						Label: "Debug React hooks",
					},
					Messages: []deck.SessionMessage{
						{Role: "user", Text: "My useEffect keeps running in an infinite loop", Timestamp: time.Now()},
						{Role: "assistant", Text: "Let me check the dependency array. The issue is that you're creating a new object reference on each render.", Timestamp: time.Now()},
						{Role: "user", Text: "That fixed it, thanks!", Timestamp: time.Now()},
					},
				},
			},
		}

		mockLLM := func(_ context.Context, _ string) (string, error) {
			return `{
				"description": "Debug React hooks infinite loops and stale closure issues. Use when debugging useEffect, useMemo, or useCallback problems.",
				"tags": ["react", "hooks", "debugging"],
				"content": "## Debug React Hooks\n\n1. Identify the problematic hook\n2. Check the dependency array\n3. Look for object reference issues\n4. Verify cleanup functions"
			}`, nil
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
	})

	It("generates a skill from multiple conversation hashes", func() {
		querier := &mockQuerier{
			details: map[string]*deck.SessionDetail{
				"abc123": {
					Summary:  deck.SessionSummary{ID: "abc123"},
					Messages: []deck.SessionMessage{{Role: "user", Text: "Fix the API endpoint", Timestamp: time.Now()}},
				},
				"def456": {
					Summary:  deck.SessionSummary{ID: "def456"},
					Messages: []deck.SessionMessage{{Role: "user", Text: "Add validation to the API", Timestamp: time.Now()}},
				},
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
			details: map[string]*deck.SessionDetail{
				"abc123": {
					Summary:  deck.SessionSummary{ID: "abc123"},
					Messages: []deck.SessionMessage{{Role: "user", Text: "Test", Timestamp: time.Now()}},
				},
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
		querier := &mockQuerier{details: map[string]*deck.SessionDetail{}}
		mockLLM := func(_ context.Context, _ string) (string, error) { return "", nil }

		gen := skill.NewGenerator(querier, mockLLM)
		_, err := gen.Generate(context.Background(), []string{"abc123"}, "test", "invalid-type", nil)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("invalid skill type"))
	})

	It("rejects empty hashes", func() {
		querier := &mockQuerier{details: map[string]*deck.SessionDetail{}}
		mockLLM := func(_ context.Context, _ string) (string, error) { return "", nil }

		gen := skill.NewGenerator(querier, mockLLM)
		_, err := gen.Generate(context.Background(), []string{}, "test", "workflow", nil)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("at least one hash"))
	})

	Context("--since/--until filtering", func() {
		var (
			querier *mockQuerier
			mockLLM deck.LLMCallFunc
			base    time.Time
		)

		BeforeEach(func() {
			base = time.Date(2026, 2, 17, 10, 0, 0, 0, time.UTC)

			querier = &mockQuerier{
				details: map[string]*deck.SessionDetail{
					"abc123": {
						Summary: deck.SessionSummary{ID: "abc123"},
						Messages: []deck.SessionMessage{
							{Role: "user", Text: "morning message", Timestamp: base},
							{Role: "assistant", Text: "morning reply", Timestamp: base.Add(1 * time.Minute)},
							{Role: "user", Text: "afternoon message", Timestamp: base.Add(4 * time.Hour)},
							{Role: "assistant", Text: "afternoon reply", Timestamp: base.Add(4*time.Hour + time.Minute)},
							{Role: "user", Text: "evening message", Timestamp: base.Add(10 * time.Hour)},
							{Role: "assistant", Text: "evening reply", Timestamp: base.Add(10*time.Hour + time.Minute)},
						},
					},
				},
			}

			mockLLM = func(_ context.Context, prompt string) (string, error) {
				return `{
					"description": "Filtered skill",
					"tags": ["test"],
					"content": "## Filtered\n\n1. Step one"
				}`, nil
			}
		})

		It("filters messages with --since", func() {
			since := base.Add(3 * time.Hour)
			opts := &skill.GenerateOptions{Since: &since}

			var capturedPrompt string
			captureLLM := func(_ context.Context, prompt string) (string, error) {
				capturedPrompt = prompt
				return mockLLM(context.Background(), prompt)
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

		It("filters messages with --until", func() {
			until := base.Add(3 * time.Hour)
			opts := &skill.GenerateOptions{Until: &until}

			var capturedPrompt string
			captureLLM := func(_ context.Context, prompt string) (string, error) {
				capturedPrompt = prompt
				return mockLLM(context.Background(), prompt)
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

		It("filters messages with both --since and --until", func() {
			since := base.Add(3 * time.Hour)
			until := base.Add(5 * time.Hour)
			opts := &skill.GenerateOptions{Since: &since, Until: &until}

			var capturedPrompt string
			captureLLM := func(_ context.Context, prompt string) (string, error) {
				capturedPrompt = prompt
				return mockLLM(context.Background(), prompt)
			}

			gen := skill.NewGenerator(querier, captureLLM)
			sk, err := gen.Generate(context.Background(), []string{"abc123"}, "filtered", "workflow", opts)
			Expect(err).NotTo(HaveOccurred())
			Expect(sk).NotTo(BeNil())

			// Only afternoon messages should be included
			Expect(capturedPrompt).NotTo(ContainSubstring("morning message"))
			Expect(capturedPrompt).To(ContainSubstring("afternoon message"))
			Expect(capturedPrompt).NotTo(ContainSubstring("evening message"))
		})

		It("returns an error when all messages are filtered out", func() {
			since := base.Add(24 * time.Hour) // future — filters everything
			opts := &skill.GenerateOptions{Since: &since}

			gen := skill.NewGenerator(querier, mockLLM)
			_, err := gen.Generate(context.Background(), []string{"abc123"}, "empty", "workflow", opts)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("no messages"))
		})

		It("passes nil opts through without filtering", func() {
			var capturedPrompt string
			captureLLM := func(_ context.Context, prompt string) (string, error) {
				capturedPrompt = prompt
				return mockLLM(context.Background(), prompt)
			}

			gen := skill.NewGenerator(querier, captureLLM)
			sk, err := gen.Generate(context.Background(), []string{"abc123"}, "all-messages", "workflow", nil)
			Expect(err).NotTo(HaveOccurred())
			Expect(sk).NotTo(BeNil())

			// All messages should be present
			Expect(capturedPrompt).To(ContainSubstring("morning message"))
			Expect(capturedPrompt).To(ContainSubstring("afternoon message"))
			Expect(capturedPrompt).To(ContainSubstring("evening message"))
		})
	})
})
