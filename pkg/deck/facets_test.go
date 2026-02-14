package deck

import (
	"context"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// mockQuerier implements Querier for testing facet extraction.
type mockQuerier struct {
	detail *SessionDetail
}

func (m *mockQuerier) Overview(_ context.Context, _ Filters) (*Overview, error) {
	return &Overview{}, nil
}

func (m *mockQuerier) SessionDetail(_ context.Context, _ string) (*SessionDetail, error) {
	return m.detail, nil
}

func (m *mockQuerier) AnalyticsOverview(_ context.Context, _ Filters) (*AnalyticsOverview, error) {
	return &AnalyticsOverview{ProviderBreakdown: map[string]int{}}, nil
}

func (m *mockQuerier) SessionAnalytics(_ context.Context, _ string) (*SessionAnalytics, error) {
	return &SessionAnalytics{}, nil
}

var _ = Describe("FacetExtractor", func() {
	It("extracts facets from a session using a mock LLM", func() {
		detail := &SessionDetail{
			Summary: SessionSummary{
				ID:    "session-1",
				Label: "Fix login bug",
			},
			Messages: []SessionMessage{
				{Role: "user", Text: "Fix the login page bug where users can't reset passwords"},
				{Role: "assistant", Text: "I'll look at the password reset handler and fix the issue."},
				{Role: "user", Text: "That worked, thanks!"},
			},
		}

		mockLLM := func(_ context.Context, _ string) (string, error) {
			return `{
				"underlying_goal": "Fix password reset bug on login page",
				"goal_category": "fix_bug",
				"outcome": "fully_achieved",
				"session_type": "single_task",
				"friction_types": ["buggy_code"],
				"brief_summary": "User requested a fix for the password reset feature on the login page. The assistant identified and fixed the issue."
			}`, nil
		}

		extractor := NewFacetExtractor(&mockQuerier{detail: detail}, mockLLM, nil)
		facet, err := extractor.Extract(context.Background(), "session-1")
		Expect(err).NotTo(HaveOccurred())
		Expect(facet.SessionID).To(Equal("session-1"))
		Expect(facet.GoalCategory).To(Equal("fix_bug"))
		Expect(facet.Outcome).To(Equal("fully_achieved"))
		Expect(facet.SessionType).To(Equal("single_task"))
		Expect(facet.UnderlyingGoal).To(ContainSubstring("password reset"))
		Expect(facet.FrictionTypes).To(ContainElement("buggy_code"))
		Expect(facet.BriefSummary).NotTo(BeEmpty())
		Expect(facet.ExtractedAt).NotTo(BeZero())
	})

	It("handles LLM response wrapped in markdown code blocks", func() {
		detail := &SessionDetail{
			Summary: SessionSummary{ID: "session-2"},
			Messages: []SessionMessage{
				{Role: "user", Text: "Refactor the auth module"},
			},
		}

		mockLLM := func(_ context.Context, _ string) (string, error) {
			return "```json\n" + `{
				"underlying_goal": "Refactor authentication module",
				"goal_category": "refactor_code",
				"outcome": "mostly_achieved",
				"session_type": "iterative_refinement",
				"friction_types": [],
				"brief_summary": "Refactored the auth module."
			}` + "\n```", nil
		}

		extractor := NewFacetExtractor(&mockQuerier{detail: detail}, mockLLM, nil)
		facet, err := extractor.Extract(context.Background(), "session-2")
		Expect(err).NotTo(HaveOccurred())
		Expect(facet.GoalCategory).To(Equal("refactor_code"))
		Expect(facet.Outcome).To(Equal("mostly_achieved"))
	})
})

var _ = Describe("aggregateFacets", func() {
	It("aggregates facet data from multiple sessions", func() {
		facets := []*SessionFacet{
			{
				SessionID:      "s1",
				GoalCategory:   "fix_bug",
				Outcome:        "fully_achieved",
				SessionType:    "single_task",
				FrictionTypes:  []string{"buggy_code", "wrong_approach"},
				UnderlyingGoal: "Fix login bug",
				BriefSummary:   "Fixed the login bug.",
				ExtractedAt:    time.Now(),
			},
			{
				SessionID:      "s2",
				GoalCategory:   "implement_feature",
				Outcome:        "mostly_achieved",
				SessionType:    "multi_task",
				FrictionTypes:  []string{"unclear_requirements", "wrong_approach"},
				UnderlyingGoal: "Add analytics dashboard",
				BriefSummary:   "Implemented analytics.",
				ExtractedAt:    time.Now().Add(-time.Hour),
			},
			{
				SessionID:      "s3",
				GoalCategory:   "fix_bug",
				Outcome:        "fully_achieved",
				SessionType:    "single_task",
				FrictionTypes:  []string{"buggy_code"},
				UnderlyingGoal: "Fix CSS layout",
				BriefSummary:   "Fixed CSS.",
				ExtractedAt:    time.Now().Add(-2 * time.Hour),
			},
		}

		analytics := aggregateFacets(facets)

		Expect(analytics.GoalDistribution["fix_bug"]).To(Equal(2))
		Expect(analytics.GoalDistribution["implement_feature"]).To(Equal(1))
		Expect(analytics.OutcomeDistribution["fully_achieved"]).To(Equal(2))
		Expect(analytics.OutcomeDistribution["mostly_achieved"]).To(Equal(1))
		Expect(analytics.SessionTypes["single_task"]).To(Equal(2))
		Expect(analytics.SessionTypes["multi_task"]).To(Equal(1))

		// Friction: wrong_approach=2, buggy_code=2, unclear_requirements=1
		Expect(analytics.TopFriction).To(HaveLen(3))
		frictionMap := map[string]int{}
		for _, f := range analytics.TopFriction {
			frictionMap[f.Type] = f.Count
		}
		Expect(frictionMap["wrong_approach"]).To(Equal(2))
		Expect(frictionMap["buggy_code"]).To(Equal(2))
		Expect(frictionMap["unclear_requirements"]).To(Equal(1))

		Expect(analytics.RecentSummaries).To(HaveLen(3))
		// Most recent first
		Expect(analytics.RecentSummaries[0].SessionID).To(Equal("s1"))
	})

	It("handles empty facets list", func() {
		analytics := aggregateFacets([]*SessionFacet{})

		Expect(analytics.GoalDistribution).To(BeEmpty())
		Expect(analytics.OutcomeDistribution).To(BeEmpty())
		Expect(analytics.SessionTypes).To(BeEmpty())
		Expect(analytics.TopFriction).To(BeEmpty())
		Expect(analytics.RecentSummaries).To(BeEmpty())
	})
})

var _ = Describe("parseFacetResponse", func() {
	It("parses valid JSON", func() {
		input := `{"underlying_goal":"Test","goal_category":"fix_bug","outcome":"fully_achieved","session_type":"single_task","friction_types":[],"brief_summary":"Test summary"}`
		facet, err := parseFacetResponse(input)
		Expect(err).NotTo(HaveOccurred())
		Expect(facet.GoalCategory).To(Equal("fix_bug"))
		Expect(facet.BriefSummary).To(Equal("Test summary"))
	})

	It("extracts JSON from surrounding text", func() {
		input := `Here is the analysis:\n{"underlying_goal":"Test","goal_category":"debug_investigate","outcome":"not_achieved","session_type":"exploration","friction_types":["tool_failure"],"brief_summary":"Debug session"}\nDone.`
		facet, err := parseFacetResponse(input)
		Expect(err).NotTo(HaveOccurred())
		Expect(facet.GoalCategory).To(Equal("debug_investigate"))
	})
})

var _ = Describe("buildTranscript", func() {
	It("formats messages into a readable transcript", func() {
		detail := &SessionDetail{
			Messages: []SessionMessage{
				{Role: "user", Text: "Hello"},
				{Role: "assistant", Text: "Hi there"},
			},
		}
		transcript := buildTranscript(detail)
		Expect(transcript).To(ContainSubstring("[user] Hello"))
		Expect(transcript).To(ContainSubstring("[assistant] Hi there"))
	})
})
