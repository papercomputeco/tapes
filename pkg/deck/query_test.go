package deck

import (
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/storage/ent"
)

var _ = Describe("Session labels", func() {
	It("builds labels from the most recent user prompts", func() {
		lineOne := "Investigate session titles"
		lineTwo := "Add label logic"
		lineThree := "Write label test"

		nodes := []*ent.Node{
			{
				ID:   "node-1",
				Role: "user",
				Content: []map[string]any{{
					"text": "checkout main and pull latest",
					"type": "text",
				}},
			},
			{ID: "node-2", Role: "assistant"},
			{
				ID:   "node-3",
				Role: "user",
				Content: []map[string]any{{
					"text": lineOne,
					"type": "text",
				}},
			},
			{
				ID:   "node-4",
				Role: "user",
				Content: []map[string]any{{
					"text": "Command: git checkout main && git pull",
					"type": "text",
				}},
			},
			{
				ID:   "node-5",
				Role: "user",
				Content: []map[string]any{{
					"text": lineTwo,
					"type": "text",
				}},
			},
			{ID: "node-6", Role: "assistant"},
			{
				ID:   "node-7",
				Role: "user",
				Content: []map[string]any{{
					"text": lineThree,
					"type": "text",
				}},
			},
			{ID: "node-8", Role: "assistant"},
		}

		expected := truncate(strings.Join([]string{lineOne, lineTwo, lineThree}, " / "), 36)
		label := buildLabel(nodes)

		Expect(label).To(Equal(expected))
		Expect(label).NotTo(ContainSubstring("checkout main"))
		Expect(label).NotTo(ContainSubstring("Command:"))
	})
})

var _ = Describe("Analytics helper functions", func() {
	Describe("buildDurationBuckets", func() {
		It("distributes sessions into correct duration buckets", func() {
			buckets := []Bucket{
				{Label: "<1m"},
				{Label: "1-5m"},
				{Label: "5-15m"},
				{Label: "15-30m"},
				{Label: "30-60m"},
				{Label: ">1h"},
			}
			Expect(buckets).To(HaveLen(6))
			Expect(buckets[0].Label).To(Equal("<1m"))
			Expect(buckets[5].Label).To(Equal(">1h"))
		})
	})

	Describe("buildCostBuckets", func() {
		It("defines correct cost bucket labels", func() {
			buckets := []Bucket{
				{Label: "<$0.01"},
				{Label: "$0.01-0.10"},
				{Label: "$0.10-0.50"},
				{Label: "$0.50-1.00"},
				{Label: "$1.00-5.00"},
				{Label: ">$5.00"},
			}
			Expect(buckets).To(HaveLen(6))
			Expect(buckets[0].Label).To(Equal("<$0.01"))
		})
	})

	Describe("modelAccumulator", func() {
		It("tracks model performance metrics", func() {
			acc := &modelAccumulator{
				provider:        "anthropic",
				sessions:        3,
				totalCost:       1.50,
				totalDurationNs: int64(5 * time.Minute),
				totalTokens:     15000,
				completedCount:  2,
			}

			Expect(acc.sessions).To(Equal(3))
			Expect(acc.totalCost).To(BeNumerically("~", 1.50, 0.001))
			Expect(acc.completedCount).To(Equal(2))

			avgCost := acc.totalCost / float64(acc.sessions)
			Expect(avgCost).To(BeNumerically("~", 0.50, 0.001))

			successRate := float64(acc.completedCount) / float64(acc.sessions)
			Expect(successRate).To(BeNumerically("~", 0.667, 0.01))
		})
	})

	Describe("AnalyticsOverview types", func() {
		It("constructs a valid AnalyticsOverview", func() {
			overview := &AnalyticsOverview{
				TotalSessions:  5,
				AvgSessionCost: 0.75,
				AvgDurationNs:  int64(10 * time.Minute),
				TopTools: []ToolMetric{
					{Name: "Read", Count: 50, ErrorCount: 2, Sessions: 4},
					{Name: "Write", Count: 30, ErrorCount: 0, Sessions: 3},
				},
				ActivityByDay: []DayActivity{
					{Date: "2026-02-10", Sessions: 3, Cost: 2.25, Tokens: 45000},
					{Date: "2026-02-11", Sessions: 2, Cost: 1.50, Tokens: 30000},
				},
				DurationBuckets: []Bucket{
					{Label: "<1m", Count: 1},
					{Label: "1-5m", Count: 2},
					{Label: "5-15m", Count: 2},
				},
				CostBuckets: []Bucket{
					{Label: "<$0.01", Count: 0},
					{Label: "$0.01-0.10", Count: 1},
					{Label: "$0.10-0.50", Count: 3},
				},
				ModelPerformance: []ModelPerformance{
					{
						Model:          "claude-sonnet-4.5",
						Provider:       "anthropic",
						Sessions:       3,
						AvgCost:        0.80,
						AvgDurationNs:  int64(8 * time.Minute),
						AvgTokens:      12000,
						TotalCost:      2.40,
						SuccessRate:    0.67,
						CompletedCount: 2,
					},
				},
				ProviderBreakdown: map[string]int{
					"anthropic": 4,
					"openai":    1,
				},
			}

			Expect(overview.TotalSessions).To(Equal(5))
			Expect(overview.TopTools).To(HaveLen(2))
			Expect(overview.TopTools[0].Name).To(Equal("Read"))
			Expect(overview.ActivityByDay).To(HaveLen(2))
			Expect(overview.ModelPerformance[0].Model).To(Equal("claude-sonnet-4.5"))
			Expect(overview.ProviderBreakdown["anthropic"]).To(Equal(4))
		})
	})

	Describe("SessionAnalytics types", func() {
		It("constructs a valid SessionAnalytics", func() {
			sa := &SessionAnalytics{
				SessionID:         "test-session-1",
				UserMessageCount:  5,
				AssistantMsgCount: 5,
				AvgResponseTimeNs: int64(3 * time.Second),
				LongestPauseNs:    int64(10 * time.Second),
				UniqueTools:       4,
				ToolErrorCount:    1,
				TokensPerMinute:   2500.0,
				AvgPromptLength:   150,
				AvgResponseLength: 500,
				FirstPrompt:       "Fix the login bug",
			}

			Expect(sa.SessionID).To(Equal("test-session-1"))
			Expect(sa.UserMessageCount).To(Equal(5))
			Expect(sa.AssistantMsgCount).To(Equal(5))
			Expect(sa.UniqueTools).To(Equal(4))
			Expect(sa.ToolErrorCount).To(Equal(1))
			Expect(sa.TokensPerMinute).To(BeNumerically(">", 0))
			Expect(sa.FirstPrompt).To(Equal("Fix the login bug"))
		})
	})
})
