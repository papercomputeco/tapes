package deckcmder

import (
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/deck"
)

var _ = Describe("Deck TUI helpers", func() {
	Describe("summarizeSessions", func() {
		It("rolls up totals and model costs", func() {
			sessions := []deck.SessionSummary{
				{
					ID:           "s1",
					Model:        "m1",
					Status:       deck.StatusCompleted,
					Duration:     2 * time.Minute,
					InputTokens:  100,
					OutputTokens: 50,
					InputCost:    0.10,
					OutputCost:   0.20,
					TotalCost:    0.30,
					ToolCalls:    2,
				},
				{
					ID:           "s2",
					Model:        "m2",
					Status:       deck.StatusFailed,
					Duration:     1 * time.Minute,
					InputTokens:  10,
					OutputTokens: 5,
					InputCost:    0.05,
					OutputCost:   0.05,
					TotalCost:    0.10,
					ToolCalls:    1,
				},
				{
					ID:           "s3",
					Model:        "m1",
					Status:       deck.StatusAbandoned,
					Duration:     3 * time.Minute,
					InputTokens:  20,
					OutputTokens: 30,
					InputCost:    0.02,
					OutputCost:   0.03,
					TotalCost:    0.05,
					ToolCalls:    0,
				},
			}

			stats := summarizeSessions(sessions)
			Expect(stats.TotalSessions).To(Equal(3))
			Expect(stats.TotalCost).To(BeNumerically("~", 0.45, 0.0001))
			Expect(stats.InputTokens).To(Equal(int64(130)))
			Expect(stats.OutputTokens).To(Equal(int64(85)))
			Expect(stats.TotalDuration).To(Equal(6 * time.Minute))
			Expect(stats.TotalToolCalls).To(Equal(3))
			Expect(stats.Completed).To(Equal(1))
			Expect(stats.Failed).To(Equal(1))
			Expect(stats.Abandoned).To(Equal(1))
			Expect(stats.SuccessRate).To(BeNumerically("~", 1.0/3.0, 0.0001))
			Expect(stats.CostByModel).To(HaveKey("m1"))
			Expect(stats.CostByModel).To(HaveKey("m2"))
			Expect(stats.CostByModel["m1"].SessionCount).To(Equal(2))
			Expect(stats.CostByModel["m1"].TotalCost).To(BeNumerically("~", 0.35, 0.0001))
			Expect(stats.CostByModel["m2"].SessionCount).To(Equal(1))
			Expect(stats.CostByModel["m2"].TotalCost).To(BeNumerically("~", 0.10, 0.0001))
		})
	})

	Describe("selectedSessions", func() {
		It("returns all sessions", func() {
			sessions := []deck.SessionSummary{{ID: "s1"}, {ID: "s2"}, {ID: "s3"}}
			model := deckModel{
				overview: &deck.Overview{Sessions: sessions},
			}

			selected := model.selectedSessions()
			Expect(selected).To(HaveLen(3))
		})
	})

	Describe("sortedMessages", func() {
		var messages []deck.SessionMessage

		BeforeEach(func() {
			base := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)
			messages = []deck.SessionMessage{
				{
					Hash:        "m1",
					Timestamp:   base.Add(1 * time.Second),
					TotalTokens: 100,
					TotalCost:   0.20,
					Delta:       5 * time.Second,
				},
				{
					Hash:        "m2",
					Timestamp:   base.Add(2 * time.Second),
					TotalTokens: 50,
					TotalCost:   0.30,
					Delta:       1 * time.Second,
				},
				{
					Hash:        "m3",
					Timestamp:   base.Add(3 * time.Second),
					TotalTokens: 200,
					TotalCost:   0.10,
					Delta:       10 * time.Second,
				},
			}
		})

		It("sorts by time", func() {
			model := deckModel{
				detail:      &deck.SessionDetail{Messages: messages},
				messageSort: 0,
			}
			ordered := model.sortedMessages()
			Expect(ordered).To(HaveLen(3))
			Expect(ordered[0].Hash).To(Equal("m1"))
			Expect(ordered[1].Hash).To(Equal("m2"))
			Expect(ordered[2].Hash).To(Equal("m3"))
		})

		It("sorts by tokens", func() {
			model := deckModel{
				detail:      &deck.SessionDetail{Messages: messages},
				messageSort: 1,
			}
			ordered := model.sortedMessages()
			Expect(ordered[0].Hash).To(Equal("m3"))
			Expect(ordered[1].Hash).To(Equal("m1"))
			Expect(ordered[2].Hash).To(Equal("m2"))
		})

		It("sorts by cost", func() {
			model := deckModel{
				detail:      &deck.SessionDetail{Messages: messages},
				messageSort: 2,
			}
			ordered := model.sortedMessages()
			Expect(ordered[0].Hash).To(Equal("m2"))
			Expect(ordered[1].Hash).To(Equal("m1"))
			Expect(ordered[2].Hash).To(Equal("m3"))
		})

		It("sorts by delta", func() {
			model := deckModel{
				detail:      &deck.SessionDetail{Messages: messages},
				messageSort: 3,
			}
			ordered := model.sortedMessages()
			Expect(ordered[0].Hash).To(Equal("m3"))
			Expect(ordered[1].Hash).To(Equal("m1"))
			Expect(ordered[2].Hash).To(Equal("m2"))
		})
	})

	Describe("stableVisibleRange", func() {
		It("keeps offset stable when cursor is within view", func() {
			start, end, offset := stableVisibleRange(10, 5, 4, 3)
			Expect(start).To(Equal(3))
			Expect(end).To(Equal(7))
			Expect(offset).To(Equal(3))
		})

		It("scrolls down when cursor moves below visible window", func() {
			start, end, offset := stableVisibleRange(10, 7, 4, 3)
			Expect(start).To(Equal(4))
			Expect(end).To(Equal(8))
			Expect(offset).To(Equal(4))
		})

		It("scrolls up when cursor moves above visible window", func() {
			start, end, offset := stableVisibleRange(10, 2, 4, 5)
			Expect(start).To(Equal(2))
			Expect(end).To(Equal(6))
			Expect(offset).To(Equal(2))
		})

		It("clamps to the start", func() {
			start, end, offset := stableVisibleRange(10, 0, 3, 0)
			Expect(start).To(Equal(0))
			Expect(end).To(Equal(3))
			Expect(offset).To(Equal(0))
		})

		It("clamps to the end", func() {
			start, end, offset := stableVisibleRange(10, 9, 3, 0)
			Expect(start).To(Equal(7))
			Expect(end).To(Equal(10))
			Expect(offset).To(Equal(7))
		})
	})

	Describe("countWrappedLines", func() {
		It("returns zero for empty strings", func() {
			Expect(countWrappedLines("", 10)).To(Equal(0))
		})

		It("counts wrapped lines based on width", func() {
			Expect(countWrappedLines("123456789", 4)).To(Equal(3))
		})

		It("counts blank lines", func() {
			Expect(countWrappedLines("a\n\nb", 10)).To(Equal(3))
		})
	})
})
