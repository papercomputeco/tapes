package deck

import (
	"strings"
	"sync"
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

var _ = Describe("Empty-model cost fallback", func() {
	intPtr := func(v int) *int { return &v }

	It("uses last-seen model for response nodes with empty model", func() {
		pricing := DefaultPricing()
		q := &Query{pricing: pricing}

		nodes := []*ent.Node{
			{
				ID:    "node-1",
				Role:  "user",
				Model: "claude-opus-4-6-20260219",
				Content: []map[string]any{{
					"text": "Hello",
					"type": "text",
				}},
				PromptTokens:     intPtr(100),
				CompletionTokens: intPtr(0),
			},
			{
				ID:               "node-2",
				Role:             "assistant",
				Model:            "", // empty model — the bug
				Content:          []map[string]any{{"text": "Hi!", "type": "text"}},
				PromptTokens:     intPtr(0),
				CompletionTokens: intPtr(50),
			},
		}

		summary, modelCosts, _, err := q.buildSessionSummaryFromNodes(nodes)
		Expect(err).NotTo(HaveOccurred())

		// The assistant node should have been costed using the user node's model
		Expect(summary.TotalCost).To(BeNumerically(">", 0))
		Expect(modelCosts).To(HaveKey("claude-opus-4.6"))
		cost := modelCosts["claude-opus-4.6"]
		Expect(cost.OutputTokens).To(Equal(int64(50)))
		Expect(cost.TotalCost).To(BeNumerically(">", 0))
	})

	It("keeps summary and message totals consistent", func() {
		pricing := DefaultPricing()
		q := &Query{pricing: pricing}

		nodes := []*ent.Node{
			{
				ID:    "node-1",
				Role:  "user",
				Model: "claude-opus-4-6-20260219",
				Content: []map[string]any{{
					"text": "Hello",
					"type": "text",
				}},
				PromptTokens: intPtr(100),
			},
			{
				ID:               "node-2",
				Role:             "assistant",
				Model:            "",
				Content:          []map[string]any{{"text": "Hi!", "type": "text"}},
				CompletionTokens: intPtr(500000),
			},
		}

		summary, _, _, err := q.buildSessionSummaryFromNodes(nodes)
		Expect(err).NotTo(HaveOccurred())

		messages, _ := q.buildSessionMessages(nodes)
		messageTotal := 0.0
		for _, msg := range messages {
			messageTotal += msg.TotalCost
		}

		Expect(summary.TotalCost).To(BeNumerically("~", messageTotal, 1e-12))
	})

	It("skips nodes when no model has been seen yet", func() {
		pricing := DefaultPricing()
		q := &Query{pricing: pricing}

		nodes := []*ent.Node{
			{
				ID:               "node-1",
				Role:             "assistant",
				Model:            "", // no model, and no prior model
				Content:          []map[string]any{{"text": "orphan", "type": "text"}},
				CompletionTokens: intPtr(50),
			},
		}

		_, modelCosts, _, err := q.buildSessionSummaryFromNodes(nodes)
		Expect(err).NotTo(HaveOccurred())
		Expect(modelCosts).To(BeEmpty())
	})
})

var _ = Describe("buildCandidateIndex", func() {
	It("indexes candidates by session ID", func() {
		candidates := []sessionCandidate{
			{summary: SessionSummary{ID: "s1"}},
			{summary: SessionSummary{ID: "s2"}},
			{summary: SessionSummary{ID: "s3"}},
		}

		idx := buildCandidateIndex(candidates)
		Expect(idx).To(HaveLen(3))
		Expect(idx["s1"].summary.ID).To(Equal("s1"))
		Expect(idx["s2"].summary.ID).To(Equal("s2"))
		Expect(idx["s3"].summary.ID).To(Equal("s3"))
	})

	It("returns an empty map for empty input", func() {
		idx := buildCandidateIndex(nil)
		Expect(idx).To(HaveLen(0))
	})

	It("points into the original slice", func() {
		candidates := []sessionCandidate{
			{summary: SessionSummary{ID: "s1", Label: "original"}},
		}
		idx := buildCandidateIndex(candidates)
		// Mutate the slice element; index pointer should see it
		candidates[0].summary.Label = "mutated"
		Expect(idx["s1"].summary.Label).To(Equal("mutated"))
	})
})

var _ = Describe("candidateByID", func() {
	candidates := []sessionCandidate{
		{summary: SessionSummary{ID: "aaa"}},
		{summary: SessionSummary{ID: "bbb"}},
		{summary: SessionSummary{ID: "ccc"}},
	}

	It("finds an existing candidate", func() {
		c, ok := candidateByID(candidates, "bbb")
		Expect(ok).To(BeTrue())
		Expect(c.summary.ID).To(Equal("bbb"))
	})

	It("returns false for a missing ID", func() {
		_, ok := candidateByID(candidates, "zzz")
		Expect(ok).To(BeFalse())
	})

	It("returns false for empty slice", func() {
		_, ok := candidateByID(nil, "aaa")
		Expect(ok).To(BeFalse())
	})
})

var _ = Describe("sessionCache", func() {
	newQuery := func() *Query {
		return &Query{pricing: DefaultPricing()}
	}

	now := time.Date(2026, 2, 20, 12, 0, 0, 0, time.UTC)

	makeCandidates := func(ids ...string) []sessionCandidate {
		out := make([]sessionCandidate, len(ids))
		for i, id := range ids {
			out[i] = sessionCandidate{
				summary: SessionSummary{
					ID:        id,
					Label:     "session " + id,
					Status:    StatusCompleted,
					StartTime: now,
					EndTime:   now.Add(5 * time.Minute),
				},
				nodes: []*ent.Node{
					{ID: id, Role: "user", CreatedAt: now},
				},
			}
		}
		return out
	}

	Describe("storeSessionCandidates and cachedSessionCandidates", func() {
		It("stores and retrieves candidates", func() {
			q := newQuery()
			q.storeSessionCandidates(makeCandidates("s1", "s2"))

			cached := q.cachedSessionCandidates()
			Expect(cached).To(HaveLen(2))
			Expect(cached[0].summary.ID).To(Equal("s1"))
			Expect(cached[1].summary.ID).To(Equal("s2"))
		})

		It("returns nil when cache is empty", func() {
			q := newQuery()
			Expect(q.cachedSessionCandidates()).To(BeNil())
		})

		It("returns nil when cache is stale", func() {
			q := newQuery()
			q.storeSessionCandidates(makeCandidates("s1"))

			// Backdate loadedAt beyond the TTL
			q.cache.mu.Lock()
			q.cache.loadedAt = time.Now().Add(-sessionCacheTTL - time.Second)
			q.cache.mu.Unlock()

			Expect(q.cachedSessionCandidates()).To(BeNil())
		})

		It("returns a copy that does not mutate the cache", func() {
			q := newQuery()
			q.storeSessionCandidates(makeCandidates("s1", "s2"))

			cached := q.cachedSessionCandidates()
			cached[0].summary.Label = "mutated"

			fresh := q.cachedSessionCandidates()
			Expect(fresh[0].summary.Label).To(Equal("session s1"))
		})
	})

	Describe("cachedSessionCandidate (by ID)", func() {
		It("returns a candidate by ID from the index", func() {
			q := newQuery()
			q.storeSessionCandidates(makeCandidates("s1", "s2", "s3"))

			c := q.cachedSessionCandidate("s2")
			Expect(c).NotTo(BeNil())
			Expect(c.summary.ID).To(Equal("s2"))
		})

		It("returns nil for a missing ID", func() {
			q := newQuery()
			q.storeSessionCandidates(makeCandidates("s1"))

			Expect(q.cachedSessionCandidate("zzz")).To(BeNil())
		})

		It("returns nil when cache is empty", func() {
			q := newQuery()
			Expect(q.cachedSessionCandidate("s1")).To(BeNil())
		})

		It("returns nil when cache is stale", func() {
			q := newQuery()
			q.storeSessionCandidates(makeCandidates("s1"))

			q.cache.mu.Lock()
			q.cache.loadedAt = time.Now().Add(-sessionCacheTTL - time.Second)
			q.cache.mu.Unlock()

			Expect(q.cachedSessionCandidate("s1")).To(BeNil())
		})

		It("returns a shallow copy (struct value) not the cached pointer", func() {
			q := newQuery()
			q.storeSessionCandidates(makeCandidates("s1"))

			c := q.cachedSessionCandidate("s1")
			c.summary.Label = "mutated"

			c2 := q.cachedSessionCandidate("s1")
			Expect(c2.summary.Label).To(Equal("session s1"))
		})
	})

	Describe("storeSessionCandidates builds the byID index", func() {
		It("populates byID from stored candidates", func() {
			q := newQuery()
			q.storeSessionCandidates(makeCandidates("x", "y"))

			q.cache.mu.RLock()
			defer q.cache.mu.RUnlock()
			Expect(q.cache.byID).To(HaveLen(2))
			Expect(q.cache.byID).To(HaveKey("x"))
			Expect(q.cache.byID).To(HaveKey("y"))
		})

		It("replaces the previous index on re-store", func() {
			q := newQuery()
			q.storeSessionCandidates(makeCandidates("a", "b"))
			q.storeSessionCandidates(makeCandidates("c"))

			q.cache.mu.RLock()
			defer q.cache.mu.RUnlock()
			Expect(q.cache.byID).To(HaveLen(1))
			Expect(q.cache.byID).To(HaveKey("c"))
			Expect(q.cache.byID).NotTo(HaveKey("a"))
		})
	})

	Describe("concurrent access", func() {
		It("handles concurrent reads and writes without panic", func() {
			q := newQuery()
			q.storeSessionCandidates(makeCandidates("s1", "s2"))

			var wg sync.WaitGroup
			for range 50 {
				wg.Add(2)
				go func() {
					defer wg.Done()
					_ = q.cachedSessionCandidates()
				}()
				go func() {
					defer wg.Done()
					_ = q.cachedSessionCandidate("s1")
				}()
			}
			// Also do some writes concurrently
			for range 10 {
				wg.Go(func() {
					q.storeSessionCandidates(makeCandidates("s1", "s2"))
				})
			}
			wg.Wait()
		})
	})
})

var _ = Describe("buildSessionMessages", func() {
	now := time.Date(2026, 2, 20, 12, 0, 0, 0, time.UTC)

	It("builds messages with correct deltas", func() {
		q := &Query{pricing: DefaultPricing()}
		nodes := []*ent.Node{
			{
				ID:        "n1",
				Role:      "user",
				Model:     "claude-sonnet-4.5",
				CreatedAt: now,
				Content:   []map[string]any{{"text": "hello", "type": "text"}},
			},
			{
				ID:        "n2",
				Role:      "assistant",
				Model:     "claude-sonnet-4.5",
				CreatedAt: now.Add(3 * time.Second),
				Content:   []map[string]any{{"text": "hi there", "type": "text"}},
			},
			{
				ID:        "n3",
				Role:      "user",
				Model:     "claude-sonnet-4.5",
				CreatedAt: now.Add(10 * time.Second),
				Content:   []map[string]any{{"text": "thanks", "type": "text"}},
			},
		}

		messages, toolFreq := q.buildSessionMessages(nodes)
		Expect(messages).To(HaveLen(3))
		Expect(toolFreq).To(BeEmpty())

		Expect(messages[0].Delta).To(Equal(time.Duration(0)))
		Expect(messages[1].Delta).To(Equal(3 * time.Second))
		Expect(messages[2].Delta).To(Equal(7 * time.Second))

		Expect(messages[0].Role).To(Equal("user"))
		Expect(messages[1].Role).To(Equal("assistant"))
		Expect(messages[0].Text).To(Equal("hello"))
		Expect(messages[1].Text).To(Equal("hi there"))
	})

	It("counts tool calls in frequency map", func() {
		q := &Query{pricing: DefaultPricing()}
		nodes := []*ent.Node{
			{
				ID:        "n1",
				Role:      "assistant",
				CreatedAt: now,
				Content: []map[string]any{
					{"type": "tool_use", "tool_name": "Read"},
					{"type": "tool_use", "tool_name": "Grep"},
					{"type": "tool_use", "tool_name": "Read"},
				},
			},
		}

		_, toolFreq := q.buildSessionMessages(nodes)
		Expect(toolFreq["Read"]).To(Equal(2))
		Expect(toolFreq["Grep"]).To(Equal(1))
	})

	It("returns empty results for empty nodes", func() {
		q := &Query{pricing: DefaultPricing()}
		messages, toolFreq := q.buildSessionMessages(nil)
		Expect(messages).To(BeEmpty())
		Expect(toolFreq).To(BeEmpty())
	})
})

var _ = Describe("buildGroupedMessages", func() {
	now := time.Date(2026, 2, 20, 12, 0, 0, 0, time.UTC)

	It("groups consecutive same-role messages within the time window", func() {
		messages := []SessionMessage{
			{Role: "user", Timestamp: now, Text: "hi"},
			{Role: "user", Timestamp: now.Add(2 * time.Second), Text: "more"},
			{Role: "assistant", Timestamp: now.Add(5 * time.Second), Text: "reply"},
		}

		groups := buildGroupedMessages(messages)
		Expect(groups).To(HaveLen(2))
		Expect(groups[0].Role).To(Equal("user"))
		Expect(groups[0].Count).To(Equal(2))
		Expect(groups[0].StartIndex).To(Equal(0))
		Expect(groups[0].EndIndex).To(Equal(2))
		Expect(groups[1].Role).To(Equal("assistant"))
		Expect(groups[1].Count).To(Equal(1))
	})

	It("splits same-role messages that exceed the time window", func() {
		messages := []SessionMessage{
			{Role: "user", Timestamp: now, Text: "first"},
			{Role: "user", Timestamp: now.Add(10 * time.Second), Text: "second"},
		}

		groups := buildGroupedMessages(messages)
		Expect(groups).To(HaveLen(2))
		Expect(groups[0].Text).To(Equal("first"))
		Expect(groups[1].Text).To(Equal("second"))
	})

	It("returns nil for empty input", func() {
		Expect(buildGroupedMessages(nil)).To(BeNil())
	})

	It("accumulates tokens and costs across grouped messages", func() {
		messages := []SessionMessage{
			{Role: "assistant", Timestamp: now, InputTokens: 100, OutputTokens: 200, InputCost: 0.01, OutputCost: 0.02, TotalCost: 0.03},
			{Role: "assistant", Timestamp: now.Add(1 * time.Second), InputTokens: 150, OutputTokens: 300, InputCost: 0.015, OutputCost: 0.03, TotalCost: 0.045},
		}

		groups := buildGroupedMessages(messages)
		Expect(groups).To(HaveLen(1))
		Expect(groups[0].InputTokens).To(Equal(int64(250)))
		Expect(groups[0].OutputTokens).To(Equal(int64(500)))
		Expect(groups[0].TotalCost).To(BeNumerically("~", 0.075, 1e-12))
	})

	It("computes deltas between groups", func() {
		messages := []SessionMessage{
			{Role: "user", Timestamp: now},
			{Role: "assistant", Timestamp: now.Add(5 * time.Second)},
			{Role: "user", Timestamp: now.Add(20 * time.Second)},
		}

		groups := buildGroupedMessages(messages)
		Expect(groups).To(HaveLen(3))
		Expect(groups[0].Delta).To(Equal(time.Duration(0)))
		Expect(groups[1].Delta).To(Equal(5 * time.Second))
		Expect(groups[2].Delta).To(Equal(15 * time.Second))
	})
})

var _ = Describe("matchesFilters", func() {
	now := time.Date(2026, 2, 20, 12, 0, 0, 0, time.UTC)

	base := SessionSummary{
		ID:        "s1",
		Model:     "claude-sonnet-4.5",
		Status:    StatusCompleted,
		Project:   "tapes",
		StartTime: now,
		EndTime:   now.Add(10 * time.Minute),
	}

	It("matches when no filters are set", func() {
		Expect(matchesFilters(base, Filters{})).To(BeTrue())
	})

	It("filters by model (normalized)", func() {
		Expect(matchesFilters(base, Filters{Model: "claude-sonnet-4-5"})).To(BeTrue())
		Expect(matchesFilters(base, Filters{Model: "gpt-4o"})).To(BeFalse())
	})

	It("filters by status", func() {
		Expect(matchesFilters(base, Filters{Status: StatusCompleted})).To(BeTrue())
		Expect(matchesFilters(base, Filters{Status: StatusFailed})).To(BeFalse())
	})

	It("filters by project", func() {
		Expect(matchesFilters(base, Filters{Project: "tapes"})).To(BeTrue())
		Expect(matchesFilters(base, Filters{Project: "other"})).To(BeFalse())
	})

	It("filters by from/to time range", func() {
		from := now.Add(-1 * time.Hour)
		to := now.Add(1 * time.Hour)
		Expect(matchesFilters(base, Filters{From: &from, To: &to})).To(BeTrue())

		// Session ends before the From filter
		early := now.Add(15 * time.Minute)
		Expect(matchesFilters(base, Filters{From: &early})).To(BeFalse())

		// Session starts after the To filter
		before := now.Add(-1 * time.Minute)
		Expect(matchesFilters(base, Filters{To: &before})).To(BeFalse())
	})
})

var _ = Describe("appendGroupedText", func() {
	It("returns next when current is empty", func() {
		Expect(appendGroupedText("", "hello")).To(Equal("hello"))
	})

	It("returns current when next is empty", func() {
		Expect(appendGroupedText("hello", "")).To(Equal("hello"))
	})

	It("joins with double newline separator", func() {
		result := appendGroupedText("first", "second")
		Expect(result).To(Equal("first\n\nsecond"))
	})

	It("stops appending when current is at max length", func() {
		current := strings.Repeat("a", maxGroupedTextChars)
		result := appendGroupedText(current, "more")
		Expect(result).To(Equal(current))
	})

	It("truncates next when remaining space is limited", func() {
		current := strings.Repeat("a", maxGroupedTextChars-10)
		result := appendGroupedText(current, strings.Repeat("b", 100))
		Expect(len(result)).To(BeNumerically("<=", maxGroupedTextChars+10))
	})
})

var _ = Describe("truncateGroupedText", func() {
	It("returns empty for empty input", func() {
		Expect(truncateGroupedText("")).To(Equal(""))
	})

	It("returns short text unchanged", func() {
		Expect(truncateGroupedText("hello")).To(Equal("hello"))
	})

	It("truncates text over the limit with ellipsis", func() {
		long := strings.Repeat("x", maxGroupedTextChars+100)
		result := truncateGroupedText(long)
		Expect(result).To(HaveSuffix("..."))
		Expect(len(result)).To(Equal(maxGroupedTextChars + 3))
	})
})
