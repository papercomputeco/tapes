package deck

import (
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Session grouping", func() {
	now := time.Date(2026, 2, 19, 12, 0, 0, 0, time.UTC)

	Describe("groupSessionCandidates", func() {
		It("groups candidates with the same label/agent/project within the time window", func() {
			candidates := []sessionCandidate{
				{summary: SessionSummary{ID: "a", Label: "fix bug", AgentName: "claude", Project: "tapes", StartTime: now, EndTime: now.Add(5 * time.Minute), Status: StatusCompleted}},
				{summary: SessionSummary{ID: "b", Label: "fix bug", AgentName: "claude", Project: "tapes", StartTime: now.Add(10 * time.Minute), EndTime: now.Add(15 * time.Minute), Status: StatusCompleted}},
			}

			groups := groupSessionCandidates(candidates)
			Expect(groups).To(HaveLen(1))
			Expect(groups[0].members).To(HaveLen(2))
			Expect(groups[0].summary.SessionCount).To(Equal(2))
		})

		It("keeps candidates in separate groups when the gap exceeds the window", func() {
			candidates := []sessionCandidate{
				{summary: SessionSummary{ID: "a", Label: "fix bug", StartTime: now, EndTime: now.Add(5 * time.Minute), Status: StatusCompleted}},
				{summary: SessionSummary{ID: "b", Label: "fix bug", StartTime: now.Add(2 * time.Hour), EndTime: now.Add(2*time.Hour + 5*time.Minute), Status: StatusCompleted}},
			}

			groups := groupSessionCandidates(candidates)
			Expect(groups).To(HaveLen(2))
		})

		It("keeps candidates with different labels in separate groups", func() {
			candidates := []sessionCandidate{
				{summary: SessionSummary{ID: "a", Label: "fix bug", StartTime: now, EndTime: now.Add(5 * time.Minute), Status: StatusCompleted}},
				{summary: SessionSummary{ID: "b", Label: "add feature", StartTime: now.Add(1 * time.Minute), EndTime: now.Add(6 * time.Minute), Status: StatusCompleted}},
			}

			groups := groupSessionCandidates(candidates)
			Expect(groups).To(HaveLen(2))
		})

		It("does not mutate the original slice order", func() {
			candidates := []sessionCandidate{
				{summary: SessionSummary{ID: "b", Label: "second", StartTime: now.Add(time.Hour), EndTime: now.Add(2 * time.Hour), Status: StatusCompleted}},
				{summary: SessionSummary{ID: "a", Label: "first", StartTime: now, EndTime: now.Add(time.Minute), Status: StatusCompleted}},
			}

			groupSessionCandidates(candidates)
			Expect(candidates[0].summary.ID).To(Equal("b"))
			Expect(candidates[1].summary.ID).To(Equal("a"))
		})

		It("aggregates tokens and costs across grouped members", func() {
			candidates := []sessionCandidate{
				{summary: SessionSummary{ID: "a", Label: "task", StartTime: now, EndTime: now.Add(5 * time.Minute), InputTokens: 100, OutputTokens: 200, TotalCost: 0.50, Status: StatusCompleted}},
				{summary: SessionSummary{ID: "b", Label: "task", StartTime: now.Add(10 * time.Minute), EndTime: now.Add(15 * time.Minute), InputTokens: 150, OutputTokens: 300, TotalCost: 0.75, Status: StatusCompleted}},
			}

			groups := groupSessionCandidates(candidates)
			Expect(groups).To(HaveLen(1))
			Expect(groups[0].summary.InputTokens).To(Equal(int64(250)))
			Expect(groups[0].summary.OutputTokens).To(Equal(int64(500)))
			Expect(groups[0].summary.TotalCost).To(BeNumerically("~", 1.25, 0.001))
		})

		It("summarizes status with failed taking priority", func() {
			candidates := []sessionCandidate{
				{summary: SessionSummary{ID: "a", Label: "task", StartTime: now, EndTime: now.Add(5 * time.Minute), Status: StatusCompleted}},
				{summary: SessionSummary{ID: "b", Label: "task", StartTime: now.Add(10 * time.Minute), EndTime: now.Add(15 * time.Minute), Status: StatusFailed}},
			}

			groups := groupSessionCandidates(candidates)
			Expect(groups).To(HaveLen(1))
			Expect(groups[0].summary.Status).To(Equal(StatusFailed))
		})
	})

	Describe("buildGroupedMessages", func() {
		It("groups consecutive same-role messages within the time window", func() {
			messages := []SessionMessage{
				{Role: "assistant", Timestamp: now, TotalTokens: 100},
				{Role: "assistant", Timestamp: now.Add(2 * time.Second), TotalTokens: 200},
				{Role: "user", Timestamp: now.Add(10 * time.Second), TotalTokens: 50},
			}

			groups := buildGroupedMessages(messages)
			Expect(groups).To(HaveLen(2))
			Expect(groups[0].Role).To(Equal("assistant"))
			Expect(groups[0].Count).To(Equal(2))
			Expect(groups[0].TotalTokens).To(Equal(int64(300)))
			Expect(groups[1].Role).To(Equal("user"))
			Expect(groups[1].Count).To(Equal(1))
		})

		It("splits groups when role changes even within the time window", func() {
			messages := []SessionMessage{
				{Role: "user", Timestamp: now, TotalTokens: 50},
				{Role: "assistant", Timestamp: now.Add(1 * time.Second), TotalTokens: 100},
			}

			groups := buildGroupedMessages(messages)
			Expect(groups).To(HaveLen(2))
		})

		It("splits groups when the time gap exceeds the window", func() {
			messages := []SessionMessage{
				{Role: "assistant", Timestamp: now, TotalTokens: 100},
				{Role: "assistant", Timestamp: now.Add(30 * time.Second), TotalTokens: 200},
			}

			groups := buildGroupedMessages(messages)
			Expect(groups).To(HaveLen(2))
		})

		It("returns nil for empty messages", func() {
			groups := buildGroupedMessages(nil)
			Expect(groups).To(BeNil())
		})

		It("sets correct start and end indices", func() {
			messages := []SessionMessage{
				{Role: "user", Timestamp: now, TotalTokens: 10},
				{Role: "user", Timestamp: now.Add(1 * time.Second), TotalTokens: 20},
				{Role: "assistant", Timestamp: now.Add(10 * time.Second), TotalTokens: 100},
			}

			groups := buildGroupedMessages(messages)
			Expect(groups).To(HaveLen(2))
			Expect(groups[0].StartIndex).To(Equal(0))
			Expect(groups[0].EndIndex).To(Equal(2))
			Expect(groups[1].StartIndex).To(Equal(2))
			Expect(groups[1].EndIndex).To(Equal(3))
		})

		It("merges tool calls and deduplicates", func() {
			messages := []SessionMessage{
				{Role: "assistant", Timestamp: now, ToolCalls: []string{"Read", "Write"}},
				{Role: "assistant", Timestamp: now.Add(1 * time.Second), ToolCalls: []string{"Write", "Bash"}},
			}

			groups := buildGroupedMessages(messages)
			Expect(groups).To(HaveLen(1))
			Expect(groups[0].ToolCalls).To(ConsistOf("Read", "Write", "Bash"))
		})
	})

	Describe("makeGroupID and parseGroupID", func() {
		It("round-trips a group ID", func() {
			key := "fix bug||tapes"
			ts := now

			id := makeGroupID(key, ts)
			Expect(id).To(HavePrefix(groupIDPrefix))

			hash, unix, ok := parseGroupID(id)
			Expect(ok).To(BeTrue())
			Expect(unix).To(Equal(ts.Unix()))
			Expect(hash).NotTo(BeEmpty())
		})

		It("returns false for non-group IDs", func() {
			_, _, ok := parseGroupID("regular-session-id")
			Expect(ok).To(BeFalse())
		})
	})

	Describe("makeGroupID and groupIDKeyHash consistency", func() {
		It("produces matching hashes for the same key", func() {
			summary := SessionSummary{
				ID:        "leaf-1",
				Label:     "Fix Bug",
				AgentName: "claude",
				Project:   "tapes",
				StartTime: now,
			}

			key := sessionGroupKey(summary)
			groupID := makeGroupID(key, now)
			hash, _, ok := parseGroupID(groupID)
			Expect(ok).To(BeTrue())

			keyHash := groupIDKeyHash(summary)
			Expect(hash).To(Equal(keyHash))
		})
	})

	Describe("findGroupByID", func() {
		It("finds a group by exact ID match", func() {
			group := &sessionGroup{
				summary: SessionSummary{ID: "group:abc123:1000"},
			}

			found := findGroupByID([]*sessionGroup{group}, "group:abc123:1000")
			Expect(found).To(Equal(group))
		})

		It("finds a group by fuzzy hash match when timestamps drift", func() {
			summary := SessionSummary{
				Label:     "task",
				AgentName: "",
				Project:   "",
				StartTime: now,
			}

			key := sessionGroupKey(summary)
			originalID := makeGroupID(key, now)

			group := &sessionGroup{
				summary: SessionSummary{
					ID:        makeGroupID(key, now.Add(30*time.Second)),
					Label:     "task",
					StartTime: now.Add(30 * time.Second),
				},
			}

			found := findGroupByID([]*sessionGroup{group}, originalID)
			Expect(found).To(Equal(group))
		})

		It("returns nil when no match exists", func() {
			group := &sessionGroup{
				summary: SessionSummary{
					ID:    "group:xyz:999",
					Label: "other",
				},
			}

			found := findGroupByID([]*sessionGroup{group}, "group:abc:1000")
			Expect(found).To(BeNil())
		})
	})

	Describe("summarizeGroupStatus", func() {
		It("returns failed when any session failed", func() {
			counts := map[string]int{StatusCompleted: 3, StatusFailed: 1}
			Expect(summarizeGroupStatus(counts)).To(Equal(StatusFailed))
		})

		It("returns abandoned when no failures but some abandoned", func() {
			counts := map[string]int{StatusCompleted: 3, StatusAbandoned: 1}
			Expect(summarizeGroupStatus(counts)).To(Equal(StatusAbandoned))
		})

		It("returns completed when all completed", func() {
			counts := map[string]int{StatusCompleted: 5}
			Expect(summarizeGroupStatus(counts)).To(Equal(StatusCompleted))
		})

		It("returns unknown when no recognized statuses", func() {
			counts := map[string]int{}
			Expect(summarizeGroupStatus(counts)).To(Equal(StatusUnknown))
		})
	})

	Describe("normalizeSessionLabel", func() {
		It("lowercases and normalizes whitespace", func() {
			Expect(normalizeSessionLabel("  Fix   Bug  ")).To(Equal("fix bug"))
		})

		It("returns empty for empty input", func() {
			Expect(normalizeSessionLabel("")).To(Equal(""))
		})
	})

	Describe("uniqueToolCalls", func() {
		It("deduplicates tool calls", func() {
			result := uniqueToolCalls([]string{"Read", "Write", "Read", "Bash"})
			Expect(result).To(Equal([]string{"Read", "Write", "Bash"}))
		})

		It("filters empty strings", func() {
			result := uniqueToolCalls([]string{"", "Read", ""})
			Expect(result).To(Equal([]string{"Read"}))
		})

		It("returns nil for empty input", func() {
			result := uniqueToolCalls(nil)
			Expect(result).To(BeNil())
		})
	})

	Describe("mergeToolCalls", func() {
		It("merges without duplicates", func() {
			result := mergeToolCalls([]string{"Read", "Write"}, []string{"Write", "Bash"})
			Expect(result).To(Equal([]string{"Read", "Write", "Bash"}))
		})

		It("returns existing when next is empty", func() {
			result := mergeToolCalls([]string{"Read"}, nil)
			Expect(result).To(Equal([]string{"Read"}))
		})

		It("returns unique next when existing is empty", func() {
			result := mergeToolCalls(nil, []string{"Read", "Read"})
			Expect(result).To(Equal([]string{"Read"}))
		})
	})

	Describe("truncateGroupedText", func() {
		It("returns text unchanged when under the limit", func() {
			Expect(truncateGroupedText("short")).To(Equal("short"))
		})

		It("truncates text exceeding the limit", func() {
			long := string(make([]byte, maxGroupedTextChars+100))
			result := truncateGroupedText(long)
			Expect(len(result)).To(BeNumerically("<=", maxGroupedTextChars+3))
			Expect(result).To(HaveSuffix("..."))
		})

		It("returns empty for empty input", func() {
			Expect(truncateGroupedText("")).To(Equal(""))
		})
	})
})
