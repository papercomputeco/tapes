package sessions_test

import (
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/llm"
	"github.com/papercomputeco/tapes/pkg/merkle"
	"github.com/papercomputeco/tapes/pkg/sessions"
)

// newNode builds a merkle.Node with a deterministic chain position. The hash
// is ignored by BuildSummary's arithmetic — only the leaf hash is used for
// the summary ID — but it needs to be unique to avoid content-address dedup
// if callers re-use buckets across tests.
func newNode(role, text, model string, parentHash *string, createdAt time.Time, stopReason string, usage *llm.Usage) *merkle.Node {
	bucket := merkle.Bucket{
		Type:      "message",
		Role:      role,
		Content:   []llm.ContentBlock{{Type: "text", Text: text}},
		Model:     model,
		Provider:  "test-provider",
		AgentName: "claude",
	}
	var parent *merkle.Node
	if parentHash != nil {
		parent = &merkle.Node{Hash: *parentHash}
	}
	n := merkle.NewNode(bucket, parent, merkle.NodeOptions{
		StopReason: stopReason,
		Usage:      usage,
		Project:    "tapes",
	})
	n.CreatedAt = createdAt
	return n
}

var _ = Describe("BuildSummary", func() {
	var (
		baseTime time.Time
		pricing  sessions.PricingTable
	)

	BeforeEach(func() {
		baseTime = time.Date(2026, 4, 1, 12, 0, 0, 0, time.UTC)
		pricing = sessions.PricingTable{
			"test-model": {Input: 10.0, Output: 30.0},
		}
	})

	It("errors on an empty chain", func() {
		_, _, _, err := sessions.BuildSummary(nil, pricing)
		Expect(err).To(HaveOccurred())
	})

	It("returns a summary for a single-node session", func() {
		node := newNode("user", "hello", "test-model", nil, baseTime, "", nil)
		summary, _, status, err := sessions.BuildSummary([]*merkle.Node{node}, pricing)
		Expect(err).NotTo(HaveOccurred())
		Expect(summary.ID).To(Equal(node.Hash))
		Expect(summary.MessageCount).To(Equal(1))
		Expect(summary.StartTime).To(Equal(baseTime))
		Expect(summary.EndTime).To(Equal(baseTime))
		Expect(summary.Duration).To(Equal(time.Duration(0)))
		// User-role leaf → abandoned, with no git activity and no tool error.
		Expect(status).To(Equal(sessions.StatusAbandoned))
		Expect(summary.Status).To(Equal(sessions.StatusAbandoned))
	})

	It("computes cost and tokens across an assistant response", func() {
		user := newNode("user", "what is 2+2?", "test-model", nil, baseTime, "", nil)
		answer := newNode("assistant", "it is 4", "test-model", &user.Hash, baseTime.Add(2*time.Second), "stop", &llm.Usage{
			PromptTokens:     1_000_000,
			CompletionTokens: 500_000,
		})
		chain := []*merkle.Node{user, answer}

		summary, modelCosts, status, err := sessions.BuildSummary(chain, pricing)
		Expect(err).NotTo(HaveOccurred())
		Expect(status).To(Equal(sessions.StatusCompleted))
		Expect(summary.MessageCount).To(Equal(2))
		Expect(summary.InputTokens).To(Equal(int64(1_000_000)))
		Expect(summary.OutputTokens).To(Equal(int64(500_000)))
		// Input: 1M tokens * $10/M = $10. Output: 0.5M * $30/M = $15. Total: $25.
		Expect(summary.InputCost).To(BeNumerically("~", 10.0, 0.0001))
		Expect(summary.OutputCost).To(BeNumerically("~", 15.0, 0.0001))
		Expect(summary.TotalCost).To(BeNumerically("~", 25.0, 0.0001))
		Expect(summary.Duration).To(Equal(2 * time.Second))
		Expect(summary.Model).To(Equal("test-model"))
		Expect(modelCosts).To(HaveKey("test-model"))
	})

	It("reports completed when tool errors are a recovered minority and the leaf ended cleanly", func() {
		// 1 error out of 3 tool_results (33%), and a clean assistant turn
		// followed — the model recovered, so this is not a failure.
		root := newNode("user", "run something", "test-model", nil, baseTime, "", nil)
		root.Bucket.Content = append(root.Bucket.Content,
			llm.ContentBlock{Type: "tool_result", IsError: true},
			llm.ContentBlock{Type: "tool_result"},
			llm.ContentBlock{Type: "tool_result"},
		)
		leaf := newNode("assistant", "done", "test-model", &root.Hash, baseTime.Add(time.Second), "stop", nil)

		_, _, status, err := sessions.BuildSummary([]*merkle.Node{root, leaf}, pricing)
		Expect(err).NotTo(HaveOccurred())
		Expect(status).To(Equal(sessions.StatusCompleted))
	})

	It("reports failed when tool errors exceed half of tool results", func() {
		root := newNode("user", "run something", "test-model", nil, baseTime, "", nil)
		root.Bucket.Content = append(root.Bucket.Content,
			llm.ContentBlock{Type: "tool_result", IsError: true},
			llm.ContentBlock{Type: "tool_result", IsError: true},
			llm.ContentBlock{Type: "tool_result"},
		)
		leaf := newNode("assistant", "done", "test-model", &root.Hash, baseTime.Add(time.Second), "stop", nil)

		_, _, status, err := sessions.BuildSummary([]*merkle.Node{root, leaf}, pricing)
		Expect(err).NotTo(HaveOccurred())
		Expect(status).To(Equal(sessions.StatusFailed))
	})

	It("reports failed when the session ends on an unrecovered tool error", func() {
		root := newNode("user", "run something", "test-model", nil, baseTime, "", nil)
		asst := newNode("assistant", "calling a tool", "test-model", &root.Hash, baseTime.Add(time.Second), "tool_use", nil)
		asst.Bucket.Content = []llm.ContentBlock{{Type: "tool_use", ToolName: "Bash"}}
		// Leaf is a user-role tool_result error with no assistant turn after it.
		errLeaf := newNode("user", "", "test-model", &asst.Hash, baseTime.Add(2*time.Second), "", nil)
		errLeaf.Bucket.Content = []llm.ContentBlock{{Type: "tool_result", IsError: true}}

		_, _, status, err := sessions.BuildSummary([]*merkle.Node{root, asst, errLeaf}, pricing)
		Expect(err).NotTo(HaveOccurred())
		Expect(status).To(Equal(sessions.StatusFailed))
	})

	It("reports completed when git activity is present even if the stop reason is unknown", func() {
		root := newNode("user", "please commit", "test-model", nil, baseTime, "", nil)
		committer := newNode("assistant", "committing", "test-model", &root.Hash, baseTime.Add(time.Second), "", nil)
		committer.Bucket.Content = []llm.ContentBlock{
			{Type: "tool_use", ToolName: "Bash", ToolInput: map[string]any{"command": "git commit -m 'fix'"}},
		}

		_, _, status, err := sessions.BuildSummary([]*merkle.Node{root, committer}, pricing)
		Expect(err).NotTo(HaveOccurred())
		Expect(status).To(Equal(sessions.StatusCompleted))
	})

	It("counts tool calls across the full chain", func() {
		root := newNode("user", "start", "test-model", nil, baseTime, "", nil)
		tool1 := newNode("assistant", "", "test-model", &root.Hash, baseTime.Add(time.Second), "", nil)
		tool1.Bucket.Content = []llm.ContentBlock{
			{Type: "tool_use", ToolName: "Read"},
			{Type: "tool_use", ToolName: "Grep"},
		}
		tool2 := newNode("assistant", "done", "test-model", &tool1.Hash, baseTime.Add(2*time.Second), "stop", nil)
		tool2.Bucket.Content = []llm.ContentBlock{
			{Type: "text", Text: "all done"},
			{Type: "tool_use", ToolName: "Bash"},
		}

		summary, _, _, err := sessions.BuildSummary([]*merkle.Node{root, tool1, tool2}, pricing)
		Expect(err).NotTo(HaveOccurred())
		Expect(summary.ToolCalls).To(Equal(3))
	})

	It("falls back to a truncated leaf hash when no user prompts are labelable", func() {
		onlyAssistant := newNode("assistant", "", "test-model", nil, baseTime, "stop", nil)
		onlyAssistant.Bucket.Content = nil

		summary, _, _, err := sessions.BuildSummary([]*merkle.Node{onlyAssistant}, pricing)
		Expect(err).NotTo(HaveOccurred())
		// truncate(hash, 12) produces 9 hash characters followed by "..." (12 total).
		Expect(summary.Label).To(Equal(onlyAssistant.Hash[:9] + "..."))
	})

	It("falls back to lastModel when a node has no model set", func() {
		root := newNode("user", "first", "test-model", nil, baseTime, "", nil)
		noModel := newNode("assistant", "reply", "", &root.Hash, baseTime.Add(time.Second), "stop", &llm.Usage{
			PromptTokens:     2_000_000,
			CompletionTokens: 0,
		})

		summary, modelCosts, _, err := sessions.BuildSummary([]*merkle.Node{root, noModel}, pricing)
		Expect(err).NotTo(HaveOccurred())
		Expect(modelCosts).To(HaveKey("test-model"))
		// 2M input tokens on test-model (input $10/M) = $20.
		Expect(summary.InputCost).To(BeNumerically("~", 20.0, 0.0001))
	})
})

var _ = Describe("DetermineStatus", func() {
	errLeaf := func(role string) *merkle.Node {
		return &merkle.Node{Bucket: merkle.Bucket{Role: role, Content: []llm.ContentBlock{{Type: "tool_result", IsError: true}}}}
	}

	It("returns unknown for a nil leaf", func() {
		Expect(sessions.DetermineStatus(nil, false, 0, 0)).To(Equal(sessions.StatusUnknown))
	})

	It("returns failed when the session ends on an unrecovered tool error", func() {
		Expect(sessions.DetermineStatus(errLeaf("user"), false, 1, 1)).To(Equal(sessions.StatusFailed))
	})

	It("does not fail on a recovered minority of tool errors", func() {
		// Clean assistant leaf; only 1 of 4 tool_results errored (25%).
		leaf := &merkle.Node{Bucket: merkle.Bucket{Role: "assistant"}, StopReason: "stop"}
		Expect(sessions.DetermineStatus(leaf, false, 4, 1)).To(Equal(sessions.StatusCompleted))
	})

	It("returns failed when tool errors exceed half of tool results", func() {
		leaf := &merkle.Node{Bucket: merkle.Bucket{Role: "assistant"}, StopReason: "stop"}
		Expect(sessions.DetermineStatus(leaf, false, 4, 3)).To(Equal(sessions.StatusFailed))
	})

	It("returns completed for git activity, outranking a high error rate", func() {
		leaf := &merkle.Node{Bucket: merkle.Bucket{Role: "assistant"}}
		Expect(sessions.DetermineStatus(leaf, true, 4, 4)).To(Equal(sessions.StatusCompleted))
	})

	It("returns failed for an unrecovered terminal error even with git activity", func() {
		Expect(sessions.DetermineStatus(errLeaf("user"), true, 1, 1)).To(Equal(sessions.StatusFailed))
	})

	It("returns abandoned for a user-role leaf with no terminal error", func() {
		leaf := &merkle.Node{Bucket: merkle.Bucket{Role: "user"}}
		Expect(sessions.DetermineStatus(leaf, false, 0, 0)).To(Equal(sessions.StatusAbandoned))
	})

	It("maps assistant-leaf stop reasons to expected statuses", func() {
		// tool_use / tool_use_response are the designed terminus of a
		// subagent or parallel-tool side-conversation — the model emitted a
		// tool request, which is the contract. length / content_filter are
		// real model-side failures (truncation, refusal).
		cases := map[string]string{
			"stop":              sessions.StatusCompleted,
			"end_turn":          sessions.StatusCompleted,
			"tool_use":          sessions.StatusCompleted,
			"tool_use_response": sessions.StatusCompleted,
			"length":            sessions.StatusFailed,
			"max_tokens":        sessions.StatusFailed,
			"content_filter":    sessions.StatusFailed,
			"some_error_code":   sessions.StatusFailed,
			"weird_thing":       sessions.StatusUnknown,
			"":                  sessions.StatusUnknown,
		}
		for reason, want := range cases {
			leaf := &merkle.Node{Bucket: merkle.Bucket{Role: "assistant"}, StopReason: reason}
			Expect(sessions.DetermineStatus(leaf, false, 0, 0)).
				To(Equal(want), "stop_reason=%q", reason)
		}
	})
})

var _ = Describe("NormalizeModel", func() {
	It("lowercases and trims", func() {
		Expect(sessions.NormalizeModel("  GPT-4O  ")).To(Equal("gpt-4o"))
	})
	It("strips Anthropic-style date suffix", func() {
		Expect(sessions.NormalizeModel("claude-sonnet-4-5-20250929")).To(Equal("claude-sonnet-4.5"))
	})
	It("strips OpenAI-style date suffix", func() {
		Expect(sessions.NormalizeModel("gpt-4o-2024-08-06")).To(Equal("gpt-4o"))
	})
	It("every Anthropic pricing key is reachable from its canonical API form", func() {
		// Guards against forgetting the matching `-N-M` → `-N.M` rewrite when
		// adding a new Anthropic row to DefaultPricing. The dated variant
		// also exercises the date-suffix stripper that real API IDs carry.
		for key := range sessions.DefaultPricing() {
			if !strings.HasPrefix(key, "claude-") || !strings.ContainsRune(key, '.') {
				continue
			}
			api := strings.ReplaceAll(key, ".", "-")
			Expect(sessions.NormalizeModel(api)).To(Equal(key), "bare API form %q", api)
			Expect(sessions.NormalizeModel(api+"-20260101")).To(Equal(key), "dated API form %q", api+"-20260101")
		}
	})
	It("returns empty for empty input", func() {
		Expect(sessions.NormalizeModel("")).To(Equal(""))
	})
})

var _ = Describe("CostForTokensWithCache", func() {
	pricing := sessions.Pricing{Input: 10.0, Output: 30.0, CacheRead: 1.0, CacheWrite: 12.5}

	It("falls through to CostForTokens when cache counts are zero", func() {
		inCost, outCost, total := sessions.CostForTokensWithCache(pricing, 1_000_000, 500_000, 0, 0)
		Expect(inCost).To(BeNumerically("~", 10.0, 0.0001))
		Expect(outCost).To(BeNumerically("~", 15.0, 0.0001))
		Expect(total).To(BeNumerically("~", 25.0, 0.0001))
	})

	It("charges cache tokens at their own rates and subtracts from base input", func() {
		// 1M total input, of which 400k are cache-write and 200k are cache-read.
		// Base input = 1M - 400k - 200k = 400k.
		//   Base input cost: 400k * $10/M = $4
		//   Cache write cost: 400k * $12.5/M = $5
		//   Cache read cost:  200k * $1/M   = $0.20
		//   Input cost total: $9.20
		// Output cost: 100k * $30/M = $3.
		inCost, outCost, total := sessions.CostForTokensWithCache(pricing, 1_000_000, 100_000, 400_000, 200_000)
		Expect(inCost).To(BeNumerically("~", 9.20, 0.0001))
		Expect(outCost).To(BeNumerically("~", 3.0, 0.0001))
		Expect(total).To(BeNumerically("~", 12.20, 0.0001))
	})
})
