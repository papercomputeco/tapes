package sessions_test

import (
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/llm"
	"github.com/papercomputeco/tapes/pkg/merkle"
	"github.com/papercomputeco/tapes/pkg/sessions"
)

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
		// Codex default model id as seen on the Responses wire.
		Expect(sessions.NormalizeModel("gpt-5.5-2026-04-23")).To(Equal("gpt-5.5"))
		Expect(sessions.NormalizeModel("gpt-5-5-2026-04-23")).To(Equal("gpt-5.5"))
	})
	It("strips the Anthropic 1M-context marker", func() {
		Expect(sessions.NormalizeModel("claude-fable-5[1m]")).To(Equal("claude-fable-5"))
		Expect(sessions.NormalizeModel("claude-opus-4-8[1m]")).To(Equal("claude-opus-4.8"))
		// Dated + marker: Anthropic puts the [1m] marker at the very end
		// (claude-sonnet-4-5-20250929[1m]), so the marker must be stripped
		// BEFORE the date suffix — otherwise the trailing "[1m]" hides the
		// date from the -YYYYMMDD stripper.
		Expect(sessions.NormalizeModel("claude-sonnet-4-5-20250929[1m]")).To(Equal("claude-sonnet-4.5"))
	})
	It("resolves claude-fable-5 pricing in bare and 1M-context form", func() {
		// claude-fable-5 has no minor version, so the dotted-key round-trip
		// loop below skips it — pin its lookup explicitly.
		pricing := sessions.DefaultPricing()
		for _, api := range []string{"claude-fable-5", "claude-fable-5[1m]"} {
			price, ok := sessions.PricingForModel(pricing, api)
			Expect(ok).To(BeTrue(), "PricingForModel(%q)", api)
			Expect(price.Input).To(BeNumerically("==", 10.00), "input $/MTok for %q", api)
			Expect(price.Output).To(BeNumerically("==", 50.00), "output $/MTok for %q", api)
		}
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
