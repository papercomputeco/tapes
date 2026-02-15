package deck

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("CostForTokens", func() {
	pricing := Pricing{Input: 3.00, Output: 15.00, CacheRead: 0.30, CacheWrite: 3.75}

	It("calculates base input and output costs", func() {
		inputCost, outputCost, totalCost := CostForTokens(pricing, 1_000_000, 500_000)
		Expect(inputCost).To(BeNumerically("~", 3.00, 0.001))
		Expect(outputCost).To(BeNumerically("~", 7.50, 0.001))
		Expect(totalCost).To(BeNumerically("~", 10.50, 0.001))
	})

	It("returns zero costs for zero tokens", func() {
		inputCost, outputCost, totalCost := CostForTokens(pricing, 0, 0)
		Expect(inputCost).To(Equal(0.0))
		Expect(outputCost).To(Equal(0.0))
		Expect(totalCost).To(Equal(0.0))
	})
})

var _ = Describe("CostForTokensWithCache", func() {
	pricing := Pricing{Input: 3.00, Output: 15.00, CacheRead: 0.30, CacheWrite: 3.75}

	It("delegates to CostForTokens when no cache tokens present", func() {
		inputCost, outputCost, totalCost := CostForTokensWithCache(pricing, 1_000_000, 500_000, 0, 0)
		expectedInput, expectedOutput, expectedTotal := CostForTokens(pricing, 1_000_000, 500_000)
		Expect(inputCost).To(Equal(expectedInput))
		Expect(outputCost).To(Equal(expectedOutput))
		Expect(totalCost).To(Equal(expectedTotal))
	})

	It("prices cache write tokens at CacheWrite rate", func() {
		// 1M total input: 500k base + 500k cache creation, 0 output
		inputCost, _, _ := CostForTokensWithCache(pricing, 1_000_000, 0, 500_000, 0)
		// base: 500k/1M * 3.00 = 1.50, cache write: 500k/1M * 3.75 = 1.875
		Expect(inputCost).To(BeNumerically("~", 1.50+1.875, 0.001))
	})

	It("prices cache read tokens at CacheRead rate", func() {
		// 1M total input: 200k base + 800k cache read, 0 output
		inputCost, _, _ := CostForTokensWithCache(pricing, 1_000_000, 0, 0, 800_000)
		// base: 200k/1M * 3.00 = 0.60, cache read: 800k/1M * 0.30 = 0.24
		Expect(inputCost).To(BeNumerically("~", 0.60+0.24, 0.001))
	})

	It("prices all three input token types at their respective rates", func() {
		// 1M total input: 200k base + 300k cache creation + 500k cache read, 100k output
		inputCost, outputCost, totalCost := CostForTokensWithCache(pricing, 1_000_000, 100_000, 300_000, 500_000)
		// base: 200k/1M * 3.00 = 0.60
		// cache write: 300k/1M * 3.75 = 1.125
		// cache read: 500k/1M * 0.30 = 0.15
		expectedInput := 0.60 + 1.125 + 0.15
		expectedOutput := 100_000.0 / 1_000_000.0 * 15.00
		Expect(inputCost).To(BeNumerically("~", expectedInput, 0.001))
		Expect(outputCost).To(BeNumerically("~", expectedOutput, 0.001))
		Expect(totalCost).To(BeNumerically("~", expectedInput+expectedOutput, 0.001))
	})

	It("floors base input at zero when cache tokens exceed total input", func() {
		// Edge case: cache counts larger than reported input
		inputCost, _, _ := CostForTokensWithCache(pricing, 100_000, 0, 200_000, 50_000)
		// base should be max(100k - 200k - 50k, 0) = 0
		// only cache costs: 200k/1M * 3.75 + 50k/1M * 0.30
		expected := 200_000.0/1_000_000.0*3.75 + 50_000.0/1_000_000.0*0.30
		Expect(inputCost).To(BeNumerically("~", expected, 0.001))
	})

	It("handles zero cache pricing gracefully", func() {
		// Provider with no cache write rate but has cache read
		noCacheWritePricing := Pricing{Input: 0.55, Output: 2.19, CacheRead: 0.14}
		inputCost, outputCost, _ := CostForTokensWithCache(noCacheWritePricing, 1_000_000, 500_000, 100_000, 200_000)
		// base: max(1M-100k-200k, 0) = 700k -> 700k/1M * 0.55 = 0.385
		// cache write: 100k/1M * 0.00 = 0, cache read: 200k/1M * 0.14 = 0.028
		Expect(inputCost).To(BeNumerically("~", 0.385+0.028, 0.001))
		Expect(outputCost).To(BeNumerically("~", 500_000.0/1_000_000.0*2.19, 0.001))
	})
})

var _ = Describe("normalizeModel", func() {
	It("lowercases and trims whitespace", func() {
		Expect(normalizeModel("  Claude-Opus-4.5  ")).To(Equal("claude-opus-4.5"))
	})

	It("strips Anthropic-style 8-digit date suffix", func() {
		Expect(normalizeModel("claude-sonnet-4-5-20250514")).To(Equal("claude-sonnet-4.5"))
	})

	It("converts -4-5 to -4.5", func() {
		Expect(normalizeModel("claude-sonnet-4-5")).To(Equal("claude-sonnet-4.5"))
	})

	It("converts -4-6 to -4.6", func() {
		Expect(normalizeModel("claude-opus-4-6")).To(Equal("claude-opus-4.6"))
	})

	It("converts -4-1 to -4.1", func() {
		Expect(normalizeModel("claude-opus-4-1")).To(Equal("claude-opus-4.1"))
	})

	It("converts -3-7 to -3.7", func() {
		Expect(normalizeModel("claude-sonnet-3-7")).To(Equal("claude-sonnet-3.7"))
	})

	It("converts -3-5 to -3.5", func() {
		Expect(normalizeModel("claude-3-5-sonnet")).To(Equal("claude-3.5-sonnet"))
	})

	It("returns empty string for empty input", func() {
		Expect(normalizeModel("")).To(Equal(""))
		Expect(normalizeModel("   ")).To(Equal(""))
	})

	It("leaves already-normalized models unchanged", func() {
		Expect(normalizeModel("gpt-4o")).To(Equal("gpt-4o"))
		Expect(normalizeModel("o3-mini")).To(Equal("o3-mini"))
		Expect(normalizeModel("deepseek-r1")).To(Equal("deepseek-r1"))
	})
})

var _ = Describe("stripOpenAIDateSuffix", func() {
	It("strips a YYYY-MM-DD date suffix", func() {
		Expect(stripOpenAIDateSuffix("gpt-4.1-2025-04-14")).To(Equal("gpt-4.1"))
	})

	It("strips date suffix from o3-mini", func() {
		Expect(stripOpenAIDateSuffix("o3-mini-2025-01-31")).To(Equal("o3-mini"))
	})

	It("does not strip non-date suffixes", func() {
		Expect(stripOpenAIDateSuffix("gpt-4o-mini")).To(Equal("gpt-4o-mini"))
	})

	It("does not strip partial date suffixes", func() {
		Expect(stripOpenAIDateSuffix("model-2025-01")).To(Equal("model-2025-01"))
	})

	It("returns short models unchanged", func() {
		Expect(stripOpenAIDateSuffix("gpt-4o")).To(Equal("gpt-4o"))
	})

	It("returns empty string unchanged", func() {
		Expect(stripOpenAIDateSuffix("")).To(Equal(""))
	})
})

var _ = Describe("PricingForModel", func() {
	pricing := DefaultPricing()

	It("resolves exact model names", func() {
		p, ok := PricingForModel(pricing, "claude-opus-4.6")
		Expect(ok).To(BeTrue())
		Expect(p.Input).To(Equal(5.00))
	})

	It("resolves models with Anthropic date suffix", func() {
		p, ok := PricingForModel(pricing, "claude-sonnet-4-5-20250514")
		Expect(ok).To(BeTrue())
		Expect(p.Input).To(Equal(3.00))
	})

	It("resolves models with OpenAI date suffix", func() {
		p, ok := PricingForModel(pricing, "gpt-4.1-2025-04-14")
		Expect(ok).To(BeTrue())
		Expect(p.Input).To(Equal(2.00))
	})

	It("resolves dash-separated version models", func() {
		p, ok := PricingForModel(pricing, "claude-opus-4-6")
		Expect(ok).To(BeTrue())
		Expect(p.Input).To(Equal(5.00))
	})

	It("resolves claude-sonnet-3.7 with date suffix", func() {
		p, ok := PricingForModel(pricing, "claude-sonnet-3-7-20250219")
		Expect(ok).To(BeTrue())
		Expect(p.Input).To(Equal(3.00))
	})

	It("resolves claude-3-haiku", func() {
		p, ok := PricingForModel(pricing, "claude-3-haiku")
		Expect(ok).To(BeTrue())
		Expect(p.Input).To(Equal(0.25))
	})

	It("resolves deepseek-r1 with cache read pricing", func() {
		p, ok := PricingForModel(pricing, "deepseek-r1")
		Expect(ok).To(BeTrue())
		Expect(p.CacheRead).To(Equal(0.14))
	})

	It("returns false for unknown models", func() {
		_, ok := PricingForModel(pricing, "totally-unknown-model")
		Expect(ok).To(BeFalse())
	})
})
