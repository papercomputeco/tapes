package deck

import (
	"encoding/json"
	"fmt"
	"maps"
	"os"
	"strings"
)

type PricingTable map[string]Pricing

func DefaultPricing() PricingTable {
	return PricingTable{
		"claude-opus-4.6":   {Input: 5.00, Output: 25.00, CacheRead: 0.50, CacheWrite: 6.25},
		"claude-opus-4.5":   {Input: 5.00, Output: 25.00, CacheRead: 0.50, CacheWrite: 6.25},
		"claude-opus-4.1":   {Input: 15.00, Output: 75.00, CacheRead: 1.50, CacheWrite: 18.75},
		"claude-opus-4":     {Input: 15.00, Output: 75.00, CacheRead: 1.50, CacheWrite: 18.75},
		"claude-sonnet-4.5": {Input: 3.00, Output: 15.00, CacheRead: 0.30, CacheWrite: 3.75},
		"claude-sonnet-4":   {Input: 3.00, Output: 15.00, CacheRead: 0.30, CacheWrite: 3.75},
		"claude-haiku-4.5":  {Input: 1.00, Output: 5.00, CacheRead: 0.10, CacheWrite: 1.25},
		"claude-3.5-sonnet": {Input: 3.00, Output: 15.00, CacheRead: 0.30, CacheWrite: 3.75},
		"claude-3.5-haiku":  {Input: 0.80, Output: 4.00, CacheRead: 0.08, CacheWrite: 1.00},
		"claude-3-opus":     {Input: 15.00, Output: 75.00, CacheRead: 1.50, CacheWrite: 18.75},
		"gpt-4o":            {Input: 2.50, Output: 10.00, CacheRead: 1.25, CacheWrite: 2.50},
		"gpt-4o-mini":       {Input: 0.15, Output: 0.60, CacheRead: 0.075, CacheWrite: 0.15},
		"gpt-4.1":           {Input: 2.00, Output: 8.00, CacheRead: 0.50, CacheWrite: 2.00},
		"gpt-4.1-mini":      {Input: 0.40, Output: 1.60, CacheRead: 0.10, CacheWrite: 0.40},
		"gpt-4.1-nano":      {Input: 0.10, Output: 0.40, CacheRead: 0.025, CacheWrite: 0.10},
		"o3":                {Input: 2.00, Output: 8.00, CacheRead: 0.50, CacheWrite: 2.00},
		"o3-mini":           {Input: 1.10, Output: 4.40, CacheRead: 0.55, CacheWrite: 1.10},
		"o4-mini":           {Input: 1.10, Output: 4.40, CacheRead: 0.275, CacheWrite: 1.10},
		"o1":                {Input: 15.00, Output: 60.00, CacheRead: 7.50, CacheWrite: 15.00},
		"deepseek-r1":       {Input: 0.55, Output: 2.19},
	}
}

func LoadPricing(path string) (PricingTable, error) {
	pricing := DefaultPricing()
	if path == "" {
		return pricing, nil
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read pricing file: %w", err)
	}

	var overrides map[string]Pricing
	if err := json.Unmarshal(data, &overrides); err != nil {
		return nil, fmt.Errorf("parse pricing file: %w", err)
	}

	maps.Copy(pricing, overrides)

	return pricing, nil
}

func PricingForModel(pricing PricingTable, model string) (Pricing, bool) {
	normalized := normalizeModel(model)
	price, ok := pricing[normalized]
	if ok {
		return price, true
	}
	price, ok = pricing[model]
	return price, ok
}

// CostForTokens calculates cost using base input/output pricing.
// For cache-aware cost calculation, use CostForTokensWithCache.
func CostForTokens(pricing Pricing, inputTokens, outputTokens int64) (float64, float64, float64) {
	inputCost := float64(inputTokens) / 1_000_000.0 * pricing.Input
	outputCost := float64(outputTokens) / 1_000_000.0 * pricing.Output
	return inputCost, outputCost, inputCost + outputCost
}

// CostForTokensWithCache calculates cost accounting for prompt caching.
// When cache token counts are available, base input tokens are calculated as:
// baseInput = totalInput - cacheCreation - cacheRead
// Each token type is priced at its respective rate.
func CostForTokensWithCache(pricing Pricing, inputTokens, outputTokens, cacheCreation, cacheRead int64) (float64, float64, float64) {
	if cacheCreation == 0 && cacheRead == 0 {
		return CostForTokens(pricing, inputTokens, outputTokens)
	}

	// Base input tokens = total input minus cached tokens
	baseInput := max(inputTokens-cacheCreation-cacheRead, 0)

	inputCost := float64(baseInput) / 1_000_000.0 * pricing.Input
	inputCost += float64(cacheCreation) / 1_000_000.0 * pricing.CacheWrite
	inputCost += float64(cacheRead) / 1_000_000.0 * pricing.CacheRead
	outputCost := float64(outputTokens) / 1_000_000.0 * pricing.Output
	return inputCost, outputCost, inputCost + outputCost
}

func normalizeModel(model string) string {
	normalized := strings.ToLower(strings.TrimSpace(model))
	if normalized == "" {
		return normalized
	}

	// Strip Anthropic-style date suffix: -YYYYMMDD (8 consecutive digits)
	if idx := strings.LastIndex(normalized, "-"); idx != -1 {
		suffix := normalized[idx+1:]
		if len(suffix) == 8 && isDigits(suffix) {
			normalized = normalized[:idx]
		}
	}

	// Strip OpenAI-style date suffix: -YYYY-MM-DD
	normalized = stripOpenAIDateSuffix(normalized)

	normalized = strings.ReplaceAll(normalized, "-4-6", "-4.6")
	normalized = strings.ReplaceAll(normalized, "-4-5", "-4.5")
	normalized = strings.ReplaceAll(normalized, "-4-1", "-4.1")
	normalized = strings.ReplaceAll(normalized, "-3-5", "-3.5")
	return normalized
}

// stripOpenAIDateSuffix removes a trailing -YYYY-MM-DD date suffix from a model name.
func stripOpenAIDateSuffix(model string) string {
	// Minimum length: base + "-YYYY-MM-DD" = at least 11 chars for the suffix
	if len(model) < 12 {
		return model
	}

	// Check if the last 10 chars match the pattern -YYYY-MM-DD
	suffix := model[len(model)-11:]
	if suffix[0] != '-' {
		return model
	}
	date := suffix[1:] // "YYYY-MM-DD"
	if len(date) == 10 && isDigits(date[0:4]) && date[4] == '-' && isDigits(date[5:7]) && date[7] == '-' && isDigits(date[8:10]) {
		return model[:len(model)-11]
	}
	return model
}

func isDigits(value string) bool {
	for _, r := range value {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}
