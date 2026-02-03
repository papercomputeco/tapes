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
		"claude-opus-4.5":   {Input: 5.00, Output: 25.00},
		"claude-sonnet-4.5": {Input: 3.00, Output: 15.00},
		"claude-sonnet-4":   {Input: 3.00, Output: 15.00},
		"claude-haiku-4.5":  {Input: 1.00, Output: 5.00},
		"claude-3.5-sonnet": {Input: 3.00, Output: 15.00},
		"claude-3.5-haiku":  {Input: 1.00, Output: 5.00},
		"claude-3-opus":     {Input: 15.00, Output: 75.00},
		"gpt-4o":            {Input: 2.50, Output: 10.00},
		"gpt-4o-mini":       {Input: 0.15, Output: 0.60},
		"deepseek-r1":       {Input: 0.55, Output: 2.19},
		"claude-opus-4-5":   {Input: 5.00, Output: 25.00},
		"claude-sonnet-4-5": {Input: 3.00, Output: 15.00},
		"claude-haiku-4-5":  {Input: 1.00, Output: 5.00},
		"claude-3-5-sonnet": {Input: 3.00, Output: 15.00},
		"claude-3-5-haiku":  {Input: 1.00, Output: 5.00},
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

func CostForTokens(pricing Pricing, inputTokens, outputTokens int64) (float64, float64, float64) {
	inputCost := float64(inputTokens) / 1_000_000.0 * pricing.Input
	outputCost := float64(outputTokens) / 1_000_000.0 * pricing.Output
	return inputCost, outputCost, inputCost + outputCost
}

func normalizeModel(model string) string {
	normalized := strings.ToLower(strings.TrimSpace(model))
	if normalized == "" {
		return normalized
	}

	if idx := strings.LastIndex(normalized, "-"); idx != -1 {
		suffix := normalized[idx+1:]
		if len(suffix) == 8 && isDigits(suffix) {
			normalized = normalized[:idx]
		}
	}

	normalized = strings.ReplaceAll(normalized, "-4-5", "-4.5")
	normalized = strings.ReplaceAll(normalized, "-3-5", "-3.5")
	return normalized
}

func isDigits(value string) bool {
	for _, r := range value {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}
