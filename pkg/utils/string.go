package utils

import (
	"strings"
)

// Truncate is a simple string truncate
func Truncate(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "..."
}

// extractTextFromContent concatenates text from content blocks.
func ExtractTextFromContent(content []map[string]any) string {
	var sb strings.Builder
	for _, block := range content {
		if t, ok := block["type"].(string); ok && t == "text" {
			if text, ok := block["text"].(string); ok {
				sb.WriteString(text)
			}
		}
	}
	return sb.String()
}
