package testutils

import (
	"github.com/papercomputeco/tapes/pkg/llm"
	"github.com/papercomputeco/tapes/pkg/merkle"
)

// NewTestBucket creates a simple bucket for testing
func NewTestBucket(role, text string) merkle.Bucket {
	return merkle.Bucket{
		Type:     "message",
		Role:     role,
		Content:  []llm.ContentBlock{{Type: "text", Text: text}},
		Model:    "test-model",
		Provider: "test-provider",
	}
}
