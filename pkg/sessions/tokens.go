package sessions

import "github.com/papercomputeco/tapes/pkg/merkle"

// NodeTokens holds all token counts for a node, including cache breakdown.
type NodeTokens struct {
	Input         int64
	Output        int64
	Total         int64
	CacheCreation int64
	CacheRead     int64
}

// TokensForNode extracts token counts from a merkle node's Usage metadata.
// Returns a zero-valued NodeTokens when Usage is nil.
func TokensForNode(n *merkle.Node) NodeTokens {
	var t NodeTokens
	if n == nil || n.Usage == nil {
		return t
	}
	t.Input = int64(n.Usage.PromptTokens)
	t.Output = int64(n.Usage.CompletionTokens)
	t.CacheCreation = int64(n.Usage.CacheCreationInputTokens)
	t.CacheRead = int64(n.Usage.CacheReadInputTokens)

	t.Total = t.Input + t.Output
	if n.Usage.TotalTokens > 0 {
		t.Total = int64(n.Usage.TotalTokens)
	}
	return t
}

// FirstModel returns the normalized model of the first node in the chain
// that has a non-empty Model field, or the empty string.
func FirstModel(nodes []*merkle.Node) string {
	for _, n := range nodes {
		if n.Bucket.Model != "" {
			return NormalizeModel(n.Bucket.Model)
		}
	}
	return ""
}
