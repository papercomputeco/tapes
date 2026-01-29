package proxy

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/papercomputeco/tapes/pkg/llm"
	"github.com/papercomputeco/tapes/pkg/merkle"
	"github.com/papercomputeco/tapes/pkg/storage/inmemory"
)

// proxyTestBucket creates a simple bucket for testing with the given role and text content
func proxyTestBucket(role, text string) merkle.Bucket {
	return merkle.Bucket{
		Type:     "message",
		Role:     role,
		Content:  []llm.ContentBlock{{Type: "text", Text: text}},
		Model:    "test-model",
		Provider: "test-provider",
	}
}

// testProxy creates a Proxy with an in-memory storer for testing.
func testProxy(t *testing.T) *Proxy {
	t.Helper()
	logger, _ := zap.NewDevelopment()
	storer := inmemory.NewInMemoryDriver()
	p, err := New(
		Config{
			ListenAddr:   ":0",
			UpstreamURL:  "http://localhost:11434",
			ProviderType: "ollama",
		},
		storer,
		logger,
	)
	require.NoError(t, err)
	return p
}

func TestContentAddressableDeduplication(t *testing.T) {
	p := testProxy(t)
	ctx := context.Background()

	// Create the same node twice - should only store once
	bucket := proxyTestBucket("user", "Hello")
	node1 := merkle.NewNode(bucket, nil)
	node2 := merkle.NewNode(bucket, nil)

	// Same content = same hash
	assert.Equal(t, node1.Hash, node2.Hash)

	// Store both
	require.NoError(t, p.driver.Put(ctx, node1))
	require.NoError(t, p.driver.Put(ctx, node2))

	// Should only have one node
	nodes, err := p.driver.List(ctx)
	require.NoError(t, err)
	assert.Len(t, nodes, 1)
}

func TestBranchingConversations(t *testing.T) {
	p := testProxy(t)
	ctx := context.Background()

	// Common prefix
	userMsg := merkle.NewNode(proxyTestBucket("user", "What is 2+2?"), nil)
	require.NoError(t, p.driver.Put(ctx, userMsg))

	// Two different responses (simulating different LLM outputs)
	response1 := merkle.NewNode(proxyTestBucket("assistant", "2+2 equals 4."), userMsg)
	response2 := merkle.NewNode(proxyTestBucket("assistant", "The answer is 4!"), userMsg)

	require.NoError(t, p.driver.Put(ctx, response1))
	require.NoError(t, p.driver.Put(ctx, response2))

	// Different content = different hashes
	assert.NotEqual(t, response1.Hash, response2.Hash)

	// But same parent
	assert.Equal(t, *response1.ParentHash, *response2.ParentHash)
	assert.Equal(t, userMsg.Hash, *response1.ParentHash)

	// Should have 3 nodes total (1 user + 2 branches)
	nodes, err := p.driver.List(ctx)
	require.NoError(t, err)
	assert.Len(t, nodes, 3)

	// 1 root, 2 leaves
	roots, err := p.driver.Roots(ctx)
	require.NoError(t, err)
	assert.Len(t, roots, 1)

	leaves, err := p.driver.Leaves(ctx)
	require.NoError(t, err)
	assert.Len(t, leaves, 2)
}

// TestMultiTurnConversationAncestry verifies that multi-turn conversations
// maintain proper ancestry chains when assistant responses are replayed.
//
// Since StopReason and Usage are stored on Node (not Bucket), they don't affect
// the content-addressable hash. This means replaying an assistant message produces
// the SAME hash as the original response, enabling proper DAG continuation
// without any special matching logic.
func TestMultiTurnConversationAncestry(t *testing.T) {
	p := testProxy(t)
	ctx := context.Background()
	providerName := "test-provider"

	// === Turn 1: User asks a question ===
	req1 := &llm.ChatRequest{
		Model: "test-model",
		Messages: []llm.Message{
			{Role: "system", Content: []llm.ContentBlock{{Type: "text", Text: "You are a helpful assistant."}}},
			{Role: "user", Content: []llm.ContentBlock{{Type: "text", Text: "What is 2+2?"}}},
		},
	}
	resp1 := &llm.ChatResponse{
		Model:      "test-model",
		StopReason: "stop",
		Usage:      &llm.Usage{PromptTokens: 10, CompletionTokens: 5, TotalTokens: 15},
		Message: llm.Message{
			Role:    "assistant",
			Content: []llm.ContentBlock{{Type: "text", Text: "2+2 equals 4."}},
		},
	}

	hash1, err := p.storeConversationTurn(ctx, providerName, req1, resp1)
	require.NoError(t, err)

	// Verify turn 1 ancestry: system -> user -> assistant (3 nodes deep)
	ancestry1, err := p.driver.Ancestry(ctx, hash1)
	require.NoError(t, err)
	assert.Len(t, ancestry1, 3, "Turn 1 should have 3 nodes in ancestry")
	assert.Equal(t, "assistant", ancestry1[0].Bucket.Role)
	assert.Equal(t, "user", ancestry1[1].Bucket.Role)
	assert.Equal(t, "system", ancestry1[2].Bucket.Role)

	// === Turn 2: User continues the conversation ===
	// The assistant message from turn 1 is replayed. Since metadata (StopReason/Usage)
	// doesn't affect the hash, this produces the SAME hash as the original response.
	req2 := &llm.ChatRequest{
		Model: "test-model",
		Messages: []llm.Message{
			{Role: "system", Content: []llm.ContentBlock{{Type: "text", Text: "You are a helpful assistant."}}},
			{Role: "user", Content: []llm.ContentBlock{{Type: "text", Text: "What is 2+2?"}}},
			{Role: "assistant", Content: []llm.ContentBlock{{Type: "text", Text: "2+2 equals 4."}}}, // Replayed
			{Role: "user", Content: []llm.ContentBlock{{Type: "text", Text: "And what is 3+3?"}}},   // New
		},
	}
	resp2 := &llm.ChatResponse{
		Model:      "test-model",
		StopReason: "stop",
		Usage:      &llm.Usage{PromptTokens: 20, CompletionTokens: 5, TotalTokens: 25},
		Message: llm.Message{
			Role:    "assistant",
			Content: []llm.ContentBlock{{Type: "text", Text: "3+3 equals 6."}},
		},
	}

	hash2, err := p.storeConversationTurn(ctx, providerName, req2, resp2)
	require.NoError(t, err)

	// Verify turn 2 ancestry: system -> user -> assistant -> user -> assistant (5 nodes deep)
	ancestry2, err := p.driver.Ancestry(ctx, hash2)
	require.NoError(t, err)
	assert.Len(t, ancestry2, 5, "Turn 2 should have 5 nodes in ancestry (full conversation history)")

	// Verify the chain from newest to oldest
	assert.Equal(t, "assistant", ancestry2[0].Bucket.Role)
	assert.Equal(t, "3+3 equals 6.", ancestry2[0].Bucket.ExtractText())
	assert.Equal(t, "user", ancestry2[1].Bucket.Role)
	assert.Equal(t, "And what is 3+3?", ancestry2[1].Bucket.ExtractText())
	assert.Equal(t, "assistant", ancestry2[2].Bucket.Role)
	assert.Equal(t, "2+2 equals 4.", ancestry2[2].Bucket.ExtractText())
	assert.Equal(t, "user", ancestry2[3].Bucket.Role)
	assert.Equal(t, "What is 2+2?", ancestry2[3].Bucket.ExtractText())
	assert.Equal(t, "system", ancestry2[4].Bucket.Role)

	// Verify that the assistant from turn 1 is reused (same hash).
	// This works because metadata (StopReason, Usage) doesn't affect the hash.
	// The replayed assistant message produces the same hash as the original response.
	assert.Equal(t, hash1, ancestry2[2].Hash, "Turn 2 should link to the original assistant response from turn 1")
}
