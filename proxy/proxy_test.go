package proxy

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/papercomputeco/tapes/pkg/merkle"
)

// testProxy creates a Proxy with an in-memory storer for testing.
func testProxy(t *testing.T) *Proxy {
	t.Helper()
	logger, _ := zap.NewDevelopment()
	storer := merkle.NewMemoryStorer()
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
	content := map[string]string{"role": "user", "content": "Hello"}
	node1 := merkle.NewNode(content, nil)
	node2 := merkle.NewNode(content, nil)

	// Same content = same hash
	assert.Equal(t, node1.Hash, node2.Hash)

	// Store both
	require.NoError(t, p.storer.Put(ctx, node1))
	require.NoError(t, p.storer.Put(ctx, node2))

	// Should only have one node
	nodes, err := p.storer.List(ctx)
	require.NoError(t, err)
	assert.Len(t, nodes, 1)
}

func TestBranchingConversations(t *testing.T) {
	p := testProxy(t)
	ctx := context.Background()

	// Common prefix
	userMsg := merkle.NewNode(map[string]string{
		"role":    "user",
		"content": "What is 2+2?",
	}, nil)
	require.NoError(t, p.storer.Put(ctx, userMsg))

	// Two different responses (simulating different LLM outputs)
	response1 := merkle.NewNode(map[string]string{
		"role":    "assistant",
		"content": "2+2 equals 4.",
	}, userMsg)
	response2 := merkle.NewNode(map[string]string{
		"role":    "assistant",
		"content": "The answer is 4!",
	}, userMsg)

	require.NoError(t, p.storer.Put(ctx, response1))
	require.NoError(t, p.storer.Put(ctx, response2))

	// Different content = different hashes
	assert.NotEqual(t, response1.Hash, response2.Hash)

	// But same parent
	assert.Equal(t, *response1.ParentHash, *response2.ParentHash)
	assert.Equal(t, userMsg.Hash, *response1.ParentHash)

	// Should have 3 nodes total (1 user + 2 branches)
	nodes, err := p.storer.List(ctx)
	require.NoError(t, err)
	assert.Len(t, nodes, 3)

	// 1 root, 2 leaves
	roots, err := p.storer.Roots(ctx)
	require.NoError(t, err)
	assert.Len(t, roots, 1)

	leaves, err := p.storer.Leaves(ctx)
	require.NoError(t, err)
	assert.Len(t, leaves, 2)
}
