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
