package mcp

import (
	"context"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.uber.org/zap"

	"github.com/papercomputeco/tapes/pkg/llm"
	"github.com/papercomputeco/tapes/pkg/merkle"
	"github.com/papercomputeco/tapes/pkg/storage/inmemory"
	"github.com/papercomputeco/tapes/pkg/vector"
)

func TestMCP(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "MCP Suite")
}

// mockEmbedder is a test embedder that returns predictable embeddings
type mockEmbedder struct {
	embeddings map[string][]float32
}

func newMockEmbedder() *mockEmbedder {
	return &mockEmbedder{
		embeddings: make(map[string][]float32),
	}
}

func (m *mockEmbedder) Embed(_ context.Context, text string) ([]float32, error) {
	if emb, ok := m.embeddings[text]; ok {
		return emb, nil
	}
	// Return a default embedding for any text
	return []float32{0.1, 0.2, 0.3}, nil
}

func (m *mockEmbedder) Close() error {
	return nil
}

// mockVectorDriver is a test vector driver
type mockVectorDriver struct {
	documents []vector.Document
	results   []vector.QueryResult
}

func newMockVectorDriver() *mockVectorDriver {
	return &mockVectorDriver{
		documents: make([]vector.Document, 0),
		results:   make([]vector.QueryResult, 0),
	}
}

func (m *mockVectorDriver) Add(_ context.Context, docs []vector.Document) error {
	m.documents = append(m.documents, docs...)
	return nil
}

func (m *mockVectorDriver) Query(_ context.Context, _ []float32, topK int) ([]vector.QueryResult, error) {
	if len(m.results) < topK {
		return m.results, nil
	}
	return m.results[:topK], nil
}

func (m *mockVectorDriver) Get(_ context.Context, _ []string) ([]vector.Document, error) {
	return m.documents, nil
}

func (m *mockVectorDriver) Delete(_ context.Context, _ []string) error {
	return nil
}

func (m *mockVectorDriver) Close() error {
	return nil
}

// testBucket creates a simple bucket for testing
func testBucket(role, text string) merkle.Bucket {
	return merkle.Bucket{
		Type:     "message",
		Role:     role,
		Content:  []llm.ContentBlock{{Type: "text", Text: text}},
		Model:    "test-model",
		Provider: "test-provider",
	}
}

var _ = Describe("MCP Server", func() {
	var (
		server       *Server
		driver       *inmemory.InMemoryDriver
		vectorDriver *mockVectorDriver
		embedder     *mockEmbedder
		ctx          context.Context
	)

	BeforeEach(func() {
		logger, _ := zap.NewDevelopment()
		driver = inmemory.NewInMemoryDriver()
		vectorDriver = newMockVectorDriver()
		embedder = newMockEmbedder()
		ctx = context.Background()

		var err error
		server, err = NewServer(Config{
			StorageDriver: driver,
			VectorDriver:  vectorDriver,
			Embedder:      embedder,
			Logger:        logger,
		})
		Expect(err).NotTo(HaveOccurred())
	})

	Describe("NewServer", func() {
		It("returns an error when storage driver is nil", func() {
			logger, _ := zap.NewDevelopment()
			_, err := NewServer(Config{
				VectorDriver: vectorDriver,
				Embedder:     embedder,
				Logger:       logger,
			})
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("storage driver is required"))
		})

		It("returns an error when vector driver is nil", func() {
			logger, _ := zap.NewDevelopment()
			_, err := NewServer(Config{
				StorageDriver: driver,
				Embedder:      embedder,
				Logger:        logger,
			})
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("vector driver is required"))
		})

		It("returns an error when embedder is nil", func() {
			logger, _ := zap.NewDevelopment()
			_, err := NewServer(Config{
				StorageDriver: driver,
				VectorDriver:  vectorDriver,
				Logger:        logger,
			})
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("embedder is required"))
		})

		It("returns an error when logger is nil", func() {
			_, err := NewServer(Config{
				StorageDriver: driver,
				VectorDriver:  vectorDriver,
				Embedder:      embedder,
			})
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("logger is required"))
		})

		It("creates a server with valid config", func() {
			Expect(server).NotTo(BeNil())
		})

		It("returns an HTTP handler", func() {
			handler := server.Handler()
			Expect(handler).NotTo(BeNil())
		})
	})

	Describe("buildSearchResult", func() {
		It("builds a result from a single node", func() {
			node := merkle.NewNode(testBucket("user", "Hello world"), nil)
			Expect(driver.Put(ctx, node)).To(Succeed())

			result := vector.QueryResult{
				Document: vector.Document{
					ID:   node.Hash,
					Hash: node.Hash,
				},
				Score: 0.95,
			}

			lineage := []*merkle.Node{node}
			searchResult := buildSearchResult(result, lineage)

			Expect(searchResult.Hash).To(Equal(node.Hash))
			Expect(searchResult.Score).To(Equal(float32(0.95)))
			Expect(searchResult.Role).To(Equal("user"))
			Expect(searchResult.Preview).To(Equal("Hello world"))
			Expect(searchResult.Depth).To(Equal(1))
			Expect(searchResult.Lineage).To(HaveLen(1))
		})

		It("builds a result from a conversation chain", func() {
			node1 := merkle.NewNode(testBucket("user", "Hello"), nil)
			node2 := merkle.NewNode(testBucket("assistant", "Hi there"), node1)
			node3 := merkle.NewNode(testBucket("user", "How are you?"), node2)

			Expect(driver.Put(ctx, node1)).To(Succeed())
			Expect(driver.Put(ctx, node2)).To(Succeed())
			Expect(driver.Put(ctx, node3)).To(Succeed())

			result := vector.QueryResult{
				Document: vector.Document{
					ID:   node3.Hash,
					Hash: node3.Hash,
				},
				Score: 0.85,
			}

			// lineage is from matched node to root
			lineage := []*merkle.Node{node3, node2, node1}
			searchResult := buildSearchResult(result, lineage)

			Expect(searchResult.Hash).To(Equal(node3.Hash))
			Expect(searchResult.Depth).To(Equal(3))
			Expect(searchResult.Role).To(Equal("user"))
			Expect(searchResult.Preview).To(Equal("How are you?"))

			// Lineage should be in chronological order (root to matched)
			Expect(searchResult.Lineage).To(HaveLen(3))
			Expect(searchResult.Lineage[0].Text).To(Equal("Hello"))
			Expect(searchResult.Lineage[1].Text).To(Equal("Hi there"))
			Expect(searchResult.Lineage[2].Text).To(Equal("How are you?"))
		})

		It("handles empty lineage gracefully", func() {
			result := vector.QueryResult{
				Document: vector.Document{
					ID:   "empty-hash",
					Hash: "empty-hash",
				},
				Score: 0.5,
			}

			lineage := []*merkle.Node{}
			searchResult := buildSearchResult(result, lineage)

			Expect(searchResult.Hash).To(Equal("empty-hash"))
			Expect(searchResult.Depth).To(Equal(0))
			Expect(searchResult.Role).To(BeEmpty())
			Expect(searchResult.Preview).To(BeEmpty())
			Expect(searchResult.Lineage).To(BeEmpty())
		})
	})
})
