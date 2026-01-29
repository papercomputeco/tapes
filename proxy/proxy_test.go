package proxy

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
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
func testProxy() *Proxy {
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
	Expect(err).NotTo(HaveOccurred())
	return p
}

var _ = Describe("Content Addressable Deduplication", func() {
	var (
		p   *Proxy
		ctx context.Context
	)

	BeforeEach(func() {
		p = testProxy()
		ctx = context.Background()
	})

	It("produces the same hash for identical content", func() {
		bucket := proxyTestBucket("user", "Hello")
		node1 := merkle.NewNode(bucket, nil)
		node2 := merkle.NewNode(bucket, nil)

		Expect(node1.Hash).To(Equal(node2.Hash))
	})

	It("deduplicates identical nodes in storage", func() {
		bucket := proxyTestBucket("user", "Hello")
		node1 := merkle.NewNode(bucket, nil)
		node2 := merkle.NewNode(bucket, nil)

		Expect(p.driver.Put(ctx, node1)).To(Succeed())
		Expect(p.driver.Put(ctx, node2)).To(Succeed())

		nodes, err := p.driver.List(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(nodes).To(HaveLen(1))
	})
})

var _ = Describe("Branching Conversations", func() {
	var (
		p   *Proxy
		ctx context.Context
	)

	BeforeEach(func() {
		p = testProxy()
		ctx = context.Background()
	})

	Context("when two different responses share the same parent", func() {
		var (
			userMsg   *merkle.Node
			response1 *merkle.Node
			response2 *merkle.Node
		)

		BeforeEach(func() {
			userMsg = merkle.NewNode(proxyTestBucket("user", "What is 2+2?"), nil)
			Expect(p.driver.Put(ctx, userMsg)).To(Succeed())

			response1 = merkle.NewNode(proxyTestBucket("assistant", "2+2 equals 4."), userMsg)
			response2 = merkle.NewNode(proxyTestBucket("assistant", "The answer is 4!"), userMsg)

			Expect(p.driver.Put(ctx, response1)).To(Succeed())
			Expect(p.driver.Put(ctx, response2)).To(Succeed())
		})

		It("produces different hashes for different content", func() {
			Expect(response1.Hash).NotTo(Equal(response2.Hash))
		})

		It("links both responses to the same parent", func() {
			Expect(*response1.ParentHash).To(Equal(*response2.ParentHash))
			Expect(userMsg.Hash).To(Equal(*response1.ParentHash))
		})

		It("stores all 3 nodes (1 user + 2 branches)", func() {
			nodes, err := p.driver.List(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(nodes).To(HaveLen(3))
		})

		It("has 1 root", func() {
			roots, err := p.driver.Roots(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(roots).To(HaveLen(1))
		})

		It("has 2 leaves", func() {
			leaves, err := p.driver.Leaves(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(leaves).To(HaveLen(2))
		})
	})
})

// Describe block for multi-turn conversation ancestry verifies that multi-turn
// conversations maintain proper ancestry chains when assistant responses are replayed.
//
// Since StopReason and Usage are stored on Node (not Bucket), they don't affect
// the content-addressable hash. This means replaying an assistant message produces
// the SAME hash as the original response, enabling proper DAG continuation
// without any special matching logic.
var _ = Describe("Multi-Turn Conversation Ancestry", func() {
	var (
		p            *Proxy
		ctx          context.Context
		providerName string
	)

	BeforeEach(func() {
		p = testProxy()
		ctx = context.Background()
		providerName = "test-provider"
	})

	Context("after turn 1 (user asks a question)", func() {
		var (
			hash1 string
			req1  *llm.ChatRequest
			resp1 *llm.ChatResponse
		)

		BeforeEach(func() {
			req1 = &llm.ChatRequest{
				Model: "test-model",
				Messages: []llm.Message{
					{Role: "system", Content: []llm.ContentBlock{{Type: "text", Text: "You are a helpful assistant."}}},
					{Role: "user", Content: []llm.ContentBlock{{Type: "text", Text: "What is 2+2?"}}},
				},
			}
			resp1 = &llm.ChatResponse{
				Model:      "test-model",
				StopReason: "stop",
				Usage:      &llm.Usage{PromptTokens: 10, CompletionTokens: 5, TotalTokens: 15},
				Message: llm.Message{
					Role:    "assistant",
					Content: []llm.ContentBlock{{Type: "text", Text: "2+2 equals 4."}},
				},
			}

			var err error
			hash1, err = p.storeConversationTurn(ctx, providerName, req1, resp1)
			Expect(err).NotTo(HaveOccurred())
		})

		It("has 3 nodes in ancestry (system -> user -> assistant)", func() {
			ancestry, err := p.driver.Ancestry(ctx, hash1)
			Expect(err).NotTo(HaveOccurred())
			Expect(ancestry).To(HaveLen(3))
		})

		It("orders ancestry from newest to oldest", func() {
			ancestry, err := p.driver.Ancestry(ctx, hash1)
			Expect(err).NotTo(HaveOccurred())
			Expect(ancestry[0].Bucket.Role).To(Equal("assistant"))
			Expect(ancestry[1].Bucket.Role).To(Equal("user"))
			Expect(ancestry[2].Bucket.Role).To(Equal("system"))
		})

		Context("after turn 2 (user continues the conversation)", func() {
			var hash2 string

			BeforeEach(func() {
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

				var err error
				hash2, err = p.storeConversationTurn(ctx, providerName, req2, resp2)
				Expect(err).NotTo(HaveOccurred())
			})

			It("has 5 nodes in ancestry (full conversation history)", func() {
				ancestry, err := p.driver.Ancestry(ctx, hash2)
				Expect(err).NotTo(HaveOccurred())
				Expect(ancestry).To(HaveLen(5))
			})

			It("orders the full chain from newest to oldest", func() {
				ancestry, err := p.driver.Ancestry(ctx, hash2)
				Expect(err).NotTo(HaveOccurred())

				Expect(ancestry[0].Bucket.Role).To(Equal("assistant"))
				Expect(ancestry[0].Bucket.ExtractText()).To(Equal("3+3 equals 6."))
				Expect(ancestry[1].Bucket.Role).To(Equal("user"))
				Expect(ancestry[1].Bucket.ExtractText()).To(Equal("And what is 3+3?"))
				Expect(ancestry[2].Bucket.Role).To(Equal("assistant"))
				Expect(ancestry[2].Bucket.ExtractText()).To(Equal("2+2 equals 4."))
				Expect(ancestry[3].Bucket.Role).To(Equal("user"))
				Expect(ancestry[3].Bucket.ExtractText()).To(Equal("What is 2+2?"))
				Expect(ancestry[4].Bucket.Role).To(Equal("system"))
			})

			It("reuses the original assistant response from turn 1 (same hash)", func() {
				ancestry, err := p.driver.Ancestry(ctx, hash2)
				Expect(err).NotTo(HaveOccurred())
				Expect(ancestry[2].Hash).To(Equal(hash1))
			})
		})
	})
})
