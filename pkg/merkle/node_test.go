package merkle_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/llm"
	"github.com/papercomputeco/tapes/pkg/merkle"
)

// testBucket creates a simple bucket for testing with the given text content
func testBucket(text string) merkle.Bucket {
	return merkle.Bucket{
		Type:     "message",
		Role:     "user",
		Content:  []llm.ContentBlock{{Type: "text", Text: text}},
		Model:    "test-model",
		Provider: "test-provider",
	}
}

var _ = Describe("Node", func() {
	Describe("NewNode", func() {
		Context("when creating a root node (no parent)", func() {
			It("creates a node with the given bucket", func() {
				bucket := testBucket("hello world")
				node := merkle.NewNode(bucket, nil)

				Expect(node.Bucket).To(Equal(bucket))
			})

			It("sets ParentHash to nil for root nodes", func() {
				node := merkle.NewNode(testBucket("test"), nil)

				Expect(node.ParentHash).To(BeNil())
			})

			It("computes a non-empty hash", func() {
				node := merkle.NewNode(testBucket("test"), nil)

				Expect(node.Hash).NotTo(BeEmpty())
			})

			It("produces consistent hashes for the same bucket", func() {
				bucket := testBucket("same content")
				node1 := merkle.NewNode(bucket, nil)
				node2 := merkle.NewNode(bucket, nil)

				Expect(node1.Hash).To(Equal(node2.Hash))
			})

			It("produces different hashes for different bucket content", func() {
				node1 := merkle.NewNode(testBucket("content A"), nil)
				node2 := merkle.NewNode(testBucket("content B"), nil)

				Expect(node1.Hash).NotTo(Equal(node2.Hash))
			})

			It("handles buckets with usage metrics", func() {
				bucket := merkle.Bucket{
					Type:       "message",
					Role:       "assistant",
					Content:    []llm.ContentBlock{{Type: "text", Text: "response"}},
					Model:      "gpt-4",
					Provider:   "openai",
					StopReason: "stop",
					Usage: &llm.Usage{
						PromptTokens:     10,
						CompletionTokens: 20,
						TotalTokens:      30,
					},
				}
				node := merkle.NewNode(bucket, nil)

				Expect(node.Hash).NotTo(BeEmpty())
				Expect(node.Bucket).To(Equal(bucket))
			})
		})

		Context("when creating a child node (with parent)", func() {
			var parent *merkle.Node

			BeforeEach(func() {
				parent = merkle.NewNode(testBucket("parent content"), nil)
			})

			It("creates a child node with the given bucket", func() {
				bucket := testBucket("child content")
				child := merkle.NewNode(bucket, parent)

				Expect(child.Bucket).To(Equal(bucket))
			})

			It("links the child to the parent via ParentHash", func() {
				child := merkle.NewNode(testBucket("child content"), parent)

				Expect(child.ParentHash).NotTo(BeNil())
				Expect(*child.ParentHash).To(Equal(parent.Hash))
			})

			It("computes a hash for the child node", func() {
				child := merkle.NewNode(testBucket("child content"), parent)

				Expect(child.Hash).NotTo(BeEmpty())
			})

			It("creates a chain of nodes", func() {
				child1 := merkle.NewNode(testBucket("child 1"), parent)
				child2 := merkle.NewNode(testBucket("child 2"), child1)
				child3 := merkle.NewNode(testBucket("child 3"), child2)

				Expect(parent.ParentHash).To(BeNil())
				Expect(*child1.ParentHash).To(Equal(parent.Hash))
				Expect(*child2.ParentHash).To(Equal(child1.Hash))
				Expect(*child3.ParentHash).To(Equal(child2.Hash))
			})

			It("produces different hashes for same bucket with different parents", func() {
				parent2 := merkle.NewNode(testBucket("different parent"), nil)
				bucket := testBucket("same content")
				child1 := merkle.NewNode(bucket, parent)
				child2 := merkle.NewNode(bucket, parent2)

				Expect(child1.Hash).NotTo(Equal(child2.Hash))
			})
		})
	})

	Describe("Hash computation", func() {
		It("produces a valid SHA-256 hex string (64 characters)", func() {
			node := merkle.NewNode(testBucket("test"), nil)

			Expect(node.Hash).To(HaveLen(64))
			Expect(node.Hash).To(MatchRegexp("^[a-f0-9]{64}$"))
		})
	})
})
