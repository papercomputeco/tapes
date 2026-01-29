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

			It("handles nodes with usage metadata", func() {
				bucket := merkle.Bucket{
					Type:     "message",
					Role:     "assistant",
					Content:  []llm.ContentBlock{{Type: "text", Text: "response"}},
					Model:    "gpt-4",
					Provider: "openai",
				}
				// StopReason and Usage are now on Node via NodeOptions, not Bucket
				node := merkle.NewNode(bucket, nil, merkle.NodeMeta{
					StopReason: "stop",
					Usage: &llm.Usage{
						PromptTokens:     10,
						CompletionTokens: 20,
						TotalTokens:      30,
					},
				})

				Expect(node.Hash).NotTo(BeEmpty())
				Expect(node.Bucket).To(Equal(bucket))
				Expect(node.StopReason).To(Equal("stop"))
				Expect(node.Usage).NotTo(BeNil())
				Expect(node.Usage.TotalTokens).To(Equal(30))
			})

			It("produces same hash regardless of metadata", func() {
				bucket := merkle.Bucket{
					Type:     "message",
					Role:     "assistant",
					Content:  []llm.ContentBlock{{Type: "text", Text: "response"}},
					Model:    "gpt-4",
					Provider: "openai",
				}

				// Node without metadata
				node1 := merkle.NewNode(bucket, nil)

				// Node with metadata - should have SAME hash since metadata doesn't affect hash
				node2 := merkle.NewNode(bucket, nil, merkle.NodeMeta{
					StopReason: "stop",
					Usage: &llm.Usage{
						PromptTokens:     10,
						CompletionTokens: 20,
						TotalTokens:      30,
					},
				})

				Expect(node1.Hash).To(Equal(node2.Hash))
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

var _ = Describe("Bucket", func() {
	Describe("ExtractText", func() {
		It("extracts text from a single text content block", func() {
			bucket := merkle.Bucket{
				Content: []llm.ContentBlock{
					{Type: "text", Text: "Hello, world!"},
				},
			}

			Expect(bucket.ExtractText()).To(Equal("Hello, world!"))
		})

		It("joins multiple text blocks with newlines", func() {
			bucket := merkle.Bucket{
				Content: []llm.ContentBlock{
					{Type: "text", Text: "First line"},
					{Type: "text", Text: "Second line"},
				},
			}

			Expect(bucket.ExtractText()).To(Equal("First line\nSecond line"))
		})

		It("extracts tool output content", func() {
			bucket := merkle.Bucket{
				Content: []llm.ContentBlock{
					{Type: "tool_result", ToolOutput: "Tool returned: success"},
				},
			}

			Expect(bucket.ExtractText()).To(Equal("Tool returned: success"))
		})

		It("combines text and tool output", func() {
			bucket := merkle.Bucket{
				Content: []llm.ContentBlock{
					{Type: "text", Text: "Running tool..."},
					{Type: "tool_result", ToolOutput: "Tool output here"},
				},
			}

			Expect(bucket.ExtractText()).To(Equal("Running tool...\nTool output here"))
		})

		It("returns empty string for empty content", func() {
			bucket := merkle.Bucket{
				Content: []llm.ContentBlock{},
			}

			Expect(bucket.ExtractText()).To(Equal(""))
		})

		It("skips content blocks without text or tool output", func() {
			bucket := merkle.Bucket{
				Content: []llm.ContentBlock{
					{Type: "image", ImageURL: "http://example.com/image.png"},
					{Type: "text", Text: "Some text"},
				},
			}

			Expect(bucket.ExtractText()).To(Equal("Some text"))
		})

		It("extracts tool_use content blocks", func() {
			bucket := merkle.Bucket{
				Content: []llm.ContentBlock{
					{
						Type:      "tool_use",
						ToolName:  "get_weather",
						ToolInput: map[string]any{"city": "Tokyo"},
					},
				},
			}

			text := bucket.ExtractText()
			Expect(text).To(ContainSubstring("Tool call: get_weather"))
			Expect(text).To(ContainSubstring("city"))
			Expect(text).To(ContainSubstring("Tokyo"))
		})

		It("extracts tool_use with multiple parameters", func() {
			bucket := merkle.Bucket{
				Content: []llm.ContentBlock{
					{
						Type:     "tool_use",
						ToolName: "search",
						ToolInput: map[string]any{
							"query": "golang testing",
							"limit": 10,
						},
					},
				},
			}

			text := bucket.ExtractText()
			Expect(text).To(ContainSubstring("Tool call: search"))
			Expect(text).To(ContainSubstring("query"))
			Expect(text).To(ContainSubstring("golang testing"))
		})

		It("extracts tool_use without parameters", func() {
			bucket := merkle.Bucket{
				Content: []llm.ContentBlock{
					{
						Type:     "tool_use",
						ToolName: "get_time",
					},
				},
			}

			Expect(bucket.ExtractText()).To(Equal("Tool call: get_time"))
		})

		It("combines text and tool_use content", func() {
			bucket := merkle.Bucket{
				Content: []llm.ContentBlock{
					{Type: "text", Text: "Let me check the weather"},
					{
						Type:      "tool_use",
						ToolName:  "get_weather",
						ToolInput: map[string]any{"city": "Paris"},
					},
				},
			}

			text := bucket.ExtractText()
			Expect(text).To(ContainSubstring("Let me check the weather"))
			Expect(text).To(ContainSubstring("Tool call: get_weather"))
			Expect(text).To(ContainSubstring("Paris"))
		})
	})
})
