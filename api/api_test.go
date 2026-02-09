package api

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.uber.org/zap"

	"github.com/papercomputeco/tapes/pkg/llm"
	"github.com/papercomputeco/tapes/pkg/merkle"
	"github.com/papercomputeco/tapes/pkg/storage"
	"github.com/papercomputeco/tapes/pkg/storage/inmemory"
)

// apiTestBucket creates a simple bucket for testing with the given role and text content
func apiTestBucket(role, text string) merkle.Bucket {
	return merkle.Bucket{
		Type:     "message",
		Role:     role,
		Content:  []llm.ContentBlock{{Type: "text", Text: text}},
		Model:    "test-model",
		Provider: "test-provider",
	}
}

var _ = Describe("buildHistory", func() {
	var (
		server    *Server
		driver    storage.Driver
		dagLoader merkle.DagLoader
		ctx       context.Context
	)

	BeforeEach(func() {
		var err error
		logger, _ := zap.NewDevelopment()
		inMem := inmemory.NewDriver()
		driver = inMem
		dagLoader = inMem
		server, err = NewServer(Config{ListenAddr: ":0"}, driver, dagLoader, inmemory.NewAgentTraceStore(), logger)
		Expect(err).ToNot(HaveOccurred())
		ctx = context.Background()
	})

	Context("when the node does not exist", func() {
		It("returns an error", func() {
			_, err := server.buildHistory(ctx, "nonexistent")
			Expect(err).To(HaveOccurred())
		})
	})

	Context("when building history for a root node", func() {
		var rootNode *merkle.Node

		BeforeEach(func() {
			rootNode = merkle.NewNode(apiTestBucket("user", "Hello"), nil)
			_, err := driver.Put(ctx, rootNode)
			Expect(err).NotTo(HaveOccurred())
		})

		It("returns a history with depth 1", func() {
			history, err := server.buildHistory(ctx, rootNode.Hash)
			Expect(err).NotTo(HaveOccurred())
			Expect(history.Depth).To(Equal(1))
		})

		It("sets the head hash to the requested node", func() {
			history, err := server.buildHistory(ctx, rootNode.Hash)
			Expect(err).NotTo(HaveOccurred())
			Expect(history.HeadHash).To(Equal(rootNode.Hash))
		})

		It("extracts message fields from node bucket", func() {
			history, err := server.buildHistory(ctx, rootNode.Hash)
			Expect(err).NotTo(HaveOccurred())
			Expect(history.Messages).To(HaveLen(1))
			Expect(history.Messages[0].Role).To(Equal("user"))
			Expect(history.Messages[0].Content).To(HaveLen(1))
			Expect(history.Messages[0].Content[0].Text).To(Equal("Hello"))
			Expect(history.Messages[0].Model).To(Equal("test-model"))
		})

		It("sets ParentHash to nil for root messages", func() {
			history, err := server.buildHistory(ctx, rootNode.Hash)
			Expect(err).NotTo(HaveOccurred())
			Expect(history.Messages[0].ParentHash).To(BeNil())
		})
	})

	Context("when building history for a conversation chain", func() {
		var node1, node2, node3 *merkle.Node

		BeforeEach(func() {
			node1 = merkle.NewNode(apiTestBucket("user", "Hello"), nil)
			node2 = merkle.NewNode(apiTestBucket("assistant", "Hi there!"), node1)
			node3 = merkle.NewNode(apiTestBucket("user", "How are you?"), node2)

			_, err := driver.Put(ctx, node1)
			Expect(err).NotTo(HaveOccurred())
			_, err = driver.Put(ctx, node2)
			Expect(err).NotTo(HaveOccurred())
			_, err = driver.Put(ctx, node3)
			Expect(err).NotTo(HaveOccurred())
		})

		It("returns the correct depth", func() {
			history, err := server.buildHistory(ctx, node3.Hash)
			Expect(err).NotTo(HaveOccurred())
			Expect(history.Depth).To(Equal(3))
		})

		It("returns messages in chronological order (oldest first)", func() {
			history, err := server.buildHistory(ctx, node3.Hash)
			Expect(err).NotTo(HaveOccurred())

			Expect(history.Messages[0].Content[0].Text).To(Equal("Hello"))
			Expect(history.Messages[1].Content[0].Text).To(Equal("Hi there!"))
			Expect(history.Messages[2].Content[0].Text).To(Equal("How are you?"))
		})

		It("correctly links parent hashes", func() {
			history, err := server.buildHistory(ctx, node3.Hash)
			Expect(err).NotTo(HaveOccurred())

			Expect(history.Messages[0].ParentHash).To(BeNil())
			Expect(history.Messages[1].ParentHash).NotTo(BeNil())
			Expect(*history.Messages[1].ParentHash).To(Equal(node1.Hash))
			Expect(history.Messages[2].ParentHash).NotTo(BeNil())
			Expect(*history.Messages[2].ParentHash).To(Equal(node2.Hash))
		})

		It("can build history from any node in the chain", func() {
			history, err := server.buildHistory(ctx, node2.Hash)
			Expect(err).NotTo(HaveOccurred())

			Expect(history.Depth).To(Equal(2))
			Expect(history.HeadHash).To(Equal(node2.Hash))
			Expect(history.Messages[0].Content[0].Text).To(Equal("Hello"))
			Expect(history.Messages[1].Content[0].Text).To(Equal("Hi there!"))
		})
	})

	Context("when node bucket has usage metrics", func() {
		var node *merkle.Node

		BeforeEach(func() {
			bucket := merkle.Bucket{
				Type:     "message",
				Role:     "assistant",
				Content:  []llm.ContentBlock{{Type: "text", Text: "Response"}},
				Model:    "gpt-4",
				Provider: "openai",
			}
			node = merkle.NewNode(bucket, nil, merkle.NodeMeta{
				StopReason: "stop",
				Usage: &llm.Usage{
					PromptTokens:     100,
					CompletionTokens: 50,
					TotalTokens:      150,
				},
			})
			_, err := driver.Put(ctx, node)
			Expect(err).NotTo(HaveOccurred())
		})

		It("extracts the provider field", func() {
			history, err := server.buildHistory(ctx, node.Hash)
			Expect(err).NotTo(HaveOccurred())
			Expect(history.Messages[0].Provider).To(Equal("openai"))
		})

		It("extracts the stop reason", func() {
			history, err := server.buildHistory(ctx, node.Hash)
			Expect(err).NotTo(HaveOccurred())
			Expect(history.Messages[0].StopReason).To(Equal("stop"))
		})

		It("extracts usage metrics", func() {
			history, err := server.buildHistory(ctx, node.Hash)
			Expect(err).NotTo(HaveOccurred())
			Expect(history.Messages[0].Usage).NotTo(BeNil())
			Expect(history.Messages[0].Usage.TotalTokens).To(Equal(150))
		})
	})
})
