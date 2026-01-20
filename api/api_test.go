package api

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.uber.org/zap"

	"github.com/papercomputeco/tapes/pkg/merkle"
)

var _ = Describe("buildHistory", func() {
	var (
		server *Server
		storer merkle.Storer
		ctx    context.Context
	)

	BeforeEach(func() {
		logger, _ := zap.NewDevelopment()
		storer = merkle.NewMemoryStorer()
		server = NewServer(Config{ListenAddr: ":0"}, storer, logger)
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
			rootNode = merkle.NewNode(map[string]any{
				"type":    "message",
				"role":    "user",
				"content": "Hello",
				"model":   "test-model",
			}, nil)
			Expect(storer.Put(ctx, rootNode)).To(Succeed())
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

		It("extracts message fields from node content", func() {
			history, err := server.buildHistory(ctx, rootNode.Hash)
			Expect(err).NotTo(HaveOccurred())
			Expect(history.Messages).To(HaveLen(1))
			Expect(history.Messages[0].Role).To(Equal("user"))
			Expect(history.Messages[0].Content).To(Equal("Hello"))
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
			node1 = merkle.NewNode(map[string]any{
				"type":    "message",
				"role":    "user",
				"content": "Hello",
				"model":   "test-model",
			}, nil)
			node2 = merkle.NewNode(map[string]any{
				"type":    "message",
				"role":    "assistant",
				"content": "Hi there!",
				"model":   "test-model",
			}, node1)
			node3 = merkle.NewNode(map[string]any{
				"type":    "message",
				"role":    "user",
				"content": "How are you?",
				"model":   "test-model",
			}, node2)

			Expect(storer.Put(ctx, node1)).To(Succeed())
			Expect(storer.Put(ctx, node2)).To(Succeed())
			Expect(storer.Put(ctx, node3)).To(Succeed())
		})

		It("returns the correct depth", func() {
			history, err := server.buildHistory(ctx, node3.Hash)
			Expect(err).NotTo(HaveOccurred())
			Expect(history.Depth).To(Equal(3))
		})

		It("returns messages in chronological order (oldest first)", func() {
			history, err := server.buildHistory(ctx, node3.Hash)
			Expect(err).NotTo(HaveOccurred())

			Expect(history.Messages[0].Content).To(Equal("Hello"))
			Expect(history.Messages[1].Content).To(Equal("Hi there!"))
			Expect(history.Messages[2].Content).To(Equal("How are you?"))
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
			Expect(history.Messages[0].Content).To(Equal("Hello"))
			Expect(history.Messages[1].Content).To(Equal("Hi there!"))
		})
	})

	Context("when node content has additional metadata", func() {
		var node *merkle.Node

		BeforeEach(func() {
			node = merkle.NewNode(map[string]any{
				"type":       "message",
				"role":       "assistant",
				"content":    "Response",
				"model":      "gpt-4",
				"provider":   "openai",
				"custom_key": "custom_value",
				"tokens":     150,
			}, nil)
			Expect(storer.Put(ctx, node)).To(Succeed())
		})

		It("extracts the provider field", func() {
			history, err := server.buildHistory(ctx, node.Hash)
			Expect(err).NotTo(HaveOccurred())
			Expect(history.Messages[0].Provider).To(Equal("openai"))
		})

		It("captures extra fields in metadata", func() {
			history, err := server.buildHistory(ctx, node.Hash)
			Expect(err).NotTo(HaveOccurred())
			Expect(history.Messages[0].Metadata).To(HaveKeyWithValue("custom_key", "custom_value"))
			Expect(history.Messages[0].Metadata).To(HaveKeyWithValue("tokens", 150))
		})

		It("excludes standard fields from metadata", func() {
			history, err := server.buildHistory(ctx, node.Hash)
			Expect(err).NotTo(HaveOccurred())
			Expect(history.Messages[0].Metadata).NotTo(HaveKey("role"))
			Expect(history.Messages[0].Metadata).NotTo(HaveKey("content"))
			Expect(history.Messages[0].Metadata).NotTo(HaveKey("model"))
			Expect(history.Messages[0].Metadata).NotTo(HaveKey("type"))
			Expect(history.Messages[0].Metadata).NotTo(HaveKey("provider"))
		})
	})

	Context("when node content is not a map", func() {
		var node *merkle.Node

		BeforeEach(func() {
			node = merkle.NewNode("plain string content", nil)
			Expect(storer.Put(ctx, node)).To(Succeed())
		})

		It("returns a message with empty fields", func() {
			history, err := server.buildHistory(ctx, node.Hash)
			Expect(err).NotTo(HaveOccurred())
			Expect(history.Messages).To(HaveLen(1))
			Expect(history.Messages[0].Role).To(BeEmpty())
			Expect(history.Messages[0].Content).To(BeNil())
			Expect(history.Messages[0].Hash).To(Equal(node.Hash))
		})
	})
})
