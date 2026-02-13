package local

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/llm"
	"github.com/papercomputeco/tapes/pkg/memory"
	"github.com/papercomputeco/tapes/pkg/merkle"
)

func newTestNode(role, text string, parent *merkle.Node) *merkle.Node {
	bucket := merkle.Bucket{
		Type:     "message",
		Role:     role,
		Content:  []llm.ContentBlock{{Type: "text", Text: text}},
		Model:    "test-model",
		Provider: "test-provider",
	}
	return merkle.NewNode(bucket, parent)
}

var _ = Describe("Local Memory Driver", func() {
	var ctx context.Context

	BeforeEach(func() {
		ctx = context.Background()
	})

	Describe("NewDriver", func() {
		It("returns a non-nil driver", func() {
			d := NewDriver(Config{Enabled: true})
			Expect(d).NotTo(BeNil())
			Expect(d.facts).NotTo(BeNil())
		})
	})

	Describe("Store", func() {
		It("extracts facts from nodes", func() {
			d := NewDriver(Config{Enabled: true})

			user := newTestNode("user", "The capital of France is Paris", nil)
			err := d.Store(ctx, []*merkle.Node{user})
			Expect(err).NotTo(HaveOccurred())

			Expect(d.facts).To(HaveKey(user.Hash))
			Expect(d.facts[user.Hash]).To(HaveLen(1))
			Expect(d.facts[user.Hash][0].Content).To(Equal("The capital of France is Paris"))
		})

		It("stores multiple nodes", func() {
			d := NewDriver(Config{Enabled: true})

			n1 := newTestNode("user", "Hello", nil)
			n2 := newTestNode("assistant", "Hi there!", n1)

			err := d.Store(ctx, []*merkle.Node{n1, n2})
			Expect(err).NotTo(HaveOccurred())

			Expect(d.facts).To(HaveLen(2))
			Expect(d.facts).To(HaveKey(n1.Hash))
			Expect(d.facts).To(HaveKey(n2.Hash))
		})

		It("handles empty node slice", func() {
			d := NewDriver(Config{Enabled: true})
			err := d.Store(ctx, []*merkle.Node{})
			Expect(err).NotTo(HaveOccurred())
			Expect(d.facts).To(BeEmpty())
		})

		It("skips nodes with no text content", func() {
			d := NewDriver(Config{Enabled: true})

			node := &merkle.Node{
				Hash: "emptyhash",
				Bucket: merkle.Bucket{
					Type:    "message",
					Role:    "user",
					Content: []llm.ContentBlock{{Type: "image", ImageURL: "http://example.com/img.png"}},
				},
			}
			err := d.Store(ctx, []*merkle.Node{node})
			Expect(err).NotTo(HaveOccurred())

			Expect(d.facts).To(BeEmpty())
		})

		It("is a no-op when disabled", func() {
			d := NewDriver(Config{Enabled: false})

			node := newTestNode("user", "some text", nil)
			err := d.Store(ctx, []*merkle.Node{node})
			Expect(err).NotTo(HaveOccurred())

			Expect(d.facts).To(BeEmpty())
		})
	})

	Describe("Recall", func() {
		It("returns facts for a stored node hash", func() {
			d := NewDriver(Config{Enabled: true})

			node := newTestNode("user", "Go was created at Google", nil)
			err := d.Store(ctx, []*merkle.Node{node})
			Expect(err).NotTo(HaveOccurred())

			facts, err := d.Recall(ctx, node.Hash)
			Expect(err).NotTo(HaveOccurred())
			Expect(facts).To(HaveLen(1))
			Expect(facts[0].Content).To(Equal("Go was created at Google"))
		})

		It("returns nil for unknown hash", func() {
			d := NewDriver(Config{Enabled: true})

			facts, err := d.Recall(ctx, "nonexistent")
			Expect(err).NotTo(HaveOccurred())
			Expect(facts).To(BeNil())
		})

		It("returns nil when disabled", func() {
			d := NewDriver(Config{Enabled: false})

			facts, err := d.Recall(ctx, "anything")
			Expect(err).NotTo(HaveOccurred())
			Expect(facts).To(BeNil())
		})

		It("returns a copy so callers cannot mutate internal state", func() {
			d := NewDriver(Config{Enabled: true})

			node := newTestNode("user", "original fact", nil)
			_ = d.Store(ctx, []*merkle.Node{node})

			facts, err := d.Recall(ctx, node.Hash)
			Expect(err).NotTo(HaveOccurred())
			Expect(facts).To(HaveLen(1))

			// Mutate the returned slice
			facts[0].Content = "mutated"

			// Internal state should be unchanged
			internal, err := d.Recall(ctx, node.Hash)
			Expect(err).NotTo(HaveOccurred())
			Expect(internal[0].Content).To(Equal("original fact"))
		})
	})

	Describe("interface compliance", func() {
		It("satisfies memory.Driver", func() {
			var _ memory.Driver = NewDriver(Config{})
		})
	})

	Describe("Close", func() {
		It("is a no-op and returns nil", func() {
			d := NewDriver(Config{})
			Expect(d.Close()).To(Succeed())
		})
	})
})
