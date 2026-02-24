package publisher

import (
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/llm"
	"github.com/papercomputeco/tapes/pkg/merkle"
)

func buildNodeForEvent() *merkle.Node {
	bucket := merkle.Bucket{
		Type:     "message",
		Role:     "assistant",
		Model:    "test-model",
		Provider: "test-provider",
		Content: []llm.ContentBlock{
			{Type: "text", Text: "hello from tests"},
		},
	}

	return merkle.NewNode(bucket, nil)
}

var _ = Describe("NewEvent", func() {
	It("returns an error when node is nil", func() {
		event, err := NewEvent(nil)
		Expect(err).To(HaveOccurred())
		Expect(event).To(BeNil())
	})

	It("sets schema, timestamp, and a copy of node data", func() {
		node := buildNodeForEvent()

		before := time.Now()
		event, err := NewEvent(node)
		after := time.Now()

		Expect(err).NotTo(HaveOccurred())
		Expect(event).NotTo(BeNil())
		Expect(event.Schema).To(Equal(SchemaNodeV1))
		Expect(event.OccurredAt).To(BeTemporally(">=", before))
		Expect(event.OccurredAt).To(BeTemporally("<=", after.Add(50*time.Millisecond)))
		Expect(event.Node).To(Equal(*node))

		originalHash := node.Hash
		node.Hash = "mutated"
		Expect(event.Node.Hash).To(Equal(originalHash))
	})
})
