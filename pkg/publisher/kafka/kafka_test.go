package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/llm"
	"github.com/papercomputeco/tapes/pkg/merkle"
	basepublisher "github.com/papercomputeco/tapes/pkg/publisher"
)

type mockWriter struct {
	writes     []Message
	writeErr   error
	closeErr   error
	closeCalls int
}

func (m *mockWriter) WriteMessages(_ context.Context, messages ...Message) error {
	if m.writeErr != nil {
		return m.writeErr
	}

	m.writes = append(m.writes, messages...)
	return nil
}

func (m *mockWriter) Close() error {
	m.closeCalls++
	return m.closeErr
}

func buildKafkaTestNode() *merkle.Node {
	return merkle.NewNode(merkle.Bucket{
		Type:     "message",
		Role:     "assistant",
		Model:    "test-model",
		Provider: "test-provider",
		Content: []llm.ContentBlock{
			{
				Type: "text",
				Text: "hello kafka",
			},
		},
	}, nil)
}

var _ = Describe("NewPublisher", func() {
	It("returns an error when brokers are not configured", func() {
		pub, err := NewPublisher(Config{
			Topic: "tapes.nodes.v1",
		})

		Expect(err).To(HaveOccurred())
		Expect(pub).To(BeNil())
	})

	It("returns an error when topic is empty", func() {
		pub, err := NewPublisher(Config{
			Brokers: []string{"localhost:9092"},
		})

		Expect(err).To(HaveOccurred())
		Expect(pub).To(BeNil())
	})
})

var _ = Describe("Publisher", func() {
	It("writes one keyed message containing a marshaled Event payload", func() {
		writer := &mockWriter{}
		pub, err := newPublisherWithWriter(Config{
			Topic:          "tapes.nodes.v1",
			PublishTimeout: 2 * time.Second,
		}, writer)
		Expect(err).NotTo(HaveOccurred())

		node := buildKafkaTestNode()
		err = pub.Publish(context.Background(), node)
		Expect(err).NotTo(HaveOccurred())

		Expect(writer.writes).To(HaveLen(1))
		Expect(string(writer.writes[0].Key)).To(Equal(node.Hash))

		var event basepublisher.Event
		Expect(json.Unmarshal(writer.writes[0].Value, &event)).To(Succeed())
		Expect(event.Schema).To(Equal(basepublisher.SchemaNodeV1))
		Expect(event.Node.Hash).To(Equal(node.Hash))
	})

	It("returns writer errors from Publish", func() {
		writer := &mockWriter{
			writeErr: errors.New("write failed"),
		}
		pub, err := newPublisherWithWriter(Config{
			Topic: "tapes.nodes.v1",
		}, writer)
		Expect(err).NotTo(HaveOccurred())

		err = pub.Publish(context.Background(), buildKafkaTestNode())
		Expect(err).To(MatchError(ContainSubstring("write failed")))
	})

	It("returns an error from Publish for nil nodes", func() {
		writer := &mockWriter{}
		pub, err := newPublisherWithWriter(Config{
			Topic: "tapes.nodes.v1",
		}, writer)
		Expect(err).NotTo(HaveOccurred())

		err = pub.Publish(context.Background(), nil)
		Expect(err).To(MatchError(basepublisher.ErrNilNode))
	})

	It("delegates Close to the underlying writer", func() {
		writer := &mockWriter{}
		pub, err := newPublisherWithWriter(Config{
			Topic: "tapes.nodes.v1",
		}, writer)
		Expect(err).NotTo(HaveOccurred())

		Expect(pub.Close()).To(Succeed())
		Expect(writer.closeCalls).To(Equal(1))
	})
})
