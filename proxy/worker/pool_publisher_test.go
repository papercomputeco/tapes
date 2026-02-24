package worker

import (
	"context"
	"errors"
	"sync"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.uber.org/zap"

	"github.com/papercomputeco/tapes/pkg/llm"
	"github.com/papercomputeco/tapes/pkg/merkle"
	"github.com/papercomputeco/tapes/pkg/publisher"
	"github.com/papercomputeco/tapes/pkg/storage/inmemory"
)

type mockPublisher struct {
	mu sync.Mutex

	published  []*merkle.Node
	publishErr error
	closeCalls int
}

func (m *mockPublisher) Publish(_ context.Context, node *merkle.Node) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if node != nil {
		copied := *node
		m.published = append(m.published, &copied)
	}

	return m.publishErr
}

func (m *mockPublisher) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closeCalls++
	return nil
}

func (m *mockPublisher) publishCalls() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.published)
}

func (m *mockPublisher) publishedHashes() []string {
	m.mu.Lock()
	defer m.mu.Unlock()

	hashes := make([]string, 0, len(m.published))
	for _, node := range m.published {
		hashes = append(hashes, node.Hash)
	}

	return hashes
}

func (m *mockPublisher) closeCallCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.closeCalls
}

func newPublisherTestPool(pub publisher.Publisher) (*Pool, *inmemory.Driver) {
	logger, _ := zap.NewDevelopment()
	driver := inmemory.NewDriver()

	wp, err := NewPool(&Config{
		Driver:     driver,
		Publisher:  pub,
		Logger:     logger,
		NumWorkers: 1,
	})
	Expect(err).NotTo(HaveOccurred())

	return wp, driver
}

func buildTurnOneJob() Job {
	return Job{
		Provider: "test-provider",
		Req: &llm.ChatRequest{
			Model: "test-model",
			Messages: []llm.Message{
				{Role: "system", Content: []llm.ContentBlock{{Type: "text", Text: "You are a helpful assistant."}}},
				{Role: "user", Content: []llm.ContentBlock{{Type: "text", Text: "What is 2+2?"}}},
			},
		},
		Resp: &llm.ChatResponse{
			Model:      "test-model",
			StopReason: "stop",
			Usage:      &llm.Usage{PromptTokens: 10, CompletionTokens: 5, TotalTokens: 15},
			Message: llm.Message{
				Role:    "assistant",
				Content: []llm.ContentBlock{{Type: "text", Text: "2+2 equals 4."}},
			},
		},
	}
}

func buildTurnTwoJob() Job {
	return Job{
		Provider: "test-provider",
		Req: &llm.ChatRequest{
			Model: "test-model",
			Messages: []llm.Message{
				{Role: "system", Content: []llm.ContentBlock{{Type: "text", Text: "You are a helpful assistant."}}},
				{Role: "user", Content: []llm.ContentBlock{{Type: "text", Text: "What is 2+2?"}}},
				{Role: "assistant", Content: []llm.ContentBlock{{Type: "text", Text: "2+2 equals 4."}}}, // Replayed
				{Role: "user", Content: []llm.ContentBlock{{Type: "text", Text: "And what is 3+3?"}}},   // New
			},
		},
		Resp: &llm.ChatResponse{
			Model:      "test-model",
			StopReason: "stop",
			Usage:      &llm.Usage{PromptTokens: 20, CompletionTokens: 5, TotalTokens: 25},
			Message: llm.Message{
				Role:    "assistant",
				Content: []llm.ContentBlock{{Type: "text", Text: "3+3 equals 6."}},
			},
		},
	}
}

var _ = Describe("Worker Pool Publisher Integration", func() {
	It("publishes only nodes that were newly inserted", func() {
		pub := &mockPublisher{}
		wp, driver := newPublisherTestPool(pub)
		ctx := context.Background()

		Expect(wp.Enqueue(buildTurnOneJob())).To(BeTrue())
		Expect(wp.Enqueue(buildTurnTwoJob())).To(BeTrue())
		wp.Close()

		nodes, err := driver.List(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(nodes).To(HaveLen(5))

		hashes := pub.publishedHashes()
		Expect(hashes).To(HaveLen(5))
		Expect(hashes).To(ConsistOf(
			nodes[0].Hash,
			nodes[1].Hash,
			nodes[2].Hash,
			nodes[3].Hash,
			nodes[4].Hash,
		))
	})

	It("continues storing when Publish returns an error", func() {
		pub := &mockPublisher{
			publishErr: errors.New("publish failed"),
		}
		wp, driver := newPublisherTestPool(pub)
		ctx := context.Background()

		Expect(wp.Enqueue(buildTurnOneJob())).To(BeTrue())
		wp.Close()

		nodes, err := driver.List(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(nodes).To(HaveLen(3))
		Expect(pub.publishCalls()).To(Equal(3))
	})

	It("closes the configured publisher when the pool closes", func() {
		pub := &mockPublisher{}
		wp, _ := newPublisherTestPool(pub)

		wp.Close()

		Expect(pub.closeCallCount()).To(Equal(1))
	})
})
