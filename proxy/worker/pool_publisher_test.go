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
	"github.com/papercomputeco/tapes/pkg/storage"
	"github.com/papercomputeco/tapes/pkg/storage/inmemory"
)

type mockPublisher struct {
	mu sync.Mutex

	published  []*publisher.Event
	publishErr error
	closeCalls int
}

func (m *mockPublisher) Publish(_ context.Context, event *publisher.Event) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if event != nil {
		copied := *event
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

func (m *mockPublisher) publishedNodeHashes() []string {
	m.mu.Lock()
	defer m.mu.Unlock()

	hashes := make([]string, 0, len(m.published))
	for _, event := range m.published {
		hashes = append(hashes, event.Node.Hash)
	}

	return hashes
}

func (m *mockPublisher) publishedRootHashes() []string {
	m.mu.Lock()
	defer m.mu.Unlock()

	hashes := make([]string, 0, len(m.published))
	for _, event := range m.published {
		hashes = append(hashes, event.RootHash)
	}

	return hashes
}

func (m *mockPublisher) closeCallCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.closeCalls
}

type ancestryFailDriver struct {
	*inmemory.Driver
	ancestryErr error
}

func (d *ancestryFailDriver) Ancestry(_ context.Context, _ string) ([]*merkle.Node, error) {
	return nil, d.ancestryErr
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

func newPublisherTestPoolWithDriver(pub publisher.Publisher, driver storage.Driver) *Pool {
	logger, _ := zap.NewDevelopment()

	wp, err := NewPool(&Config{
		Driver:     driver,
		Publisher:  pub,
		Logger:     logger,
		NumWorkers: 1,
	})
	Expect(err).NotTo(HaveOccurred())

	return wp
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
	It("publishes only newly inserted nodes keyed to the conversation root", func() {
		pub := &mockPublisher{}
		wp, driver := newPublisherTestPool(pub)
		ctx := context.Background()

		Expect(wp.Enqueue(buildTurnOneJob())).To(BeTrue())
		Expect(wp.Enqueue(buildTurnTwoJob())).To(BeTrue())
		wp.Close()

		nodes, err := driver.List(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(nodes).To(HaveLen(5))

		leaves, err := driver.Leaves(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(leaves).To(HaveLen(1))

		ancestry, err := driver.Ancestry(ctx, leaves[0].Hash)
		Expect(err).NotTo(HaveOccurred())
		Expect(ancestry).NotTo(BeEmpty())
		rootHash := ancestry[len(ancestry)-1].Hash

		nodeHashes := pub.publishedNodeHashes()
		Expect(nodeHashes).To(HaveLen(5))
		Expect(nodeHashes).To(ConsistOf(
			nodes[0].Hash,
			nodes[1].Hash,
			nodes[2].Hash,
			nodes[3].Hash,
			nodes[4].Hash,
		))

		rootHashes := pub.publishedRootHashes()
		Expect(rootHashes).To(HaveLen(5))
		for _, publishedRootHash := range rootHashes {
			Expect(publishedRootHash).To(Equal(rootHash))
		}
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

	It("skips publishing for a turn when root derivation fails", func() {
		pub := &mockPublisher{}
		backing := inmemory.NewDriver()
		driver := &ancestryFailDriver{
			Driver:      backing,
			ancestryErr: errors.New("ancestry failed"),
		}
		wp := newPublisherTestPoolWithDriver(pub, driver)
		ctx := context.Background()

		Expect(wp.Enqueue(buildTurnOneJob())).To(BeTrue())
		wp.Close()

		nodes, err := backing.List(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(nodes).To(HaveLen(3))
		Expect(pub.publishCalls()).To(Equal(0))
	})

	It("closes the configured publisher when the pool closes", func() {
		pub := &mockPublisher{}
		wp, _ := newPublisherTestPool(pub)

		wp.Close()

		Expect(pub.closeCallCount()).To(Equal(1))
	})
})
