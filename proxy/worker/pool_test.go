package worker

import (
	"context"
	"errors"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.uber.org/zap"

	"github.com/papercomputeco/tapes/pkg/eventstream"
	"github.com/papercomputeco/tapes/pkg/llm"
	"github.com/papercomputeco/tapes/pkg/storage/inmemory"
)

// newTestPool creates a worker pool backed by an in-memory driver.
// Callers should "wp.Close()" to drain enqueued jobs before asserting storage state.
func newTestPool() (*Pool, *inmemory.Driver) {
	return newTestPoolWithPublisher(nil)
}

func newTestPoolWithPublisher(publisher eventstream.Publisher) (*Pool, *inmemory.Driver) {
	logger, _ := zap.NewDevelopment()
	driver := inmemory.NewDriver()

	wp, err := NewPool(&Config{
		Driver:    driver,
		Logger:    logger,
		Publisher: publisher,
	})
	Expect(err).NotTo(HaveOccurred())

	return wp, driver
}

type mockPublisher struct {
	mu         sync.Mutex
	events     []*eventstream.TurnPersistedEvent
	publishErr error
}

func (m *mockPublisher) PublishTurn(_ context.Context, event *eventstream.TurnPersistedEvent) error {
	if m.publishErr != nil {
		return m.publishErr
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	m.events = append(m.events, event)
	return nil
}

func (m *mockPublisher) Close() error {
	return nil
}

func (m *mockPublisher) Events() []*eventstream.TurnPersistedEvent {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]*eventstream.TurnPersistedEvent(nil), m.events...)
}

var _ = Describe("Worker Pool", func() {
	var (
		wp     *Pool
		driver *inmemory.Driver
		ctx    context.Context
	)

	BeforeEach(func() {
		wp, driver = newTestPool()
		ctx = context.Background()
	})

	Describe("Enqueue", func() {
		It("returns true when the queue has capacity", func() {
			ok := wp.Enqueue(Job{
				Provider: "test-provider",
				Req: &llm.ChatRequest{
					Model: "test-model",
					Messages: []llm.Message{
						{Role: "user", Content: []llm.ContentBlock{{Type: "text", Text: "hello"}}},
					},
				},
				Resp: &llm.ChatResponse{
					Model: "test-model",
					Message: llm.Message{
						Role:    "assistant",
						Content: []llm.ContentBlock{{Type: "text", Text: "hi"}},
					},
				},
			})
			Expect(ok).To(BeTrue())
			wp.Close()
		})
	})

	Describe("Publisher Hook", func() {
		It("publishes a turn persisted event after storage succeeds", func() {
			mockPub := &mockPublisher{}
			wp, driver = newTestPoolWithPublisher(mockPub)

			startedAt := time.Now().Add(-250 * time.Millisecond).UTC()
			completedAt := startedAt.Add(250 * time.Millisecond).UTC()
			ok := wp.Enqueue(Job{
				Provider:    "test-provider",
				AgentName:   "codex",
				Path:        "/v1/chat/completions",
				StartedAt:   startedAt,
				CompletedAt: completedAt,
				Streaming:   true,
				HTTPStatus:  200,
				Req: &llm.ChatRequest{
					Model: "test-model",
					Messages: []llm.Message{
						{Role: "user", Content: []llm.ContentBlock{{Type: "text", Text: "hello"}}},
					},
				},
				Resp: &llm.ChatResponse{
					Model:      "test-model",
					StopReason: "stop",
					Message: llm.Message{
						Role:    "assistant",
						Content: []llm.ContentBlock{{Type: "text", Text: "hi"}},
					},
				},
			})
			Expect(ok).To(BeTrue())

			wp.Close()

			events := mockPub.Events()
			Expect(events).To(HaveLen(1))

			event := events[0]
			Expect(event.SchemaVersion).To(Equal(eventstream.SchemaVersionV1))
			Expect(event.EventType).To(Equal(eventstream.EventTypeTurnPersisted))
			Expect(event.Source.Provider).To(Equal("test-provider"))
			Expect(event.Source.AgentName).To(Equal("codex"))
			Expect(event.RequestMeta.Path).To(Equal("/v1/chat/completions"))
			Expect(event.RequestMeta.Streaming).To(BeTrue())
			Expect(event.RequestMeta.HTTPStatus).To(Equal(200))
			Expect(event.RequestMeta.DurationMs).To(Equal(int64(250)))

			Expect(event.DAG.HeadHash).NotTo(BeEmpty())
			Expect(event.EventID).To(Equal(event.DAG.HeadHash))
			Expect(event.DAG.TurnNodeHashes).To(HaveLen(2))
			Expect(event.DAG.NewNodeHashes).To(HaveLen(2))
			Expect(event.DAG.RootHash).To(Equal(event.DAG.TurnNodeHashes[0]))
			Expect(event.DAG.HeadHash).To(Equal(event.DAG.TurnNodeHashes[1]))

			Expect(event.Turn.Provider).To(Equal("test-provider"))
			Expect(event.Turn.Request).NotTo(BeNil())
			Expect(event.Turn.Response).NotTo(BeNil())

			nodes, err := driver.List(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(nodes).To(HaveLen(2))
		})

		It("continues storage when publisher returns an error", func() {
			mockPub := &mockPublisher{publishErr: errors.New("publish failed")}
			wp, driver = newTestPoolWithPublisher(mockPub)

			ok := wp.Enqueue(Job{
				Provider: "test-provider",
				Req: &llm.ChatRequest{
					Model: "test-model",
					Messages: []llm.Message{
						{Role: "user", Content: []llm.ContentBlock{{Type: "text", Text: "hello"}}},
					},
				},
				Resp: &llm.ChatResponse{
					Model: "test-model",
					Message: llm.Message{
						Role:    "assistant",
						Content: []llm.ContentBlock{{Type: "text", Text: "hi"}},
					},
				},
			})
			Expect(ok).To(BeTrue())

			wp.Close()

			nodes, err := driver.List(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(nodes).To(HaveLen(2))

			Expect(mockPub.Events()).To(BeEmpty())
		})
	})

	Describe("Multi-Turn Conversation Storage", func() {
		// These tests exercise the worker pool's storeConversationTurn logic
		// by enqueuing jobs and draining via wp.Close() before asserting storage state.

		Context("after turn 1 (user asks a question)", func() {
			BeforeEach(func() {
				req1 := &llm.ChatRequest{
					Model: "test-model",
					Messages: []llm.Message{
						{Role: "system", Content: []llm.ContentBlock{{Type: "text", Text: "You are a helpful assistant."}}},
						{Role: "user", Content: []llm.ContentBlock{{Type: "text", Text: "What is 2+2?"}}},
					},
				}
				resp1 := &llm.ChatResponse{
					Model:      "test-model",
					StopReason: "stop",
					Usage:      &llm.Usage{PromptTokens: 10, CompletionTokens: 5, TotalTokens: 15},
					Message: llm.Message{
						Role:    "assistant",
						Content: []llm.ContentBlock{{Type: "text", Text: "2+2 equals 4."}},
					},
				}

				wp.Enqueue(Job{
					Provider: "test-provider",
					Req:      req1,
					Resp:     resp1,
				})

				// Drain the worker pool to ensure storage completes before assertions
				wp.Close()
			})

			It("has 3 nodes in ancestry (system -> user -> assistant)", func() {
				leaves, err := driver.Leaves(ctx)
				Expect(err).NotTo(HaveOccurred())
				Expect(leaves).To(HaveLen(1))

				ancestry, err := driver.Ancestry(ctx, leaves[0].Hash)
				Expect(err).NotTo(HaveOccurred())
				Expect(ancestry).To(HaveLen(3))
			})

			It("orders ancestry from newest to oldest", func() {
				leaves, err := driver.Leaves(ctx)
				Expect(err).NotTo(HaveOccurred())

				ancestry, err := driver.Ancestry(ctx, leaves[0].Hash)
				Expect(err).NotTo(HaveOccurred())
				Expect(ancestry[0].Bucket.Role).To(Equal("assistant"))
				Expect(ancestry[1].Bucket.Role).To(Equal("user"))
				Expect(ancestry[2].Bucket.Role).To(Equal("system"))
			})
		})

		Context("multi-turn conversation with replayed messages", func() {
			BeforeEach(func() {
				// Turn 1
				req1 := &llm.ChatRequest{
					Model: "test-model",
					Messages: []llm.Message{
						{Role: "system", Content: []llm.ContentBlock{{Type: "text", Text: "You are a helpful assistant."}}},
						{Role: "user", Content: []llm.ContentBlock{{Type: "text", Text: "What is 2+2?"}}},
					},
				}
				resp1 := &llm.ChatResponse{
					Model:      "test-model",
					StopReason: "stop",
					Usage:      &llm.Usage{PromptTokens: 10, CompletionTokens: 5, TotalTokens: 15},
					Message: llm.Message{
						Role:    "assistant",
						Content: []llm.ContentBlock{{Type: "text", Text: "2+2 equals 4."}},
					},
				}

				wp.Enqueue(Job{
					Provider: "test-provider",
					Req:      req1,
					Resp:     resp1,
				})

				// Turn 2 (replays turn 1 messages + adds new)
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

				wp.Enqueue(Job{
					Provider: "test-provider",
					Req:      req2,
					Resp:     resp2,
				})

				// Drain the worker pool to ensure all storage completes
				wp.Close()
			})

			It("has 5 nodes in ancestry (full conversation history)", func() {
				leaves, err := driver.Leaves(ctx)
				Expect(err).NotTo(HaveOccurred())
				Expect(leaves).To(HaveLen(1))

				ancestry, err := driver.Ancestry(ctx, leaves[0].Hash)
				Expect(err).NotTo(HaveOccurred())
				Expect(ancestry).To(HaveLen(5))
			})

			It("orders the full chain from newest to oldest", func() {
				leaves, err := driver.Leaves(ctx)
				Expect(err).NotTo(HaveOccurred())

				ancestry, err := driver.Ancestry(ctx, leaves[0].Hash)
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

			It("reuses the original assistant response from turn 1 (same hash via dedup)", func() {
				// The replayed assistant message "2+2 equals 4." should have the same
				// hash in both turns since content-addressing is deterministic.
				nodes, err := driver.List(ctx)
				Expect(err).NotTo(HaveOccurred())

				// 5 unique nodes: system, user1, assistant1, user2, assistant2
				Expect(nodes).To(HaveLen(5))
			})
		})
	})
})
