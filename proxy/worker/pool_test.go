package worker

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/llm"
	tapeslogger "github.com/papercomputeco/tapes/pkg/logger"
	"github.com/papercomputeco/tapes/pkg/storage"
	"github.com/papercomputeco/tapes/pkg/storage/inmemory"
)

// newTestPool creates a worker pool backed by an in-memory driver.
// Callers should "wp.Close()" to drain enqueued jobs before asserting storage state.
func newTestPool() (*Pool, storage.Driver) {
	logger := tapeslogger.NewNoop()
	driver := inmemory.NewDriver()

	wp, err := NewPool(&Config{
		Driver: driver,
		Logger: logger,
	})
	Expect(err).NotTo(HaveOccurred())

	return wp, driver
}

var _ = Describe("Worker Pool", func() {
	var (
		wp     *Pool
		driver storage.Driver
		ctx    context.Context
	)

	BeforeEach(func() {
		wp, driver = newTestPool()
		ctx = context.Background()
	})

	Describe("Enqueue", func() {
		It("closes cleanly when publisher is nil", func() {
			Expect(func() {
				wp.Close()
			}).NotTo(Panic())
		})

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

	Describe("Len", func() {
		It("reports zero on an empty, drained pool", func() {
			wp.Close()
			Expect(wp.Len()).To(Equal(0))
		})

		It("reflects pending items still on the queue", func() {
			// The drained-pool spec above would still pass against a
			// stub `Len() int { return 0 }` regression — that's the
			// hazard the production gauge actually reads. Exercise the
			// accessor with non-zero pending items.
			//
			// Bypassing NewPool here is deliberate: NewPool starts
			// workers that would drain whatever we enqueue before the
			// assertion runs, so a pool wired to a stub queue with no
			// consumers is the only way to read Len() deterministically
			// across non-empty states.
			p := &Pool{queue: make(chan Job, 4)}
			Expect(p.Len()).To(Equal(0))

			p.queue <- Job{Provider: "first"}
			Expect(p.Len()).To(Equal(1))

			p.queue <- Job{Provider: "second"}
			Expect(p.Len()).To(Equal(2))
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
