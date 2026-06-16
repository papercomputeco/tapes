package worker

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/llm"
	tapeslogger "github.com/papercomputeco/tapes/pkg/logger"
	"github.com/papercomputeco/tapes/pkg/storage/inmemory"
)

// newTestPool creates a worker pool backed by a bare in-memory driver
// (no raw-turn or sessions surface), used for the queue-mechanics specs.
func newTestPool() *Pool {
	wp, err := NewPool(&Config{
		Driver: inmemory.NewDriver(),
		Logger: tapeslogger.NewNoop(),
	})
	Expect(err).NotTo(HaveOccurred())
	return wp
}

var _ = Describe("Worker Pool", func() {
	var wp *Pool

	BeforeEach(func() {
		wp = newTestPool()
	})

	Describe("Enqueue", func() {
		It("closes cleanly on a drained pool", func() {
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
})
