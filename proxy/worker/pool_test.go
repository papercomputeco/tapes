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

	Describe("Admit", func() {
		// Bypass NewPool as the Len spec does: real workers would drain the
		// queue and release reserved weight before the assertions run, so a
		// consumer-less pool is the only way to pin in-flight bytes.
		weighted := func(weight int) Job {
			return Job{Provider: "p", Req: &llm.ChatRequest{Model: "m"}, Weight: weight}
		}

		It("rejects a job that would exceed the byte budget even when slots are free", func() {
			// Ample slots, tight budget: only the byte budget can bite.
			p := &Pool{
				queue:      make(chan Job, 8),
				byteBudget: 100,
				logger:     tapeslogger.NewNoop(),
			}

			Expect(p.Admit(weighted(80))).To(Equal(RejectNone))
			// 80+80 overshoots the budget while 7 slots stay open.
			Expect(p.Admit(weighted(80))).To(Equal(RejectByteBudget))
			Expect(p.Len()).To(Equal(1)) // count cap was not the cause

			// Releasing the queued job's weight on completion readmits it.
			p.release(80)
			Expect(p.Admit(weighted(80))).To(Equal(RejectNone))
		})

		It("distinguishes a byte-budget rejection from a slot-count rejection", func() {
			// Single slot, roomy budget: fill the slot, then a fitting job
			// can only be turned away by the count cap.
			full := &Pool{queue: make(chan Job, 1), byteBudget: 1 << 20, logger: tapeslogger.NewNoop()}
			Expect(full.Admit(weighted(1))).To(Equal(RejectNone))
			slotReject := full.Admit(weighted(1))
			Expect(slotReject).To(Equal(RejectQueueFull))

			// Open slots, tight budget: only the byte budget can turn a job away.
			tight := &Pool{queue: make(chan Job, 8), byteBudget: 100, logger: tapeslogger.NewNoop()}
			byteReject := tight.Admit(weighted(150))
			Expect(byteReject).To(Equal(RejectByteBudget))

			Expect(byteReject).NotTo(Equal(slotReject))
		})
	})
})
