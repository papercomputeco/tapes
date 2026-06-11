package storagetest

import (
	"context"
	"encoding/json"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/storage"
)

// RunDeriveQueueSpecs registers a Describe block exercising the
// storage.DeriveQueue capability (the derive worker's dirty-session
// queue) plus the PutRawTurn → dirty-mark coupling. The driver
// returned by makeDriver MUST implement both storage.DeriveQueue and
// storage.RawTurnStore — register these specs only for drivers that
// host the raw layer (Postgres does; in-memory does not).
func RunDeriveQueueSpecs(label string, makeDriver DriverFactory) bool {
	return ginkgo.Describe("DeriveQueue ["+label+"]", func() {
		var (
			ctx    context.Context
			driver storage.Driver
			queue  storage.DeriveQueue
			raw    storage.RawTurnStore
		)

		const (
			harnessID = "claude-code"
			sessionA  = "11111111-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
			sessionB  = "22222222-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
		)

		mark := func(session string) {
			gomega.Expect(queue.MarkDeriveDirty(ctx, "", harnessID, session)).To(gomega.Succeed())
		}

		get := func(session string) *storage.DeriveQueueEntry {
			e, err := queue.GetDeriveDirty(ctx, "", harnessID, session)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			return e
		}

		// future/past cutoffs for ListDeriveDirty: "settled" entries are
		// those dirtied before the cutoff.
		future := func() time.Time { return time.Now().Add(time.Hour) }
		past := func() time.Time { return time.Now().Add(-time.Hour) }

		ginkgo.BeforeEach(func() {
			ctx = context.Background()
			driver = makeDriver()

			var ok bool
			queue, ok = driver.(storage.DeriveQueue)
			gomega.Expect(ok).To(gomega.BeTrue(), "driver must implement storage.DeriveQueue")
			raw, ok = driver.(storage.RawTurnStore)
			gomega.Expect(ok).To(gomega.BeTrue(), "driver must implement storage.RawTurnStore")
		})

		ginkgo.AfterEach(func() {
			if driver != nil {
				_ = driver.Close()
			}
		})

		ginkgo.Describe("MarkDeriveDirty", func() {
			ginkgo.It("queues a session and bumps dirtied_at on re-mark", func() {
				mark(sessionA)
				first := get(sessionA)
				gomega.Expect(first).NotTo(gomega.BeNil())

				time.Sleep(10 * time.Millisecond)
				mark(sessionA)
				second := get(sessionA)
				gomega.Expect(second).NotTo(gomega.BeNil())
				gomega.Expect(second.DirtiedAt.After(first.DirtiedAt)).To(gomega.BeTrue(),
					"re-marking must bump dirtied_at (the debounce signal)")

				entries, err := queue.ListDeriveDirty(ctx, future(), 10)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				gomega.Expect(entries).To(gomega.HaveLen(1), "re-mark must not duplicate the row")
			})
		})

		ginkgo.Describe("ListDeriveDirty", func() {
			ginkgo.It("returns only settled entries, oldest first", func() {
				mark(sessionA)
				time.Sleep(10 * time.Millisecond)
				mark(sessionB)

				entries, err := queue.ListDeriveDirty(ctx, future(), 10)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				gomega.Expect(entries).To(gomega.HaveLen(2))
				gomega.Expect(entries[0].HarnessSessionID).To(gomega.Equal(sessionA))
				gomega.Expect(entries[1].HarnessSessionID).To(gomega.Equal(sessionB))

				settled, err := queue.ListDeriveDirty(ctx, past(), 10)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				gomega.Expect(settled).To(gomega.BeEmpty(),
					"entries dirtied after the cutoff are still debouncing")
			})
		})

		ginkgo.Describe("GetDeriveDirty", func() {
			ginkgo.It("returns nil for a clean session", func() {
				gomega.Expect(get("never-dirtied")).To(gomega.BeNil())
			})
		})

		ginkgo.Describe("ClearDeriveDirty", func() {
			ginkgo.It("clears only when dirtied_at is unchanged", func() {
				mark(sessionA)
				read := get(sessionA)
				gomega.Expect(read).NotTo(gomega.BeNil())

				// A raw turn lands "mid-derive": dirtied_at bumps.
				time.Sleep(10 * time.Millisecond)
				mark(sessionA)

				cleared, err := queue.ClearDeriveDirty(ctx, *read)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				gomega.Expect(cleared).To(gomega.BeFalse(),
					"a stale dirtied_at must not clear the re-dirtied session")
				gomega.Expect(get(sessionA)).NotTo(gomega.BeNil())

				// With the current dirtied_at the clear lands.
				cur := get(sessionA)
				cleared, err = queue.ClearDeriveDirty(ctx, *cur)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				gomega.Expect(cleared).To(gomega.BeTrue())
				gomega.Expect(get(sessionA)).To(gomega.BeNil())
			})
		})

		ginkgo.Describe("PutRawTurn coupling", func() {
			rawRecord := func(requestID, session string) storage.RawTurnRecord {
				return storage.RawTurnRecord{
					Source:           storage.RawTurnSourceWire,
					Provider:         "anthropic",
					HarnessID:        harnessID,
					HarnessSessionID: session,
					RequestID:        requestID,
					RawRequest:       json.RawMessage(`{"model":"m","messages":[]}`),
					Response:         json.RawMessage(`{"message":{"role":"assistant","content":[{"type":"text","text":"hi"}]}}`),
				}
			}

			ginkgo.It("marks the session dirty when a raw turn lands", func() {
				_, err := raw.PutRawTurn(ctx, rawRecord("req-dirty-1", sessionA))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				gomega.Expect(get(sessionA)).NotTo(gomega.BeNil())
			})

			ginkgo.It("marks even when the raw row dedupes (re-POST = re-derive signal)", func() {
				_, err := raw.PutRawTurn(ctx, rawRecord("req-dup", sessionA))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				cur := get(sessionA)
				gomega.Expect(cur).NotTo(gomega.BeNil())
				_, err = queue.ClearDeriveDirty(ctx, *cur)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())

				inserted, err := raw.PutRawTurn(ctx, rawRecord("req-dup", sessionA))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				gomega.Expect(inserted).To(gomega.BeFalse(), "same request_id must dedupe")
				gomega.Expect(get(sessionA)).NotTo(gomega.BeNil(),
					"the deduped re-POST must still mark the session dirty")
			})

			ginkgo.It("does not mark rows without a session key", func() {
				_, err := raw.PutRawTurn(ctx, rawRecord("req-no-session", ""))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				entries, err := queue.ListDeriveDirty(ctx, future(), 10)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				gomega.Expect(entries).To(gomega.BeEmpty())
			})
		})

		ginkgo.Describe("SweepDeriveDirty", func() {
			ginkgo.It("re-enqueues raw-layer sessions whose marks were lost", func() {
				_, err := raw.PutRawTurn(ctx, storage.RawTurnRecord{
					Source:           storage.RawTurnSourceWire,
					Provider:         "anthropic",
					HarnessID:        harnessID,
					HarnessSessionID: sessionA,
					RequestID:        "req-sweep-a",
					RawRequest:       json.RawMessage(`{"model":"m","messages":[]}`),
				})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())

				// Simulate the lost mark by clearing the queue.
				cur := get(sessionA)
				gomega.Expect(cur).NotTo(gomega.BeNil())
				_, err = queue.ClearDeriveDirty(ctx, *cur)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())

				enqueued, err := queue.SweepDeriveDirty(ctx)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				gomega.Expect(enqueued).To(gomega.Equal(int64(1)))
				gomega.Expect(get(sessionA)).NotTo(gomega.BeNil())
			})

			ginkgo.It("keeps an in-flight debounce window (no dirtied_at reset)", func() {
				// The raw turn both populates raw_turns (so the sweep
				// sees the session) and marks it dirty.
				_, err := raw.PutRawTurn(ctx, storage.RawTurnRecord{
					Source:           storage.RawTurnSourceWire,
					Provider:         "anthropic",
					HarnessID:        harnessID,
					HarnessSessionID: sessionA,
					RequestID:        "req-sweep-window",
					RawRequest:       json.RawMessage(`{"model":"m","messages":[]}`),
				})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				before := get(sessionA)
				gomega.Expect(before).NotTo(gomega.BeNil())

				time.Sleep(10 * time.Millisecond)
				enqueued, err := queue.SweepDeriveDirty(ctx)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				gomega.Expect(enqueued).To(gomega.BeZero())

				after := get(sessionA)
				gomega.Expect(after.DirtiedAt).To(gomega.BeTemporally("==", before.DirtiedAt),
					"sweeping must not bump an already-queued session's dirtied_at")
			})
		})
	})
}
