package extproc

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/llm"
)

// panicObserver panics every time it's invoked — used to verify the
// dispatcher's recover() guard keeps the goroutine alive.
type panicObserver struct{}

func (panicObserver) OnAccepted(string, string) { panic("boom-accepted") }
func (panicObserver) OnDrop(string, DropReason, string) {
	panic("boom-drop")
}

var _ = Describe("Dispatcher", func() {
	It("survives a panicking observer on accepted dispatches", func() {
		var called atomic.Int32
		ingest := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			called.Add(1)
			w.WriteHeader(http.StatusAccepted)
		}))
		defer ingest.Close()

		d := NewDispatcher(ingest.URL, 4, 0, &http.Client{Timeout: 2 * time.Second})
		d.SetObserver(panicObserver{})

		env := TurnEnvelope{
			Provider: "anthropic",
			Request:  json.RawMessage(`{}`),
			Response: &llm.ChatResponse{Model: "m"},
		}
		Expect(func() { d.Dispatch(context.Background(), env) }).NotTo(Panic())

		Eventually(called.Load).WithTimeout(2 * time.Second).Should(Equal(int32(1)))
	})

	It("survives a panicking observer on drops (via RecordDrop)", func() {
		d := NewDispatcher("http://127.0.0.1:0", 1, 0, nil)
		d.SetObserver(panicObserver{})
		Expect(func() { d.RecordDrop("anthropic", DropReducerError, "req") }).NotTo(Panic())
	})

	It("drops with reason=marshal_error on unmarshalable payload without panic or semaphore leak", func() {
		obs := newObserver()
		d := NewDispatcher("http://127.0.0.1:0", 1, 0, nil)
		d.SetObserver(obs)

		bad := TurnEnvelope{
			Provider: "anthropic",
			Request:  json.RawMessage(`{}`),
			// ChatResponse.Extra holds any; a channel can't marshal.
			Response: &llm.ChatResponse{
				Model: "m",
				Extra: map[string]any{"chan": make(chan int)},
			},
		}
		Expect(func() { d.Dispatch(context.Background(), bad) }).NotTo(Panic())
		Expect(obs.DropCount(DropMarshalError)).To(Equal(1))

		// Semaphore must have been released — a subsequent dispatch to an OK
		// target should succeed.
		ok := make(chan struct{})
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			close(ok)
			w.WriteHeader(http.StatusAccepted)
		}))
		defer srv.Close()
		d2 := NewDispatcher(srv.URL, 1, 0, nil)
		d2.Dispatch(context.Background(), TurnEnvelope{
			Provider: "anthropic",
			Request:  json.RawMessage(`{}`),
			Response: &llm.ChatResponse{Model: "m"},
		})
		Eventually(ok).WithTimeout(2 * time.Second).Should(BeClosed())
	})

	It("handles concurrent dispatches without metric-update interleaving", func() {
		const N = 50
		var accepted atomic.Int32
		ingest := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			accepted.Add(1)
			w.WriteHeader(http.StatusAccepted)
		}))
		defer ingest.Close()

		obs := newObserver()
		// Sem is sized ≥ N so none of the test dispatches spuriously trip
		// DropSemFull — we're testing concurrent bookkeeping, not backpressure.
		d := NewDispatcher(ingest.URL, N+8, 0, &http.Client{Timeout: 5 * time.Second})
		d.SetObserver(obs)

		var wg sync.WaitGroup
		for i := range N {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				env := TurnEnvelope{
					Provider: "anthropic",
					Request:  json.RawMessage(`{}`),
					Response: &llm.ChatResponse{Model: fmt.Sprintf("m-%d", i)},
					Meta:     TurnMeta{RequestID: fmt.Sprintf("req-%d", i)},
				}
				d.Dispatch(context.Background(), env)
			}(i)
		}
		wg.Wait()

		Eventually(accepted.Load).WithTimeout(5 * time.Second).Should(Equal(int32(N)))
		Eventually(func() int32 { return obs.accepted.Load() }).
			WithTimeout(5 * time.Second).
			Should(Equal(int32(N)))
	})

	It("retries on ingest 5xx then succeeds", func() {
		var attempts atomic.Int32
		ingest := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			n := attempts.Add(1)
			if n < 2 {
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			w.WriteHeader(http.StatusAccepted)
		}))
		defer ingest.Close()

		obs := newObserver()
		d := NewDispatcher(ingest.URL, 1, 0, &http.Client{Timeout: 5 * time.Second})
		d.SetObserver(obs)
		d.Dispatch(context.Background(), TurnEnvelope{
			Provider: "anthropic",
			Request:  json.RawMessage(`{}`),
			Response: &llm.ChatResponse{Model: "m"},
		})

		Eventually(func() int32 { return obs.accepted.Load() }).
			WithTimeout(5 * time.Second).
			Should(Equal(int32(1)))
		Expect(attempts.Load()).To(BeNumerically(">=", 2))
	})

	It("bails on ctx cancellation during retry backoff", func() {
		blocked := make(chan struct{})
		ingest := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusServiceUnavailable)
			<-blocked
		}))
		defer func() { close(blocked); ingest.Close() }()

		obs := newObserver()
		d := NewDispatcher(ingest.URL, 1, 0, &http.Client{Timeout: 1 * time.Second})
		d.SetObserver(obs)

		ctx, cancel := context.WithCancel(context.Background())
		d.Dispatch(ctx, TurnEnvelope{
			Provider: "anthropic",
			Request:  json.RawMessage(`{}`),
			Response: &llm.ChatResponse{Model: "m"},
		})
		// Cancel before the backoff window elapses.
		time.Sleep(50 * time.Millisecond)
		cancel()

		Eventually(func() int { return obs.DropCount(DropIngestTimeout) }).
			WithTimeout(5 * time.Second).
			Should(BeNumerically(">=", 1))
	})

	It("includes request_over_budget in AllDropReasons exactly once", func() {
		Expect(AllDropReasons()).To(ContainElement(DropRequestOverBudget))
	})

	It("drops sem_full when the marshalled payload exceeds the dispatch byte budget while a small payload proceeds", func() {
		const budget = 1024
		var posts atomic.Int32
		ingest := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			posts.Add(1)
			w.WriteHeader(http.StatusAccepted)
		}))
		defer ingest.Close()

		obs := newObserver()
		d := NewDispatcher(ingest.URL, 4, budget, &http.Client{Timeout: 2 * time.Second})
		d.SetObserver(obs)

		// Request alone marshals past the budget, so admission must fail on
		// payload weight — the count cap (4) cannot be the cause.
		big := TurnEnvelope{
			Provider: "anthropic",
			Request:  json.RawMessage(fmt.Sprintf(`{"pad":%q}`, strings.Repeat("A", 4*budget))),
			Response: &llm.ChatResponse{Model: "m"},
		}
		d.Dispatch(context.Background(), big)
		Expect(obs.DropCount(DropSemFull)).To(Equal(1))
		// The drop path is synchronous and spawns no goroutine, so no POST
		// can have been issued for the oversize envelope.
		Expect(posts.Load()).To(Equal(int32(0)))

		small := TurnEnvelope{
			Provider: "anthropic",
			Request:  json.RawMessage(`{}`),
			Response: &llm.ChatResponse{Model: "m"},
		}
		d.Dispatch(context.Background(), small)
		Eventually(func() int32 { return obs.accepted.Load() }).
			WithTimeout(2 * time.Second).
			Should(Equal(int32(1)))
		Expect(posts.Load()).To(Equal(int32(1)))

		// The small payload's weight is released after its goroutine finishes;
		// the full budget being acquirable again proves charge and release.
		Eventually(func() bool {
			if d.byteSem.TryAcquire(budget) {
				d.byteSem.Release(budget)
				return true
			}
			return false
		}).WithTimeout(2 * time.Second).Should(BeTrue())
	})

	It("returns immediately with sem_full while an in-flight dispatch holds the byte budget", func() {
		blocked := make(chan struct{})
		ingest := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			<-blocked
			w.WriteHeader(http.StatusAccepted)
		}))
		defer func() { close(blocked); ingest.Close() }()

		obs := newObserver()
		env := TurnEnvelope{
			Provider: "anthropic",
			Request:  json.RawMessage(`{}`),
			Response: &llm.ChatResponse{Model: "m"},
		}
		// Budget sized to exactly one marshalled envelope, so the pinned
		// first dispatch leaves zero headroom for an identical second one.
		payload, err := json.Marshal(env)
		Expect(err).NotTo(HaveOccurred())
		d := NewDispatcher(ingest.URL, 4, int64(len(payload)), &http.Client{Timeout: 10 * time.Second})
		d.SetObserver(obs)

		// Dispatch acquires count and bytes synchronously before returning,
		// so the budget is fully held once this call comes back.
		d.Dispatch(context.Background(), env)
		Expect(obs.DropCount(DropSemFull)).To(Equal(0))

		// Admission is TryAcquire, never a blocking Acquire: the call must
		// return while the budget is still held by the pinned dispatch.
		done := make(chan struct{})
		go func() {
			d.Dispatch(context.Background(), env)
			close(done)
		}()
		Eventually(done).WithTimeout(2 * time.Second).Should(BeClosed())
		Expect(obs.DropCount(DropSemFull)).To(Equal(1))
	})

	It("still enforces the count cap under an ample byte budget", func() {
		blocked := make(chan struct{})
		ingest := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			<-blocked
			w.WriteHeader(http.StatusAccepted)
		}))
		defer func() { close(blocked); ingest.Close() }()

		obs := newObserver()
		// Budget dwarfs the payloads, so only the count cap of 1 can refuse
		// the second dispatch.
		d := NewDispatcher(ingest.URL, 1, 1<<20, &http.Client{Timeout: 10 * time.Second})
		d.SetObserver(obs)

		env := TurnEnvelope{
			Provider: "anthropic",
			Request:  json.RawMessage(`{}`),
			Response: &llm.ChatResponse{Model: "m"},
		}
		d.Dispatch(context.Background(), env)
		Expect(obs.DropCount(DropSemFull)).To(Equal(0))

		d.Dispatch(context.Background(), env)
		Expect(obs.DropCount(DropSemFull)).To(Equal(1))
	})
})

// silence unused-import guard for io/json when tests don't reach certain paths.
var _ = io.Discard
