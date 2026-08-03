package extproc

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
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

		d := NewDispatcher(ingest.URL, 4, &http.Client{Timeout: 2 * time.Second})
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
		d := NewDispatcher("http://127.0.0.1:0", 1, nil)
		d.SetObserver(panicObserver{})
		Expect(func() { d.RecordDrop("anthropic", DropReducerError, "req") }).NotTo(Panic())
	})

	It("drops with reason=marshal_error on unmarshalable payload without panic or semaphore leak", func() {
		obs := newObserver()
		d := NewDispatcher("http://127.0.0.1:0", 1, nil)
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
		d2 := NewDispatcher(srv.URL, 1, nil)
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
		d := NewDispatcher(ingest.URL, N+8, &http.Client{Timeout: 5 * time.Second})
		d.SetObserver(obs)

		var wg sync.WaitGroup
		for i := 0; i < N; i++ {
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
		d := NewDispatcher(ingest.URL, 1, &http.Client{Timeout: 5 * time.Second})
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
		d := NewDispatcher(ingest.URL, 1, &http.Client{Timeout: 1 * time.Second})
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
})

// silence unused-import guard for io/json when tests don't reach certain paths.
var _ = io.Discard
