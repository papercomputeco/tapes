package worker

import (
	"context"
	"errors"
	"sync"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/llm"
	tapeslogger "github.com/papercomputeco/tapes/pkg/logger"
	"github.com/papercomputeco/tapes/pkg/sessions"
	"github.com/papercomputeco/tapes/pkg/storage"
	"github.com/papercomputeco/tapes/pkg/storage/inmemory"
)

// captureDriver is an in-memory storage.Driver that ALSO satisfies
// storage.RawTurnStore and storage.SessionIngester, so the worker pool's
// capture path — append to the raw-turn layer + upsert the sessions row
// — can be exercised without Postgres. It records the calls it received.
// Node persistence is retired, so the worker never calls Driver.Put; the
// embedded in-memory driver is only here to satisfy the Driver interface.
type captureDriver struct {
	*inmemory.Driver

	mu          sync.Mutex
	ingestCalls []storage.IngestTurnRequest
	rawTurns    []storage.RawTurnRecord
	failIngest  error
	sessionID   string
}

func newCaptureDriver() *captureDriver {
	return &captureDriver{
		Driver:    inmemory.NewDriver(),
		sessionID: "00000000-0000-0000-0000-000000000001",
	}
}

func (d *captureDriver) PutRawTurn(_ context.Context, rec storage.RawTurnRecord) (bool, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.rawTurns = append(d.rawTurns, rec)
	return true, nil
}

func (d *captureDriver) ListRawTurns(_ context.Context, _ int64, _ int32) ([]storage.RawTurnRecord, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make([]storage.RawTurnRecord, len(d.rawTurns))
	copy(out, d.rawTurns)
	return out, nil
}

func (d *captureDriver) CountRawTurns(_ context.Context) (int64, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return int64(len(d.rawTurns)), nil
}

func (d *captureDriver) IngestTurn(_ context.Context, req storage.IngestTurnRequest) (storage.IngestTurnResult, error) {
	d.mu.Lock()
	d.ingestCalls = append(d.ingestCalls, req)
	failure := d.failIngest
	d.failIngest = nil
	d.mu.Unlock()

	if failure != nil {
		return storage.IngestTurnResult{}, failure
	}
	return storage.IngestTurnResult{SessionID: d.sessionID}, nil
}

func (d *captureDriver) IngestCalls() []storage.IngestTurnRequest {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make([]storage.IngestTurnRequest, len(d.ingestCalls))
	copy(out, d.ingestCalls)
	return out
}

func (d *captureDriver) RawTurns() []storage.RawTurnRecord {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make([]storage.RawTurnRecord, len(d.rawTurns))
	copy(out, d.rawTurns)
	return out
}

func newCaptureTestPool(driver storage.Driver) *Pool {
	wp, err := NewPool(&Config{
		Driver: driver,
		Logger: tapeslogger.NewNoop(),
	})
	Expect(err).NotTo(HaveOccurred())
	return wp
}

// sampleCaptureJob is a single user->assistant turn. RawRequest is the
// verbatim provider request the proxy would have forwarded; supply nil
// to model a caller that does not capture into the raw layer.
func sampleCaptureJob(envelope *sessions.IngestEnvelope, rawRequest []byte) Job {
	return Job{
		Provider:   "ollama",
		AgentName:  "test-agent",
		RawRequest: rawRequest,
		Req: &llm.ChatRequest{
			Model: "test-model",
			Messages: []llm.Message{
				{Role: "user", Content: []llm.ContentBlock{{Type: "text", Text: "ping"}}},
			},
		},
		Resp: &llm.ChatResponse{
			Model:      "test-model",
			StopReason: "stop",
			Usage:      &llm.Usage{PromptTokens: 11, CompletionTokens: 7, TotalTokens: 18},
			Message: llm.Message{
				Role:    "assistant",
				Content: []llm.ContentBlock{{Type: "text", Text: "pong"}},
			},
		},
		Session: envelope,
	}
}

var rawBody = []byte(`{"model":"test-model","messages":[{"role":"user","content":"ping"}]}`)

var _ = Describe("Worker pool capture", func() {
	Context("when the driver hosts the raw-turn and sessions surfaces", func() {
		It("appends a raw turn and upserts the session for a captured turn", func() {
			driver := newCaptureDriver()
			wp := newCaptureTestPool(driver)

			envelope := &sessions.IngestEnvelope{
				OrgID:            "550e8400-e29b-41d4-a716-446655440000",
				AuthSubject:      "user-42",
				HarnessID:        "claude",
				HarnessSessionID: "harness-abc",
			}
			wp.Enqueue(sampleCaptureJob(envelope, rawBody))
			wp.Close()

			raws := driver.RawTurns()
			Expect(raws).To(HaveLen(1))
			Expect(raws[0].Provider).To(Equal("ollama"))
			Expect(raws[0].Source).To(Equal(storage.RawTurnSourceWire))
			Expect(raws[0].HarnessSessionID).To(Equal("harness-abc"))
			Expect([]byte(raws[0].RawRequest)).To(Equal(rawBody))

			calls := driver.IngestCalls()
			Expect(calls).To(HaveLen(1))
			Expect(calls[0].Session).NotTo(BeNil())
			Expect(calls[0].Session.HarnessSessionID).To(Equal("harness-abc"))
			// 1 user message node + 1 response node.
			Expect(calls[0].Nodes).To(HaveLen(2))
			Expect(calls[0].Nodes[0].Bucket.Role).To(Equal("user"))
			Expect(calls[0].Nodes[1].Bucket.Role).To(Equal("assistant"))
		})

		It("synthesizes a harness_session_id when the envelope is absent", func() {
			driver := newCaptureDriver()
			wp := newCaptureTestPool(driver)

			// No envelope: the proxy attaches one in practice, but a
			// caller that omits it still captures into the raw layer under
			// a synthetic, Merkle-root-derived identity.
			wp.Enqueue(sampleCaptureJob(nil, rawBody))
			wp.Close()

			raws := driver.RawTurns()
			Expect(raws).To(HaveLen(1))
			Expect(raws[0].HarnessID).To(Equal(sessions.HarnessIDUnknown))
			Expect(raws[0].HarnessSessionID).NotTo(BeEmpty())

			// No envelope -> no sessions row is upserted.
			Expect(driver.IngestCalls()).To(BeEmpty())
		})

		It("skips the raw write when the turn carries no verbatim request", func() {
			driver := newCaptureDriver()
			wp := newCaptureTestPool(driver)

			envelope := &sessions.IngestEnvelope{HarnessID: "claude", HarnessSessionID: "harness-abc"}
			wp.Enqueue(sampleCaptureJob(envelope, nil))
			wp.Close()

			Expect(driver.RawTurns()).To(BeEmpty())
			// The session is still upserted from the in-memory chain.
			Expect(driver.IngestCalls()).To(HaveLen(1))
		})

		It("does not crash the pool when the session ingest fails", func() {
			driver := newCaptureDriver()
			driver.failIngest = errors.New("simulated ingest failure")
			wp := newCaptureTestPool(driver)

			envelope := &sessions.IngestEnvelope{HarnessID: "claude", HarnessSessionID: "harness-zzz"}
			wp.Enqueue(sampleCaptureJob(envelope, rawBody))
			wp.Close()

			// The raw write landed before the failing session ingest, and
			// the failure is logged, not propagated.
			Expect(driver.RawTurns()).To(HaveLen(1))
			Expect(driver.IngestCalls()).To(HaveLen(1))
		})
	})

	Context("when the driver hosts neither surface", func() {
		It("is a no-op: nothing is persisted and no nodes are written", func() {
			driver := inmemory.NewDriver()
			wp := newCaptureTestPool(driver)

			envelope := &sessions.IngestEnvelope{HarnessID: "claude", HarnessSessionID: "harness-abc"}
			wp.Enqueue(sampleCaptureJob(envelope, rawBody))
			wp.Close()

			nodes, err := driver.List(context.Background())
			Expect(err).NotTo(HaveOccurred())
			Expect(nodes).To(BeEmpty())
		})
	})
})
