package worker

import (
	"context"
	"errors"
	"sync"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/llm"
	tapeslogger "github.com/papercomputeco/tapes/pkg/logger"
	"github.com/papercomputeco/tapes/pkg/merkle"
	"github.com/papercomputeco/tapes/pkg/sessions"
	"github.com/papercomputeco/tapes/pkg/storage"
	"github.com/papercomputeco/tapes/pkg/storage/inmemory"
)

// sessionAwareDriver is an in-memory storage.Driver that ALSO satisfies
// storage.SessionIngester so the worker pool's dispatcher branch can
// be exercised without spinning up Postgres. It records the IngestTurn
// calls it received and falls back to the embedded in-memory driver
// for everything else (the legacy Put path stays valid for tests that
// don't supply a session envelope, e.g. for retry / dedup assertions).
type sessionAwareDriver struct {
	*inmemory.Driver

	mu        sync.Mutex
	calls     []storage.IngestTurnRequest
	failNext  error
	sessionID string
}

func newSessionAwareDriver() *sessionAwareDriver {
	return &sessionAwareDriver{
		Driver:    inmemory.NewDriver(),
		sessionID: "00000000-0000-0000-0000-000000000001",
	}
}

func (d *sessionAwareDriver) IngestTurn(ctx context.Context, req storage.IngestTurnRequest) (storage.IngestTurnResult, error) {
	d.mu.Lock()
	d.calls = append(d.calls, req)
	failure := d.failNext
	d.failNext = nil
	d.mu.Unlock()

	if failure != nil {
		return storage.IngestTurnResult{}, failure
	}

	var newNodes []*merkle.Node
	for _, n := range req.Nodes {
		if n == nil {
			continue
		}
		isNew, err := d.Put(ctx, n)
		if err != nil {
			return storage.IngestTurnResult{}, err
		}
		if isNew {
			newNodes = append(newNodes, n)
		}
	}

	return storage.IngestTurnResult{
		SessionID:       d.sessionID,
		NewNodes:        newNodes,
		CountersUpdated: len(newNodes) > 0,
	}, nil
}

func (d *sessionAwareDriver) Calls() []storage.IngestTurnRequest {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make([]storage.IngestTurnRequest, len(d.calls))
	copy(out, d.calls)
	return out
}

func newSessionTestPool(driver storage.Driver) *Pool {
	logger := tapeslogger.NewNoop()
	wp, err := NewPool(&Config{
		Driver: driver,
		Logger: logger,
	})
	Expect(err).NotTo(HaveOccurred())
	return wp
}

func sampleJobWithEnvelope(envelope *sessions.IngestEnvelope) Job {
	return Job{
		Provider:  "test-provider",
		AgentName: "test-agent",
		Req: &llm.ChatRequest{
			Model: "test-model",
			Messages: []llm.Message{
				{Role: "system", Content: []llm.ContentBlock{{Type: "text", Text: "you are helpful"}}},
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

var _ = Describe("Worker pool session-ingester dispatch", func() {
	Context("when the driver implements SessionIngester and the job carries a session envelope", func() {
		It("routes the turn through IngestTurn with the full chain and token deltas", func() {
			driver := newSessionAwareDriver()
			wp := newSessionTestPool(driver)

			envelope := &sessions.IngestEnvelope{
				OrgID:            "550e8400-e29b-41d4-a716-446655440000",
				AuthSubject:      "user-42",
				HarnessID:        "claude",
				HarnessSessionID: "harness-abc",
			}
			wp.Enqueue(sampleJobWithEnvelope(envelope))
			wp.Close()

			calls := driver.Calls()
			Expect(calls).To(HaveLen(1))

			call := calls[0]
			Expect(call.Session).NotTo(BeNil())
			Expect(call.Session.HarnessSessionID).To(Equal("harness-abc"))
			Expect(call.Session.HarnessIDOrUnknown()).To(Equal("claude"))

			// 2 message nodes + 1 response node.
			Expect(call.Nodes).To(HaveLen(3))
			Expect(call.Nodes[0].Bucket.Role).To(Equal("system"))
			Expect(call.Nodes[1].Bucket.Role).To(Equal("user"))
			Expect(call.Nodes[2].Bucket.Role).To(Equal("assistant"))

			Expect(call.InputTokens).To(Equal(int64(11)))
			Expect(call.OutputTokens).To(Equal(int64(7)))
			Expect(call.CostUSD).To(Equal(0.0))
		})

		It("falls back to per-node Put when the envelope is absent", func() {
			driver := newSessionAwareDriver()
			wp := newSessionTestPool(driver)

			wp.Enqueue(sampleJobWithEnvelope(nil))
			wp.Close()

			Expect(driver.Calls()).To(BeEmpty(), "IngestTurn must not be called when Session is nil")

			// Legacy Put path should have stored every node.
			nodes, err := driver.List(context.Background())
			Expect(err).NotTo(HaveOccurred())
			Expect(nodes).To(HaveLen(3))
		})

		It("returns an error from processJob without crashing the pool when IngestTurn fails", func() {
			driver := newSessionAwareDriver()
			driver.failNext = errors.New("simulated ingest failure")
			wp := newSessionTestPool(driver)

			envelope := &sessions.IngestEnvelope{
				OrgID:            "550e8400-e29b-41d4-a716-446655440000",
				HarnessID:        "claude",
				HarnessSessionID: "harness-zzz",
			}
			wp.Enqueue(sampleJobWithEnvelope(envelope))
			wp.Close()

			calls := driver.Calls()
			Expect(calls).To(HaveLen(1))
			// IngestTurn errored, so nothing was Put through the fallback either.
			nodes, err := driver.List(context.Background())
			Expect(err).NotTo(HaveOccurred())
			Expect(nodes).To(BeEmpty())
		})
	})

	Context("when the driver does NOT implement SessionIngester", func() {
		It("uses the legacy Put loop even when a session envelope is supplied", func() {
			driver := inmemory.NewDriver()
			wp := newSessionTestPool(driver)

			envelope := &sessions.IngestEnvelope{
				OrgID:            "550e8400-e29b-41d4-a716-446655440000",
				HarnessID:        "claude",
				HarnessSessionID: "harness-abc",
			}
			wp.Enqueue(sampleJobWithEnvelope(envelope))
			wp.Close()

			nodes, err := driver.List(context.Background())
			Expect(err).NotTo(HaveOccurred())
			Expect(nodes).To(HaveLen(3))
		})
	})
})
