// Package worker provides an asynchronous worker pool and utils for persisting
// conversation turns using the provided storage.Driver.
//
// The pool decouples storage operations from the proxy's HTTP hot path so that the
// client-proxy-upstream interaction is fully transparent.
//
// Embedding writes deliberately do NOT happen here: the derive worker
// family is the single writer of embeddings (pkg/spanembed), keyed by
// deterministic span identity, so the ingest hot path stays pure
// capture.
package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"sync"

	"github.com/papercomputeco/tapes/pkg/derive"
	"github.com/papercomputeco/tapes/pkg/llm"
	"github.com/papercomputeco/tapes/pkg/merkle"
	"github.com/papercomputeco/tapes/pkg/sessions"
	"github.com/papercomputeco/tapes/pkg/storage"
)

var (
	defaultNumWorkers   uint = 3
	defaultJobQueueSize uint = 256
)

// Job is a unit of work for the worker pool to execute against.
type Job struct {
	Provider  string
	AgentName string
	// ThreadID is the harness sub-thread that fired this call ("" for
	// the main thread), captured at the wire and stamped onto the
	// turn's nodes as non-hashed metadata.
	ThreadID string
	Req      *llm.ChatRequest
	Resp     *llm.ChatResponse

	// RawRequest is the verbatim provider request body the proxy
	// received, persisted unparsed into the immutable raw-turn layer so
	// the deriver re-parses it (and fields unknown to this build
	// survive). Empty for callers that don't capture into raw_turns
	// (e.g. the in-memory test driver); the raw write is skipped then.
	RawRequest json.RawMessage

	// Session is the optional session-tracking envelope attached to
	// the turn. When non-nil and the driver supports session-aware
	// ingest (Postgres), the worker UPSERTs the turn's `sessions` row
	// and folds its derived_status so the deriver can resolve the
	// session and attach its spans. When nil OR when the driver does
	// not implement that capability (e.g. inmemory), no sessions row is
	// written — this keeps unit tests working without a Postgres
	// backend. The local proxy always attaches an envelope so its
	// captured turns surface in the deck.
	Session *sessions.IngestEnvelope
}

// Config is the configuration options for the worker pool.
type Config struct {
	// Driver is the storage backend: the raw-turn layer plus the
	// sessions surface (Postgres). Drivers without those capabilities
	// (the in-memory test driver) make capture a no-op.
	Driver storage.Driver

	// NumWorkers is the number of background workers in the pool.
	NumWorkers uint

	// QueueSize is the capacity of the buffered job channel (defaults to 256).
	QueueSize uint

	// Project is the git repository or project name to tag on stored nodes.
	Project string

	// Logger is the provided logger
	Logger *slog.Logger
}

// Pool processes storage jobs asynchronously via a worker pool.
type Pool struct {
	config *Config
	queue  chan Job
	wg     sync.WaitGroup
	logger *slog.Logger
}

// NewPool creates a new Storer and starts its worker goroutines.
func NewPool(c *Config) (*Pool, error) {
	if c.NumWorkers == 0 {
		c.NumWorkers = defaultNumWorkers
	}

	if c.QueueSize == 0 {
		c.QueueSize = defaultJobQueueSize
	}

	if c.NumWorkers > uint(math.MaxInt) {
		return nil, fmt.Errorf("NumWorkers %d exceeds max int", c.NumWorkers)
	}

	wp := &Pool{
		config: c,
		queue:  make(chan Job, c.QueueSize),
		logger: c.Logger,
	}

	wp.wg.Add(int(c.NumWorkers))
	for i := range c.NumWorkers {
		go wp.worker(i)
	}

	return wp, nil
}

// Enqueue submits a job for processing by the worker pool.
// Returns true if enqueued, false if the queue is full, resulting in the job being dropped
func (p *Pool) Enqueue(job Job) bool {
	select {
	case p.queue <- job:
		p.logger.Debug("job queued",
			"provider", job.Provider,
			"model", job.Req.Model,
		)
		return true
	default:
		p.logger.Error("job not queued, queue full, job dropped",
			"provider", job.Provider,
			"model", job.Req.Model,
		)
		return false
	}
}

// Len returns the current number of jobs buffered in the queue. It is a
// best-effort snapshot — workers may pick up items between the read and any
// downstream observation — and is intended for metric instrumentation rather
// than for routing decisions.
func (p *Pool) Len() int {
	return len(p.queue)
}

// Close signals workers to stop and waits for in-flight jobs to drain.
// Call this during graceful shutdown after the proxy HTTP server has stopped.
func (p *Pool) Close() {
	close(p.queue)
	p.wg.Wait()
}

// worker is the inner worker thread that continuously pulls jobs off the jobs queue
func (p *Pool) worker(id uint) {
	defer p.wg.Done()
	p.logger.Debug("worker started", "worker_id", id)

	for job := range p.queue {
		p.processJob(job)
	}

	p.logger.Debug("storage worker stopped", "worker_id", id)
}

// processJob captures one conversation turn. It builds the turn's merkle
// chain once (in memory — the merkle layer is no longer persisted),
// appends the verbatim turn to the immutable raw-turn layer (the
// deriver's input), and ensures the turn's sessions row exists so the
// deriver can resolve it and attach the turn's spans.
func (p *Pool) processJob(job Job) {
	ctx := context.Background()

	chain := buildTurnChain(job, p.config.Project)
	if len(chain) == 0 {
		p.logger.Error("capture skipped: turn produced no nodes",
			"provider", job.Provider,
		)
		return
	}

	p.persistRawTurn(ctx, job, chain)
	p.ingestSession(ctx, job, chain)
}

// rawTurnMeta is the minimal capture-side meta block the proxy stamps
// onto a raw turn. The deriver reads thread_id for sub-thread
// attribution; ts_request is omitted because single-process local
// capture inserts the row at ~capture time, so the deriver's
// received_at fallback is accurate.
type rawTurnMeta struct {
	ThreadID string `json:"thread_id,omitempty"`
}

// persistRawTurn appends one captured turn to the immutable raw-turn
// layer when the driver hosts it (Postgres). This is the load-bearing
// repoint that makes local proxy capture span-model-native: the row is
// marked derive-dirty by PutRawTurn, so the derive worker projects it
// into the sessions/traces/spans surface the deck and API read.
//
// A bare proxied call carries no session envelope, so the turn is
// attributed a synthetic harness_session_id from its Merkle root hash
// (the same scheme the node ingest path uses) — stable across a
// conversation's turns, so they group into one derivable session.
//
// Failures are logged, never propagated: a raw-layer outage must not
// take down capture.
func (p *Pool) persistRawTurn(ctx context.Context, job Job, chain []*merkle.Node) {
	rawStore, ok := p.config.Driver.(storage.RawTurnStore)
	if !ok || len(job.RawRequest) == 0 {
		return
	}

	root := chain[0]
	head := chain[len(chain)-1]

	envelope, harnessSessionID, err := sessions.ResolveHarnessSessionID(job.Session, root.Hash)
	if err != nil {
		p.logger.Error("raw turn skipped: resolve harness_session_id",
			"provider", job.Provider, "error", err)
		return
	}

	response, err := json.Marshal(job.Resp)
	if err != nil {
		p.logger.Error("raw turn skipped: marshal response",
			"provider", job.Provider, "error", err)
		return
	}

	meta, err := json.Marshal(rawTurnMeta{ThreadID: job.ThreadID})
	if err != nil {
		p.logger.Error("raw turn skipped: marshal meta",
			"provider", job.Provider, "error", err)
		return
	}

	sessionJSON, err := json.Marshal(envelope)
	if err != nil {
		p.logger.Error("raw turn skipped: marshal session envelope",
			"provider", job.Provider, "error", err)
		return
	}

	if _, err := rawStore.PutRawTurn(ctx, storage.RawTurnRecord{
		OrgID:            envelope.OrgID,
		Source:           storage.RawTurnSourceWire,
		Provider:         job.Provider,
		AgentName:        job.AgentName,
		HarnessID:        envelope.HarnessIDOrUnknown(),
		HarnessSessionID: harnessSessionID,
		// The leaf (response) node hash is content-addressed and unique
		// per turn, so it both dedupes an identical re-send and ties the
		// raw row back to the turn's Merkle identity.
		RequestID:       head.Hash,
		RawRequest:      job.RawRequest,
		Response:        response,
		Meta:            meta,
		SessionEnvelope: sessionJSON,
	}); err != nil {
		p.logger.Error("raw turn persist failed",
			"provider", job.Provider,
			"harness_session_id", harnessSessionID,
			"error", err,
		)
	}
}

// ingestSession ensures the turn's sessions row exists (and folds its
// derived_status/title) via the SessionIngester, keyed by the same
// synthetic harness_session_id persistRawTurn records — so the derive
// worker resolves the session and attaches the turn's spans. No nodes
// are persisted: the chain is passed only so IngestTurn can derive the
// session identity and status from it in memory.
//
// Drivers without the SessionIngester capability (the in-memory test
// driver) have no sessions surface, so this is a no-op for them.
func (p *Pool) ingestSession(ctx context.Context, job Job, chain []*merkle.Node) {
	ingester, ok := p.config.Driver.(storage.SessionIngester)
	if !ok || job.Session == nil {
		return
	}

	if _, err := ingester.IngestTurn(ctx, storage.IngestTurnRequest{
		Session:      job.Session,
		Nodes:        chain,
		DerivedTitle: derive.SessionTitle(chain[len(chain)-1].Kind, job.Resp),
	}); err != nil {
		p.logger.Error("session ingest failed",
			"provider", job.Provider,
			"error", err,
		)
	}
}

// buildTurnChain materializes the ordered (root → leaf) chain of nodes
// for a single conversation turn. The construction lives in pkg/derive
// so the offline re-deriver produces byte-identical chains from the
// raw-turn store; this wrapper just adapts a worker Job.
func buildTurnChain(job Job, project string) []*merkle.Node {
	return derive.TurnChain(derive.CallContext{
		Provider:  job.Provider,
		AgentName: job.AgentName,
		ThreadID:  job.ThreadID,
		Project:   project,
	}, job.Req, job.Resp)
}
