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
	"errors"
	"fmt"
	"log/slog"
	"math"
	"sync"

	"github.com/papercomputeco/tapes/pkg/derive"
	"github.com/papercomputeco/tapes/pkg/llm"
	"github.com/papercomputeco/tapes/pkg/merkle"
	"github.com/papercomputeco/tapes/pkg/publisher"
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
	// the turn at the ingest HTTP boundary. When non-nil and the
	// driver supports session-aware ingest (Postgres), the worker
	// pool runs the transactional path: a `sessions` row is UPSERTed,
	// nodes are inserted with a non-NULL session_id FK, and counters
	// are rolled up — all in one Tx. When nil OR when the driver does
	// not implement that capability (e.g. inmemory), the legacy
	// per-node Put loop runs and session metadata is dropped on the
	// floor — this keeps the local CLI proxy and unit tests working
	// without a Postgres backend.
	Session *sessions.IngestEnvelope
}

// Config is the configuration options for the worker pool.
type Config struct {
	// Driver is the storage backend for persisting nodes.
	Driver storage.Driver

	// Publisher is an optional event publisher for newly inserted nodes.
	// If nil, publishing is disabled.
	Publisher publisher.Publisher

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

	if p.config.Publisher == nil {
		return
	}

	if err := p.config.Publisher.Close(); err != nil {
		p.logger.Warn("failed to close publisher", "error", err)
	}
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

// processJob processes a Job, storing the conversation turn.
func (p *Pool) processJob(job Job) {
	ctx := context.Background()

	// Span-model-native capture: append the turn to the immutable
	// raw-turn layer when the driver hosts it, so the deriver projects
	// it into sessions/traces/spans. This is the read surface the deck
	// and API serve from. (The node-DAG write below is retired in the
	// node-layer retirement; the raw write is what survives.)
	p.persistRawTurn(ctx, job)

	head, newNodes, err := p.storeConversationTurn(ctx, job)
	if err != nil {
		p.logger.Error("async DAG storage failed",
			"provider", job.Provider,
			"error", err,
		)
		return
	}

	p.logger.Info("conversation stored",
		"head", head,
		"provider", job.Provider,
	)

	// If Kafka is configured, publish newly inserted nodes
	if p.config.Publisher != nil && len(newNodes) > 0 {
		p.publishConversationTurn(ctx, head, newNodes)
	}
}

func (p *Pool) publishConversationTurn(ctx context.Context, head string, newNodes []*merkle.Node) {
	rootHash, err := p.deriveRootHash(ctx, head)
	if err != nil {
		p.logger.Error("failed to derive root hash for event publishing",
			"head", head,
			"error", err,
		)
		return
	}

	for _, node := range newNodes {
		event, err := publisher.NewEvent(rootHash, node)
		if err != nil {
			p.logger.Error("failed to build event",
				"hash", node.Hash,
				"error", err,
			)
			continue
		}

		if err := p.config.Publisher.Publish(ctx, event); err != nil {
			p.logger.Error("failed to publish event",
				"hash", node.Hash,
				"error", err,
			)
		}
	}
}

func (p *Pool) deriveRootHash(ctx context.Context, head string) (string, error) {
	ancestry, err := p.config.Driver.Ancestry(ctx, head)
	if err != nil {
		return "", fmt.Errorf("get ancestry: %w", err)
	}
	if len(ancestry) == 0 {
		return "", errors.New("empty ancestry")
	}

	root := ancestry[len(ancestry)-1]
	if root == nil || root.Hash == "" {
		return "", errors.New("empty root hash")
	}

	return root.Hash, nil
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
// take down capture, and the node-DAG write surfaces storage errors
// independently.
func (p *Pool) persistRawTurn(ctx context.Context, job Job) {
	rawStore, ok := p.config.Driver.(storage.RawTurnStore)
	if !ok || len(job.RawRequest) == 0 {
		return
	}

	chain := buildTurnChain(job, p.config.Project)
	if len(chain) == 0 {
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

// storeConversationTurn stores a request-response pair in the merkle dag.
// Returns the head hash and the slice of nodes that were newly Put.
//
// When the configured driver implements storage.SessionIngester AND
// the job carries a session-tracking envelope, the entire turn (every
// message node plus the response node) is folded into a single
// transactional IngestTurn call — so a sessions row is UPSERTed,
// nodes are inserted with a non-NULL session_id FK, and counters are
// rolled up atomically.
//
// Otherwise (legacy in-memory driver, or a turn without an envelope),
// the original per-node Put loop runs unchanged.
func (p *Pool) storeConversationTurn(ctx context.Context, job Job) (string, []*merkle.Node, error) {
	chain := buildTurnChain(job, p.config.Project)
	if len(chain) == 0 {
		return "", nil, errors.New("conversation turn produced no nodes")
	}
	head := chain[len(chain)-1]

	if ingester, ok := p.config.Driver.(storage.SessionIngester); ok && job.Session != nil {
		return p.ingestTurnViaSessionIngester(ctx, ingester, job, chain, head)
	}

	newNodes, err := p.putChainSequentially(ctx, chain)
	if err != nil {
		return "", nil, err
	}
	return head.Hash, newNodes, nil
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

// putChainSequentially is the legacy per-node Put loop used when the
// driver cannot (or shouldn't) host sessions. Identical to the
// pre-session-tracking behavior.
func (p *Pool) putChainSequentially(ctx context.Context, chain []*merkle.Node) ([]*merkle.Node, error) {
	var newNodes []*merkle.Node
	for i, node := range chain {
		isNew, err := p.config.Driver.Put(ctx, node)
		if err != nil {
			if i == len(chain)-1 {
				return nil, fmt.Errorf("storing response node: %w", err)
			}
			return nil, fmt.Errorf("storing message node: %w", err)
		}
		p.logger.Debug("stored node in DAG",
			"hash", node.Hash,
			"role", node.Bucket.Role,
			"is_new", isNew,
		)
		if isNew {
			newNodes = append(newNodes, node)
		}
	}
	return newNodes, nil
}

// ingestTurnViaSessionIngester routes the chain through the
// transactional SessionIngester path so the sessions row, node
// inserts, and counter rollup commit atomically. CostUSD is stubbed
// at 0: this repo's worker has no pricing lookup wired in. The
// sessions total_cost_usd column defaults to 0 so writing a 0 delta
// is a true no-op.
func (p *Pool) ingestTurnViaSessionIngester(
	ctx context.Context,
	ingester storage.SessionIngester,
	job Job,
	chain []*merkle.Node,
	head *merkle.Node,
) (string, []*merkle.Node, error) {
	// Values mirror provider-reported usage; not re-derived. A stale or
	// zero Usage from the upstream provider produces undercounted
	// session totals — the data source is the provider response we
	// already trust elsewhere in the worker.
	var inputTokens, outputTokens int64
	if usage := job.Resp.Usage; usage != nil {
		inputTokens = int64(usage.PromptTokens)
		outputTokens = int64(usage.CompletionTokens)
	}

	res, err := ingester.IngestTurn(ctx, storage.IngestTurnRequest{
		Session:      job.Session,
		Nodes:        chain,
		InputTokens:  inputTokens,
		OutputTokens: outputTokens,
		CostUSD:      0, // pricing lookup not wired in this repo.
		DerivedTitle: derive.SessionTitle(chain[len(chain)-1].Kind, job.Resp),
	})
	if err != nil {
		return "", nil, fmt.Errorf("session ingester: %w", err)
	}

	p.logger.Debug("ingested conversation turn via session ingester",
		"session_id", res.SessionID,
		"new_nodes", len(res.NewNodes),
		"counters_updated", res.CountersUpdated,
	)
	return head.Hash, res.NewNodes, nil
}
