// Package worker provides an asynchronous worker pool and utils for persisting
// conversation turns using the provided storage.Driver and generating embeddings
// using the provided embeddings.Embedder.
//
// The pool decouples storage operations from the proxy's HTTP hot path so that the
// client-proxy-upstream interaction is fully transparent.
package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"sync"

	"github.com/papercomputeco/tapes/pkg/embeddings"
	"github.com/papercomputeco/tapes/pkg/llm"
	"github.com/papercomputeco/tapes/pkg/merkle"
	"github.com/papercomputeco/tapes/pkg/publisher"
	"github.com/papercomputeco/tapes/pkg/sessions"
	"github.com/papercomputeco/tapes/pkg/storage"
	"github.com/papercomputeco/tapes/pkg/vector"
)

var (
	defaultNumWorkers   uint = 3
	defaultJobQueueSize uint = 256
)

// Job is a unit of work for the worker pool to execute against.
type Job struct {
	Provider  string
	AgentName string
	Req       *llm.ChatRequest
	Resp      *llm.ChatResponse

	// SpanContext is optional harness-supplied trace/span identity attached
	// to the provider request by an extension or ingest envelope.
	SpanContext *storage.SpanContext

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

	// VectorDriver is the optional vector store driver for embeddings.
	VectorDriver vector.Driver

	// Embedder generates optional text embeddings.
	// A configured Embedder is required if VectorDriver is set.
	Embedder embeddings.Embedder

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

// processJob processes a Job, storing the conversation turn and setting the
// embedding if provided.
func (p *Pool) processJob(job Job) {
	ctx := context.Background()

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

	// If the vector store is configured, process newly inserted nodes
	if p.config.VectorDriver != nil && p.config.Embedder != nil && len(newNodes) > 0 {
		p.logger.Debug("storing embeddings for new nodes",
			"new_node_count", len(newNodes),
		)
		p.storeEmbeddings(ctx, newNodes)
	}

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

// storeConversationTurn stores a request-response pair.
//
// Span-aware drivers use the experimental Lapdog-style read model first:
// explicit session -> trace/turn -> span rows. In that mode prompt snapshots
// are span payloads and no Merkle nodes are created for the turn.
//
// Legacy drivers continue to use the Merkle DAG path so existing tests,
// publisher behavior, and in-memory local workflows remain compatible while
// the span model is rolled out.
func (p *Pool) storeConversationTurn(ctx context.Context, job Job) (string, []*merkle.Node, error) {
	if ingester, ok := p.config.Driver.(storage.SpanIngester); ok {
		res, err := ingester.IngestSpanTurn(ctx, storage.IngestSpanTurnRequest{
			Session:     job.Session,
			SpanContext: job.SpanContext,
			Provider:    job.Provider,
			AgentName:   job.AgentName,
			Project:     p.config.Project,
			Request:     job.Req,
			Response:    job.Resp,
			CostUSD:     0, // pricing lookup is not wired in this worker yet.
		})
		if err != nil {
			return "", nil, fmt.Errorf("span ingester: %w", err)
		}
		return res.TraceID, nil, nil
	}

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
// for a single conversation turn: one node per request message,
// followed by the assistant response node. ParentHash linkage is set
// via merkle.NewNode so every node's hash is stable before any I/O.
func buildTurnChain(job Job, project string) []*merkle.Node {
	if job.Req == nil || job.Resp == nil {
		return nil
	}

	chain := make([]*merkle.Node, 0, len(job.Req.Messages)+1)
	var parent *merkle.Node

	for _, msg := range job.Req.Messages {
		bucket := merkle.Bucket{
			Type:      "message",
			Role:      msg.Role,
			Content:   msg.Content,
			Model:     job.Req.Model,
			Provider:  job.Provider,
			AgentName: job.AgentName,
		}
		node := merkle.NewNode(bucket, parent, merkle.NodeOptions{Project: project})
		chain = append(chain, node)
		parent = node
	}

	responseBucket := merkle.Bucket{
		Type:      "message",
		Role:      job.Resp.Message.Role,
		Content:   job.Resp.Message.Content,
		Model:     job.Resp.Model,
		Provider:  job.Provider,
		AgentName: job.AgentName,
	}
	responseNode := merkle.NewNode(
		responseBucket,
		parent,
		merkle.NodeOptions{
			StopReason: job.Resp.StopReason,
			Usage:      job.Resp.Usage,
			Project:    project,
		},
	)
	chain = append(chain, responseNode)
	return chain
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

// storeEmbeddings generates and stores embeddings for the given nodes.
// Only called for nodes that were newly inserted into the DAG.
// Errors are logged but not returned to avoid failing the main storage operation.
func (p *Pool) storeEmbeddings(ctx context.Context, nodes []*merkle.Node) {
	for _, node := range nodes {
		text := node.Bucket.ExtractText()
		if text == "" {
			p.logger.Debug("skipping embedding for node with no text content",
				"hash", node.Hash,
			)
			continue
		}

		embedding, err := p.config.Embedder.Embed(ctx, text)
		if err != nil {
			p.logger.Warn("failed to generate embedding",
				"hash", node.Hash,
				"error", err,
			)
			continue
		}

		doc := vector.Document{
			ID:        node.Hash,
			Hash:      node.Hash,
			Embedding: embedding,
		}

		if err := p.config.VectorDriver.Add(ctx, []vector.Document{doc}); err != nil {
			p.logger.Warn("failed to store embedding",
				"hash", node.Hash,
				"error", err,
			)
			continue
		}

		p.logger.Debug("stored embedding",
			"hash", node.Hash,
			"embedding_dim", len(embedding),
		)
	}
}
