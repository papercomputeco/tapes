// Package worker provides an asynchronous worker pool and utils for persisting
// conversation turns using the provided storage.Driver and generating embeddings
// using the provided embeddings.Embedder.
//
// The pool decouples storage operations from the proxy's HTTP hot path so that the
// client-proxy-upstream interaction is fully transparent.
package worker

import (
	"context"
	"fmt"
	"math"
	"sync"

	"go.uber.org/zap"

	"github.com/papercomputeco/tapes/pkg/embeddings"
	"github.com/papercomputeco/tapes/pkg/llm"
	"github.com/papercomputeco/tapes/pkg/merkle"
	"github.com/papercomputeco/tapes/pkg/storage"
	"github.com/papercomputeco/tapes/pkg/vector"
)

var (
	defaultNumWorkers   uint = 3
	defaultJobQueueSize uint = 256
)

// Job is a unit of work for the worker pool to execute against.
type Job struct {
	Provider string
	Req      *llm.ChatRequest
	Resp     *llm.ChatResponse
}

// Config is the configuration options for the worker pool.
type Config struct {
	// Driver is the storage backend for persisting nodes.
	Driver storage.Driver

	// VectorDriver is the optional vector store driver for embeddings.
	VectorDriver vector.Driver

	// Embedder generates optional text embeddings.
	// A configured Embedder is required if VectorDriver is set.
	Embedder embeddings.Embedder

	// NumWorkers is the number of background workers in the pool.
	NumWorkers uint

	// QueueSize is the capacity of the buffered job channel (defaults to 256).
	QueueSize uint

	// Logger is the provided zap logger
	Logger *zap.Logger
}

// Pool processes storage jobs asynchronously via a worker pool.
type Pool struct {
	config *Config
	queue  chan Job
	wg     sync.WaitGroup
	logger *zap.Logger
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
			zap.String("provider", job.Provider),
			zap.String("model", job.Req.Model),
		)
		return true
	default:
		p.logger.Error("job not queued, queue full, job dropped",
			zap.String("provider", job.Provider),
			zap.String("model", job.Req.Model),
		)
		return false
	}
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
	p.logger.Debug("worker started", zap.Uint("worker_id", id))

	for job := range p.queue {
		p.processJob(job)
	}

	p.logger.Debug("storage worker stopped", zap.Uint("worker_id", id))
}

// processJob processes a Job, storing the conversation turn and setting the
// embedding if provided.
func (p *Pool) processJob(job Job) {
	ctx := context.Background()

	head, newNodes, err := p.storeConversationTurn(ctx, job)
	if err != nil {
		p.logger.Error("async DAG storage failed",
			zap.String("provider", job.Provider),
			zap.Error(err),
		)
		return
	}

	p.logger.Info("conversation stored",
		zap.String("head", head),
		zap.String("provider", job.Provider),
	)

	// If the vector store is configured, process newly inserted nodes
	if p.config.VectorDriver != nil && p.config.Embedder != nil && len(newNodes) > 0 {
		p.logger.Debug("storing embeddings for new nodes",
			zap.Int("new_node_count", len(newNodes)),
		)
		p.storeEmbeddings(ctx, newNodes)
	}
}

// storeConversationTurn stores a request-response pair in the merkle dag.
// Returns the head hash and the slice of nodes that were newly Put.
func (p *Pool) storeConversationTurn(ctx context.Context, job Job) (string, []*merkle.Node, error) {
	var parent *merkle.Node
	var newNodes []*merkle.Node

	// Store each message from the request as nodes.
	for _, msg := range job.Req.Messages {
		bucket := merkle.Bucket{
			Type:     "message",
			Role:     msg.Role,
			Content:  msg.Content,
			Model:    job.Req.Model,
			Provider: job.Provider,
		}

		node := merkle.NewNode(bucket, parent)

		isNew, err := p.config.Driver.Put(ctx, node)
		if err != nil {
			return "", nil, fmt.Errorf("storing message node: %w", err)
		}

		p.logger.Debug("stored message in DAG",
			zap.String("hash", node.Hash),
			zap.String("role", msg.Role),
			zap.String("content", msg.GetText()),
			zap.Bool("is_new", isNew),
		)

		if isNew {
			newNodes = append(newNodes, node)
		}
		parent = node
	}

	responseBucket := merkle.Bucket{
		Type:     "message",
		Role:     job.Resp.Message.Role,
		Content:  job.Resp.Message.Content,
		Model:    job.Resp.Model,
		Provider: job.Provider,
	}

	responseNode := merkle.NewNode(
		responseBucket,
		parent,
		merkle.NodeMeta{
			StopReason: job.Resp.StopReason,
			Usage:      job.Resp.Usage,
		},
	)

	isNew, err := p.config.Driver.Put(ctx, responseNode)
	if err != nil {
		return "", nil, fmt.Errorf("storing response node: %w", err)
	}

	p.logger.Debug("stored response in DAG",
		zap.String("hash", responseNode.Hash),
		zap.String("content_preview", job.Resp.Message.GetText()),
		zap.Bool("is_new", isNew),
	)

	if isNew {
		newNodes = append(newNodes, responseNode)
	}

	return responseNode.Hash, newNodes, nil
}

// storeEmbeddings generates and stores embeddings for the given nodes.
// Only called for nodes that were newly inserted into the DAG.
// Errors are logged but not returned to avoid failing the main storage operation.
func (p *Pool) storeEmbeddings(ctx context.Context, nodes []*merkle.Node) {
	for _, node := range nodes {
		text := node.Bucket.ExtractText()
		if text == "" {
			p.logger.Debug("skipping embedding for node with no text content",
				zap.String("hash", node.Hash),
			)
			continue
		}

		embedding, err := p.config.Embedder.Embed(ctx, text)
		if err != nil {
			p.logger.Warn("failed to generate embedding",
				zap.String("hash", node.Hash),
				zap.Error(err),
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
				zap.String("hash", node.Hash),
				zap.Error(err),
			)
			continue
		}

		p.logger.Debug("stored embedding",
			zap.String("hash", node.Hash),
			zap.Int("embedding_dim", len(embedding)),
		)
	}
}
