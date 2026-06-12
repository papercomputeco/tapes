package spanembed

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/papercomputeco/tapes/pkg/embeddings"
)

// DefaultBatchSize bounds one candidate page.
const DefaultBatchSize = 100

// Source lists embed candidates. *Store implements it; tests
// substitute a fake.
type Source interface {
	ListCandidates(ctx context.Context, after Key, limit int) ([]Candidate, error)
}

// Sink persists embeddings. *Store implements it; tests substitute a
// fake.
type Sink interface {
	Upsert(ctx context.Context, rec Record) error
	PruneOrphans(ctx context.Context) (int64, error)
}

// PassConfig configures one embed pass.
type PassConfig struct {
	// Model names the embedding model; stored per row so a model
	// switch re-embeds existing spans.
	Model string

	// Dimensions is the expected embedding dimensionality. The first
	// vector the model returns is checked against it, so a
	// model/dims misconfiguration aborts the pass with one clear
	// error instead of failing every row's insert against the sized
	// vector column.
	Dimensions uint

	// BatchSize bounds one candidate page (default DefaultBatchSize).
	BatchSize int
}

// Pass walks every eligible span and embeds the ones whose embedding
// is missing or stale. Idempotent by construction: span identity keys
// the writes and a content hash gates them, so running the pass twice
// (or concurrently with a re-derive) embeds each span at most once per
// content+model.
type Pass struct {
	src      Source
	sink     Sink
	embedder embeddings.Embedder
	cfg      PassConfig
	logger   *slog.Logger
}

// NewPass creates an embed pass.
func NewPass(src Source, sink Sink, embedder embeddings.Embedder, cfg PassConfig, log *slog.Logger) (*Pass, error) {
	if src == nil || sink == nil {
		return nil, errors.New("embed pass requires a candidate source and an embedding sink")
	}
	if embedder == nil {
		return nil, errors.New("embed pass requires an embedder")
	}
	if cfg.Model == "" {
		return nil, errors.New("embed pass requires an explicit model name")
	}
	if cfg.Dimensions == 0 {
		return nil, errors.New("embed pass requires explicit embedding dimensions")
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = DefaultBatchSize
	}
	return &Pass{
		src:      src,
		sink:     sink,
		embedder: embedder,
		cfg:      cfg,
		logger:   log,
	}, nil
}

// Run executes one full pass: prune orphaned embeddings, then page
// through every main llm span and embed the missing/stale ones.
//
// Error discipline: a per-span embed or write failure is counted and
// logged but never aborts the pass — the span stays un-embedded and
// the next run retries it. Only infrastructure failures (candidate
// listing) abort, since they would starve every remaining page.
func (p *Pass) Run(ctx context.Context) (*Report, error) {
	report := &Report{}

	pruned, err := p.sink.PruneOrphans(ctx)
	if err != nil {
		return report, fmt.Errorf("prune orphaned span embeddings: %w", err)
	}
	report.Pruned = pruned

	var after Key
	for {
		if err := ctx.Err(); err != nil {
			return report, err
		}
		page, err := p.src.ListCandidates(ctx, after, p.cfg.BatchSize)
		if err != nil {
			return report, fmt.Errorf("list embed candidates: %w", err)
		}
		if len(page) == 0 {
			break
		}
		for i := range page {
			if err := ctx.Err(); err != nil {
				return report, err
			}
			if err := p.processCandidate(ctx, &page[i], report); err != nil {
				return report, err
			}
		}
		after = page[len(page)-1].Key()
	}

	p.logger.Info("span embed pass complete",
		"scanned", report.Scanned,
		"embedded", report.Embedded,
		"up_to_date", report.UpToDate,
		"empty", report.Empty,
		"failed", report.Failed,
		"pruned", report.Pruned,
	)
	return report, nil
}

// processCandidate embeds one candidate when due. The returned error
// is fatal to the pass (configuration, not data): today that is only
// the model returning vectors of the wrong dimensionality.
func (p *Pass) processCandidate(ctx context.Context, c *Candidate, report *Report) error {
	report.Scanned++
	text, hash, act := decide(c, p.cfg.Model)
	switch act {
	case actionEmpty:
		report.Empty++
		return nil
	case actionUpToDate:
		report.UpToDate++
		return nil
	case actionEmbed:
	}

	embedding, err := p.embedder.Embed(ctx, text)
	if err != nil {
		report.Failed++
		p.logger.Warn("span embed failed",
			"trace_id", c.TraceID,
			"span_id", c.SpanID,
			"error", err,
		)
		return nil
	}
	if uint(len(embedding)) != p.cfg.Dimensions {
		return fmt.Errorf(
			"embedding model %q returned %d dimensions but %d are configured; fix --embedding-model/--embedding-dimensions so the vector table matches",
			p.cfg.Model, len(embedding), p.cfg.Dimensions,
		)
	}

	if err := p.sink.Upsert(ctx, Record{
		OrgID:       c.OrgID,
		TraceID:     c.TraceID,
		SpanID:      c.SpanID,
		SessionID:   c.SessionID,
		Model:       p.cfg.Model,
		ContentHash: hash,
		Embedding:   embedding,
	}); err != nil {
		report.Failed++
		p.logger.Warn("span embedding write failed",
			"trace_id", c.TraceID,
			"span_id", c.SpanID,
			"error", err,
		)
		return nil
	}
	report.Embedded++
	return nil
}
