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

// DefaultMaxTextBytes caps a single span's rendered text. Chunking splits an
// oversized span into ceil(tokens/budget) pieces, so an unbounded span would
// fan out into hundreds of embed calls and rows and hold several copies of its
// text in memory at once. Past this ceiling the span is recorded as a
// deterministic "too_large" failure instead — bounding per-span memory, embed
// cost, and chunk count regardless of batch size or pod limits. ~1 MiB is ~33
// chunks at the model's per-chunk budget. 0 disables the guard.
const DefaultMaxTextBytes = 1 << 20

// reasonTooLarge labels the failure recorded for a span whose rendered text
// exceeds PassConfig.MaxTextBytes.
const reasonTooLarge = "too_large"

// Source lists embed candidates. *Store implements it; tests
// substitute a fake.
type Source interface {
	ListCandidates(ctx context.Context, after Key, limit int) ([]Candidate, error)
}

// Sink persists embeddings. *Store implements it; tests substitute a
// fake.
type Sink interface {
	UpsertSpanChunks(ctx context.Context, rec ChunkRecord) error
	RecordFailure(ctx context.Context, rec FailureRecord) error
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

	// MaxTextBytes caps a span's rendered text; a larger span is recorded
	// as a "too_large" failure rather than embedded, bounding per-span
	// memory, chunk count, and embed cost. Zero takes DefaultMaxTextBytes;
	// negative disables the guard.
	MaxTextBytes int
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
	if cfg.MaxTextBytes == 0 {
		cfg.MaxTextBytes = DefaultMaxTextBytes
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
	report := &Report{FailuresByReason: map[string]int{}}

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
		"chunked", report.Chunked,
		"up_to_date", report.UpToDate,
		"empty", report.Empty,
		"poisoned", report.Poisoned,
		"failed", report.Failed,
		"pruned", report.Pruned,
	)
	return report, nil
}

// maxSplitDepth bounds how many times an oversized span is recursively split
// before it is declared un-embeddable and recorded as a failure. Each level at
// least halves the text, so the depth doubles as a guard against a pathological
// input (e.g. one enormous token-dense line) looping forever.
const maxSplitDepth = 4

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
	case actionPoisoned:
		report.Poisoned++
		return nil
	case actionEmbed:
	}

	if p.cfg.MaxTextBytes > 0 && len(text) > p.cfg.MaxTextBytes {
		return p.recordTooLarge(ctx, c, hash, text, report)
	}

	vectors, oversizeTokens, err := p.embedChunked(ctx, text)
	if oversizeTokens > 0 {
		report.Oversized++
		report.OversizeTokens = append(report.OversizeTokens, oversizeTokens)
	}
	if err != nil {
		return p.handleEmbedError(ctx, c, hash, oversizeTokens, report, err)
	}
	for _, v := range vectors {
		if uint(len(v)) != p.cfg.Dimensions {
			return fmt.Errorf(
				"embedding model %q returned %d dimensions but %d are configured; fix --embedding-model/--embedding-dimensions so the vector table matches",
				p.cfg.Model, len(v), p.cfg.Dimensions,
			)
		}
	}

	if err := p.sink.UpsertSpanChunks(ctx, ChunkRecord{
		OrgID:       c.OrgID,
		TraceID:     c.TraceID,
		SpanID:      c.SpanID,
		SessionID:   c.SessionID,
		Model:       p.cfg.Model,
		ContentHash: hash,
		Embeddings:  vectors,
	}); err != nil {
		// A write failure is transient (DB hiccup); the next pass retries.
		report.Failed++
		p.logger.Warn("span embedding write failed",
			"trace_id", c.TraceID,
			"span_id", c.SpanID,
			"error", err,
		)
		return nil
	}
	report.Embedded++
	report.ChunkRows += len(vectors)
	if len(vectors) > 1 {
		report.Chunked++
	}
	return nil
}

// handleEmbedError records and logs an embed failure, sorting it into a
// deterministic failure (recorded so the span is skipped until its content or
// model changes) or a transient one (left for the next pass to retry). It never
// aborts the pass.
func (p *Pass) handleEmbedError(ctx context.Context, c *Candidate, hash string, oversizeTokens int, report *Report, embedErr error) error {
	report.Failed++

	apiErr, ok := embeddings.AsAPIError(embedErr)
	deterministic := ok && !apiErr.Retryable()
	if !deterministic {
		// Transient (rate limit, server/transport error) or an unclassified
		// error: assume it may succeed later and retry next pass.
		p.logger.Warn("span embed failed",
			"trace_id", c.TraceID,
			"span_id", c.SpanID,
			"error", embedErr,
		)
		return nil
	}

	reason := failureReason(apiErr)
	report.FailuresByReason[reason]++
	if err := p.sink.RecordFailure(ctx, FailureRecord{
		OrgID:       c.OrgID,
		TraceID:     c.TraceID,
		SpanID:      c.SpanID,
		SessionID:   c.SessionID,
		Model:       p.cfg.Model,
		ContentHash: hash,
		Reason:      reason,
		ErrorDetail: embedErr.Error(),
		TokenCount:  oversizeTokens,
	}); err != nil {
		// Recording the marker failed; the span simply gets retried next
		// pass, so log and move on.
		p.logger.Warn("recording span embed failure failed",
			"trace_id", c.TraceID,
			"span_id", c.SpanID,
			"error", err,
		)
		return nil
	}
	p.logger.Warn("span embed failed permanently; recorded",
		"trace_id", c.TraceID,
		"span_id", c.SpanID,
		"reason", reason,
		"token_count", oversizeTokens,
		"error", embedErr,
	)
	return nil
}

// recordTooLarge marks a span whose rendered text exceeds MaxTextBytes as a
// deterministic failure, so it is skipped (not chunked into hundreds of pieces)
// until its content shrinks — bounding per-span memory, embed cost, and chunk
// count. Recorded with an estimated token count so the failure row shows how
// large it was.
func (p *Pass) recordTooLarge(ctx context.Context, c *Candidate, hash, text string, report *Report) error {
	report.Failed++
	report.FailuresByReason[reasonTooLarge]++
	if err := p.sink.RecordFailure(ctx, FailureRecord{
		OrgID:       c.OrgID,
		TraceID:     c.TraceID,
		SpanID:      c.SpanID,
		SessionID:   c.SessionID,
		Model:       p.cfg.Model,
		ContentHash: hash,
		Reason:      reasonTooLarge,
		ErrorDetail: fmt.Sprintf("rendered text %d bytes exceeds max %d", len(text), p.cfg.MaxTextBytes),
		TokenCount:  estimateTokens(text),
	}); err != nil {
		p.logger.Warn("recording too-large span failure failed",
			"trace_id", c.TraceID,
			"span_id", c.SpanID,
			"error", err,
		)
		return nil
	}
	p.logger.Warn("span too large to embed; recorded",
		"trace_id", c.TraceID,
		"span_id", c.SpanID,
		"bytes", len(text),
		"max_bytes", p.cfg.MaxTextBytes,
	)
	return nil
}

// embedChunked embeds text, transparently splitting it into pieces when the
// model rejects it for exceeding the context window, and returns one embedding
// per piece in order (a single embedding for text that fit). oversizeTokens is
// the token count the model reported for the whole text (0 when it fit or the
// count was unparseable), surfaced for telemetry. A returned error is terminal
// for this span: a non-oversize failure, or text still oversize past the split
// depth cap.
func (p *Pass) embedChunked(ctx context.Context, text string) (vectors [][]float32, oversizeTokens int, err error) {
	vec, err := p.embedder.Embed(ctx, text)
	if err == nil {
		return [][]float32{vec}, 0, nil
	}
	apiErr, ok := embeddings.AsAPIError(err)
	if !ok || !apiErr.IsOversize() {
		return nil, 0, err
	}
	// OpenAI's embeddings oversize error reports no token count, so estimate
	// from length when the provider omits it — this both sizes the split and
	// gives the oversize_tokens metric a non-zero value.
	reported := apiErr.RequestedTokens
	if reported == 0 {
		reported = estimateTokens(text)
	}
	chunks, splitErr := p.embedSplit(ctx, text, reported, 1)
	if splitErr != nil {
		return nil, reported, splitErr
	}
	return chunks, reported, nil
}

// embedSplit splits text into pieces sized against reportedTokens and embeds
// each, recursing on any piece the model still rejects as oversize until
// maxSplitDepth.
func (p *Pass) embedSplit(ctx context.Context, text string, reportedTokens, depth int) ([][]float32, error) {
	parts := splitParts(text, reportedTokens)
	if len(parts) < 2 {
		return nil, errors.New("oversized embedding input cannot be split further")
	}
	var out [][]float32
	for _, part := range parts {
		vec, err := p.embedder.Embed(ctx, part)
		if err == nil {
			out = append(out, vec)
			continue
		}
		apiErr, ok := embeddings.AsAPIError(err)
		if !ok || !apiErr.IsOversize() {
			return nil, err
		}
		if depth >= maxSplitDepth {
			return nil, fmt.Errorf("embedding input still oversize after %d splits: %w", depth, err)
		}
		sub, err := p.embedSplit(ctx, part, apiErr.RequestedTokens, depth+1)
		if err != nil {
			return nil, err
		}
		out = append(out, sub...)
	}
	return out, nil
}

// failureReason labels a deterministic failure for the failure record and the
// metric: "oversize" when the model rejected the size, otherwise the HTTP
// status (e.g. "api_400").
func failureReason(e *embeddings.APIError) string {
	if e.IsOversize() {
		return "oversize"
	}
	if e.Status > 0 {
		return fmt.Sprintf("api_%d", e.Status)
	}
	return "api_error"
}
