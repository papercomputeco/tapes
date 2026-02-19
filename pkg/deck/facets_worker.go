package deck

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
)

// FacetWorker processes sessions in the background to extract facets.
type FacetWorker struct {
	extractor *FacetExtractor
	store     FacetStore
	query     Querier

	done  atomic.Int64
	total atomic.Int64
}

// NewFacetWorker creates a new FacetWorker.
func NewFacetWorker(extractor *FacetExtractor, store FacetStore, query Querier) *FacetWorker {
	return &FacetWorker{
		extractor: extractor,
		store:     store,
		query:     query,
	}
}

// Progress returns the number of sessions processed and total to process.
func (w *FacetWorker) Progress() (done, total int) {
	return int(w.done.Load()), int(w.total.Load())
}

// Run starts background facet extraction. It queries all sessions, skips those
// with existing facets, and processes the rest with bounded concurrency.
// It blocks until all sessions are processed or the context is cancelled.
func (w *FacetWorker) Run(ctx context.Context) {
	filters := Filters{Sort: "date", SortDir: "desc"}
	overview, err := w.query.Overview(ctx, filters)
	if err != nil {
		slog.Warn("facets worker: failed to load sessions", "error", err)
		return
	}

	// Collect session IDs that need processing
	var pending []string
	for _, session := range overview.Sessions {
		if ctx.Err() != nil {
			return
		}
		_, err := w.store.GetFacet(ctx, session.ID)
		if err == nil {
			// Already has facets
			continue
		}
		pending = append(pending, session.ID)
	}

	w.total.Store(int64(len(pending)))
	w.done.Store(0)

	if len(pending) == 0 {
		return
	}

	// Process with bounded concurrency (2 workers to avoid rate limits)
	const maxConcurrency = 2
	sem := make(chan struct{}, maxConcurrency)
	var wg sync.WaitGroup

	for _, sessionID := range pending {
		if ctx.Err() != nil {
			break
		}

		sem <- struct{}{}
		wg.Add(1)
		go func(sid string) {
			defer wg.Done()
			defer func() { <-sem }()

			if ctx.Err() != nil {
				return
			}

			_, err := w.extractor.Extract(ctx, sid)
			if err != nil {
				slog.Warn("facets worker: extraction failed", "session", sid, "error", err)
			}
			w.done.Add(1)
		}(sessionID)
	}

	wg.Wait()
}
