package deck

import (
	"sync"
	"time"
)

const (
	sessionCacheTTL = 30 * time.Second
)

// sessionCandidate is a cached entry in the session overview, paired with
// its per-model cost breakdown and derived status. Used by HTTPQuery to
// hold the results of /v1/sessions/summary calls so the grouping and
// detail paths can look candidates up by ID without re-fetching.
type sessionCandidate struct {
	summary    SessionSummary
	modelCosts map[string]ModelCost
	status     string
}

// sessionCache is the small in-memory cache HTTPQuery uses to remember the
// most recent session overview between TUI refreshes. Entries expire after
// sessionCacheTTL so a stale dashboard doesn't keep showing data after the
// underlying store has changed.
type sessionCache struct {
	mu         sync.RWMutex
	candidates []sessionCandidate
	byID       map[string]*sessionCandidate
	loadedAt   time.Time
}

// cachedSessionCandidates returns a copy of the cached candidate list, or
// nil if the cache is empty or stale.
func (c *sessionCache) cachedSessionCandidates() []sessionCandidate {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if len(c.candidates) == 0 {
		return nil
	}
	if time.Since(c.loadedAt) > sessionCacheTTL {
		return nil
	}
	return copySessionCandidates(c.candidates)
}

// cachedSessionCandidate returns a single candidate by session ID from the
// cache index, or nil if the cache is stale/empty or the ID is not found.
func (c *sessionCache) cachedSessionCandidate(sessionID string) *sessionCandidate {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if len(c.byID) == 0 {
		return nil
	}
	if time.Since(c.loadedAt) > sessionCacheTTL {
		return nil
	}

	cand, ok := c.byID[sessionID]
	if !ok {
		return nil
	}
	cp := *cand
	return &cp
}

// storeSessionCandidates replaces the cache contents with a fresh snapshot.
func (c *sessionCache) storeSessionCandidates(candidates []sessionCandidate) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.candidates = copySessionCandidates(candidates)
	c.byID = buildCandidateIndex(c.candidates)
	c.loadedAt = time.Now()
}

// buildCandidateIndex returns a map keyed by session ID pointing into the
// given slice. The pointers are valid for the lifetime of the slice.
func buildCandidateIndex(candidates []sessionCandidate) map[string]*sessionCandidate {
	idx := make(map[string]*sessionCandidate, len(candidates))
	for i := range candidates {
		idx[candidates[i].summary.ID] = &candidates[i]
	}
	return idx
}

func copySessionCandidates(candidates []sessionCandidate) []sessionCandidate {
	if len(candidates) == 0 {
		return nil
	}
	cloned := make([]sessionCandidate, len(candidates))
	copy(cloned, candidates)
	return cloned
}
