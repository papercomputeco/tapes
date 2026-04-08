package deck

import (
	"sync"
	"time"
)

const (
	sessionCacheTTL = 30 * time.Second
)

type sessionCache struct {
	mu         sync.RWMutex
	candidates []sessionCandidate
	byID       map[string]*sessionCandidate
	loadedAt   time.Time
}

// cachedSessionCandidates is a method on *sessionCache so it can be reused
// by both the SQLite-backed Query and HTTPQuery without duplication.
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

func (c *sessionCache) storeSessionCandidates(candidates []sessionCandidate) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.candidates = copySessionCandidates(candidates)
	c.byID = buildCandidateIndex(c.candidates)
	c.loadedAt = time.Now()
}

// Wrapper methods on *Query forwarding to the underlying cache so the
// existing SQLite-backed code path keeps compiling unchanged.
func (q *Query) cachedSessionCandidates() []sessionCandidate {
	return q.cache.cachedSessionCandidates()
}

func (q *Query) cachedSessionCandidate(sessionID string) *sessionCandidate {
	return q.cache.cachedSessionCandidate(sessionID)
}

func (q *Query) storeSessionCandidates(candidates []sessionCandidate) {
	q.cache.storeSessionCandidates(candidates)
}

// candidateByID performs a linear scan for a session ID in a slice.
// Used on the slow path after a fresh load before the index is populated.
func candidateByID(candidates []sessionCandidate, sessionID string) (sessionCandidate, bool) {
	for _, c := range candidates {
		if c.summary.ID == sessionID {
			return c, true
		}
	}
	return sessionCandidate{}, false
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
