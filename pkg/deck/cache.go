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

func (q *Query) cachedSessionCandidates() []sessionCandidate {
	q.cache.mu.RLock()
	defer q.cache.mu.RUnlock()

	if len(q.cache.candidates) == 0 {
		return nil
	}
	if time.Since(q.cache.loadedAt) > sessionCacheTTL {
		return nil
	}

	return copySessionCandidates(q.cache.candidates)
}

// cachedSessionCandidate returns a single candidate by session ID from the
// cache index, or nil if the cache is stale/empty or the ID is not found.
func (q *Query) cachedSessionCandidate(sessionID string) *sessionCandidate {
	q.cache.mu.RLock()
	defer q.cache.mu.RUnlock()

	if len(q.cache.byID) == 0 {
		return nil
	}
	if time.Since(q.cache.loadedAt) > sessionCacheTTL {
		return nil
	}

	c, ok := q.cache.byID[sessionID]
	if !ok {
		return nil
	}

	cp := *c
	return &cp
}

func (q *Query) storeSessionCandidates(candidates []sessionCandidate) {
	q.cache.mu.Lock()
	defer q.cache.mu.Unlock()
	q.cache.candidates = copySessionCandidates(candidates)
	q.cache.byID = buildCandidateIndex(q.cache.candidates)
	q.cache.loadedAt = time.Now()
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
