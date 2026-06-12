package deck

import (
	"sync"
	"time"
)

const (
	sessionCacheTTL = 30 * time.Second
)

// sessionCache is the small in-memory cache HTTPQuery uses to remember the
// most recent overview page(s) between TUI refreshes, so SessionDetail can
// reuse the already-fetched SessionSummary instead of refetching the row.
// Entries expire after sessionCacheTTL so a stale dashboard doesn't keep
// showing data after the underlying store has changed.
type sessionCache struct {
	mu       sync.RWMutex
	byID     map[string]SessionSummary
	loadedAt time.Time
}

// cachedSummary returns a copy of the cached summary for sessionID, or nil
// if the cache is stale/empty or the ID is not present.
func (c *sessionCache) cachedSummary(sessionID string) *SessionSummary {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if len(c.byID) == 0 {
		return nil
	}
	if time.Since(c.loadedAt) > sessionCacheTTL {
		return nil
	}

	summary, ok := c.byID[sessionID]
	if !ok {
		return nil
	}
	return &summary
}

// storeSummaries replaces the cache contents with a fresh snapshot.
func (c *sessionCache) storeSummaries(summaries []SessionSummary) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.byID = make(map[string]SessionSummary, len(summaries))
	for _, summary := range summaries {
		c.byID[summary.ID] = summary
	}
	c.loadedAt = time.Now()
}

// appendSummaries merges summaries into the current cache snapshot,
// replacing duplicate session IDs.
func (c *sessionCache) appendSummaries(summaries []SessionSummary) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.byID == nil {
		c.byID = make(map[string]SessionSummary, len(summaries))
	}
	for _, summary := range summaries {
		c.byID[summary.ID] = summary
	}
	c.loadedAt = time.Now()
}
