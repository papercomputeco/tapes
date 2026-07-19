package api

import (
	"strings"
	"sync"
	"time"
)

// statsCacheTTL bounds how stale a cached /v1/stats aggregate may be
// served. 60s matches the console's client-side staleTime, so the end-user
// freshness contract is unchanged: the dashboard already treats stats as
// up-to-a-minute stale.
const statsCacheTTL = 60 * time.Second

// statsCacheMaxEntries caps the cache map. Keys are (org, minute-snapped
// window), so steady-state growth is a handful of entries per active org
// per minute; the cap is a backstop against a caller sweeping arbitrary
// windows, not a working-set tuning knob.
const statsCacheMaxEntries = 1024

type statsCacheEntry struct {
	response  StatsResponse
	expiresAt time.Time
}

// statsCache memoizes the span-layer /v1/stats aggregate per
// (org, minute-snapped window). The aggregate scans every span turn in the
// window on each request, and dashboard clients re-request the same logical
// window with millisecond-unique `since` values — without snapping and
// memoizing, the identical number is recomputed for every page view.
type statsCache struct {
	mu      sync.Mutex
	entries map[string]statsCacheEntry
	// now is injectable so TTL expiry is testable without sleeping.
	now func() time.Time
}

func newStatsCache() *statsCache {
	return &statsCache{
		entries: make(map[string]statsCacheEntry),
		now:     time.Now,
	}
}

func (c *statsCache) get(key string) (StatsResponse, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.entries[key]
	if !ok || c.now().After(e.expiresAt) {
		return StatsResponse{}, false
	}
	return e.response, true
}

func (c *statsCache) set(key string, resp StatsResponse) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.entries) >= statsCacheMaxEntries {
		now := c.now()
		for k, e := range c.entries {
			if now.After(e.expiresAt) {
				delete(c.entries, k)
			}
		}
		// Everything still live means a pathological key sweep inside one
		// TTL; dropping the map wholesale is cheaper than tracking LRU
		// order for a cache whose entries cost one aggregate query each.
		if len(c.entries) >= statsCacheMaxEntries {
			clear(c.entries)
		}
	}
	c.entries[key] = statsCacheEntry{response: resp, expiresAt: c.now().Add(statsCacheTTL)}
}

// snapStatsWindow widens the requested window to whole-minute boundaries:
// since floors, until ceils. Dashboard clients anchor `since` on their own
// clock at request time, so two requests for the same logical window ("last
// 30d") differ by milliseconds and would never share a cache entry. The
// snapped window always CONTAINS the requested one (never drops data the
// caller asked for); it over-includes at most 60s per edge, which is noise
// for a whole-window aggregate.
func snapStatsWindow(since, until *time.Time) (*time.Time, *time.Time) {
	if since != nil {
		t := since.UTC().Truncate(time.Minute)
		since = &t
	}
	if until != nil {
		t := until.UTC().Truncate(time.Minute)
		if t.Before(until.UTC()) {
			t = t.Add(time.Minute)
		}
		until = &t
	}
	return since, until
}

// statsCacheKey identifies one (org, window) aggregate. Callers pass the
// already-snapped window so equal logical windows collide.
func statsCacheKey(orgID string, since, until *time.Time) string {
	var b strings.Builder
	b.WriteString(orgID)
	b.WriteByte('|')
	if since != nil {
		b.WriteString(since.UTC().Format(time.RFC3339))
	}
	b.WriteByte('|')
	if until != nil {
		b.WriteString(until.UTC().Format(time.RFC3339))
	}
	return b.String()
}
