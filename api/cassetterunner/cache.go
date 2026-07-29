package cassetterunner

import (
	"sync"

	"github.com/papercomputeco/tapes/pkg/cassette"
	"github.com/papercomputeco/tapes/pkg/openapi"
)

// entry is one cassette's OpenAPI document as core republished it.
//
// document and parsed are the same content in the two shapes callers need —
// bytes to serve, tree to merge — and are always replaced together. status and
// problem are the only fields that change in place, so a cassette can go stale
// without disturbing the bytes clients are still being handed.
//
// There is deliberately no ETag here. The validator belongs to the source that
// was fetched, not to the cassette that was published: a source has a
// validator before it has an identity, so holding one in both places would be
// two answers to one question.
type entry struct {
	document []byte
	parsed   *openapi.Document
	digest   cassette.Digest
	source   string
	status   openapi.Status
	problem  string
}

// specCache holds the documents core serves on behalf of its cassettes.
//
// It guards its own state rather than sharing the runner's lock. The refresh
// path is a long sequence of fallible steps and this cache is read by request
// handlers throughout, so keeping the locking here is what lets the refresh
// code read as the pipeline it is.
type specCache struct {
	mutex   sync.RWMutex
	entries map[cassette.Name]*entry
}

func newSpecCache() *specCache {
	return &specCache{entries: make(map[cassette.Name]*entry)}
}

// publish installs the document core will serve for a cassette, replacing any
// previous one and clearing whatever problem it had.
func (cache *specCache) publish(name cassette.Name, published *publication, source string) {
	cache.mutex.Lock()
	defer cache.mutex.Unlock()

	cache.entries[name] = &entry{
		document: published.document,
		parsed:   published.parsed,
		digest:   published.digest,
		source:   source,
		status:   openapi.Fresh,
	}
}

// evict drops a cassette's cached document.
func (cache *specCache) evict(name cassette.Name) {
	cache.mutex.Lock()
	defer cache.mutex.Unlock()

	delete(cache.entries, name)
}

// evictSource drops a cassette's document only when it still belongs to the
// removed source. Another configured source may already have replaced it under
// the same cassette name.
func (cache *specCache) evictSource(name cassette.Name, source string) {
	cache.mutex.Lock()
	defer cache.mutex.Unlock()

	cached := cache.entries[name]
	if cached != nil && cached.source == source {
		delete(cache.entries, name)
	}
}

// markFresh clears a cassette's problem after a successful revalidation.
//
// The source must match the one the cached document came from. A cassette can
// be claimed by more than one configured source, and a losing duplicate must
// not be able to speak for the winner's freshness.
func (cache *specCache) markFresh(name cassette.Name, source string) {
	cache.mutex.Lock()
	defer cache.mutex.Unlock()

	cached := cache.entries[name]
	if cached == nil || cached.source != source {
		return
	}
	cached.status = openapi.Fresh
	cached.problem = ""
}

// markStale records a problem against the document a source published.
//
// The cached bytes are deliberately left in place: a client that generated
// code from this surface should not lose it because a container restarted. The
// source match is what keeps a failing duplicate from staling the winner.
func (cache *specCache) markStale(name cassette.Name, source, problem string) {
	cache.mutex.Lock()
	defer cache.mutex.Unlock()

	cached := cache.entries[name]
	if cached == nil || cached.document == nil || cached.source != source {
		return
	}
	cached.status = openapi.Stale
	cached.problem = problem
}

// hasSource reports whether name currently has a document published by source.
func (cache *specCache) hasSource(name cassette.Name, source string) bool {
	cache.mutex.RLock()
	defer cache.mutex.RUnlock()

	cached := cache.entries[name]

	return cached != nil && cached.document != nil && cached.source == source
}

// status reports how current the cached document for a cassette is.
func (cache *specCache) status(name cassette.Name) openapi.Status {
	cache.mutex.RLock()
	defer cache.mutex.RUnlock()

	cached := cache.entries[name]
	if cached == nil || cached.document == nil {
		return openapi.Missing
	}

	return cached.status
}

// spec returns a copy of one cassette's document and its digest.
func (cache *specCache) spec(name cassette.Name) ([]byte, cassette.Digest, bool) {
	cache.mutex.RLock()
	defer cache.mutex.RUnlock()

	cached := cache.entries[name]
	if cached == nil || cached.document == nil {
		return nil, "", false
	}

	return append([]byte(nil), cached.document...), cached.digest, true
}

// problem returns the current explanation for a cassette, empty when it is
// healthy or unknown.
func (cache *specCache) problem(name cassette.Name) string {
	cache.mutex.RLock()
	defer cache.mutex.RUnlock()

	if cached := cache.entries[name]; cached != nil {
		return cached.problem
	}

	return ""
}

// documents returns every cached document keyed by cassette name. The parsed
// trees are never mutated after publication, so they are safe to hand out.
func (cache *specCache) documents() map[string]*openapi.Document {
	cache.mutex.RLock()
	defer cache.mutex.RUnlock()

	documents := make(map[string]*openapi.Document, len(cache.entries))
	for name, cached := range cache.entries {
		if cached.parsed != nil {
			documents[string(name)] = cached.parsed
		}
	}

	return documents
}
