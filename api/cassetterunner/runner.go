// Package cassetterunner resolves and serves the cassettes an API server
// publishes.
//
// It owns one lifecycle: fetch each configured cassette's OpenAPI document,
// admit or refuse it against the contracts this core serves, republish the
// paths it declares onto core's public surface, and cache the result so the
// API can still answer for a cassette that is currently down.
//
// Everything here is about a *running* fleet — which cassettes are installed,
// which were refused, what their current documents are. The vocabulary those
// cassettes are described in lives in pkg/cassette, which knows nothing about
// any of this.
package cassetterunner

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/papercomputeco/tapes/pkg/cassette"
	"github.com/papercomputeco/tapes/pkg/cassette/manifest"
	"github.com/papercomputeco/tapes/pkg/openapi"
)

// defaultFetchTimeout bounds a single cassette spec fetch.
const defaultFetchTimeout = 10 * time.Second

// SpecCache is the published-document surface the API server reads.
//
// The server depends on this rather than on *Runner so handler tests can state
// a cassette surface directly instead of standing up a fetch loop full of real
// documents.
type SpecCache interface {
	// Status reports how current the cached document for a cassette is.
	Status(name cassette.Name) openapi.Status

	// Spec returns one cassette's cached document and its digest.
	Spec(name cassette.Name) ([]byte, cassette.Digest, bool)

	// Document returns the merged OpenAPI document.
	Document() ([]byte, error)

	// Refresh fetches the current document from every configured source.
	Refresh(ctx context.Context) []error
}

// Config configures a Runner.
type Config struct {
	// Registry is the fleet this runner resolves into. A nil registry gets a
	// fresh empty one.
	Registry *Registry

	// Contracts is the set of tapes contract versions the owning core serves.
	// A cassette whose depends.core falls outside the set is refused, because
	// its grant plan would name a tapes_<version> schema this deployment does
	// not have.
	//
	// It is supplied by the server rather than read from a constant so the
	// dependency runs one way: a cassette declares what it needs, and core
	// decides whether it can serve that. An empty set admits nothing, which is
	// the honest failure for a runner never told what its core serves.
	Contracts []cassette.ContractVersion

	// Title and Version describe the merged document served at /openapi.
	// Version should be the contract core advertises, so the aggregate and the
	// discovery document cannot disagree about which surface this is.
	Title   string
	Version string

	// Client fetches cassette documents. A nil client gets one that refuses
	// redirects, which is load-bearing: following one would let a cassette be
	// admitted from a document served by a different origin than the one core
	// proxies traffic to.
	Client *http.Client
}

// sourceState is one configured OpenAPI URL and what resolving it produced.
type sourceState struct {
	// url is the exact configured document URL, and the stable subject any
	// rejection for this source is filed under.
	url string

	// name is the cassette this source resolved to, empty until it does. A
	// resolved source is pinned to that name: a source that starts serving a
	// different cassette is a misconfiguration, not a rename.
	name cassette.Name

	// etag revalidates this source. It is held per source rather than per
	// cassette because a source has a validator before it has an identity.
	etag string

	// resolved reports whether this source has ever been admitted.
	resolved bool
}

// Runner resolves configured cassette sources into a registry and keeps their
// published OpenAPI documents cached, for one API server lifetime.
type Runner struct {
	registry  *Registry
	specs     *specCache
	client    *http.Client
	contracts []cassette.ContractVersion
	title     string
	version   string

	// refreshMutex serializes whole refresh passes. It is separate from the
	// state lock below because a pass performs network I/O, and holding a data
	// lock across that would block every reader for the length of a timeout.
	refreshMutex sync.Mutex

	// mutex guards the source catalog only. The registry and the spec cache
	// guard themselves.
	mutex   sync.RWMutex
	sources []sourceState
}

// NewRunner returns a runner that admits cassettes depending on any of the
// contracts in config.
func NewRunner(config Config) *Runner {
	registry := config.Registry
	if registry == nil {
		registry = NewRegistry()
	}
	client := config.Client
	if client == nil {
		client = &http.Client{
			Timeout: defaultFetchTimeout,
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		}
	}
	title := config.Title
	if title == "" {
		title = "tapes"
	}

	return &Runner{
		registry:  registry,
		specs:     newSpecCache(),
		client:    client,
		contracts: append([]cassette.ContractVersion(nil), config.Contracts...),
		title:     title,
		version:   config.Version,
	}
}

// Registry returns the fleet this runner resolves into.
func (runner *Runner) Registry() *Registry { return runner.registry }

// SetSources installs the exact full OpenAPI URLs to resolve, in configured
// order. Resolution stays asynchronous and retryable: an unreachable source is
// a source that has not resolved yet, not a startup failure.
//
// State for retained URLs survives reordering so their identity and ETag remain
// pinned. Removing a URL immediately withdraws the cassette it owned from the
// registry and cache; otherwise a source no longer visited by Refresh would
// remain published forever.
func (runner *Runner) SetSources(sources []string) {
	runner.refreshMutex.Lock()
	defer runner.refreshMutex.Unlock()

	runner.mutex.Lock()
	previous := runner.sources
	retained := make([]bool, len(previous))
	configured := make(map[string]struct{}, len(sources))
	next := make([]sourceState, len(sources))
	for nextIndex, source := range sources {
		configured[source] = struct{}{}
		next[nextIndex].url = source
		for previousIndex := range previous {
			if !retained[previousIndex] && previous[previousIndex].url == source {
				next[nextIndex] = previous[previousIndex]
				retained[previousIndex] = true

				break
			}
		}
	}
	runner.sources = next
	runner.mutex.Unlock()

	for index, state := range previous {
		_, urlStillConfigured := configured[state.url]
		if retained[index] || urlStillConfigured {
			continue
		}
		runner.registry.ClearRejection(safeSource(state.url))
		if state.name == "" {
			continue
		}

		// Removal is the reverse of publication: hide the routable instance
		// before dropping its document, preserving the invariant that every
		// cassette visible through discovery and proxy lookup has a spec.
		runner.registry.remove(state.name, state.url)
		runner.specs.evictSource(state.name, state.url)
	}
}

// Refresh resolves every configured source once.
//
// Errors are returned per source rather than joined, because each one is a
// separate operator-facing problem: the caller logs them individually and each
// is already filed as a rejection against the source that produced it.
func (runner *Runner) Refresh(ctx context.Context) []error {
	runner.refreshMutex.Lock()
	defer runner.refreshMutex.Unlock()

	errs := make([]error, 0)
	for index := range runner.sourceURLs() {
		if err := runner.refreshSource(ctx, index); err != nil {
			errs = append(errs, err)
		}
	}

	return errs
}

// refreshSource resolves one configured source: fetch, admit, republish,
// register.
func (runner *Runner) refreshSource(ctx context.Context, index int) error {
	state := runner.source(index)

	origin, err := sourceOrigin(state.url)
	if err != nil {
		return runner.failSource(index, err)
	}

	etag := state.etag
	currentlyPublished := false
	if state.resolved {
		instance, registered := runner.registry.Get(state.name)
		currentlyPublished = registered && instance.Source == state.url &&
			runner.specs.hasSource(state.name, state.url)
		if !currentlyPublished {
			// A configured-priority peer may have replaced this source's
			// publication. Its ETag only validates the displaced document,
			// which is no longer cached, so the source must send a full copy
			// before it can become the winner again.
			etag = ""
		}
	}

	result, err := runner.fetch(ctx, state.url, etag)
	if err != nil {
		return runner.failSource(index, fmt.Errorf("cassette source %s: %w", safeSource(state.url), err))
	}
	if result.notModified && currentlyPublished {
		runner.markSourceFresh(index)

		return nil
	}
	if result.document == nil {
		return runner.failSource(index,
			fmt.Errorf("cassette source %s answered 304 without a currently published document", safeSource(state.url)))
	}

	declared, err := runner.admit(result.document)
	if err != nil {
		return runner.failSource(index, fmt.Errorf("cassette source %s: %w", safeSource(state.url), err))
	}

	name := declared.CassetteName()
	if state.resolved && state.name != name {
		return runner.failSource(index, fmt.Errorf("cassette source %s changed name from %q to %q",
			safeSource(state.url), state.name, name))
	}
	if err := runner.checkPriority(name, state.url, index); err != nil {
		return runner.failSource(index, err)
	}

	digest, err := declared.Digest()
	if err != nil {
		return runner.failSource(index, err)
	}
	instance := &Instance{
		Name:     name,
		Manifest: declared,
		Digest:   digest,
		URL:      origin,
		Anchors:  declared.Anchors(),
		Source:   state.url,
	}

	published, err := republish(result.document, instance)
	if err != nil {
		return runner.failSource(index, err)
	}

	// The document is cached before the instance is registered, never after.
	// A reader that sees a cassette in the registry must be able to fetch its
	// spec; the reverse gap — a document cached for a cassette nobody can
	// name — is invisible, so it is the safe side to fail on.
	runner.specs.publish(name, published, state.url)
	if err := runner.registry.Put(instance); err != nil {
		runner.specs.evict(name)

		return runner.failSource(index, err)
	}

	runner.resolveSource(index, name, result.etag)
	runner.registry.ClearRejection(state.url)

	return nil
}

// admit parses the manifest a document must carry and checks it against the
// contracts this core serves.
func (runner *Runner) admit(document *openapi.Document) (cassette.Manifest, error) {
	declared, present, err := manifest.FromDocument(document)
	if err != nil {
		return nil, err
	}
	if !present {
		return nil, fmt.Errorf("missing root extension %q", manifest.Extension)
	}
	if err := declared.Validate(runner.contracts); err != nil {
		return nil, err
	}

	return declared, nil
}

// checkPriority refuses a source that would take a name an earlier configured
// source already holds. Configured order is the tiebreak, so the winner does
// not depend on which source happened to answer first.
func (runner *Runner) checkPriority(name cassette.Name, source string, index int) error {
	existing, ok := runner.registry.Get(name)
	if !ok || existing.Source == source {
		return nil
	}
	if existingIndex := runner.sourceIndex(existing.Source); existingIndex >= 0 && existingIndex >= index {
		return nil
	}

	return fmt.Errorf("cassette %q is already registered by earlier source %s", name, safeSource(existing.Source))
}

func (runner *Runner) source(index int) sourceState {
	runner.mutex.RLock()
	defer runner.mutex.RUnlock()

	return runner.sources[index]
}

func (runner *Runner) sourceURLs() []string {
	runner.mutex.RLock()
	defer runner.mutex.RUnlock()

	urls := make([]string, len(runner.sources))
	for index := range runner.sources {
		urls[index] = runner.sources[index].url
	}

	return urls
}

func (runner *Runner) sourceIndex(source string) int {
	runner.mutex.RLock()
	defer runner.mutex.RUnlock()

	for index := range runner.sources {
		if runner.sources[index].url == source {
			return index
		}
	}

	return -1
}

// resolveSource pins a source to the cassette it produced.
func (runner *Runner) resolveSource(index int, name cassette.Name, etag string) {
	runner.mutex.Lock()
	defer runner.mutex.Unlock()

	runner.sources[index].name = name
	runner.sources[index].etag = etag
	runner.sources[index].resolved = true
}

// failSource records a source problem: against the cassette whose document it
// published, if it had one, and against the source itself for the operator.
func (runner *Runner) failSource(index int, err error) error {
	state := runner.source(index)
	if state.name != "" {
		runner.specs.markStale(state.name, state.url, err.Error())
	}
	runner.registry.SetRejection(safeSource(state.url), err)

	return err
}

// markSourceFresh clears a source's problem after a successful revalidation.
func (runner *Runner) markSourceFresh(index int) {
	state := runner.source(index)
	runner.specs.markFresh(state.name, state.url)
	runner.registry.ClearRejection(safeSource(state.url))
}

// Status reports how current the cached document for a cassette is.
func (runner *Runner) Status(name cassette.Name) openapi.Status {
	return runner.specs.status(name)
}

// Spec returns one cassette's cached document and its digest.
func (runner *Runner) Spec(name cassette.Name) ([]byte, cassette.Digest, bool) {
	return runner.specs.spec(name)
}

// Problem returns why a cassette's document is stale or missing, empty when
// there is nothing wrong or nothing known.
func (runner *Runner) Problem(name cassette.Name) string {
	return runner.specs.problem(name)
}

// Document returns every cached cassette document merged into one description
// of the whole surface this core publishes.
func (runner *Runner) Document() ([]byte, error) {
	return openapi.Merge(runner.title, runner.version, runner.specs.documents())
}

// compile-time proof that a Runner is what the API server wants.
var _ SpecCache = (*Runner)(nil)
