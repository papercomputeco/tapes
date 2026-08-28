package cassetterunner

import (
	"errors"
	"net/url"
	"sort"
	"strings"
	"sync"

	"github.com/papercomputeco/tapes/pkg/cassette"
)

// Registry is the set of cassettes core is serving, plus the ones it refused.
//
// It is safe for concurrent use: the route table is read on every proxied
// request while the runner may be resolving additional cassettes.
type Registry struct {
	mutex      sync.RWMutex
	instances  map[cassette.Name]*Instance
	order      []cassette.Name
	rejections []Rejection

	// armed is the set of filter claims whose published view survived the
	// most recent probe, keyed by claimKey. It is state rather than a
	// derivation because readability is a fact about the database, not the
	// manifest: ClaimsFor withholds anything not in this set, so an
	// un-armed claim is invisible to the request path while remaining
	// admitted for ownership.
	armed map[string]struct{}
}

// NewRegistry returns an empty cassette registry.
func NewRegistry() *Registry {
	return &Registry{
		instances:  make(map[cassette.Name]*Instance),
		rejections: make([]Rejection, 0),
		armed:      make(map[string]struct{}),
	}
}

// Put installs a resolved instance, replacing any cassette already registered
// under the same name.
//
// Replacement rather than refusal is right because the runner has already
// applied configured source priority by the time it gets here: two sources
// claiming one name is settled upstream, so what reaches Put is always the
// winner, and a re-resolved winner must overwrite its own earlier document.
func (registry *Registry) Put(instance *Instance) error {
	if instance == nil {
		return errors.New("registry: nil instance")
	}

	registry.mutex.Lock()
	defer registry.mutex.Unlock()

	if err := registry.check(instance); err != nil {
		return err
	}
	previous, exists := registry.instances[instance.Name]
	if exists {
		registry.pruneClaimState(previous, instance)
	} else {
		registry.order = append(registry.order, instance.Name)
	}
	registry.instances[instance.Name] = instance

	return nil
}

// pruneClaimState drops arming state and claim rejections that belonged to
// the previous manifest and are absent from the replacement. Claims carried
// over identically keep their armed state, so a steady-state re-registration
// is not an effective-claim-set change; a claim whose probe-relevant fields
// moved starts un-armed and must be proved against its new view. Runs under
// the write lock.
func (registry *Registry) pruneClaimState(previous, next *Instance) {
	nextKeys := make(map[string]struct{})
	nextSubjects := make(map[string]struct{})
	for _, claim := range ManifestClaims(next.Name, next.Manifest) {
		nextKeys[claimKey(claim)] = struct{}{}
		nextSubjects[claimSubject(claim)] = struct{}{}
	}
	for _, claim := range ManifestClaims(previous.Name, previous.Manifest) {
		if _, kept := nextKeys[claimKey(claim)]; !kept {
			delete(registry.armed, claimKey(claim))
		}
		if _, kept := nextSubjects[claimSubject(claim)]; !kept {
			registry.clearRejection(claimSubject(claim))
		}
	}
}

// ArmClaim marks one admitted claim executable and clears the rejection
// filed while it was un-armed. The verdict itself belongs to the caller —
// the registry only stores it. It reports whether the effective claim set
// changed, so a caller can notify hook-declaring cassettes on real
// transitions only.
func (registry *Registry) ArmClaim(claim ActiveClaim) bool {
	registry.mutex.Lock()
	defer registry.mutex.Unlock()

	registry.clearRejection(claimSubject(claim))
	key := claimKey(claim)
	if _, armed := registry.armed[key]; armed {
		return false
	}
	registry.armed[key] = struct{}{}

	return true
}

// DisarmClaim marks one claim not executable and files why under the
// claim's stable subject, replacing any earlier reason. The request path
// then treats the claimed param exactly as unclaimed — fail-open by
// contract — while discovery's problems list carries the reason. It reports
// whether the effective claim set changed.
func (registry *Registry) DisarmClaim(claim ActiveClaim, reason error) bool {
	registry.mutex.Lock()
	defer registry.mutex.Unlock()

	registry.setRejection(claimSubject(claim), reason)
	key := claimKey(claim)
	if _, armed := registry.armed[key]; !armed {
		return false
	}
	delete(registry.armed, key)

	return true
}

// remove drops name only when it is still owned by source. The ownership check
// prevents removal of an old source from evicting the configured-priority
// winner that replaced it under the same cassette name.
func (registry *Registry) remove(name cassette.Name, source string) bool {
	registry.mutex.Lock()
	defer registry.mutex.Unlock()

	instance, exists := registry.instances[name]
	if !exists || instance.Source != source {
		return false
	}
	// Arming state and claim rejections die with the instance: a withdrawn
	// cassette must not stale-arm a later re-admission, and its problems
	// are no longer anyone's to fix.
	for _, claim := range ManifestClaims(instance.Name, instance.Manifest) {
		delete(registry.armed, claimKey(claim))
		registry.clearRejection(claimSubject(claim))
	}
	delete(registry.instances, name)
	for index, ordered := range registry.order {
		if ordered == name {
			registry.order = append(registry.order[:index], registry.order[index+1:]...)

			break
		}
	}

	return true
}

// check validates an instance as a routable proxy target. It runs under the
// write lock and mutates nothing.
func (registry *Registry) check(instance *Instance) error {
	problems := make([]cassette.Problem, 0)
	add := func(field, message string) {
		problems = append(problems, cassette.Problem{Field: field, Message: message})
	}

	if _, err := cassette.ParseName(string(instance.Name)); err != nil {
		add("name", err.Error())
	}
	if message := validateURL(instance.URL); message != "" {
		add("url", message)
	}

	if len(problems) > 0 {
		subject := "cassette " + string(instance.Name)
		if instance.Name == "" {
			subject = "cassette " + instance.Source
		}

		return &cassette.ValidationError{Subject: subject, Problems: problems}
	}

	return nil
}

// SetRejection records the current problem for a subject without accumulating
// duplicates across retries.
func (registry *Registry) SetRejection(subject string, err error) {
	if err == nil {
		return
	}

	registry.mutex.Lock()
	defer registry.mutex.Unlock()

	registry.setRejection(subject, err)
}

// setRejection is SetRejection under an already-held write lock.
func (registry *Registry) setRejection(subject string, err error) {
	if err == nil {
		return
	}
	for index := range registry.rejections {
		if registry.rejections[index].Subject == subject {
			registry.rejections[index].Reason = err.Error()

			return
		}
	}
	registry.rejections = append(registry.rejections, Rejection{Subject: subject, Reason: err.Error()})
}

// ClearRejection removes a resolved subject's current problem.
func (registry *Registry) ClearRejection(subject string) {
	registry.mutex.Lock()
	defer registry.mutex.Unlock()

	registry.clearRejection(subject)
}

// clearRejection is ClearRejection under an already-held write lock.
func (registry *Registry) clearRejection(subject string) {
	for index := range registry.rejections {
		if registry.rejections[index].Subject == subject {
			registry.rejections = append(registry.rejections[:index], registry.rejections[index+1:]...)

			return
		}
	}
}

// Get returns a registered cassette by name.
func (registry *Registry) Get(name cassette.Name) (*Instance, bool) {
	registry.mutex.RLock()
	defer registry.mutex.RUnlock()

	instance, ok := registry.instances[name]

	return instance, ok
}

// Instances returns the registered cassettes in name order, so that discovery
// documents and CLI output are stable between calls.
func (registry *Registry) Instances() []*Instance {
	registry.mutex.RLock()
	defer registry.mutex.RUnlock()

	instances := make([]*Instance, 0, len(registry.instances))
	for _, name := range registry.order {
		instances = append(instances, registry.instances[name])
	}
	sort.Slice(instances, func(i, j int) bool { return instances[i].Name < instances[j].Name })

	return instances
}

// Rejections returns the cassettes core refused, in the order they failed. It
// is never nil: an empty list on the wire means "nothing failed", while a null
// makes a client guess whether the field is unsupported.
func (registry *Registry) Rejections() []Rejection {
	registry.mutex.RLock()
	defer registry.mutex.RUnlock()

	return append(make([]Rejection, 0, len(registry.rejections)), registry.rejections...)
}

// routes returns the proxy table in cassette name order.
func (registry *Registry) routes() []route {
	instances := registry.Instances()
	table := make([]route, 0, len(instances))
	for _, instance := range instances {
		table = append(table, route{prefix: instance.Prefix(), name: instance.Name})
	}

	return table
}

// Lookup resolves a canonical public path to a cassette and the path to
// forward on that cassette's own listener.
//
// path is in its escaped (wire) form and stays that way: the forwarded path
// feeds RewriteProxyRequest, which needs the client's own bytes to preserve
// them hop to hop. Prefixes are plain ASCII with nothing to escape, so
// matching on the escaped path is equivalent to matching on the decoded one.
func (registry *Registry) Lookup(path string) (*Instance, string, bool) {
	for _, entry := range registry.routes() {
		if !segmentPrefix(entry.prefix, path) {
			continue
		}
		instance, ok := registry.Get(entry.name)
		if !ok {
			continue
		}

		return instance, instance.Local(path), true
	}

	return nil, "", false
}

// segmentPrefix reports whether path lies within prefix's subtree, comparing
// whole path segments. String prefixing alone would make /api/sum shadow
// /api/summary, which is a real cassette name away from being a live bug.
func segmentPrefix(prefix, path string) bool {
	return path == prefix || strings.HasPrefix(path, strings.TrimSuffix(prefix, "/")+"/")
}

// validateURL reports why an endpoint is unusable as a proxy target, or the
// empty string when it is fine.
func validateURL(endpoint string) string {
	if endpoint == "" {
		return "is required"
	}
	parsed, err := url.Parse(endpoint)
	if err != nil {
		return "must be a valid URL"
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return "must use the http or https scheme"
	}
	if parsed.Hostname() == "" {
		return "must include a host"
	}
	if parsed.RawQuery != "" || parsed.Fragment != "" {
		return "must not carry a query or fragment"
	}

	return ""
}
