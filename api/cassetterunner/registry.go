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
}

// NewRegistry returns an empty cassette registry.
func NewRegistry() *Registry {
	return &Registry{
		instances:  make(map[cassette.Name]*Instance),
		rejections: make([]Rejection, 0),
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
	if _, exists := registry.instances[instance.Name]; !exists {
		registry.order = append(registry.order, instance.Name)
	}
	registry.instances[instance.Name] = instance

	return nil
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
	if parsed.Host == "" {
		return "must include a host"
	}
	if parsed.RawQuery != "" || parsed.Fragment != "" {
		return "must not carry a query or fragment"
	}

	return ""
}
