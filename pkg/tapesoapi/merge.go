package tapesoapi

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

// ConflictPolicy decides what happens when two fragments contribute the same
// key.
type ConflictPolicy int

// The available conflict policies.
const (
	// PolicyError collects every conflict and fails the compile. It is the
	// default: an aggregate whose contents depend on which document loaded
	// first is worse than one that refuses to build.
	PolicyError ConflictPolicy = iota

	// PolicyFirstWins keeps the earlier contribution in merge order.
	PolicyFirstWins

	// PolicyLastWins keeps the later contribution in merge order.
	PolicyLastWins
)

// String names the policy for error messages.
func (p ConflictPolicy) String() string {
	switch p {
	case PolicyFirstWins:
		return "first-wins"
	case PolicyLastWins:
		return "last-wins"
	case PolicyError:
		return "error"
	default:
		return fmt.Sprintf("policy(%d)", int(p))
	}
}

// Conflict is one key contributed by more than one fragment.
type Conflict struct {
	// Kind is what collided: "path", "component", or "info".
	Kind string

	// Key names the collision — "GET /users/{id}", "schemas/User".
	Key string

	// Sources are every contributor, in merge order.
	Sources []Provenance
}

func (c Conflict) String() string {
	names := make([]string, 0, len(c.Sources))
	for _, source := range c.Sources {
		names = append(names, source.String())
	}

	return fmt.Sprintf("%s %s defined by %s", c.Kind, c.Key, strings.Join(names, " and "))
}

// ConflictError reports every collision at once.
//
// Collect-all rather than fail-fast is deliberate. Someone aggregating a fleet
// of documents wants the whole list so they can fix it in one pass; failing on
// the first conflict turns that into one recompile per collision.
type ConflictError struct {
	Conflicts []Conflict
}

func (e *ConflictError) Error() string {
	if len(e.Conflicts) == 1 {
		return "openapi merge conflict: " + e.Conflicts[0].String()
	}
	var b strings.Builder
	fmt.Fprintf(&b, "%d openapi merge conflicts:", len(e.Conflicts))
	for _, conflict := range e.Conflicts {
		fmt.Fprintf(&b, "\n  - %s", conflict.String())
	}

	return b.String()
}

// merged is the accumulated document, before rendering.
type merged struct {
	info       *Info
	infoSource Provenance
	servers    []Server
	tags       []Tag
	security   []SecurityRequirement
	paths      map[string]*PathItem
	webhooks   map[string]*PathItem
	components *Components
	extensions map[string]any

	// versions records the versions the inputs were authored against, so a
	// compile can refuse to downgrade rather than silently drop what it cannot
	// express.
	versions map[Version][]Provenance

	// componentOwners records which fragment contributed each component and
	// what it hashed to, which is what lets a second identical contribution
	// dedupe silently while a differing one names both sides.
	componentOwners map[string]componentOwner

	conflicts []Conflict
	warnings  []string
	policy    ConflictPolicy
}

func newMerged(policy ConflictPolicy) *merged {
	return &merged{
		paths:      map[string]*PathItem{},
		webhooks:   map[string]*PathItem{},
		components: &Components{},
		versions:   map[Version][]Provenance{},
		policy:     policy,
	}
}

// mergeFragments folds fragments into one document.
//
// Fragments are merged in provenance order rather than insertion order, so the
// result does not depend on which goroutine registered a route first. That is
// the property that makes compiled output byte-stable and therefore diffable.
//
// Provenance alone is not quite a total order — nothing stops two contributions
// from carrying the same one — so content breaks the tie. Without that, two
// fragments with identical provenance would merge in insertion order, and the
// order-independence guarantee would hold everywhere except the one case a
// caller cannot see coming.
func mergeFragments(fragments []Fragment, policy ConflictPolicy) (*merged, error) {
	// The sort keys are computed up front rather than inside the comparator,
	// because sorting permutes the slice: a comparator that derived or cached
	// anything by index would be reading a different fragment each time.
	type ordered struct {
		key      string
		fragment Fragment
	}
	sortable := make([]ordered, len(fragments))
	for index, fragment := range fragments {
		sortable[index] = ordered{
			key:      fragment.Provenance.sortKey() + "\x00" + structuralHash(fragment),
			fragment: fragment,
		}
	}
	sort.Slice(sortable, func(i, j int) bool { return sortable[i].key < sortable[j].key })

	out := newMerged(policy)
	for _, entry := range sortable {
		out.absorb(entry.fragment)
	}
	out.finalize()

	if len(out.conflicts) > 0 {
		return nil, &ConflictError{Conflicts: out.conflicts}
	}

	return out, nil
}

func (m *merged) absorb(fragment Fragment) {
	if fragment.Version != "" {
		m.versions[fragment.Version] = append(m.versions[fragment.Version], fragment.Provenance)
	}
	m.absorbInfo(fragment)
	m.servers = append(m.servers, fragment.Servers...)
	m.tags = append(m.tags, fragment.Tags...)
	m.security = append(m.security, fragment.Security...)
	for key, value := range fragment.Extensions {
		if m.extensions == nil {
			m.extensions = map[string]any{}
		}
		m.extensions[key] = value
	}
	m.absorbPaths(fragment.Paths, m.paths, "path")
	m.absorbPaths(fragment.Webhooks, m.webhooks, "webhook")
	m.absorbComponents(fragment)
}

func (m *merged) absorbInfo(fragment Fragment) {
	if fragment.Info == nil {
		return
	}
	switch {
	case m.info == nil:
		m.info = fragment.Info
		m.infoSource = fragment.Provenance
	case fragment.Authoritative:
		m.info = fragment.Info
		m.infoSource = fragment.Provenance
	default:
		// Two documents describing themselves is not an error when they agree;
		// it is only a conflict when a reader would have to pick.
		if infoEqual(m.info, fragment.Info) {
			return
		}
		m.conflict("info", "info", m.infoSource, fragment.Provenance)
	}
}

func infoEqual(left, right *Info) bool {
	return structuralHash(left) == structuralHash(right)
}

func (m *merged) absorbPaths(in, into map[string]*PathItem, kind string) {
	for _, path := range sortedKeys(in) {
		item := in[path]
		existing, ok := into[path]
		if !ok {
			into[path] = item

			continue
		}
		// Path-level metadata merges; operations do not. Two fragments each
		// contributing a different method to one path is the normal case — a
		// Fiber router registers GET and DELETE from separate call sites.
		mergePathMetadata(existing, item)
		for _, method := range item.Methods() {
			incoming := item.Operations[method]
			current, collides := existing.Operations[method]
			if !collides {
				existing.Operations[method] = incoming

				continue
			}
			// Whole-operation resolution only. Field-merging two different
			// definitions of one operation produces a description nobody wrote
			// and nobody can debug back to a source.
			key := method + " " + path
			if structuralHash(current) == structuralHash(incoming) {
				continue
			}
			switch m.policy {
			case PolicyFirstWins:
				m.warn(kind, key, current.provenance, incoming.provenance, "kept the first")
			case PolicyLastWins:
				existing.Operations[method] = incoming
				m.warn(kind, key, current.provenance, incoming.provenance, "kept the last")
			case PolicyError:
				m.conflict(kind, key, current.provenance, incoming.provenance)
			default:
				// A policy this package does not recognize fails rather than
				// picks: a winner nobody chose is the outcome nobody can debug.
				m.conflict(kind, key, current.provenance, incoming.provenance)
			}
		}
	}
}

func mergePathMetadata(into, from *PathItem) {
	if into.Summary == "" {
		into.Summary = from.Summary
	}
	if into.Description == "" {
		into.Description = from.Description
	}
	if len(into.Servers) == 0 {
		into.Servers = from.Servers
	}
	into.Parameters = append(into.Parameters, from.Parameters...)
	for key, value := range from.Extensions {
		if into.Extensions == nil {
			into.Extensions = map[string]any{}
		}
		if _, present := into.Extensions[key]; !present {
			into.Extensions[key] = value
		}
	}
	if into.Operations == nil {
		into.Operations = map[string]*Operation{}
	}
}

func (m *merged) absorbComponents(fragment Fragment) {
	if fragment.Components == nil {
		return
	}
	source := fragment.Provenance
	mergeComponentMap(m, "schemas", fragment.Components.Schemas, &m.components.Schemas, source)
	mergeComponentMap(m, "responses", fragment.Components.Responses, &m.components.Responses, source)
	mergeComponentMap(m, "parameters", fragment.Components.Parameters, &m.components.Parameters, source)
	mergeComponentMap(m, "requestBodies", fragment.Components.RequestBodies, &m.components.RequestBodies, source)
	mergeComponentMap(m, "headers", fragment.Components.Headers, &m.components.Headers, source)
	mergeComponentMap(m, "examples", fragment.Components.Examples, &m.components.Examples, source)
	mergeComponentMap(m, "securitySchemes", fragment.Components.SecuritySchemes, &m.components.SecuritySchemes, source)
}

// componentOwners tracks which fragment contributed each component, so a
// conflict names both sides.
type componentOwner struct {
	provenance Provenance
	hash       string
}

// mergeComponentMap folds one component section, deduping structurally
// identical contributions.
//
// The structural-equality check first is what makes a shared type usable: two
// packages that both register an identical `User` produce one component, not a
// conflict. Only genuinely different definitions under one name are a problem.
func mergeComponentMap[V any](m *merged, section string, in map[string]V, into *map[string]V, source Provenance) {
	if len(in) == 0 {
		return
	}
	if *into == nil {
		*into = make(map[string]V, len(in))
	}
	if m.componentOwners == nil {
		m.componentOwners = map[string]componentOwner{}
	}
	for _, name := range sortedKeys(in) {
		value := in[name]
		key := section + "/" + name
		hash := structuralHash(value)
		owner, taken := m.componentOwners[key]
		if !taken {
			(*into)[name] = value
			m.componentOwners[key] = componentOwner{provenance: source, hash: hash}

			continue
		}
		if owner.hash == hash {
			continue
		}
		switch m.policy {
		case PolicyFirstWins:
			m.warn("component", key, owner.provenance, source, "kept the first")
		case PolicyLastWins:
			(*into)[name] = value
			m.componentOwners[key] = componentOwner{provenance: source, hash: hash}
			m.warn("component", key, owner.provenance, source, "kept the last")
		case PolicyError:
			m.conflict("component", key, owner.provenance, source)
		default:
			// See the operation conflict above: an unrecognized policy fails.
			m.conflict("component", key, owner.provenance, source)
		}
	}
}

func (m *merged) conflict(kind, key string, sources ...Provenance) {
	m.conflicts = append(m.conflicts, Conflict{Kind: kind, Key: key, Sources: sources})
}

func (m *merged) warn(kind, key string, first, second Provenance, resolution string) {
	m.warnings = append(m.warnings,
		fmt.Sprintf("%s %s defined by %s and %s; %s", kind, key, first, second, resolution))
}

// finalize collapses the set-union fields into stable, deduplicated order.
func (m *merged) finalize() {
	m.servers = dedupeServers(m.servers)
	m.tags = dedupeTags(m.tags)
	m.security = dedupeSecurity(m.security)
	sort.Strings(m.warnings)
}

func dedupeServers(in []Server) []Server {
	seen := map[string]struct{}{}
	out := make([]Server, 0, len(in))
	for _, server := range in {
		if _, ok := seen[server.URL]; ok {
			continue
		}
		seen[server.URL] = struct{}{}
		out = append(out, server)
	}

	return out
}

// dedupeTags keeps the first description offered for a tag and sorts by name,
// so the tag list does not reorder when an unrelated document is added.
func dedupeTags(in []Tag) []Tag {
	seen := map[string]int{}
	out := make([]Tag, 0, len(in))
	for _, tag := range in {
		if index, ok := seen[tag.Name]; ok {
			if out[index].Description == "" {
				out[index].Description = tag.Description
			}

			continue
		}
		seen[tag.Name] = len(out)
		out = append(out, tag)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })

	return out
}

func dedupeSecurity(in []SecurityRequirement) []SecurityRequirement {
	seen := map[string]struct{}{}
	out := make([]SecurityRequirement, 0, len(in))
	for _, requirement := range in {
		key := structuralHash(requirement)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, requirement)
	}

	return out
}

// structuralHash is content identity for dedupe and conflict detection.
//
// It hashes the JSON encoding, which sorts map keys, so two values that differ
// only in construction order hash the same. Unexported fields — provenance in
// particular — do not encode, which is what lets the same operation arriving
// from two sources be recognised as one thing rather than as a conflict.
func structuralHash(value any) string {
	encoded, err := json.Marshal(value)
	if err != nil {
		// A value that will not encode cannot be compared structurally. Making
		// it hash uniquely means it is treated as different from everything,
		// which surfaces as a conflict rather than as a silent dedupe.
		return "unhashable:" + fmt.Sprintf("%p", value)
	}
	sum := sha256.Sum256(encoded)

	return hex.EncodeToString(sum[:])
}
