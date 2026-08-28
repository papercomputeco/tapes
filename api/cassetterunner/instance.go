package cassetterunner

import (
	"strings"

	"github.com/papercomputeco/tapes/pkg/cassette"
)

// PublicNamespace is the path every cassette core serves lives beneath.
//
// One namespace for anything cassette-shaped is worth more than a shorter
// path: the same prefix lists the installed cassettes, serves their specs, and
// routes their traffic, so a client that found a cassette in the discovery
// document does not then have to learn that its endpoints live somewhere else.
//
// It lives here rather than in pkg/cassette because it is a fact about the
// routes this API server publishes. A cassette declares its name; where that
// name is mounted is core's decision.
const PublicNamespace = "/v1/cassettes"

// Instance is one cassette core is serving: its identity, its contract, and
// the endpoint traffic is proxied to.
//
// It is the loaded form of a cassette, which is why it lives here rather than
// in pkg/cassette: that package defines what a cassette *is* — the manifest
// schema and the vocabulary manifests are written in — and stays free of
// anything about a particular server's running fleet.
type Instance struct {
	// Name is the cassette's validated identity, which determines its route
	// prefix, its Postgres schema, and its role name.
	Name cassette.Name

	// Manifest is the cassette's parsed declaration.
	Manifest cassette.Manifest

	// Digest identifies the canonical versioned metadata embedded in the
	// cassette's OpenAPI document. The republished document has its own digest.
	Digest cassette.Digest

	// URL is the reverse-proxy target: the origin the cassette's own listener
	// answers on.
	URL string

	// Anchors are the paths on the cassette's own listener, not on the
	// aggregated surface. They are carried here rather than read from the
	// manifest on demand so that everything downstream of admission is
	// schema-independent.
	Anchors cassette.Anchors

	// Source is the exact configured OpenAPI document URL. It is also the
	// stable subject any problem with this cassette is filed under.
	Source string

	// MCPTools are the cassette operations admitted for MCP tool calling.
	// They are replaced with the instance after a successful spec refresh.
	MCPTools []MCPTool
}

// Prefix returns the cassette's canonical route prefix on the public surface.
func (instance *Instance) Prefix() string {
	return PublicNamespace + "/" + string(instance.Name)
}

// LocalPrefix returns the prefix the cassette serves on its own listener.
//
// This is the other half of the mapping core performs, and the only rewriting
// in the pipeline: the cassette declares in its manifest which prefix path it
// mounts itself beneath, and core swaps that head for the public namespace. An
// empty prefix means the cassette mounts directly under its own name.
func (instance *Instance) LocalPrefix() string {
	if instance.Anchors.Prefix == "" {
		return "/" + string(instance.Name)
	}

	return "/" + instance.Anchors.Prefix + "/" + string(instance.Name)
}

// Local maps a path on the public surface onto the cassette's own listener by
// swapping the canonical public prefix for the cassette's local one. Escaped
// paths pass through in their escaped form: both prefixes are plain ASCII, so
// the swap never touches a percent-encoded byte.
func (instance *Instance) Local(path string) string {
	return instance.LocalPrefix() + strings.TrimPrefix(path, instance.Prefix())
}

// Public maps a path on the cassette's own listener onto the public surface.
// It is the inverse of Local, and it is what turns a fetched OpenAPI document
// into one describing paths a client can actually call through core.
func (instance *Instance) Public(path string) string {
	return instance.Prefix() + strings.TrimPrefix(path, instance.LocalPrefix())
}

// Rejection is a cassette core refused to serve, and why.
//
// Rejections are first-class rather than log lines because they are the answer
// to the question an operator actually asks — "why is my cassette not there?" —
// and that question is asked over HTTP, from a machine that cannot read the
// server's stderr.
type Rejection struct {
	// Subject names what was rejected: the configured OpenAPI URL, with any
	// credential redacted.
	Subject string `json:"subject"`

	// Reason is the human-facing explanation. It is never parsed.
	Reason string `json:"reason"`
}

// route is one entry in the proxy table.
type route struct {
	prefix string
	name   cassette.Name
}
