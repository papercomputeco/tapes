// Package v1alpha1 defines the versioned cassette metadata embedded in an
// OpenAPI document, including strict parsing, validation, and pure derivations.
package v1alpha1

import "github.com/papercomputeco/tapes/pkg/cassette"

// Kind is the cassette metadata schema version understood by this package.
const (
	Kind     = "cassette/v1alpha1"
	jsonNull = "null"
)

// DefaultPrefixPath is the prefix a cassette mounts its API beneath when its
// metadata does not specify one.
const DefaultPrefixPath = "api"

// SchemaKind returns the schema kind declared by the metadata.
func (m *Manifest) SchemaKind() string {
	if m == nil {
		return ""
	}

	return m.Kind
}

// CassetteName returns the metadata's schema-independent cassette identity.
func (m *Manifest) CassetteName() cassette.Name {
	if m == nil {
		return ""
	}

	return m.Cassette.Name
}

// Anchors returns the metadata's API anchors in schema-independent form. The
// prefix is normalized here, so no caller has to know that the raw field keeps
// whatever the author wrote because the digest is computed over it.
func (m *Manifest) Anchors() cassette.Anchors {
	if m == nil {
		return cassette.Anchors{}
	}

	return cassette.Anchors{
		Health:  m.API.Health,
		OpenAPI: m.API.OpenAPI,
		Prefix:  m.API.PrefixPath(),
	}
}
