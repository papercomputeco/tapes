// Package cassette defines schema-independent cassette contracts shared by
// versioned cassette manifest packages.
package cassette

// Manifest is the common behavior exposed by every supported cassette manifest
// schema. Version-specific wire types and parsers live in child packages such
// as v1alpha1.
type Manifest interface {
	// SchemaKind returns the manifest's kind string (i.e., "v1alpha1", "v1").
	SchemaKind() string

	// CassetteName returns the cassette's validated identity.
	CassetteName() Name

	// Anchors returns the paths the cassette serves on its own listener.
	Anchors() Anchors

	// Validate checks the manifest's intrinsic constraints, and checks its
	// declared contract dependency against the set the caller says it serves.
	// The supported set is a parameter rather than a package constant because
	// this package describes what a cassette declares about itself; which
	// contracts are actually served is a fact about a running core, and a
	// cassette must never have to know which core will mount it.
	Validate(supported []ContractVersion) error

	// MarshalCanonical returns the manifest's deterministic representation.
	MarshalCanonical() ([]byte, error)

	// Digest returns the identity of the canonical representation.
	Digest() (Digest, error)

	// GrantPlan returns the database access desired by deployment tooling.
	GrantPlan() GrantPlan
}

// Version identifies a cassette release.
type Version string

// ContractVersion names a major revision of the tapes read surface — the
// tapes_<version> schema and the /v1 API — that a cassette declares it reads.
//
// It is deliberately major-only ("v1", "v2"), not a full release version: a
// cassette depends on the shape of the contract, not on the build of core
// serving it. The set of contracts any particular core serves is owned by that
// core and injected into Validate; nothing in this package names one.
type ContractVersion string

// Digest is a content digest in "sha256:<hex>" form.
type Digest string

// Anchors are the paths a cassette serves on its own listener, lifted out of
// whichever manifest schema declared them.
//
// They exist as a schema-independent type so that everything downstream of
// admission — registration, proxying, republication — works the same way for
// every manifest version. Without it each consumer would type-assert its way
// back to a concrete schema, which is both a panic waiting to happen and a
// reason every new schema version would ripple outward.
type Anchors struct {
	// Health is the cassette's liveness path.
	Health string

	// OpenAPI is where the cassette publishes its own document.
	OpenAPI string

	// Prefix is the normalized head the cassette mounts its API beneath,
	// empty when it mounts directly under its own name.
	Prefix string
}

// GrantPlan is the schema-independent database state desired by a cassette.
// Tapes publishes this declaration but does not apply it; the deployment system
// that starts the cassette owns roles, credentials, grants, and schema setup.
type GrantPlan struct {
	Role      string   `json:"role"`
	Schema    string   `json:"schema"`
	OwnSchema bool     `json:"own_schema"`
	Selects   []string `json:"selects,omitempty"`
	Tables    []string `json:"tables,omitempty"`
}
