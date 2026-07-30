// Package v30 holds the OpenAPI 3.0 reference documents this module is tested
// against.
//
// They are a package rather than loose files so a test can reach them through
// an embedded FS instead of a relative path — which means the same fixtures are
// usable from any package in the module, and adding one is a file rather than a
// file plus a path someone has to keep correct.
//
// Each fixture exists to pin one specific behaviour, and each says which in its
// own header comment. A fixture that no test explains is a fixture nobody dares
// change.
package v30

import "embed"

// FS holds every 3.0 reference document, under `fixtures/`.
//
//go:embed fixtures
var FS embed.FS

// The fixtures, by path, so a test names a constant rather than a string that
// can drift from the file it points at.
const (
	// Petstore is a minimal, complete document: the baseline round trip.
	Petstore = "fixtures/petstore.yaml"

	// NullableAndBounds carries 3.0's spellings of nullability and exclusive
	// bounds. Its twin lives in the v3.1 fixtures.
	NullableAndBounds = "fixtures/nullable-and-bounds.yaml"

	// ComponentsAndRefs references every component section, so a namespacing
	// pass that handles only schemas is caught.
	ComponentsAndRefs = "fixtures/components-and-refs.yaml"

	// VendorExtensions carries `x-` keys at every level, including a reference
	// buried inside one.
	VendorExtensions = "fixtures/vendor-extensions.yaml"

	// DiscriminatedUnion carries a oneOf discriminator whose mapping values are
	// references.
	DiscriminatedUnion = "fixtures/discriminated-union.yaml"

	// ConflictLeft and ConflictRight collide on a path and a component, and
	// agree on one other component.
	ConflictLeft  = "fixtures/conflict-left.yaml"
	ConflictRight = "fixtures/conflict-right.yaml"
)

// Read returns one fixture's bytes. It panics rather than returning an error:
// the argument is a constant in this package and the file is embedded in the
// binary, so a failure is a build-time mistake, not a runtime condition.
func Read(name string) []byte {
	data, err := FS.ReadFile(name)
	if err != nil {
		panic("tapesoapi/v3.0: " + err.Error())
	}

	return data
}
