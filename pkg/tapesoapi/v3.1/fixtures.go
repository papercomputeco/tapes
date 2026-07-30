// Package v31 holds the OpenAPI 3.1 reference documents this module is tested
// against.
//
// 3.1 is not a render target yet. These fixtures exist anyway, and are already
// exercised, because the claim that 3.1 support is additive rather than a
// rewrite is only worth anything if it is checked: the ingest side already
// reads 3.1 into the same IR, and the compile side already refuses to downgrade
// what it cannot express. Those are the two halves that would be hard to retrofit.
package v31

import "embed"

// FS holds every 3.1 reference document, under `fixtures/`.
//
//go:embed fixtures
var FS embed.FS

// The fixtures, by path.
const (
	// NullableAndBounds is the 3.1 spelling of the v3.0 fixture of the same
	// name. Both must decode to the same IR.
	NullableAndBounds = "fixtures/nullable-and-bounds.yaml"

	// WebhooksAndConst carries the 3.1-only constructs a 3.0 target cannot
	// express, for the downgrade guard.
	WebhooksAndConst = "fixtures/webhooks-and-const.yaml"
)

// Read returns one fixture's bytes. It panics rather than returning an error:
// the argument is a constant in this package and the file is embedded in the
// binary, so a failure is a build-time mistake, not a runtime condition.
func Read(name string) []byte {
	data, err := FS.ReadFile(name)
	if err != nil {
		panic("tapesoapi/v3.1: " + err.Error())
	}

	return data
}
