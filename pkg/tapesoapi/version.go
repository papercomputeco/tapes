package tapesoapi

import (
	"errors"
	"fmt"
)

// Version is an OpenAPI specification version this package can render.
//
// The internal model is version-neutral: it stores the union of what 3.0 and
// 3.1 can express, and the version decision happens once, at render time. That
// is what keeps 3.1 support additive rather than a second parser.
type Version string

const (
	// V30 renders OpenAPI 3.0.3. It is the default because it is what the
	// published tapes contracts are consumed as — progenitor, which generates
	// paper's Rust client, is 3.0.x-only.
	V30 Version = "3.0.3"

	// V31 renders OpenAPI 3.1.0.
	V31 Version = "3.1.0"
)

// String returns the version string written to the document's `openapi` field.
func (v Version) String() string { return string(v) }

// Valid reports whether v is a version this package renders.
func (v Version) Valid() bool { return v == V30 || v == V31 }

// ParseVersion maps a document's `openapi` field onto a render target.
//
// Patch releases of a minor version are all rendered the same way — 3.0.0 and
// 3.0.3 differ in wording, not in what a document may contain — so the whole
// 3.0.x line maps to V30 and the whole 3.1.x line to V31.
func ParseVersion(declared string) (Version, error) {
	switch {
	case declared == "":
		return "", errors.New("document is missing the openapi version field")
	case len(declared) >= 4 && declared[:4] == "3.0.":
		return V30, nil
	case declared == "3.0":
		return V30, nil
	case len(declared) >= 4 && declared[:4] == "3.1.":
		return V31, nil
	case declared == "3.1":
		return V31, nil
	case len(declared) >= 2 && declared[:2] == "2.":
		return "", fmt.Errorf("openapi %s is Swagger 2.0, which this package does not read; convert it to 3.0 first", declared)
	default:
		return "", fmt.Errorf("unsupported openapi version %q (supported: 3.0.x, 3.1.x)", declared)
	}
}
