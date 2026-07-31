// Package manifest decodes a cassette manifest of any supported schema
// version into the schema-independent cassette.Manifest contract.
//
// It is a package of its own rather than a function on pkg/cassette because
// dispatch has to import every versioned schema package, and those in turn
// import pkg/cassette for the vocabulary they implement.
package manifest

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/papercomputeco/tapes/pkg/cassette"
	"github.com/papercomputeco/tapes/pkg/cassette/v1alpha1"
	"github.com/papercomputeco/tapes/pkg/tapesoapi"
)

// Extension is the OpenAPI root extension a cassette embeds its manifest in.
//
// The manifest travels inside the OpenAPI document rather than beside it so
// that there is one artifact to fetch and one thing to configure — and so a
// spec and the metadata describing it can never be fetched at two different
// versions.
const Extension = "x-tapes-cassette"

// Parse decodes a manifest of any schema version this build understands.
//
// The supported set is closed here, and that is the right place for it: a
// parser knows its own grammar, and this is the one version question core
// answers from its own knowledge. It is not the same question as which tapes
// contract a cassette depends on — that one is answered against the set the
// owning server injects, because it is a fact about a deployment rather than
// about a build.
func Parse(data []byte) (cassette.Manifest, error) {
	var header struct {
		Kind string `json:"kind"`
	}
	if err := json.Unmarshal(data, &header); err != nil {
		return nil, fmt.Errorf("decode cassette metadata header: %w", err)
	}

	switch header.Kind {
	case v1alpha1.Kind:
		return v1alpha1.Parse(data)
	case "":
		return nil, fmt.Errorf("cassette metadata is missing kind (expected %q)", v1alpha1.Kind)
	default:
		return nil, fmt.Errorf("unsupported cassette metadata kind %q (supported: %q)", header.Kind, v1alpha1.Kind)
	}
}

// FromDocument extracts and parses the manifest embedded in an OpenAPI
// document.
//
// present reports whether the document carried the extension at all, which is
// what lets a caller tell "this is not a cassette spec" apart from "this is a
// cassette spec that does not parse" — two situations with different answers.
func FromDocument(document *tapesoapi.Document) (cassette.Manifest, bool, error) {
	if document == nil {
		return nil, false, errors.New("no OpenAPI document")
	}
	metadata, present, err := document.Extension(Extension)
	if err != nil {
		return nil, false, fmt.Errorf("read root extension %q: %w", Extension, err)
	}
	if !present {
		return nil, false, nil
	}
	parsed, err := Parse(metadata)
	if err != nil {
		return nil, true, err
	}

	return parsed, true, nil
}
