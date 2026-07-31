package manifest

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/BurntSushi/toml"

	"github.com/papercomputeco/tapes/pkg/cassette"
)

// File is the conventional name of a cassette's manifest on disk.
const File = "cassette.toml"

// ParseTOML decodes the manifest a cassette publishes for its orchestrator.
//
// This is the same document as the one embedded in the OpenAPI spec, in the
// encoding a human writes and a deployment tool reads. The two exist for two
// different readers at two different times: an orchestrator has to decide
// whether it can run a cassette at all — which image, which port, which
// contract of tapes, what to provision in Postgres — and it has to decide that
// before anything is listening for core to fetch a spec from.
//
// Decoding transcodes to JSON and hands off to Parse rather than decoding into
// the schema structs directly. That is deliberate. A second set of struct tags
// would be a second definition of the same document, free to drift from the
// first, and it would need its own answers on unknown fields, duplicate keys,
// and number width. Going through JSON means TOML is an encoding of this
// contract and never a dialect of it: the same kind dispatch, the same strict
// parser, the same validation, and one place to change when a field is added.
func ParseTOML(data []byte) (cassette.Manifest, error) {
	// Decoding into a map rather than a struct is what keeps this transcoding
	// and not parsing: unknown keys survive to be refused by the strict JSON
	// parser downstream, where every other manifest is refused too. Duplicate
	// keys never get that far — TOML rejects them itself, as JSON does.
	var raw map[string]any
	if _, err := toml.Decode(string(data), &raw); err != nil {
		return nil, fmt.Errorf("decode cassette manifest TOML: %w", err)
	}

	encoded, err := json.Marshal(raw)
	if err != nil {
		return nil, fmt.Errorf("re-encode cassette manifest: %w", err)
	}

	return Parse(encoded)
}

// Load reads and parses a cassette manifest from disk.
//
// The path is named in full rather than assembled from a directory and File,
// because an orchestrator that keeps manifests in a catalog does not store them
// under their conventional name and should not have to pretend it does.
func Load(path string) (cassette.Manifest, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read cassette manifest: %w", err)
	}

	parsed, err := ParseTOML(data)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}

	return parsed, nil
}
