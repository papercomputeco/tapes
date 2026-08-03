package headers

// Coverage gate for the envelope fixture corpus, from extproc's side.
//
// envelope_fixtures_test.go is the *parser oracle*: it proves this package's
// ParseSessionEnvelope matches what the corpus declares. That is only half of
// the parity property. The corpus is a contract between independent
// implementations, and it holds them together through three links:
//
//  1. each implementation conforms to its copy of the corpus,
//  2. every copy of the corpus is the same bytes, and
//  3. the corpus still covers the rules the contract claims to pin.
//
// Link 1 is the oracle's. This file is 3.
//
// Link 2 used to be here too, as a recomputation of DIGEST over a vendored
// copy of the cases. That check existed because the corpus lived in another
// repository: the sync script's --check mode needed a tapes checkout, which
// extproc's CI never had, so a hand-edited case would have left this parser
// and the tapes reader testing against different bytes while both stayed
// green. There is no second copy to drift now — both read fixtures/envelope —
// and pkg/backfill/envelope_corpus_test.go recomputes the seal over those
// exact bytes in this same test run. Recomputing it a second time here would
// assert that a file matches itself. The seal still matters, and still guards
// the copies that remain outside this module: paper's producer-side vendor
// under crates/paper-daemon/vendor/ is sealed by the same DIGEST.
//
// Link 3 is scoped deliberately. The rules asserted below are the ones this
// parser implements: the decode transform and the metadata alphabet. Producer
// concerns (the session-name byte cap, the header budget) and validation
// outcomes (reject-400) are not extproc's surface — this package's oracle
// deliberately does not assert them, and the backfill-side gate covers them
// where they belong. Asserting them here would be theatre.

import (
	"encoding/json"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"testing"
	"unicode/utf8"
)

// corpusDir locates fixtures/envelope relative to this file, so the tests do
// not depend on the working directory `go test` was invoked from.
func corpusDir() string {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		panic("runtime.Caller failed")
	}
	return filepath.Join(filepath.Dir(file), "..", "..", "fixtures", "envelope")
}

// corpus case schema, decoded strictly so a typo'd field name is caught
// rather than silently ignored.
type corpusCaseFull struct {
	file        string
	Name        string            `json:"name"`
	Category    string            `json:"category"`
	Harness     string            `json:"harness"`
	Description string            `json:"description"`
	Direction   string            `json:"direction"`
	Headers     map[string]string `json:"headers"`
	Envelope    fixtureEnvelope   `json:"envelope"`
	EncodeFrom  *fixtureEnvelope  `json:"encode_from"`
	Error       *fixtureError     `json:"error"`
	Grounding   string            `json:"grounding"`
	Notes       string            `json:"notes"`
}

func loadCorpusCasesFull(t *testing.T) []corpusCaseFull {
	t.Helper()

	matches, err := filepath.Glob(filepath.Join(corpusDir(), "cases", "*.json"))
	if err != nil {
		t.Fatalf("glob corpus cases: %v", err)
	}
	sort.Strings(matches)

	out := make([]corpusCaseFull, 0, len(matches))
	for _, m := range matches {
		b, err := os.ReadFile(m)
		if err != nil {
			t.Fatalf("read %s: %v", m, err)
		}
		var c corpusCaseFull
		dec := json.NewDecoder(strings.NewReader(string(b)))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&c); err != nil {
			t.Fatalf("%s: %v", filepath.Base(m), err)
		}
		c.file = filepath.Base(m)
		out = append(out, c)
	}
	return out
}

// TestCorpusSchema checks the corpus cases are structurally what the
// oracle assumes. A truncated or malformed case would otherwise make the
// oracle assert less than it appears to, quietly.
func TestCorpusSchema(t *testing.T) {
	categories := map[string]bool{"valid": true, "percent-encoding": true, "budget": true, "unknown": true, "error": true}
	harnesses := map[string]bool{"claude": true, "codex": true, "pi": true, "unknown": true}
	directions := map[string]bool{"roundtrip": true, "decode": true, "encode": true}
	dispositions := map[string]bool{"reject-400": true, "drop-field": true}

	seen := map[string]string{}
	for _, c := range loadCorpusCasesFull(t) {
		if c.Name+".json" != c.file {
			t.Errorf("%s: name %q must match the filename; knownDeviations keys off the name",
				c.file, c.Name)
		}
		if prev, dup := seen[c.Name]; dup {
			t.Errorf("%s: duplicate case name %q (also %s)", c.file, c.Name, prev)
		}
		seen[c.Name] = c.file

		if !categories[c.Category] {
			t.Errorf("%s: unknown category %q", c.file, c.Category)
		}
		if !harnesses[c.Harness] {
			t.Errorf("%s: unknown harness %q", c.file, c.Harness)
		}
		if !directions[c.Direction] {
			t.Errorf("%s: unknown direction %q", c.file, c.Direction)
		}
		if c.Grounding == "" {
			t.Errorf("%s: grounding is required", c.file)
		}
		if len(c.Headers) == 0 {
			t.Errorf("%s: headers is required", c.file)
		}
		for k := range c.Headers {
			if k != strings.ToLower(k) {
				t.Errorf("%s: header %q must be lower-cased", c.file, k)
			}
		}
		if c.Direction == "encode" && c.EncodeFrom == nil {
			t.Errorf("%s: direction=encode is the lossy direction and must carry encode_from", c.file)
		}
		if c.Direction != "encode" && c.EncodeFrom != nil {
			t.Errorf("%s: direction=%s round-trips, so encode_from would duplicate envelope", c.file, c.Direction)
		}
		if c.Error != nil && !dispositions[c.Error.Disposition] {
			t.Errorf("%s: unknown error disposition %q", c.file, c.Error.Disposition)
		}
	}
}

// decodedHeader mirrors the decode step closely enough to classify a case.
// Written against the stdlib rather than calling decodeEnvelopeHeaderValue:
// asking the parser under test whether the corpus covers the parser would be
// circular.
func decodedHeader(raw string) (string, bool) {
	if decoded, err := url.PathUnescape(raw); err == nil {
		return decoded, false
	}
	return raw, true
}

func containsControlByte(s string) bool {
	for _, r := range s {
		if r < 0x20 || r == 0x7F {
			return true
		}
	}
	return false
}

// TestCorpusCoverage fails if the corpus stops exercising a decoder
// rule this parser implements.
//
// Stated as properties rather than case names so the assertions survive
// renames. Deleting the only case that covers a rule is otherwise invisible:
// every remaining case still passes and the contract quietly shrinks, which is
// how two parsers drift apart without anything going red.
func TestCorpusCoverage(t *testing.T) {
	cases := loadCorpusCasesFull(t)

	rules := []struct {
		what  string
		why   string
		holds func(corpusCaseFull) bool
	}{
		{
			what: "percent-decoding of non-ASCII",
			why:  "the stored value is the logical value; storing the escaped form would force a decoder into every consumer",
			holds: func(c corpusCaseFull) bool {
				for _, f := range []string{c.Envelope.Cwd, c.Envelope.Name} {
					if utf8.RuneCountInString(f) != len(f) {
						return true
					}
				}
				return false
			},
		},
		{
			what: "a literal '+' surviving the decode",
			why:  "PathUnescape keeps '+'; QueryUnescape turns it into a space, so 'go+rust' would silently become 'go rust'",
			holds: func(c corpusCaseFull) bool {
				for h, raw := range c.Headers {
					if !strings.Contains(raw, "+") {
						continue
					}
					switch h {
					case TapesCwd:
						return strings.Contains(c.Envelope.Cwd, "+")
					case TapesSessionName:
						return strings.Contains(c.Envelope.Name, "+")
					}
				}
				return false
			},
		},
		{
			what: "a malformed percent-encoding falling back to the raw value",
			why:  "decoding failure is non-fatal, so the row still records something recognisable",
			holds: func(c corpusCaseFull) bool {
				for h, raw := range c.Headers {
					if _, malformed := decodedHeader(raw); !malformed {
						continue
					}
					switch h {
					case TapesCwd:
						return c.Envelope.Cwd == raw
					case TapesSessionName:
						return c.Envelope.Name == raw
					}
				}
				return false
			},
		},
		{
			what: "the control-byte guard on cwd",
			why:  "escaping stops a control byte forging a header on the wire; the guard stops it reaching storage",
			holds: func(c corpusCaseFull) bool {
				raw, ok := c.Headers[TapesCwd]
				if !ok {
					return false
				}
				decoded, _ := decodedHeader(raw)
				return containsControlByte(decoded) && c.Envelope.Cwd == ""
			},
		},
		{
			what: "the control-byte guard on session-name",
			why:  "the guard is a property of the decoder, not of one field — and session names are user-supplied free text, the likelier injection vector",
			holds: func(c corpusCaseFull) bool {
				raw, ok := c.Headers[TapesSessionName]
				if !ok {
					return false
				}
				decoded, _ := decodedHeader(raw)
				return containsControlByte(decoded) && c.Envelope.Name == ""
			},
		},
		{
			what: "the metadata alphabet being part of the contract",
			why:  "a strict decoder and a permissive one produce different rows from identical bytes; this repo's deviation for that is only meaningful while a case exercises it",
			holds: func(c corpusCaseFull) bool {
				raw := c.Headers[TapesHarnessMetadata]
				return strings.Contains(raw, "=") && len(c.Envelope.HarnessMetadata) == 0
			},
		},
		{
			what: "metadata dropped as a non-fatal malformed field",
			why:  "unparseable metadata must not cost the rest of the envelope",
			holds: func(c corpusCaseFull) bool {
				return c.Error != nil && c.Error.Field == "harness_metadata" && c.Error.Disposition == "drop-field"
			},
		},
	}

	for _, r := range rules {
		covered := false
		for _, c := range cases {
			if r.holds(c) {
				covered = true
				break
			}
		}
		if !covered {
			t.Errorf("no vendored case covers: %s\n"+
				"  why it matters: %s\n\n"+
				"  Either the case was deleted upstream or this copy is stale. Re-sync, and if\n"+
				"  it really is gone upstream, add a case there rather than dropping the rule.",
				r.what, r.why)
		}
	}
}
