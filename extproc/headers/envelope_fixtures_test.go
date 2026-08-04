package headers

// Executable oracle for the shared envelope fixture corpus at
// fixtures/envelope/cases.
//
// The corpus pins the X-Tapes-* header -> session-envelope contract for every
// consumer, in any language. On its own it is inert JSON: nothing loads it, so
// it can silently drift from the parsers it claims to describe. This test is
// the consumer that keeps THIS parser honest — it runs each case's header set
// through the real ParseSessionEnvelope and asserts the envelope the case
// declares.
//
// extproc is the *parser* (decode) side of the contract. The *producer*
// (encode) side is paper's paperd, which vendors the same corpus and table-
// tests its header emitter against it. One corpus, both sides, no drift.
//
// Two things this test deliberately does NOT assert:
//
//   - Validation outcomes. A case's `error.disposition == "reject-400"` is the
//     *ingest* boundary's job; extproc's own doc comment is explicit that
//     "tapes-ingest is the validation surface; this layer just extracts what
//     it can". So a reject-400 case is asserted for its parse result only.
//   - org_id / auth_subject as parser output. They are not X-Tapes-* headers
//     and ParseSessionEnvelope does not read them; they are server-trusted
//     values the gateway sets from validated JWT claims. The test reads them
//     through the same Get() the processor uses so the full envelope is still
//     covered end to end.

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"testing"
)

type fixtureEnvelope struct {
	OrgID                  string          `json:"org_id"`
	AuthSubject            string          `json:"auth_subject"`
	HarnessID              string          `json:"harness_id"`
	HarnessSessionID       string          `json:"harness_session_id"`
	HarnessVersion         string          `json:"harness_version"`
	Cwd                    string          `json:"cwd"`
	Name                   string          `json:"name"`
	ParentHarnessSessionID string          `json:"parent_harness_session_id"`
	HarnessMetadata        json.RawMessage `json:"harness_metadata"`
}

type fixtureError struct {
	Field       string `json:"field"`
	Rule        string `json:"rule"`
	Disposition string `json:"disposition"`
}

type envelopeFixtureCase struct {
	Name        string            `json:"name"`
	Category    string            `json:"category"`
	Harness     string            `json:"harness"`
	Direction   string            `json:"direction"`
	Description string            `json:"description"`
	Headers     map[string]string `json:"headers"`
	Envelope    fixtureEnvelope   `json:"envelope"`
	EncodeFrom  *fixtureEnvelope  `json:"encode_from"`
	Error       *fixtureError     `json:"error"`
}

// deviation records one field where THIS parser deliberately disagrees with
// the corpus's declared envelope: why, and what it produces instead.
//
// alt is what makes an entry more than a suppression. Asserting only "this
// field differs" would let the parser drift to any *other* wrong value and
// stay green, so every entry has to pin the alternative it actually produces.
type deviation struct {
	why string
	// alt reports whether the parse result still matches what this deviation
	// claims this parser produces, and describes the mismatch when it does not.
	alt func(SessionEnvelope) (bool, string)
}

// knownDeviations names every field where THIS parser deliberately disagrees
// with the corpus's declared envelope.
//
// The corpus records the behaviour of the tapes reader, which is not the only
// legitimate parser: where extproc differs on purpose, the difference is
// recorded here rather than papered over. Every entry is asserted twice — the
// alternative must match, AND the deviation must still be real. If upstream
// (or extproc) changes so that a listed field starts agreeing, the test fails
// and tells you to delete the entry. That keeps this table from rotting into a
// list of stale excuses.
//
// Adding an entry here is a contract decision, not a test fix. A genuinely
// unintended divergence should change the parser instead.
//
// Read the two entries below as different kinds of thing. The first is a
// representational difference that reaches the same end state. The second is a
// real difference in what gets stored, and it is recorded here to make it
// visible, not to bless it.
var knownDeviations = map[string]map[string]deviation{
	// cwd used to deviate here: the corpus recorded the tapes reader storing
	// cwd verbatim (still percent-encoded) while extproc decoded it. That is
	// settled — every reader decodes, and the corpus now declares the decoded
	// value — so the entry is gone and the cwd cases are asserted like any
	// other field. The deviation test is what forced the deletion: it fails
	// once a declared deviation stops being real.

	// The corpus records the tapes reader RETAINING any valid-JSON metadata
	// (arrays included) and leaving object-ness to envelope validation, which
	// then rejects with a 400. extproc's SessionEnvelope.HarnessMetadata is
	// typed map[string]any, so a non-object payload cannot be represented at
	// all; the parser drops the field and flags HarnessMetadataMalformed.
	// Same end state for the operator (no metadata is stored), reached one
	// layer earlier.
	"error-metadata-not-object": {
		"harness_metadata": {
			why: "extproc drops non-object metadata at parse time; the corpus records the tapes reader retaining it for validation to reject",
			alt: func(got SessionEnvelope) (bool, string) {
				if got.HarnessMetadata != nil {
					return false, fmt.Sprintf("got %v, want the field dropped", got.HarnessMetadata)
				}
				if !got.HarnessMetadataMalformed {
					return false, "expected HarnessMetadataMalformed to be set when the field is dropped"
				}
				return true, ""
			},
		},
	},

	// A REAL divergence, not a representational one: identical wire bytes
	// produce different stored rows on the two capture paths.
	//
	// The metadata-alphabet deviation used to live here: decodeMetadata
	// accepted four base64 alphabets, so a padded payload was stored through
	// this path and dropped through the tapes reader. Ruled (PCC-1066) in the
	// corpus's favour — the decoder now accepts base64url(no-pad) alone — so
	// the entry is gone and the padded case asserts like any other field.
}

func deviates(caseName, field string) (deviation, bool) {
	d, ok := knownDeviations[caseName][field]
	return d, ok
}

// loadEnvelopeFixtures reads the vendored corpus. Go runs a package's tests
// with the package directory as cwd, so the relative path is stable.
func loadEnvelopeFixtures(t *testing.T) []envelopeFixtureCase {
	t.Helper()

	matches, err := filepath.Glob(filepath.Join(corpusDir(), "cases", "*.json"))
	if err != nil {
		t.Fatalf("glob envelope fixtures: %v", err)
	}
	if len(matches) == 0 {
		t.Fatalf("no envelope fixture cases found under %s", filepath.Join(corpusDir(), "cases"))
	}
	sort.Strings(matches)

	cases := make([]envelopeFixtureCase, 0, len(matches))
	for _, m := range matches {
		b, err := os.ReadFile(m)
		if err != nil {
			t.Fatalf("read %s: %v", m, err)
		}
		var c envelopeFixtureCase
		if err := json.Unmarshal(b, &c); err != nil {
			t.Fatalf("parse %s: %v", m, err)
		}
		if c.Name == "" {
			t.Fatalf("%s: case is missing a name", m)
		}
		cases = append(cases, c)
	}
	return cases
}

// harnessIDOrUnknown applies the normalization every consumer of the contract
// applies: a missing or empty harness-id means "unknown". ParseSessionEnvelope
// already does this when any x-tapes-* header is present; when NO envelope
// header arrives at all it returns Present=false with an empty HarnessID, and
// the dispatcher omits the session block entirely. Both spellings mean the
// same thing to a downstream reader, so compare through the normalization
// rather than pinning extproc's internal representation of "absent".
func harnessIDOrUnknown(id string) string {
	if id == "" {
		return "unknown"
	}
	return id
}

// jsonEqualMetadata compares parsed metadata against the fixture's raw JSON
// structurally, so JSON key ordering is not part of the contract. Absent and
// empty compare equal.
func jsonEqualMetadata(got map[string]any, want json.RawMessage) (bool, string) {
	if len(want) == 0 || string(want) == "null" {
		if len(got) == 0 {
			return true, ""
		}
		return false, fmt.Sprintf("got %v, want absent", got)
	}
	var wantVal any
	if err := json.Unmarshal(want, &wantVal); err != nil {
		return false, fmt.Sprintf("fixture metadata is not valid JSON: %v", err)
	}
	if got == nil {
		return false, fmt.Sprintf("got absent, want %v", wantVal)
	}
	if !reflect.DeepEqual(any(got), wantVal) {
		return false, fmt.Sprintf("got %v, want %v", got, wantVal)
	}
	return true, ""
}

func TestEnvelopeFixtureCorpus(t *testing.T) {
	cases := loadEnvelopeFixtures(t)

	// Guard against a vendored corpus that silently lost its cases — a glob
	// that matches one stale file would otherwise "pass".
	if len(cases) < 15 {
		t.Fatalf("only %d envelope fixture cases loaded; the vendored corpus looks truncated", len(cases))
	}

	seen := map[string]bool{}
	for _, c := range cases {
		seen[c.Name] = true
	}
	for name := range knownDeviations {
		if !seen[name] {
			t.Errorf("knownDeviations names case %q, which is not in the vendored corpus; "+
				"delete the entry or re-sync the fixtures", name)
		}
	}

	for _, c := range cases {
		t.Run(c.Name, func(t *testing.T) {
			hdrs := mkEnvelopeHeaders(c.Headers)
			got := ParseSessionEnvelope(hdrs)
			want := c.Envelope

			// --- server-trusted identity (read the way the processor does) ---
			if v := Get(hdrs, PaperAuthOrgID); v != want.OrgID {
				t.Errorf("org_id: got %q, want %q", v, want.OrgID)
			}
			if v := Get(hdrs, PaperAuthSubject); v != want.AuthSubject {
				t.Errorf("auth_subject: got %q, want %q", v, want.AuthSubject)
			}

			// --- verbatim string fields ---
			if v := harnessIDOrUnknown(got.HarnessID); v != harnessIDOrUnknown(want.HarnessID) {
				t.Errorf("harness_id: got %q, want %q", v, want.HarnessID)
			}
			if got.HarnessSessionID != want.HarnessSessionID {
				t.Errorf("harness_session_id: got %q, want %q", got.HarnessSessionID, want.HarnessSessionID)
			}
			if got.HarnessVersion != want.HarnessVersion {
				t.Errorf("harness_version: got %q, want %q", got.HarnessVersion, want.HarnessVersion)
			}
			if got.Name != want.Name {
				t.Errorf("name: got %q, want %q", got.Name, want.Name)
			}

			// An empty parent header is "no parent" here: Get returns "" and
			// the dispatcher's omitempty drops the key. The corpus spells the
			// same state as an absent field, so "" == absent.
			if got.ParentHarnessSessionID != want.ParentHarnessSessionID {
				t.Errorf("parent_harness_session_id: got %q, want %q",
					got.ParentHarnessSessionID, want.ParentHarnessSessionID)
			}

			// --- cwd ---
			// No deviation: both parsers percent-decode cwd and both refuse a
			// decoded value carrying control bytes, so the corpus's declared
			// envelope is asserted directly. cwd-control-bytes-escaped is the
			// case that pins the refusal — it declares no cwd at all, because
			// the decoded path contains a newline.
			if got.Cwd != want.Cwd {
				t.Errorf("cwd: got %q, want %q", got.Cwd, want.Cwd)
			}

			// --- metadata (see knownDeviations) ---
			if dev, isDeviation := deviates(c.Name, "harness_metadata"); isDeviation {
				if agrees, _ := jsonEqualMetadata(got.HarnessMetadata, want.HarnessMetadata); agrees {
					t.Errorf("harness_metadata no longer deviates from the corpus; "+
						"delete the knownDeviations entry for %q (recorded reason: %s)", c.Name, dev.why)
				}
				if ok, detail := dev.alt(got); !ok {
					t.Errorf("harness_metadata: the deviation recorded for %q no longer produces "+
						"what it claims: %s", c.Name, detail)
				}
			} else {
				if ok, detail := jsonEqualMetadata(got.HarnessMetadata, want.HarnessMetadata); !ok {
					t.Errorf("harness_metadata: %s", detail)
				}
				// The malformed flag must track the drop exactly: a case whose
				// metadata parsed cleanly must not be flagged, and a case whose
				// metadata the corpus drops (invalid base64) must be.
				metadataHeaderPresent := c.Headers[TapesHarnessMetadata] != ""
				wantMalformed := metadataHeaderPresent && len(want.HarnessMetadata) == 0
				if got.HarnessMetadataMalformed != wantMalformed {
					t.Errorf("HarnessMetadataMalformed: got %v, want %v",
						got.HarnessMetadataMalformed, wantMalformed)
				}
			}
		})
	}
}

// TestEnvelopeFixturePresenceDetection pins the one piece of extproc's parser
// the corpus has no field for: Present, which decides whether the dispatcher
// emits a session block at all. Any x-tapes-* header flips it; the corpus's
// unknown-missing-harness-id case (x-paper-auth-* only) is the one case that
// must NOT.
func TestEnvelopeFixturePresenceDetection(t *testing.T) {
	for _, c := range loadEnvelopeFixtures(t) {
		t.Run(c.Name, func(t *testing.T) {
			hasTapesHeader := false
			for k := range c.Headers {
				if len(k) >= len(TapesEnvelopePrefix) && k[:len(TapesEnvelopePrefix)] == TapesEnvelopePrefix {
					hasTapesHeader = true
					break
				}
			}
			got := ParseSessionEnvelope(mkEnvelopeHeaders(c.Headers))
			if got.Present != hasTapesHeader {
				t.Errorf("Present: got %v, want %v (x-tapes-* header present: %v)",
					got.Present, hasTapesHeader, hasTapesHeader)
			}
		})
	}
}
