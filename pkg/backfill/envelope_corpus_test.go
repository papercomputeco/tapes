package backfill

// Parity gate for the shared envelope fixture corpus.
//
// envelope_fixtures_test.go is the *parser oracle*: it proves this repo's
// reader matches what the corpus declares. That is only half of the parity
// property. The corpus is a contract between independent implementations in
// different repositories and different languages, and it holds them together
// through exactly three links:
//
//  1. each implementation conforms to its copy of the corpus,
//  2. every copy of the corpus is the same bytes, and
//  3. the corpus still covers the rules the contract claims to pin.
//
// Link 1 is the oracle's job. This file is links 2 and 3, which nothing was
// checking:
//
//   - DIGEST seals the corpus. A consumer that vendors the cases also vendors
//     the digest and recomputes it, so a hand-edit to a vendored copy — which
//     would leave two implementations quietly testing against different bytes
//     while both stayed green — fails there instead of never being noticed.
//     It also forces a corpus change to show up as a one-line reviewable diff
//     rather than as a silent edit inside a wall of JSON.
//
//   - The coverage assertions below are stated as properties, not case names,
//     so they survive renames but fail if the behaviour stops being pinned by
//     anything. Deleting the only case that covers a rule is otherwise
//     invisible: every remaining case passes and the contract quietly shrinks.
//     Each rule here is one the two readers could plausibly disagree on.
//
// This is a pure-fixture gate: no database, no network, no services. That is
// deliberate — it is what lets it run as its own fast CI check rather than
// riding along with the suite that needs Postgres.

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"unicode/utf8"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// corpusCase is the full case schema, as documented in
// fixtures/envelope/README.md. envelope_fixtures_test.go decodes only the
// fields the parser oracle needs; this one validates the whole shape, so it
// decodes everything.
type corpusCase struct {
	file        string
	Name        string            `json:"name"`
	Category    string            `json:"category"`
	Harness     string            `json:"harness"`
	Description string            `json:"description"`
	Direction   string            `json:"direction"`
	Headers     map[string]string `json:"headers"`
	Envelope    corpusEnvelope    `json:"envelope"`
	EncodeFrom  *corpusEnvelope   `json:"encode_from"`
	Error       *corpusError      `json:"error"`
	Grounding   string            `json:"grounding"`
	Notes       string            `json:"notes"`
}

type corpusEnvelope struct {
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

type corpusError struct {
	Field       string `json:"field"`
	Rule        string `json:"rule"`
	Disposition string `json:"disposition"`
}

// The synthetic identity values every case must use. Real WorkOS ids in a
// fixture would be a PII leak that survives in git history forever, and the
// corpus is vendored into at least three repositories, so it would leak into
// all of them. See fixtures/README.md on why these stay synthetic.
const (
	syntheticAuthSubject = "user_synthetic_fixture_subject"
	syntheticOrgPrefix   = "00000000-"
)

func envelopeCorpusDir() string {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		panic("runtime.Caller failed")
	}
	return filepath.Join(filepath.Dir(file), "..", "..", "fixtures", "envelope")
}

// corpusDigest seals the case corpus: the name and content of every case,
// in sorted order.
//
// The algorithm is deliberately trivial so that a consumer in another
// language can reimplement it from this comment alone without pulling in a
// canonical-JSON library: for each case file, sorted by base name, feed
// "<basename>  <sha256-hex-of-file-bytes>\n" into a sha256, and hex the
// result. Hashing raw bytes rather than parsed JSON is the point — the sync
// script copies bytes, so the digest has to notice a reformat too.
//
// It covers names as well as contents so that an addition, a deletion, and a
// rename are all caught, not just an edit to a file that already existed.
func corpusDigest(casesDir string) (string, []string, error) {
	matches, err := filepath.Glob(filepath.Join(casesDir, "*.json"))
	if err != nil {
		return "", nil, err
	}
	if len(matches) == 0 {
		return "", nil, fmt.Errorf("no case files under %s", casesDir)
	}
	sort.Strings(matches)

	outer := sha256.New()
	names := make([]string, 0, len(matches))
	for _, m := range matches {
		b, err := os.ReadFile(m)
		if err != nil {
			return "", nil, err
		}
		base := filepath.Base(m)
		names = append(names, base)
		fmt.Fprintf(outer, "%s  %s\n", base, hex.EncodeToString(sha256Sum(b)))
	}
	return hex.EncodeToString(outer.Sum(nil)), names, nil
}

func sha256Sum(b []byte) []byte {
	sum := sha256.Sum256(b)
	return sum[:]
}

func loadCorpusCases(casesDir string) ([]corpusCase, error) {
	matches, err := filepath.Glob(filepath.Join(casesDir, "*.json"))
	if err != nil {
		return nil, err
	}
	sort.Strings(matches)
	cases := make([]corpusCase, 0, len(matches))
	for _, m := range matches {
		b, err := os.ReadFile(m)
		if err != nil {
			return nil, err
		}
		var c corpusCase
		// DisallowUnknownFields: an unrecognised key is almost always a typo
		// in a field name ("groundings", "encodeFrom"), and a typo'd optional
		// field is invisible — the case silently stops pinning what its
		// author thought it pinned.
		dec := json.NewDecoder(strings.NewReader(string(b)))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&c); err != nil {
			return nil, fmt.Errorf("%s: %w", filepath.Base(m), err)
		}
		c.file = filepath.Base(m)
		cases = append(cases, c)
	}
	return cases, nil
}

// decodedHeader mirrors the reader's decode step closely enough to classify a
// case for coverage purposes. It is intentionally written against the stdlib
// rather than calling decodeEnvelopeHeaderValue: asking the parser under test
// whether the corpus covers the parser would be circular.
func decodedHeader(raw string) (value string, malformed bool) {
	if decoded, err := url.PathUnescape(raw); err == nil {
		return decoded, false
	}
	return raw, true
}

func hasControlByte(s string) bool {
	for _, r := range s {
		if r < 0x20 || r == 0x7F {
			return true
		}
	}
	return false
}

func hasNonASCII(s string) bool {
	return utf8.RuneCountInString(s) != len(s)
}

var _ = Describe("envelope fixture corpus (parity gate)", func() {
	var (
		corpusDir string
		casesDir  string
		cases     []corpusCase
	)

	BeforeEach(func() {
		corpusDir = envelopeCorpusDir()
		casesDir = filepath.Join(corpusDir, "cases")
		var err error
		cases, err = loadCorpusCases(casesDir)
		Expect(err).NotTo(HaveOccurred())
		Expect(cases).NotTo(BeEmpty(), "no envelope fixture cases found under %s", casesDir)
	})

	Describe("the corpus is sealed", func() {
		It("matches the digest recorded in DIGEST", func() {
			got, names, err := corpusDigest(casesDir)
			Expect(err).NotTo(HaveOccurred())

			digestPath := filepath.Join(corpusDir, "DIGEST")
			raw, err := os.ReadFile(digestPath)
			Expect(err).NotTo(HaveOccurred(),
				"fixtures/envelope/DIGEST is missing; it seals the corpus for every consumer that vendors it")

			want := strings.TrimSpace(string(raw))
			// The failure message carries the new value so that a legitimate
			// corpus change is a copy-paste, not a puzzle. A gate nobody can
			// satisfy quickly is a gate someone deletes.
			Expect(want).To(Equal("sha256:"+got), strings.Join([]string{
				"the envelope corpus no longer matches fixtures/envelope/DIGEST.",
				"",
				"If you changed the corpus on purpose, write this line into fixtures/envelope/DIGEST:",
				"",
				"    sha256:" + got,
				"",
				fmt.Sprintf("and re-sync every vendored copy (%d cases).", len(names)),
				"Consumers vendor the digest alongside the cases, so a stale copy fails there.",
			}, "\n"))
		})
	})

	Describe("every case conforms to the documented schema", func() {
		// Table entries cannot be built here (cases load in BeforeEach), so
		// this is one It that reports every offending case rather than dying
		// on the first — a schema sweep is much more useful whole.
		It("declares the required fields with legal values", func() {
			categories := map[string]bool{"valid": true, "percent-encoding": true, "budget": true, "unknown": true, "error": true}
			harnesses := map[string]bool{"claude": true, "codex": true, "pi": true, "unknown": true}
			directions := map[string]bool{"roundtrip": true, "decode": true, "encode": true}
			dispositions := map[string]bool{"reject-400": true, "drop-field": true}

			seen := map[string]string{}
			for _, c := range cases {
				id := c.file

				Expect(c.Name).NotTo(BeEmpty(), "%s: name is required", id)
				Expect(c.Name+".json").To(Equal(c.file),
					"%s: name %q must match the filename; consumers key deviations and skips off the name", id, c.Name)
				if prev, dup := seen[c.Name]; dup {
					Fail(fmt.Sprintf("%s: duplicate case name %q (also %s)", id, c.Name, prev))
				}
				seen[c.Name] = id

				Expect(categories).To(HaveKey(c.Category), "%s: unknown category %q", id, c.Category)
				Expect(harnesses).To(HaveKey(c.Harness), "%s: unknown harness %q", id, c.Harness)
				Expect(directions).To(HaveKey(c.Direction), "%s: unknown direction %q", id, c.Direction)
				Expect(c.Description).NotTo(BeEmpty(), "%s: description is required", id)
				Expect(c.Grounding).NotTo(BeEmpty(),
					"%s: grounding is required — a case that cannot say which rule it pins cannot be reviewed", id)
				Expect(c.Headers).NotTo(BeEmpty(), "%s: headers is required", id)

				for k := range c.Headers {
					Expect(k).To(Equal(strings.ToLower(k)),
						"%s: header %q must be lower-cased, as an HTTP/2 intermediary carries it", id, k)
				}

				// encode_from exists precisely to record what was lost. On a
				// case that loses nothing it is dead weight that will rot.
				if c.Direction == "encode" {
					Expect(c.EncodeFrom).NotTo(BeNil(),
						"%s: direction=encode is the lossy direction and must carry encode_from", id)
				} else {
					Expect(c.EncodeFrom).To(BeNil(),
						"%s: direction=%s round-trips, so encode_from would duplicate envelope", id, c.Direction)
				}

				if c.Error != nil {
					Expect(dispositions).To(HaveKey(c.Error.Disposition),
						"%s: unknown error disposition %q", id, c.Error.Disposition)
					Expect(c.Error.Field).NotTo(BeEmpty(), "%s: error.field is required", id)
					Expect(c.Error.Rule).NotTo(BeEmpty(), "%s: error.rule is required", id)
				}

				// Identity hygiene. org_id may be a deliberately malformed
				// value, but only when the case says so in its error block.
				if c.Envelope.AuthSubject != "" {
					Expect(c.Envelope.AuthSubject).To(Equal(syntheticAuthSubject),
						"%s: auth_subject must be the synthetic placeholder, never a real subject", id)
				}
				if c.Envelope.OrgID != "" && !strings.HasPrefix(c.Envelope.OrgID, syntheticOrgPrefix) {
					Expect(c.Error).NotTo(BeNil(),
						"%s: org_id %q is neither a synthetic placeholder nor declared malformed", id, c.Envelope.OrgID)
					Expect(c.Error.Field).To(Equal("org_id"),
						"%s: org_id %q is not synthetic, so the case must declare it as the org_id error", id, c.Envelope.OrgID)
				}
			}
		})
	})

	Describe("the corpus still covers the rules the contract claims", func() {
		// Each of these is a rule two independent readers could disagree on
		// while every other case stayed green. A rule with no case is a rule
		// that is not actually pinned, however emphatically the README states
		// it.
		type rule struct {
			what  string
			why   string
			holds func(corpusCase) bool
		}

		rules := []rule{
			{
				what: "percent-decoding of non-ASCII",
				why:  "the stored value is the logical value; a reader that stored the escaped form would force a decoder into every consumer",
				holds: func(c corpusCase) bool {
					for _, f := range []string{c.Envelope.Cwd, c.Envelope.Name} {
						if hasNonASCII(f) {
							return true
						}
					}
					return false
				},
			},
			{
				what: "a literal '+' surviving the decode",
				why:  "PathUnescape keeps '+'; QueryUnescape turns it into a space, so 'go+rust' would silently become 'go rust'",
				holds: func(c corpusCase) bool {
					for h, raw := range c.Headers {
						if !strings.Contains(raw, "+") {
							continue
						}
						switch h {
						case "x-tapes-cwd":
							return strings.Contains(c.Envelope.Cwd, "+")
						case "x-tapes-session-name":
							return strings.Contains(c.Envelope.Name, "+")
						}
					}
					return false
				},
			},
			{
				what: "a malformed percent-encoding falling back to the raw value",
				why:  "decoding failure is non-fatal, so the row still records something recognisable",
				holds: func(c corpusCase) bool {
					for h, raw := range c.Headers {
						_, malformed := decodedHeader(raw)
						if !malformed {
							continue
						}
						switch h {
						case "x-tapes-cwd":
							return c.Envelope.Cwd == raw
						case "x-tapes-session-name":
							return c.Envelope.Name == raw
						}
					}
					return false
				},
			},
			{
				what: "the control-byte guard on cwd",
				why:  "escaping stops a control byte forging a header on the wire; the guard stops it reaching storage",
				holds: func(c corpusCase) bool {
					raw, ok := c.Headers["x-tapes-cwd"]
					if !ok {
						return false
					}
					decoded, _ := decodedHeader(raw)
					return hasControlByte(decoded) && c.Envelope.Cwd == ""
				},
			},
			{
				what: "the control-byte guard on session-name",
				why:  "the guard is a property of the decoder, not of one field — and session names are the likelier injection vector, being user-supplied free text",
				holds: func(c corpusCase) bool {
					raw, ok := c.Headers["x-tapes-session-name"]
					if !ok {
						return false
					}
					decoded, _ := decodedHeader(raw)
					return hasControlByte(decoded) && c.Envelope.Name == ""
				},
			},
			{
				what: "metadata dropped as a non-fatal malformed field",
				why:  "unparseable metadata must not cost the rest of the envelope",
				holds: func(c corpusCase) bool {
					return c.Error != nil && c.Error.Field == "harness_metadata" && c.Error.Disposition == "drop-field"
				},
			},
			{
				what: "metadata rejected at validation",
				why:  "object-ness is enforced by validation, not silently at parse, so the caller learns the metadata was wrong",
				holds: func(c corpusCase) bool {
					return c.Error != nil && c.Error.Field == "harness_metadata" && c.Error.Disposition == "reject-400"
				},
			},
			{
				what: "the metadata alphabet being part of the contract",
				why:  "a strict reader and a reader that tries several alphabets produce different rows from identical bytes",
				holds: func(c corpusCase) bool {
					raw := c.Headers["x-tapes-harness-metadata"]
					return strings.Contains(raw, "=") && len(c.Envelope.HarnessMetadata) == 0
				},
			},
			{
				what: "an empty parent header being dropped rather than stored",
				why:  "'' and absent are different envelopes, and validation rejects one of them",
				holds: func(c corpusCase) bool {
					raw, ok := c.Headers["x-tapes-parent-harness-session-id"]
					return ok && raw == "" && c.Envelope.ParentHarnessSessionID == ""
				},
			},
			{
				what: "a non-UUID org_id being rejected",
				why:  "org_id is server-trusted, so a malformed one means the gateway is wrong and must not be stored",
				holds: func(c corpusCase) bool {
					return c.Error != nil && c.Error.Field == "org_id" && c.Error.Disposition == "reject-400"
				},
			},
			{
				what: "the session-name byte cap",
				why:  "truncation happens before encoding and at a codepoint boundary, which is easy to get wrong by one byte",
				holds: func(c corpusCase) bool {
					return c.Category == "budget" && c.Direction == "encode" && c.EncodeFrom != nil &&
						len(c.EncodeFrom.Name) > len(c.Envelope.Name)
				},
			},
			{
				what: "oversize metadata being dropped whole",
				why:  "the header budget is enforced by dropping metadata first, not by truncating it into invalid JSON",
				holds: func(c corpusCase) bool {
					return c.Category == "budget" && c.EncodeFrom != nil &&
						len(c.EncodeFrom.HarnessMetadata) > 0 && len(c.Envelope.HarnessMetadata) == 0
				},
			},
			{
				what: "a missing harness-id normalising to unknown",
				why:  "every consumer must agree on the spelling of 'no harness told us who it was'",
				holds: func(c corpusCase) bool {
					_, present := c.Headers["x-tapes-harness-id"]
					return (!present || c.Headers["x-tapes-harness-id"] == "") && c.Envelope.HarnessID == "unknown"
				},
			},
		}

		It("has at least one case per rule", func() {
			for _, r := range rules {
				covered := false
				for _, c := range cases {
					if r.holds(c) {
						covered = true
						break
					}
				}
				Expect(covered).To(BeTrue(), strings.Join([]string{
					fmt.Sprintf("no case covers: %s", r.what),
					fmt.Sprintf("why it matters: %s", r.why),
					"",
					"Either a case was deleted or renamed away from this behaviour, or the rule",
					"was never pinned. Add a case rather than relaxing the rule — the contract is",
					"only as strong as the cases that exercise it.",
				}, "\n"))
			}
		})

		It("covers every harness the contract names", func() {
			seen := map[string]bool{}
			for _, c := range cases {
				seen[c.Harness] = true
			}
			for _, h := range []string{"claude", "codex", "pi", "unknown"} {
				Expect(seen).To(HaveKey(h), "no case for harness %q", h)
			}
		})
	})
})
