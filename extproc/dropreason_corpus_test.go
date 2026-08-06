package extproc

// Authored-home gate for the shared drop-reason fixture corpus
// (fixtures/drop-reason/), which specifies the vocabulary of answers to "why
// was this turn not captured" and — the part that had never been written down —
// which of those answers are capture policy every implementation shares and
// which are one deployment's transport and runtime.
//
// The gate lives here rather than beside the policy vocabulary in pkg/capture
// because this is the one place both halves meet: the policy reasons the shared
// home owns and the transport reasons this adapter correctly keeps. A gate in
// pkg/capture could only ever check half the corpus, and the failure it needs
// to catch — a reason drifting into the wrong half, or existing in only one
// place — is visible only from here.
//
// Four gates, mirroring the sibling corpora:
//
//  1. the oracle — every case's executable examples hold against the predicate
//     the processor actually runs;
//  2. conformance, in both directions — the corpus, pkg/capture's policy set,
//     and this adapter's enum name exactly the same reasons, with the same
//     strings and the same classes;
//  3. the DIGEST seal;
//  4. coverage — the corpus still exercises each rule, stated as properties so
//     the assertions survive case renames.

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/capture"
)

// otherCaptureHome is where the second capture path lives. Failure messages
// name it because the whole point of specifying this vocabulary is that a
// change to the policy half is a change to both paths.
const otherCaptureHome = "tapesctl crates/tapesctl/src/start/proxy.rs"

func dropReasonCorpusDir() string {
	_, file, _, ok := runtime.Caller(0)
	Expect(ok).To(BeTrue(), "runtime.Caller failed")
	return filepath.Join(filepath.Dir(file), "..", "fixtures", "drop-reason")
}

// --- the case schema, mirroring fixtures/drop-reason/README.md -------------

type dropExampleRequest struct {
	Method string `json:"method"`
	Path   string `json:"path"`
}

type dropExample struct {
	Description string             `json:"description"`
	Request     dropExampleRequest `json:"request"`
	Expect      string             `json:"expect"`
}

type dropReasonCase struct {
	file           string
	Name           string        `json:"name"`
	Class          string        `json:"class"`
	Constant       string        `json:"constant"`
	Summary        string        `json:"summary"`
	Trigger        string        `json:"trigger"`
	Grounding      string        `json:"grounding"`
	Examples       []dropExample `json:"examples"`
	NotExpressible string        `json:"not_expressible"`
	Notes          string        `json:"notes"`
}

func loadDropReasonCases() []dropReasonCase {
	matches, err := filepath.Glob(filepath.Join(dropReasonCorpusDir(), "cases", "*.json"))
	Expect(err).NotTo(HaveOccurred())
	Expect(matches).NotTo(BeEmpty(), "no drop-reason corpus cases under %s", dropReasonCorpusDir())
	sort.Strings(matches)

	out := make([]dropReasonCase, 0, len(matches))
	for _, m := range matches {
		b, readErr := os.ReadFile(m)
		Expect(readErr).NotTo(HaveOccurred())
		var c dropReasonCase
		dec := json.NewDecoder(bytes.NewReader(b))
		// An unknown field is a case written against a schema this consumer
		// does not implement. Failing is the point: silently ignoring it
		// would let a case assert something nobody checks.
		dec.DisallowUnknownFields()
		Expect(dec.Decode(&c)).To(Succeed(), "%s", filepath.Base(m))
		c.file = filepath.Base(m)
		out = append(out, c)
	}
	return out
}

// extprocDropConstants maps the Go constant name a case declares onto the value
// this package actually carries.
//
// Hand-maintained on purpose: it is the join between a name in a JSON file and
// an identifier the compiler knows about, and there is no reflection over
// package-level constants that would build it. It cannot rot silently — the
// conformance gate asserts it covers AllDropReasons() exactly, so a constant
// added or renamed anywhere fails here until it is added here too.
var extprocDropConstants = map[string]DropReason{
	"DropUpstreamStatus":     DropUpstreamStatus,
	"DropNonTurnRequest":     DropNonTurnRequest,
	"DropRequestDecode":      DropRequestDecode,
	"DropEmptyResponse":      DropEmptyResponse,
	"DropUnknownProvider":    DropUnknownProvider,
	"DropResponseDecode":     DropResponseDecode,
	"DropReducerError":       DropReducerError,
	"DropClientDisconnect":   DropClientDisconnect,
	"DropUpstreamNoResponse": DropUpstreamNoResponse,
	"DropMissingStatus":      DropMissingStatus,
	"DropSemFull":            DropSemFull,
	"DropIngestReject":       DropIngestReject,
	"DropIngestTimeout":      DropIngestTimeout,
	"DropMarshalError":       DropMarshalError,
}

var _ = Describe("drop-reason corpus", func() {
	cases := loadDropReasonCases()

	// --- gate 1: the oracle -------------------------------------------------

	Describe("oracle", func() {
		for _, tc := range cases {
			c := tc
			It(c.Name, func() {
				Expect(c.Name+".json").To(Equal(c.file), "name must match the filename")
				Expect(c.Class).To(BeElementOf("policy", "transport"))
				Expect(c.Summary).NotTo(BeEmpty(), "summary is required")
				Expect(c.Trigger).NotTo(BeEmpty(), "trigger is required")
				Expect(c.Grounding).NotTo(BeEmpty(),
					"grounding is required: a reason whose class cannot be argued is a reason\n"+
						"  the next implementation will guess about")
				Expect(c.Constant).NotTo(BeEmpty(), "constant is required")

				// Every reason either carries executable examples or says why
				// it cannot. Neither is optional: a reason with no examples and
				// no explanation reads as an oversight, and the next person
				// cannot tell whether writing one was tried.
				Expect(len(c.Examples) > 0).NotTo(Equal(c.NotExpressible != ""),
					"a case must carry examples or not_expressible, and not both")

				for _, ex := range c.Examples {
					Expect(ex.Description).NotTo(BeEmpty(), "each example needs a description")
					Expect(ex.Expect).To(BeElementOf("eligible", "dropped"))

					// Only non_turn_request is a pure function of request data
					// today, so it is the only reason whose examples can be
					// executed. A second one gains a branch here — and until
					// then, an examples block on any other reason is a case
					// asserting something nobody runs.
					Expect(c.Name).To(Equal(string(capture.DropNonTurnRequest)),
						"only %s carries executable examples today; %s must declare\n"+
							"  not_expressible or gain an evaluator here",
						capture.DropNonTurnRequest, c.Name)

					got := isCapturableTurnRequest(ex.Request.Method, ex.Request.Path)
					Expect(got).To(Equal(ex.Expect == "eligible"),
						"%s: %s (%s %q).\n  This is shared capture policy. If it genuinely changed, the same\n"+
							"  change belongs in the other capture path too: %s.",
						c.file, ex.Description, ex.Request.Method, ex.Request.Path, otherCaptureHome)
				}
			})
		}
	})

	// --- gate 2: conformance, in both directions ----------------------------

	Describe("vocabulary", func() {
		corpusNames := func(class string) []string {
			var out []string
			for _, c := range cases {
				if class == "" || c.Class == class {
					out = append(out, c.Name)
				}
			}
			sort.Strings(out)
			return out
		}
		reasonStrings := func(rs []DropReason) []string {
			out := make([]string, 0, len(rs))
			for _, r := range rs {
				out = append(out, string(r))
			}
			sort.Strings(out)
			return out
		}

		It("specifies exactly the reasons this adapter carries", func() {
			Expect(corpusNames("")).To(Equal(reasonStrings(AllDropReasons())),
				"the corpus and this adapter's enum have drifted. A reason that exists in\n"+
					"  code and not in the corpus is unspecified — nobody has decided whether it\n"+
					"  is capture policy or this deployment's plumbing, which is the decision\n"+
					"  fixtures/drop-reason/ exists to force.")
		})

		It("agrees with pkg/capture on which reasons are policy", func() {
			policy := make([]DropReason, 0, len(capture.PolicyDropReasons()))
			for _, r := range capture.PolicyDropReasons() {
				policy = append(policy, DropReason(r))
			}
			Expect(corpusNames("policy")).To(Equal(reasonStrings(policy)),
				"the corpus and pkg/capture disagree about the policy half. Adding a policy\n"+
					"  reason means adding it to capture.PolicyDropReasons and to the corpus;\n"+
					"  adding one to only one of them is the drift this gate exists to catch.")
		})

		It("keeps every transport reason out of the shared vocabulary", func() {
			for _, c := range cases {
				if c.Class != "transport" {
					continue
				}
				Expect(capture.IsPolicyDropReason(capture.DropReason(c.Name))).To(BeFalse(),
					"%s is specified as transport but is in capture.PolicyDropReasons.\n"+
						"  A reason cannot be both: promoting it makes one deployment's plumbing\n"+
						"  a contract every implementation must satisfy.", c.Name)
			}
		})

		It("names a real constant carrying the specified string", func() {
			Expect(extprocDropConstants).To(HaveLen(len(AllDropReasons())),
				"the constant lookup table has drifted from the enum")
			for _, c := range cases {
				got, ok := extprocDropConstants[c.Constant]
				Expect(ok).To(BeTrue(), "%s: no constant named %s in this package", c.file, c.Constant)
				Expect(string(got)).To(Equal(c.Name),
					"%s: constant %s carries %q but the corpus specifies %q.\n"+
						"  These strings are wire-visible — metric label values and log fields —\n"+
						"  so a rename is a dashboard change, not an internal one.",
					c.file, c.Constant, string(got), c.Name)
			}
		})

		It("sources every policy reason's string from pkg/capture", func() {
			// The point of the plumbing: this adapter must not be able to
			// change a policy reason's spelling on its own. If these were
			// independent declarations that happened to match, this test would
			// pass and the next edit could still diverge.
			for _, r := range capture.PolicyDropReasons() {
				Expect(AllDropReasons()).To(ContainElement(DropReason(r)),
					"capture specifies policy reason %q and this adapter does not carry it", r)
			}
		})
	})

	// --- gate 3: the seal ---------------------------------------------------

	It("matches its sealed DIGEST", func() {
		matches, err := filepath.Glob(filepath.Join(dropReasonCorpusDir(), "cases", "*.json"))
		Expect(err).NotTo(HaveOccurred())
		Expect(matches).NotTo(BeEmpty())
		sort.Strings(matches)

		outer := sha256.New()
		for _, m := range matches {
			b, readErr := os.ReadFile(m)
			Expect(readErr).NotTo(HaveOccurred())
			sum := sha256.Sum256(b)
			fmt.Fprintf(outer, "%s  %s\n", filepath.Base(m), hex.EncodeToString(sum[:]))
		}
		got := "sha256:" + hex.EncodeToString(outer.Sum(nil))

		sealed, err := os.ReadFile(filepath.Join(dropReasonCorpusDir(), "DIGEST"))
		Expect(err).NotTo(HaveOccurred())
		Expect(got).To(Equal(strings.TrimSpace(string(sealed))),
			"drop-reason corpus digest mismatch.\n  recomputed: %s\n"+
				"If the corpus change is intentional, write that value into\n"+
				"fixtures/drop-reason/DIGEST and re-sync every vendored copy\n"+
				"from the same commit.", got)
	})

	// --- gate 4: coverage ---------------------------------------------------

	Describe("still covers", func() {
		covers := func(holds func(dropReasonCase) bool) bool {
			for _, c := range cases {
				if holds(c) {
					return true
				}
			}
			return false
		}
		exampleHolds := func(c dropReasonCase, pick func(dropExample) bool) bool {
			for _, ex := range c.Examples {
				if pick(ex) {
					return true
				}
			}
			return false
		}

		rules := map[string]func(dropReasonCase) bool{
			"both halves of the taxonomy": func(c dropReasonCase) bool {
				return c.Class == "transport"
			},
			"a health probe on a turn path, refused for its method": func(c dropReasonCase) bool {
				return exampleHolds(c, func(ex dropExample) bool {
					return strings.EqualFold(ex.Request.Method, "HEAD") && ex.Expect == "dropped"
				})
			},
			"a non-POST read method on a turn path": func(c dropReasonCase) bool {
				return exampleHolds(c, func(ex dropExample) bool {
					return strings.EqualFold(ex.Request.Method, "GET") && ex.Expect == "dropped"
				})
			},
			"an endpoint adjacent to a turn path that is not conversation": func(c dropReasonCase) bool {
				return exampleHolds(c, func(ex dropExample) bool {
					return strings.Contains(ex.Request.Path, "count_tokens") && ex.Expect == "dropped"
				})
			},
			"case-insensitive method matching": func(c dropReasonCase) bool {
				return exampleHolds(c, func(ex dropExample) bool {
					m := ex.Request.Method
					return m != "" && m != strings.ToUpper(m) && ex.Expect == "eligible"
				})
			},
			"an absent method treated as capturable": func(c dropReasonCase) bool {
				return exampleHolds(c, func(ex dropExample) bool {
					return ex.Request.Method == "" && ex.Expect == "eligible"
				})
			},
			"a turn path behind a gateway prefix": func(c dropReasonCase) bool {
				return exampleHolds(c, func(ex dropExample) bool {
					return strings.Count(strings.Trim(ex.Request.Path, "/"), "/") > 1 &&
						ex.Expect == "eligible"
				})
			},
			"every provider family's turn path": func(c dropReasonCase) bool {
				want := []string{"/v1/messages", "/v1/chat/completions", "/v1/responses",
					"/codex/responses", "/api/chat"}
				for _, w := range want {
					if !exampleHolds(c, func(ex dropExample) bool {
						return strings.HasSuffix(ex.Request.Path, w) && ex.Expect == "eligible"
					}) {
						return false
					}
				}
				return true
			},
		}

		for name, holds := range rules {
			what, rule := name, holds
			It(what, func() {
				Expect(covers(rule)).To(BeTrue(),
					"no case covers %s any more. Deleting the last case for a rule silently\n"+
						"  un-pins it for every consumer, including %s.", what, otherCaptureHome)
			})
		}
	})
})
