package capture_test

// Oracle and authored-home gate for the shared content-encoding fixture corpus
// (fixtures/content-encoding/), which pins the captured-body decode POLICY:
// which codings are readable, how stacked layers compose, how much output is
// allowed, and what a corrupt or half-arrived stream is worth.
//
// This package's DecodeContentEncoding is the reference implementation, so a
// red test here means either the reference changed or a case is wrong — and
// the corpus records what the reference actually does, not what its prose
// claims. The second implementation of the same policy lives in another
// language and another repository (tapesctl
// crates/tapesctl/src/start/content_encoding.rs); it is not yet wired to this
// corpus, and until it is, the two agreeing remains a claim rather than a
// gate. That gap is the whole reason for PCC-1126.
//
// Three gates, mirroring the envelope and thread corpora:
//
//  1. the oracle — every case decodes to its declared outcome;
//  2. the DIGEST seal — the authored home recomputes it, so a corpus change is
//     a one-line reviewable diff and vendored copies can detect staleness;
//  3. coverage — the corpus still exercises each policy rule, stated as
//     properties so the assertions survive case renames.

import (
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"

	"github.com/klauspost/compress/zstd"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/capture"
)

// otherHome is where the second implementation of this policy lives; failure
// messages name it so a genuine policy change lands on both sides.
const otherHome = "tapesctl crates/tapesctl/src/start/content_encoding.rs"

func corpusDir() string {
	_, file, _, ok := runtime.Caller(0)
	Expect(ok).To(BeTrue(), "runtime.Caller failed")
	return filepath.Join(filepath.Dir(file), "..", "..", "fixtures", "content-encoding")
}

// --- the case schema, mirroring fixtures/content-encoding/README.md ---------

type plaintextSpec struct {
	UTF8       *string `json:"utf8"`
	RepeatUTF8 *struct {
		Text  string `json:"text"`
		Count int    `json:"count"`
	} `json:"repeat_utf8"`
	RepeatByte *struct {
		Byte  int `json:"byte"`
		Count int `json:"count"`
	} `json:"repeat_byte"`
}

type truncateSpec struct {
	DropTailBytes *int  `json:"drop_tail_bytes"`
	KeepHeadRatio []int `json:"keep_head_ratio"`
}

type buildSpec struct {
	Plaintext plaintextSpec `json:"plaintext"`
	Layers    []string      `json:"layers"`
	Truncate  *truncateSpec `json:"truncate"`
}

type bodySpec struct {
	BytesB64 *string    `json:"bytes_b64"`
	Build    *buildSpec `json:"build"`
}

type decodedSpec struct {
	EqualsPlaintext           bool    `json:"equals_plaintext"`
	BytesB64                  *string `json:"bytes_b64"`
	NonemptyPrefixOfPlaintext bool    `json:"nonempty_prefix_of_plaintext"`
}

type errorSpec struct {
	Class           string   `json:"class"`
	MessageContains []string `json:"message_contains"`
	Detail          string   `json:"detail"`
}

type expectSpec struct {
	Outcome string       `json:"outcome"`
	Decoded *decodedSpec `json:"decoded"`
	Error   *errorSpec   `json:"error"`
}

type encodingCase struct {
	file        string
	Name        string          `json:"name"`
	Category    string          `json:"category"`
	Description string          `json:"description"`
	Encoding    *string         `json:"encoding"`
	Body        bodySpec        `json:"body"`
	Expect      expectSpec      `json:"expect"`
	Grounding   string          `json:"grounding"`
	Contested   json.RawMessage `json:"contested"`
	Notes       string          `json:"notes"`
}

func loadEncodingCases() []encodingCase {
	matches, err := filepath.Glob(filepath.Join(corpusDir(), "cases", "*.json"))
	Expect(err).NotTo(HaveOccurred())
	Expect(matches).NotTo(BeEmpty(), "no content-encoding corpus cases under %s", corpusDir())
	sort.Strings(matches)

	out := make([]encodingCase, 0, len(matches))
	for _, m := range matches {
		b, err := os.ReadFile(m)
		Expect(err).NotTo(HaveOccurred())
		var c encodingCase
		dec := json.NewDecoder(bytes.NewReader(b))
		// An unknown field is a case written against a schema this consumer
		// does not implement. Failing is the point: silently ignoring it
		// would let a case assert something no one checks.
		dec.DisallowUnknownFields()
		Expect(dec.Decode(&c)).To(Succeed(), "%s", filepath.Base(m))
		c.file = filepath.Base(m)
		out = append(out, c)
	}
	return out
}

// --- building a case's bytes -----------------------------------------------

func buildPlaintext(p plaintextSpec) []byte {
	set := 0
	var out []byte
	if p.UTF8 != nil {
		set++
		out = []byte(*p.UTF8)
	}
	if p.RepeatUTF8 != nil {
		set++
		out = bytes.Repeat([]byte(p.RepeatUTF8.Text), p.RepeatUTF8.Count)
	}
	if p.RepeatByte != nil {
		set++
		Expect(p.RepeatByte.Byte).To(And(BeNumerically(">=", 0), BeNumerically("<=", 255)))
		out = bytes.Repeat([]byte{byte(p.RepeatByte.Byte)}, p.RepeatByte.Count)
	}
	Expect(set).To(Equal(1), "plaintext must set exactly one form")
	return out
}

func applyLayer(body []byte, layer string) []byte {
	var buf bytes.Buffer
	switch layer {
	case "gzip":
		w := gzip.NewWriter(&buf)
		_, err := w.Write(body)
		Expect(err).NotTo(HaveOccurred())
		Expect(w.Close()).To(Succeed())
	case "zstd":
		w, err := zstd.NewWriter(&buf)
		Expect(err).NotTo(HaveOccurred())
		_, err = w.Write(body)
		Expect(err).NotTo(HaveOccurred())
		Expect(w.Close()).To(Succeed())
	default:
		Fail(fmt.Sprintf("case recipe names a layer this consumer cannot build: %q", layer))
	}
	return buf.Bytes()
}

// buildBody returns the wire bytes for a case, plus the plaintext they were
// built from (nil for a bytes_b64 case, which has no plaintext).
func buildBody(c encodingCase) (body, plaintext []byte) {
	Expect(c.Body.BytesB64 == nil).NotTo(Equal(c.Body.Build == nil),
		"%s: body must set exactly one of bytes_b64 or build", c.file)

	if c.Body.BytesB64 != nil {
		raw, err := base64.StdEncoding.DecodeString(*c.Body.BytesB64)
		Expect(err).NotTo(HaveOccurred(), "%s: bytes_b64", c.file)
		return raw, nil
	}

	plaintext = buildPlaintext(c.Body.Build.Plaintext)
	body = plaintext
	// Layers are listed in header order — left is applied first — so the
	// body is built left-to-right and decoded right-to-left.
	for _, layer := range c.Body.Build.Layers {
		body = applyLayer(body, layer)
	}
	if t := c.Body.Build.Truncate; t != nil {
		switch {
		case t.DropTailBytes != nil:
			Expect(len(body)).To(BeNumerically(">", *t.DropTailBytes))
			body = body[:len(body)-*t.DropTailBytes]
		case len(t.KeepHeadRatio) == 2:
			num, den := t.KeepHeadRatio[0], t.KeepHeadRatio[1]
			Expect(den).To(BeNumerically(">", 0))
			body = body[:len(body)*num/den]
		default:
			Fail(fmt.Sprintf("%s: truncate must set exactly one form", c.file))
		}
	}
	return body, plaintext
}

// --- classifying this implementation's errors ------------------------------

// classify maps a DecodeContentEncoding error onto the corpus's failure
// taxonomy.
//
// The corpus names three classes because both implementations already
// distinguish them: Rust structurally, as DecodeError::{Unsupported, TooLarge,
// Read}; Go only by message. This function is that difference, isolated — if
// Go ever grows typed errors, this is the only thing that changes, and the
// corpus does not.
func classify(err error) string {
	msg := err.Error()
	switch {
	case strings.Contains(msg, "unsupported encoding"):
		return "unsupported"
	case strings.Contains(msg, "exceeds") && strings.Contains(msg, "bytes"):
		return "oversize"
	default:
		return "undecodable"
	}
}

// --- gate 1: the oracle ----------------------------------------------------

var _ = Describe("content-encoding corpus", func() {
	cases := loadEncodingCases()

	Describe("oracle", func() {
		for _, tc := range cases {
			c := tc
			It(c.Name, func() {
				Expect(c.Name+".json").To(Equal(c.file), "name must match the filename")
				Expect(c.Grounding).NotTo(BeEmpty(), "grounding is required")
				Expect(c.Description).NotTo(BeEmpty(), "description is required")
				Expect(c.Category).To(BeElementOf(
					"identity", "supported", "stacked", "salvage", "limit", "error"))

				body, plaintext := buildBody(c)

				// A null encoding is an absent header; the decoder takes the
				// same empty string for both, which is what the
				// identity-header-absent / identity-empty-header pair pins.
				encoding := ""
				if c.Encoding != nil {
					encoding = *c.Encoding
				}

				got, stats, err := capture.DecodeContentEncoding(body, encoding)

				switch c.Expect.Outcome {
				case "decoded", "salvaged":
					Expect(err).NotTo(HaveOccurred(),
						"case expects %s.\n  This corpus is the shared capture-decode policy. If the\n"+
							"  policy genuinely changed, the same change belongs in the other\n"+
							"  implementation too: %s.", c.Expect.Outcome, otherHome)
					Expect(stats.Truncated).To(Equal(c.Expect.Outcome == "salvaged"),
						"a salvaged decode must be reported as truncated, and a clean one must not be")

					Expect(c.Expect.Decoded).NotTo(BeNil(), "a non-error outcome must declare decoded")
					d := c.Expect.Decoded
					switch {
					case d.EqualsPlaintext:
						Expect(plaintext).NotTo(BeNil(), "equals_plaintext needs a build recipe")
						Expect(got).To(Equal(plaintext))
					case d.BytesB64 != nil:
						want, decErr := base64.StdEncoding.DecodeString(*d.BytesB64)
						Expect(decErr).NotTo(HaveOccurred())
						Expect(got).To(Equal(want))
					case d.NonemptyPrefixOfPlaintext:
						Expect(plaintext).NotTo(BeNil(), "nonempty_prefix_of_plaintext needs a build recipe")
						Expect(got).NotTo(BeEmpty(), "a salvage must produce output to be a salvage")
						Expect(bytes.HasPrefix(plaintext, got)).To(BeTrue(),
							"a salvaged body must be a prefix of the original, not a corruption of it")
					default:
						Fail("decoded must set exactly one form")
					}

				case "error":
					Expect(err).To(HaveOccurred(),
						"case expects an error.\n  A decoder that started accepting this input silently changed\n"+
							"  the capture policy; the same change belongs in %s.", otherHome)
					Expect(c.Expect.Error).NotTo(BeNil(), "an error outcome must declare error")
					Expect(c.Expect.Error.Class).To(BeElementOf("unsupported", "oversize", "undecodable"))
					Expect(classify(err)).To(Equal(c.Expect.Error.Class), "error %v", err)
					for _, want := range c.Expect.Error.MessageContains {
						Expect(err.Error()).To(ContainSubstring(want))
					}

				default:
					Fail(fmt.Sprintf("unknown outcome %q", c.Expect.Outcome))
				}
			})
		}
	})

	// --- gate 2: the seal ---------------------------------------------------

	It("matches its sealed DIGEST", func() {
		matches, err := filepath.Glob(filepath.Join(corpusDir(), "cases", "*.json"))
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

		sealed, err := os.ReadFile(filepath.Join(corpusDir(), "DIGEST"))
		Expect(err).NotTo(HaveOccurred())
		Expect(got).To(Equal(strings.TrimSpace(string(sealed))),
			"content-encoding corpus digest mismatch.\n  recomputed: %s\n"+
				"If the corpus change is intentional, write that value into\n"+
				"fixtures/content-encoding/DIGEST and re-sync every vendored copy\n"+
				"from the same commit.", got)
	})

	// --- gate 3: coverage ---------------------------------------------------

	// Deleting the only case that covers a policy rule is otherwise invisible,
	// and that is exactly how independent implementations drift. Stated as
	// properties rather than case names so the assertions survive renames.
	Describe("still covers", func() {
		covers := func(holds func(encodingCase) bool) bool {
			for _, c := range cases {
				if holds(c) {
					return true
				}
			}
			return false
		}
		layersOf := func(c encodingCase) []string {
			if c.Body.Build == nil {
				return nil
			}
			return c.Body.Build.Layers
		}
		usesCoding := func(c encodingCase, coding string) bool {
			if c.Encoding == nil {
				return false
			}
			return strings.Contains(strings.ToLower(*c.Encoding), coding)
		}

		rules := map[string]func(encodingCase) bool{
			"an absent Content-Encoding header": func(c encodingCase) bool {
				return c.Encoding == nil && c.Expect.Outcome == "decoded"
			},
			"a present-but-empty header": func(c encodingCase) bool {
				return c.Encoding != nil && *c.Encoding == "" && c.Expect.Outcome == "decoded"
			},
			"the explicit identity token as a no-op": func(c encodingCase) bool {
				return usesCoding(c, "identity") && len(layersOf(c)) == 0 &&
					c.Expect.Outcome == "decoded"
			},
			"case-insensitive, whitespace-tolerant token matching": func(c encodingCase) bool {
				if c.Encoding == nil {
					return false
				}
				e := *c.Encoding
				return e != strings.ToLower(strings.TrimSpace(e)) && c.Expect.Outcome != "error"
			},
			"gzip": func(c encodingCase) bool {
				return usesCoding(c, "gzip") && c.Expect.Outcome == "decoded"
			},
			"the x-gzip alias": func(c encodingCase) bool {
				return usesCoding(c, "x-gzip") && c.Expect.Outcome == "decoded"
			},
			"zstd": func(c encodingCase) bool {
				return usesCoding(c, "zstd") && c.Expect.Outcome == "decoded"
			},
			"stacked layers peeled right-to-left": func(c encodingCase) bool {
				return len(layersOf(c)) > 1 && c.Expect.Outcome == "decoded"
			},
			"identity mixed in with a real coding": func(c encodingCase) bool {
				return usesCoding(c, "identity") && len(layersOf(c)) == 1
			},
			"salvage of a truncated gzip stream": func(c encodingCase) bool {
				return c.Expect.Outcome == "salvaged" && usesCoding(c, "gzip")
			},
			"salvage of a truncated zstd stream": func(c encodingCase) bool {
				return c.Expect.Outcome == "salvaged" && usesCoding(c, "zstd")
			},
			"salvage refused when the stream produced nothing": func(c encodingCase) bool {
				return c.Category == "salvage" && c.Expect.Outcome == "error"
			},
			"a body at exactly the output cap": func(c encodingCase) bool {
				b := c.Body.Build
				return b != nil && b.Plaintext.RepeatByte != nil &&
					b.Plaintext.RepeatByte.Count == capture.MaxDecompressedBytes &&
					c.Expect.Outcome == "decoded"
			},
			"a body one byte over the output cap": func(c encodingCase) bool {
				return c.Expect.Outcome == "error" && c.Expect.Error.Class == "oversize"
			},
			"a coding with no decoder": func(c encodingCase) bool {
				return c.Expect.Outcome == "error" && c.Expect.Error.Class == "unsupported"
			},
			"a corrupt body under a supported coding": func(c encodingCase) bool {
				return c.Expect.Outcome == "error" && c.Expect.Error.Class == "undecodable" &&
					c.Category == "error" && c.Body.BytesB64 != nil && *c.Body.BytesB64 != ""
			},
			"an error naming both the failing token and the whole header": func(c encodingCase) bool {
				return c.Expect.Outcome == "error" && len(c.Expect.Error.MessageContains) > 1
			},
			"a recorded contested decision": func(c encodingCase) bool {
				return len(c.Contested) > 0
			},
		}

		for name, holds := range rules {
			what, rule := name, holds
			It(what, func() {
				Expect(covers(rule)).To(BeTrue(),
					"no case covers %s any more. Deleting the last case for a policy\n"+
						"  rule silently un-pins it for every consumer, including %s.", what, otherHome)
			})
		}
	})
})
