package backfill

// Executable scrub gate for the committed wire recordings
// (fixtures/recordings/<set>/turn-*).
//
// The recordings are captured from real upstream traffic, so their provenance
// rests entirely on review — and review is exactly what missed a batch of
// identifiers the first time these landed. The recorder redacts credential
// *headers* on both sides; it does not touch bodies, response metadata, or
// non-credential identifiers, and a human reading 17 bundles of base64 will
// not reliably spot an account UUID buried in a JSON-encoded metadata field.
//
// So the bar stops being "somebody looked" and becomes this test. It is
// deliberately shaped around identifier *forms* rather than known values: the
// point is to catch the next capture's leak, not to re-assert that this one is
// clean. A new corpus that carries a machine fingerprint, an account or org
// UUID, a provider request id, a trace context, an e-mail address or a home
// directory path fails here rather than in a public repository.
//
// When a form is legitimately present, allow it narrowly and say why — see
// allowedSessionIDs below. Widening a pattern to make a red test pass defeats
// the gate.

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// recordingsRoot locates fixtures/recordings relative to this file, so the
// test does not depend on the working directory `go test` was invoked from.
func recordingsRoot() string {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		panic("runtime.Caller failed")
	}
	return filepath.Join(filepath.Dir(file), "..", "..", "fixtures", "recordings")
}

// scrubPattern is one identifier form the corpus must not contain.
type scrubPattern struct {
	name string
	re   *regexp.Regexp
	// group selects which submatch carries the identifying value. Zero means
	// the whole match. Patterns that need surrounding context to recognise a
	// value — a bare 64-hex string is only meaningful as a device id when it
	// sits behind that key — match the context and report the value, so the
	// allow-list below can name a synthetic stand-in without also having to
	// reproduce its JSON escaping.
	group int
	// why explains what an operator is looking at when this fires.
	why string
}

var scrubPatterns = []scrubPattern{
	{
		name: "account/org UUID",
		// Any UUID. Session ids are the one legitimate UUID in this corpus
		// and are allowed explicitly below; everything else — account_uuid,
		// anthropic-organization-id — identifies a person or a tenant.
		re:  regexp.MustCompile(`\b[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\b`),
		why: "identifies an account or organization",
	},
	{
		name: "device fingerprint",
		// Claude Code sends metadata.user_id as a JSON blob carrying a stable
		// 64-hex device id — a fingerprint of the capturing machine.
		re:    regexp.MustCompile(`\\?"device_id\\?"\s*:\s*\\?"([0-9a-f]{64})\\?"`),
		group: 1,
		why:   "fingerprints the machine the capture ran on",
	},
	{
		name: "provider request id",
		re:   regexp.MustCompile(`\breq_(?:[A-Za-z0-9]*[a-z][A-Za-z0-9]*)\b`),
		why:  "correlates the recording with provider-side telemetry",
	},
	{
		name: "e-mail address",
		re:   regexp.MustCompile(`[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}`),
		why:  "personal or operator identity",
	},
	{
		name: "home directory path",
		re:   regexp.MustCompile(`/(?:Users|home)/[A-Za-z0-9._-]+`),
		why:  "identifies the operator and leaks local layout",
	},
	{
		name: "bearer/API credential",
		re:   regexp.MustCompile(`\b(?:sk-[A-Za-z0-9-]{12,}|eyJ[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,})`),
		why:  "a credential",
	},
}

// allowed lists exact strings that match a pattern but are known-safe, each
// with a reason. Everything here is either synthetic or scoped to the fixture
// session itself.
var allowed = map[string]string{
	// Substituted stand-ins. Present by design; see fixtures/manifest.json.
	"00000000-0000-4000-8000-000000000000": "synthetic organization id",
	"00000000-0000-4000-8000-000000000001": "synthetic account uuid",
	"redacted@examples.test":               "synthetic operator address",
	strings.Repeat("0", 64):                "synthetic device id",

	// Claude Code's own commit-message boilerplate in the system prompt.
	"noreply@anthropic.com": "product boilerplate, not operator identity",
}

// allowedSessionIDs are the harness session UUIDs belonging to the fixture
// sessions themselves. They identify nothing outside this synthetic capture,
// and they are load-bearing: the envelope and sub-thread attribution the
// corpus exercises are keyed on them, so replacing them would make the
// recordings stop describing a coherent session.
//
// They are read from each bundle's own x-tapes-harness-session-id rather than
// hardcoded, so adding a capture set does not require editing this list — but
// a UUID that is NOT some bundle's session id still fails.
func allowedSessionIDs(bundles []bundleFiles) map[string]struct{} {
	out := map[string]struct{}{}
	for _, b := range bundles {
		for _, kv := range b.headers {
			k := strings.ToLower(kv[0])
			if k == "x-tapes-harness-session-id" || k == "x-claude-code-session-id" {
				out[kv[1]] = struct{}{}
			}
		}
	}
	return out
}

type bundleFiles struct {
	name    string
	headers [][2]string
	// texts maps a human label ("request body", "meta.json") to content.
	texts map[string]string
}

// loadRecordingBundles reads every committed bundle. Response streams are
// stored gzipped; they are covered by the reducer tests and are not decoded
// here — this gate is about the plaintext surfaces a reader would scan.
func loadRecordingBundles() []bundleFiles {
	root := recordingsRoot()
	sets, err := os.ReadDir(root)
	if err != nil {
		panic(fmt.Sprintf("read %s: %v", root, err))
	}

	var out []bundleFiles
	for _, set := range sets {
		if !set.IsDir() {
			continue
		}
		turns, err := os.ReadDir(filepath.Join(root, set.Name()))
		if err != nil {
			panic(err)
		}
		for _, turn := range turns {
			if !turn.IsDir() || !strings.HasPrefix(turn.Name(), "turn-") {
				continue
			}
			dir := filepath.Join(root, set.Name(), turn.Name())

			metaBytes, err := os.ReadFile(filepath.Join(dir, "meta.json"))
			if err != nil {
				panic(err)
			}
			reqBytes, err := os.ReadFile(filepath.Join(dir, "request.json"))
			if err != nil {
				panic(err)
			}
			var req struct {
				Headers [][2]string `json:"headers"`
				BodyB64 string      `json:"body_b64"`
			}
			if err := json.Unmarshal(reqBytes, &req); err != nil {
				panic(err)
			}
			body, err := base64.StdEncoding.DecodeString(req.BodyB64)
			if err != nil {
				panic(err)
			}

			out = append(out, bundleFiles{
				name:    filepath.Join(set.Name(), turn.Name()),
				headers: req.Headers,
				texts: map[string]string{
					"meta.json":       string(metaBytes),
					"request headers": string(mustJSON(req.Headers)),
					"request body":    string(body),
				},
			})
		}
	}
	if len(out) == 0 {
		panic("no recording bundles found under " + root)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].name < out[j].name })
	return out
}

func mustJSON(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}

var _ = Describe("committed wire recordings", func() {
	It("carry no identifying values", func() {
		bundles := loadRecordingBundles()
		sessionIDs := allowedSessionIDs(bundles)

		var findings []string
		for _, b := range bundles {
			for label, text := range b.texts {
				for _, p := range scrubPatterns {
					for _, m := range p.re.FindAllStringSubmatch(text, -1) {
						hit := m[0]
						if p.group > 0 && p.group < len(m) {
							hit = m[p.group]
						}
						if _, ok := allowed[hit]; ok {
							continue
						}
						if _, ok := sessionIDs[hit]; ok {
							continue
						}
						findings = append(findings,
							fmt.Sprintf("%s: %s in %s: %q (%s)", b.name, p.name, label, hit, p.why))
					}
				}
			}
		}
		sort.Strings(findings)
		Expect(findings).To(BeEmpty(),
			"recordings must not carry identifying values — scrub them at the source and record the substitution in fixtures/manifest.json")
	})
})
