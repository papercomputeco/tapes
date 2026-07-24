package devcmder

// Tier-1 re-derive gate (PCC-687): the derived projection is a pure,
// deterministic function of the raw capture layer, so rendering a corpus
// through the deriver and the real API renderers twice — in the same
// binary — must be byte-identical. This catches map-iteration order,
// time.Now(), RNG, and fold races at the WIRE layer, downstream of both
// the merkle-node idempotency specs (pkg/derive corpus_test) and the
// SpanSet golden (clone_equivalence_test), neither of which serializes
// the API response.
//
// It is self-referential — render twice, diff — so there is no golden to
// re-pin: a failure is always real nondeterminism, never a stale fixture.

import (
	"fmt"
	"path/filepath"
	"runtime"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// corpusDir resolves the shared corpus directory relative to this test
// file, so the gate is independent of the test's working dir. The corpora
// live in one place, pkg/seed/corpus (embedded by the demo seed), rather
// than being duplicated under pkg/derive/testdata.
func corpusDir() string {
	_, file, _, ok := runtime.Caller(0)
	Expect(ok).To(BeTrue(), "runtime.Caller failed")
	return filepath.Join(filepath.Dir(file), "..", "..", "..", "pkg", "seed", "corpus")
}

var _ = Describe("trace-fixtures re-derive determinism (Tier 1)", func() {
	DescribeTable("rendering a corpus twice is byte-identical across every fixture artifact",
		func(corpus string) {
			path := filepath.Join(corpusDir(), corpus)

			arts1, sum1, err := buildFixtureArtifacts(path)
			Expect(err).NotTo(HaveOccurred())
			arts2, sum2, err := buildFixtureArtifacts(path)
			Expect(err).NotTo(HaveOccurred())

			// Guard against a vacuous pass: an empty render diffs clean.
			Expect(sum1.traces).To(BeNumerically(">", 0), "corpus derived to zero traces")
			Expect(sum2).To(Equal(sum1))
			Expect(arts2).To(HaveLen(len(arts1)))

			for i := range arts1 {
				Expect(arts2[i].name).To(Equal(arts1[i].name))

				b1, err := marshalFixture(arts1[i].v)
				Expect(err).NotTo(HaveOccurred())
				b2, err := marshalFixture(arts2[i].v)
				Expect(err).NotTo(HaveOccurred())

				if diff := firstLineDiff(b1, b2); diff != "" {
					Fail(fmt.Sprintf("nondeterministic render of %s (%s):\n%s",
						arts1[i].name, corpus, diff))
				}
			}
		},
		Entry("cb9a87e5 — plan mode, 2 subagents", "corpus-cb9a87e5.jsonl.gz"),
		Entry("9fec0da7 — compaction, multi-model", "corpus-9fec0da7.jsonl.gz"),
		Entry("0440f43d — 19 sessions, scale", "corpus-0440f43d.jsonl.gz"),
	)
})

// firstLineDiff localizes the first divergence between two rendered
// fixtures into a short, readable message, so a nondeterminism failure
// points at the exact JSON line rather than dumping the whole payload.
// Returns "" when the two are byte-identical.
func firstLineDiff(a, b []byte) string {
	if string(a) == string(b) {
		return ""
	}
	al := strings.Split(string(a), "\n")
	bl := strings.Split(string(b), "\n")
	for i := 0; i < len(al) || i < len(bl); i++ {
		var x, y string
		if i < len(al) {
			x = al[i]
		}
		if i < len(bl) {
			y = bl[i]
		}
		if x != y {
			return fmt.Sprintf("  first diverged at line %d\n    run 1: %s\n    run 2: %s",
				i+1, truncFixtureLine(x), truncFixtureLine(y))
		}
	}
	return "  fixtures differ in trailing bytes"
}

func truncFixtureLine(s string) string {
	const maxLen = 240
	if len(s) > maxLen {
		return s[:maxLen] + "…"
	}
	return s
}
