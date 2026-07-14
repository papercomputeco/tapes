package devcmder

// check-invariants regression: the structural invariants hold on a real
// derived composite (the deriver's output for a curated corpus), and each
// invariant actually fires when its property is broken — so the checker
// can't silently pass on a regression it was built to catch.

import (
	"encoding/json"
	"path/filepath"
	"runtime"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// compositeFixtureBytes renders a corpus and returns the full composite
// (session-traces-<s>.json) JSON — the same bytes trace-fixtures writes.
func compositeFixtureBytes(corpus string) []byte {
	_, file, _, ok := runtime.Caller(0)
	Expect(ok).To(BeTrue())
	path := filepath.Join(filepath.Dir(file), "..", "..", "..", "pkg", "derive", "testdata", corpus)

	arts, _, err := buildFixtureArtifacts(path)
	Expect(err).NotTo(HaveOccurred())
	// arts[0] is the full composite (see buildFixtureArtifacts).
	Expect(arts[0].name).To(HavePrefix("session-traces-"))
	Expect(arts[0].name).NotTo(HaveSuffix(".slim.json"))
	b, err := marshalFixture(arts[0].v)
	Expect(err).NotTo(HaveOccurred())
	return b
}

var _ = Describe("check-invariants", func() {
	const corpus = "corpus-cb9a87e5.jsonl.gz"

	It("passes on a real derived composite", func() {
		v, err := checkCompositeBytes(compositeFixtureBytes(corpus))
		Expect(err).NotTo(HaveOccurred())
		Expect(v).To(BeEmpty(), "unexpected invariant violations: %v", v)
	})

	// Each mutation breaks exactly one invariant; the checker must report it.
	DescribeTable("catches a broken invariant",
		func(mutate func(map[string]any), wantSubstr string) {
			var doc map[string]any
			Expect(json.Unmarshal(compositeFixtureBytes(corpus), &doc)).To(Succeed())
			mutate(doc)
			raw, err := json.Marshal(doc)
			Expect(err).NotTo(HaveOccurred())

			v, err := checkCompositeBytes(raw)
			Expect(err).NotTo(HaveOccurred())
			Expect(v).To(ContainElement(ContainSubstring(wantSubstr)))
		},
		Entry("wrong schema", func(d map[string]any) {
			d["schema"] = "1999-01-01"
		}, "schema ="),
		Entry("non-agent first span", func(d map[string]any) {
			firstSpan(d)["kind"] = "llm"
		}, "want root agent span"),
		Entry("out-of-order seq", func(d map[string]any) {
			spans := traceSpans(d, 0)
			spans[1].(map[string]any)["seq"] = float64(-1)
		}, "not seq-ordered"),
		Entry("dangling link", func(d map[string]any) {
			d["links"] = append(d["links"].([]any), map[string]any{
				"kind": "emits", "from_trace_id": "ghost", "from_span_id": "x",
				"to_trace_id": "ghost", "to_span_id": "y",
			})
		}, "dangling"),
		Entry("kind_counts drift", func(d map[string]any) {
			rollup := d["session"].(map[string]any)["rollup"].(map[string]any)
			kc := rollup["kind_counts"].(map[string]any)
			for k := range kc {
				kc[k] = kc[k].(float64) + 9
				break
			}
		}, "kind_counts sums to"),
		Entry("resurrected dropped field", func(d map[string]any) {
			firstSpan(d)["metadata"] = map[string]any{"leaked": true}
		}, `dropped field "metadata"`),
	)
})

func traceSpans(d map[string]any, ti int) []any {
	traces := d["traces"].([]any)
	return traces[ti].(map[string]any)["spans"].([]any)
}

func firstSpan(d map[string]any) map[string]any {
	return traceSpans(d, 0)[0].(map[string]any)
}
