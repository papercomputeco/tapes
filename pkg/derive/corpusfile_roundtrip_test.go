package derive_test

// Round-trip gate for the corpus writer that backs `tapes dev
// dump-corpus`. A corpus file is the deriver's replayable input, so the
// writer must be a true inverse of the loader: dumping rows and loading
// them back must reproduce both the raw decomposition (wire vs
// transcript, in order) and — the property that actually matters — a
// byte-identical derived projection. This is the DB-free half of the
// dump-corpus verification; the clearing exercises the DB scan itself.

import (
	"bytes"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/derive"
	"github.com/papercomputeco/tapes/pkg/storage"
)

// deriveFrom runs the full projection pipeline over an already-loaded
// raw decomposition, mirroring deriveCorpus without the on-disk load.
func deriveFrom(wire, transcripts []storage.RawTurnRecord) (*derive.DerivedSet, *derive.SpanSet) {
	set, err := derive.BuildDerivedSet(wire, "")
	Expect(err).NotTo(HaveOccurred())
	files := make([]*derive.TranscriptFile, 0, len(transcripts))
	for i := range transcripts {
		file, err := derive.ParseTranscriptFile(&transcripts[i])
		Expect(err).NotTo(HaveOccurred())
		files = append(files, file)
	}
	derive.ReconcileTranscripts(set, files)
	return set, derive.EmitSpans(set)
}

var _ = Describe("corpus writer round-trip (dump-corpus)", func() {
	DescribeTable("WriteCorpus then LoadCorpus reproduces the rows and the projection",
		func(path string) {
			wire, transcripts := loadCorpus(path)

			// The dump command streams one flat slice of a session's
			// rows (wire + transcript) in id order; WriteCorpus is the
			// batch form of that stream.
			flat := make([]storage.RawTurnRecord, 0, len(wire)+len(transcripts))
			flat = append(flat, wire...)
			flat = append(flat, transcripts...)

			var buf bytes.Buffer
			Expect(derive.WriteCorpus(&buf, flat)).To(Succeed())

			wire2, transcripts2, err := derive.LoadCorpus(&buf)
			Expect(err).NotTo(HaveOccurred())

			// Structural inverse: the source split and row counts survive.
			Expect(wire2).To(HaveLen(len(wire)))
			Expect(transcripts2).To(HaveLen(len(transcripts)))

			// Semantic inverse: the reloaded rows derive to a
			// byte-identical projection.
			setA, spansA := deriveFrom(wire, transcripts)
			setB, spansB := deriveFrom(wire2, transcripts2)
			Expect(canonicalProjection(setB, spansB)).To(Equal(canonicalProjection(setA, spansA)))
		},
		Entry("cb9a87e5 — plan mode, 2 subagents", "testdata/corpus-cb9a87e5.jsonl.gz"),
		Entry("9fec0da7 — compaction, multi-model", "testdata/corpus-9fec0da7.jsonl.gz"),
		Entry("0440f43d — 19 sessions, scale", "testdata/corpus-0440f43d.jsonl.gz"),
	)
})
