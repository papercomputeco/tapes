package rawequiv_test

import (
	"context"
	"encoding/json"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/ingest"
	"github.com/papercomputeco/tapes/pkg/rawequiv"
)

var _ = Describe("Raw-response equivalence", func() {
	ctx := context.Background()
	opts := rawequiv.Options{MaxDiffs: 10}

	Describe("a dual-send turn whose bytes still reduce the same way", func() {
		It("is equivalent for every committed recording", func() {
			for _, rec := range loadRecordings() {
				out := rawequiv.Check(ctx, rec.row(), opts)
				Expect(out.Class).To(Equal(rawequiv.ClassEquivalent),
					"%s: %s %s %v", rec.name, out.Class, out.Reason, out.Diffs)
			}
		})

		It("reports which capture-side stamps mode=raw could restore", func() {
			rec := loadRecordings()[0]
			out := rawequiv.Check(ctx, rec.row(), opts)

			Expect(out.Stamps).NotTo(BeNil())
			// These recordings carry duration_ms and ts_request, so both
			// stamps resolve from the envelope rather than falling back.
			Expect(out.Stamps.Duration).To(Equal(ingest.StampSourceElapsed))
			Expect(out.Stamps.CreatedAt).To(Equal(ingest.StampSourceTsRequest))
		})

		It("stays equivalent through a jsonb-style normalization of the stored side", func() {
			// Postgres jsonb reorders keys, drops insignificant whitespace and
			// renormalizes numbers. None of that is a reduction difference, so
			// a comparison that reported it would be unusable against a real
			// database. Re-encoding the stored side through a generic tree
			// reproduces exactly those transformations.
			rec := loadRecordings()[0]
			row := rec.row()

			var tree map[string]any
			Expect(json.Unmarshal(row.StoredReduction, &tree)).To(Succeed())
			reordered, err := json.Marshal(tree)
			Expect(err).NotTo(HaveOccurred())
			row.StoredReduction = reordered

			Expect(rawequiv.Check(ctx, row, opts).Class).To(Equal(rawequiv.ClassEquivalent))
		})

		It("tolerates only the two documented time fields", func() {
			Expect(rawequiv.TolerableFields).To(HaveLen(2))
			names := []string{
				rawequiv.TolerableFields[0].String(),
				rawequiv.TolerableFields[1].String(),
			}
			Expect(names).To(ConsistOf("created_at", "usage.total_duration_ns"))
			for _, f := range rawequiv.TolerableFields {
				Expect(f.Why).NotTo(BeEmpty(), "%s needs a justification", f)
			}
		})
	})

	Describe("a genuinely divergent turn", func() {
		It("is reported when the stored reduction lost a field the bytes carry", func() {
			rec := loadRecordings()[0]
			tree := storedTree(rec.row())
			tree["stop_reason"] = "wrong_reason"

			out := rawequiv.Check(ctx, withStored(rec.row(), tree), opts)

			Expect(out.Class).To(Equal(rawequiv.ClassDivergent))
			Expect(out.Diffs).NotTo(BeEmpty())

			paths := make([]string, 0, len(out.Diffs))
			for _, d := range out.Diffs {
				paths = append(paths, d.Path)
			}
			Expect(paths).To(ContainElement("stop_reason"))
		})

		It("is reported when a content block is missing from the stored reduction", func() {
			rec := loadRecordings()[0]
			tree := storedTree(rec.row())

			message, ok := tree["message"].(map[string]any)
			Expect(ok).To(BeTrue())
			content, ok := message["content"].([]any)
			Expect(ok).To(BeTrue())
			Expect(content).NotTo(BeEmpty())
			message["content"] = content[:len(content)-1]

			out := rawequiv.Check(ctx, withStored(rec.row(), tree), opts)

			Expect(out.Class).To(Equal(rawequiv.ClassDivergent))
			Expect(out.Diffs).To(ContainElement(HaveField("Kind", rawequiv.DiffLength)))
		})

		It("never prints message text, even when the text is what differs", func() {
			const secret = "SUPER-SECRET-PROMPT-TEXT-THAT-MUST-NOT-LEAK"

			rec := loadRecordings()[0]
			tree := storedTree(rec.row())
			message, ok := tree["message"].(map[string]any)
			Expect(ok).To(BeTrue())
			content, ok := message["content"].([]any)
			Expect(ok).To(BeTrue())
			Expect(content).NotTo(BeEmpty())

			block, ok := content[0].(map[string]any)
			Expect(ok).To(BeTrue())
			block["text"] = secret

			out := rawequiv.Check(ctx, withStored(rec.row(), tree), opts)
			Expect(out.Class).To(Equal(rawequiv.ClassDivergent))

			report := rawequiv.NewReport(rawequiv.Window{}, 10)
			report.Add(out)

			var text strings.Builder
			report.WriteText(&text)
			Expect(text.String()).NotTo(ContainSubstring(secret))
			Expect(text.String()).To(ContainSubstring("message.content[0].text"))

			machine, err := json.Marshal(report)
			Expect(err).NotTo(HaveOccurred())
			Expect(string(machine)).NotTo(ContainSubstring(secret))
		})

		It("bounds the number of differences it records", func() {
			rec := loadRecordings()[0]
			tree := storedTree(rec.row())
			// Replace the whole message so nearly everything under it differs.
			tree["message"] = map[string]any{
				"role": "system",
				"content": []any{
					map[string]any{"type": "text", "text": "a"},
					map[string]any{"type": "text", "text": "b"},
					map[string]any{"type": "text", "text": "c"},
				},
			}

			out := rawequiv.Check(ctx, withStored(rec.row(), tree), rawequiv.Options{MaxDiffs: 2})
			Expect(out.Class).To(Equal(rawequiv.ClassDivergent))
			Expect(len(out.Diffs)).To(BeNumerically("<=", 2))
			Expect(out.DiffsTruncated).To(BeTrue())
		})
	})

	Describe("a turn whose stored bytes cannot be decoded", func() {
		It("is classed undecodable rather than divergent", func() {
			rec := loadRecordings()[0]
			row := rec.row()
			// brotli is not a supported encoding; the decoder errors rather
			// than handing compressed bytes to a reducer expecting text.
			row.RawResponseEncoding = "br"

			out := rawequiv.Check(ctx, row, opts)

			Expect(out.Class).To(Equal(rawequiv.ClassUndecodable))
			Expect(out.Reason).To(ContainSubstring("br"))
			Expect(out.Class.Blocking()).To(BeTrue(),
				"an undecodable row would carry no reduction at all under mode=raw")
		})

		It("is classed undecodable when the bytes are corrupt for their encoding", func() {
			rec := loadRecordings()[0]
			row := rec.row()
			row.RawResponse = []byte("this is not gzip")
			row.RawResponseEncoding = "gzip"

			Expect(rawequiv.Check(ctx, row, opts).Class).To(Equal(rawequiv.ClassUndecodable))
		})
	})

	Describe("rows with nothing to compare", func() {
		It("skips a turn whose verbatim bytes were withheld or dropped", func() {
			rec := loadRecordings()[0]
			row := rec.row()
			// raw_response_dropped is set both when a producer withheld the
			// bytes at the transport limit and when ingest dropped them over
			// its storage cap. Either way the bytes are gone.
			row.RawResponse = nil
			row.RawResponseDropped = true

			out := rawequiv.Check(ctx, row, opts)

			Expect(out.Class).To(Equal(rawequiv.ClassSkippedDropped))
			Expect(out.Class.Blocking()).To(BeFalse())
			Expect(out.Stamps).To(BeNil(), "no reduction should have been attempted")
		})

		It("distinguishes a dropped turn from one that never carried bytes", func() {
			rec := loadRecordings()[0]
			row := rec.row()
			row.RawResponse = nil

			Expect(rawequiv.Check(ctx, row, opts).Class).To(Equal(rawequiv.ClassSkippedNoRaw))
		})

		It("skips a raw-only turn, which has no adapter reduction to compare", func() {
			rec := loadRecordings()[0]
			row := rec.row()
			row.StoredReduction = nil

			Expect(rawequiv.Check(ctx, row, opts).Class).To(Equal(rawequiv.ClassSkippedNoReduction))
		})

		It("reports a provider with no server-side reducer as blocking", func() {
			rec := loadRecordings()[0]
			row := rec.row()
			row.Provider = "ollama"

			out := rawequiv.Check(ctx, row, opts)

			Expect(out.Class).To(Equal(rawequiv.ClassNoReducer))
			Expect(out.Class.Blocking()).To(BeTrue(),
				"mode=raw cannot serve a provider it cannot reduce")
		})
	})

	Describe("the report", func() {
		It("counts every examined row exactly once and gates on blocking rows", func() {
			rec := loadRecordings()[0]

			report := rawequiv.NewReport(rawequiv.Window{Limit: 4}, 10)
			report.Add(rawequiv.Check(ctx, rec.row(), opts))

			dropped := rec.row()
			dropped.RawResponse = nil
			dropped.RawResponseDropped = true
			report.Add(rawequiv.Check(ctx, dropped, opts))

			undecodable := rec.row()
			undecodable.RawResponseEncoding = "br"
			report.Add(rawequiv.Check(ctx, undecodable, opts))

			Expect(report.Total).To(Equal(3))
			Expect(report.Counts[rawequiv.ClassEquivalent]).To(Equal(1))
			Expect(report.Counts[rawequiv.ClassSkippedDropped]).To(Equal(1))
			Expect(report.Counts[rawequiv.ClassUndecodable]).To(Equal(1))
			Expect(report.Blocking()).To(Equal(1))

			total := 0
			for _, n := range report.Counts {
				total += n
			}
			Expect(total).To(Equal(report.Total), "classes must partition the rows")
		})

		It("says nothing was proven when the window held no comparable turns", func() {
			rec := loadRecordings()[0]
			row := rec.row()
			row.RawResponse = nil

			report := rawequiv.NewReport(rawequiv.Window{}, 10)
			report.Add(rawequiv.Check(ctx, row, opts))

			var text strings.Builder
			report.WriteText(&text)
			Expect(text.String()).To(ContainSubstring("Nothing was proven"))
		})

		It("carries its own definition of equivalence", func() {
			report := rawequiv.NewReport(rawequiv.Window{}, 10)
			var text strings.Builder
			report.WriteText(&text)

			Expect(text.String()).To(ContainSubstring("created_at"))
			Expect(text.String()).To(ContainSubstring("usage.total_duration_ns"))
		})
	})
})
