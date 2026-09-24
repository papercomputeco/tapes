package postgres_test

// Row-iterating span readers: the composite session read and the
// per-trace read must stream from pgx rows with one span live at a time,
// in the same order the slice readers serve, and resume from a cursor
// exactly. These specs seed spans_20260615 directly — the deriver owns those
// rows in production, but the property under test is the read, so the rows
// are stated rather than derived.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"iter"
	"runtime"
	"strings"
	"time"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/storage"
	"github.com/papercomputeco/tapes/pkg/storage/postgres"
)

var _ = Describe("span iterators", func() {
	var (
		driver    storage.Driver
		pgDriver  *postgres.Driver
		ctx       context.Context
		orgID     string
		sessionID string
	)

	base := time.Date(2026, 6, 1, 9, 0, 0, 0, time.UTC)

	BeforeEach(func() {
		ctx = context.Background()
		var err error
		driver, err = postgres.NewDriver(ctx, testPostgresDSN)
		Expect(err).NotTo(HaveOccurred())
		var ok bool
		pgDriver, ok = driver.(*postgres.Driver)
		Expect(ok).To(BeTrue())

		// A fresh org and session per spec isolate these rows without
		// truncating tables other suites use; spans hang off the session
		// row through ON DELETE CASCADE, so the AfterEach delete is enough.
		orgID = newTestOrgID()
		sessionID = uuid.New().String()
		_, err = pgDriver.DB().Exec(ctx, `
			INSERT INTO sessions (id, org_id, auth_subject, harness_id, harness_session_id, started_at, last_seen_at)
			VALUES ($1, $2, 'test', 'claude', $3, $4, $4)`,
			mustUUID(sessionID), mustUUID(orgID), "iter-"+sessionID, base)
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		if pgDriver != nil {
			_, err := pgDriver.DB().Exec(ctx, "DELETE FROM sessions WHERE id = $1", mustUUID(sessionID))
			Expect(err).NotTo(HaveOccurred())
		}
		if driver != nil {
			driver.Close()
		}
	})

	insertTurn := func(traceID string) {
		_, err := pgDriver.DB().Exec(ctx, `
			INSERT INTO span_turns_20260615
			    (org_id, trace_id, session_id, started_at, duration_ns,
			     total_input_tokens, total_output_tokens, total_cost_usd, tool_calls)
			VALUES ($1, $2, $3, $4, 0, 0, 0, 0, 0)`,
			mustUUID(orgID), traceID, mustUUID(sessionID), base)
		Expect(err).NotTo(HaveOccurred())
	}

	insertSpan := func(traceID, spanID string, seq int64, startedAt time.Time, input string) {
		_, err := pgDriver.DB().Exec(ctx, `
			INSERT INTO spans_20260615 (org_id, trace_id, span_id, session_id, kind, started_at, seq, input)
			VALUES ($1, $2, $3, $4, 'llm', $5, $6, $7::jsonb)`,
			mustUUID(orgID), traceID, spanID, mustUUID(sessionID), startedAt, seq, input)
		Expect(err).NotTo(HaveOccurred())
	}

	// insertPreviewedSpan writes a span the way the deriver does now: the
	// payload and its stored previews side by side. The preview is
	// deliberately not the projection of the payload so a read that
	// recomputed it from input/output would be told apart from one that
	// served the stored column.
	insertPreviewedSpan := func(traceID, spanID string, seq int64, input, output, inputPreview, outputPreview string) {
		_, err := pgDriver.DB().Exec(ctx, `
			INSERT INTO spans_20260615
			    (org_id, trace_id, span_id, session_id, kind, started_at, seq, input, output, input_preview, output_preview)
			VALUES ($1, $2, $3, $4, 'tool', $5, $6, $7::jsonb, $8::jsonb, $9::jsonb, $10::jsonb)`,
			mustUUID(orgID), traceID, spanID, mustUUID(sessionID), base.Add(time.Duration(seq)*time.Second), seq,
			input, output, inputPreview, outputPreview)
		Expect(err).NotTo(HaveOccurred())
	}

	// seedOrderingFixture writes rows that only the full four-column key
	// orders: trace ids that sort differently from insertion order, a
	// pre-seq trace (every seq 0) that started_at must order, and a tie on
	// (seq, started_at) that only span_id breaks. Returns the span count.
	seedOrderingFixture := func() int {
		for _, t := range []string{"trc-b", "trc-a", "trc-c"} {
			insertTurn(t)
		}
		n := 0
		// trc-c: ordinary derived seqs, inserted out of seq order.
		for _, seq := range []int64{2, 0, 1} {
			insertSpan("trc-c", fmt.Sprintf("c-%d", seq), seq, base.Add(time.Duration(seq)*time.Second), `[{"type":"text","text":"c"}]`)
			n++
		}
		// trc-a: pre-seq rows (seq 0) — started_at orders them.
		for _, ms := range []int{300, 100, 200} {
			insertSpan("trc-a", fmt.Sprintf("a-%03d", ms), 0, base.Add(time.Duration(ms)*time.Millisecond), `[{"type":"text","text":"a"}]`)
			n++
		}
		// trc-b: two rows tied on (seq, started_at); span_id is the last key.
		for _, id := range []string{"b-2", "b-1"} {
			insertSpan("trc-b", id, 0, base, `[{"type":"text","text":"b"}]`)
			n++
		}
		insertSpan("trc-b", "b-0", 1, base, `[{"type":"text","text":"b"}]`)
		n++
		return n
	}

	// seedBulk writes n llm spans, spansPerTrace to a trace, each carrying
	// an input block of payloadBytes text.
	seedBulk := func(n, spansPerTrace, payloadBytes int) {
		traces := (n + spansPerTrace - 1) / spansPerTrace
		for t := range traces {
			insertTurn(fmt.Sprintf("trc-%04d", t))
		}
		payload := `[{"type":"text","text":"` + strings.Repeat("x", payloadBytes) + `"}]`
		_, err := pgDriver.DB().Exec(ctx, `
			INSERT INTO spans_20260615 (org_id, trace_id, span_id, session_id, kind, started_at, seq, input)
			SELECT $1,
			       'trc-' || lpad((((g - 1) / $5))::text, 4, '0'),
			       'span-' || lpad(g::text, 6, '0'),
			       $2, 'llm',
			       $3::timestamptz + (g * interval '1 millisecond'),
			       (g - 1) % $5,
			       $4::jsonb
			FROM generate_series(1, $6) AS g`,
			mustUUID(orgID), mustUUID(sessionID), base, payload, spansPerTrace, n)
		Expect(err).NotTo(HaveOccurred())
	}

	// collect drains an iterator into a non-nil slice (an exhausted resume
	// compares equal to an empty tail).
	collect := func(seq iter.Seq2[storage.SpanRecord, error]) []storage.SpanRecord {
		out := []storage.SpanRecord{}
		for rec, err := range seq {
			Expect(err).NotTo(HaveOccurred())
			out = append(out, rec)
		}
		return out
	}

	cursorOf := func(rec storage.SpanRecord) storage.SpanCursor {
		return storage.SpanCursor{TraceID: rec.TraceID, Seq: rec.Seq, StartedAt: rec.StartedAt, SpanID: rec.SpanID}
	}

	It("holds one row at a time", func() {
		const (
			n            = 5000
			payloadBytes = 4096
		)
		seedBulk(n, 500, payloadBytes)

		// Warm-up pass: the pool connection and pgx's read buffers are
		// allocated on first use, and belong in the baseline rather than in
		// the growth being measured.
		Expect(collectCount(pgDriver.IterateSessionSpans(ctx, sessionID, storage.SpanCursor{}, storage.PayloadFull))).To(Equal(n))

		gcAndRead := func(m *runtime.MemStats) {
			// Two cycles: the first only moves sync.Pool contents (pgx's
			// buffer pool) to the victim cache; the second drops them.
			runtime.GC()
			runtime.GC()
			runtime.ReadMemStats(m)
		}

		var before, atFirst, atLast runtime.MemStats
		gcAndRead(&before)
		i := 0
		var rowBytes int
		for rec, err := range pgDriver.IterateSessionSpans(ctx, sessionID, storage.SpanCursor{}, storage.PayloadFull) {
			Expect(err).NotTo(HaveOccurred())
			i++
			switch i {
			case 1:
				rowBytes = len(rec.Input) + len(rec.Output) + len(rec.Usage) + len(rec.Verdict)
				Expect(rowBytes).To(BeNumerically(">=", payloadBytes))
				gcAndRead(&atFirst)
			case n:
				// Measured with the last record still live, not after the
				// loop has released everything.
				gcAndRead(&atLast)
				runtime.KeepAlive(rec)
			}
		}
		Expect(i).To(Equal(n))

		budget := int64(4 * rowBytes)
		growthFromBaseline := int64(atLast.HeapAlloc) - int64(before.HeapAlloc)
		growthAcrossStream := int64(atLast.HeapAlloc) - int64(atFirst.HeapAlloc)
		fmt.Fprintf(GinkgoWriter, "row payload %d B; heap growth baseline->last row %d B, first row->last row %d B; budget %d B\n",
			rowBytes, growthFromBaseline, growthAcrossStream, budget)
		// The scaling check: both readings are taken inside the loop with a
		// record live, so the only difference between them is what 4,999
		// intervening rows left behind — which must be nothing.
		Expect(growthAcrossStream).To(BeNumerically("<", budget),
			"live heap must not grow between the first and the last row")
		// The pre-iteration baseline additionally differs by pgx's pooled
		// read buffer (8–16 KiB, re-allocated inside the query once the GCs
		// above emptied its sync.Pool), which is why it gets a looser bound:
		// still a handful of rows, nowhere near the 20 MiB the session holds.
		Expect(growthFromBaseline).To(BeNumerically("<", 16*int64(rowBytes)),
			"live heap after 5,000 rows must stay within a few rows of the pre-iteration baseline")
	})

	It("streams the session in the composite order", func() {
		n := seedOrderingFixture()

		_, listed, _, err := pgDriver.ListSessionSpanModel(ctx, sessionID)
		Expect(err).NotTo(HaveOccurred())
		Expect(listed).To(HaveLen(n))

		iterated := collect(pgDriver.IterateSessionSpans(ctx, sessionID, storage.SpanCursor{}, storage.PayloadFull))
		Expect(iterated).To(Equal(listed))

		// Pin the key explicitly so a regression in the slice reader's ORDER
		// BY cannot mask one here.
		keys := make([]string, 0, len(iterated))
		for _, rec := range iterated {
			keys = append(keys, rec.TraceID+"/"+rec.SpanID)
		}
		Expect(keys).To(Equal([]string{
			"trc-a/a-100", "trc-a/a-200", "trc-a/a-300",
			"trc-b/b-1", "trc-b/b-2", "trc-b/b-0",
			"trc-c/c-0", "trc-c/c-1", "trc-c/c-2",
		}))

		// The per-trace reader agrees with its slice counterpart.
		for _, trace := range []string{"trc-a", "trc-b", "trc-c"} {
			byTrace, err := pgDriver.ListTraceSpans(ctx, orgID, trace)
			Expect(err).NotTo(HaveOccurred())
			Expect(collect(pgDriver.IterateTraceSpans(ctx, orgID, trace, storage.SpanCursor{}, storage.PayloadFull))).To(Equal(byTrace), trace)
		}
	})

	It("resumes after a cursor without gap or repeat", func() {
		n := seedOrderingFixture()
		full := collect(pgDriver.IterateSessionSpans(ctx, sessionID, storage.SpanCursor{}, storage.PayloadFull))
		Expect(full).To(HaveLen(n))

		// Stop mid-stream by breaking out of the range (which must close the
		// rows), take the cursor from the last record seen, and restart.
		const stopAfter = 4
		var head []storage.SpanRecord
		for rec, err := range pgDriver.IterateSessionSpans(ctx, sessionID, storage.SpanCursor{}, storage.PayloadFull) {
			Expect(err).NotTo(HaveOccurred())
			head = append(head, rec)
			if len(head) == stopAfter {
				break
			}
		}
		Eventually(func() int32 { return pgDriver.DB().Stat().AcquiredConns() }).Should(BeZero(),
			"breaking out of the range must release the pooled connection")

		tail := collect(pgDriver.IterateSessionSpans(ctx, sessionID, cursorOf(head[len(head)-1]), storage.PayloadFull))
		Expect(append(head, tail...)).To(Equal(full))

		// Every position is an exact restart point, including the ones that
		// only started_at or span_id distinguish.
		for k := range full {
			resumed := collect(pgDriver.IterateSessionSpans(ctx, sessionID, cursorOf(full[k]), storage.PayloadFull))
			Expect(resumed).To(Equal(full[k+1:]), "resume after %s/%s", full[k].TraceID, full[k].SpanID)
			seen := map[string]bool{}
			merged := append(append([]storage.SpanRecord{}, full[:k+1]...), resumed...)
			for _, rec := range merged {
				key := rec.TraceID + "/" + rec.SpanID
				Expect(seen).NotTo(HaveKey(key), "repeated span")
				seen[key] = true
			}
		}

		// Per-trace resumption at every position of every trace. trc-a and
		// trc-b are the tied-seq traces: a seq-only cursor would skip rows
		// there, so they are the ones that matter.
		for _, trace := range []string{"trc-a", "trc-b", "trc-c"} {
			byTrace := collect(pgDriver.IterateTraceSpans(ctx, orgID, trace, storage.SpanCursor{}, storage.PayloadFull))
			Expect(byTrace).To(HaveLen(3), trace)
			for k := range byTrace {
				resumed := collect(pgDriver.IterateTraceSpans(ctx, orgID, trace, cursorOf(byTrace[k]), storage.PayloadFull))
				Expect(resumed).To(Equal(byTrace[k+1:]), "resume %s after %s", trace, byTrace[k].SpanID)
			}
		}
	})

	Describe("payload=preview", func() {
		const (
			input         = `[{"type":"text","text":"the whole payload"}]`
			output        = `[{"type":"tool_result","tool_output":"the whole result"}]`
			inputPreview  = `[{"type":"text","text":"stored input preview"}]`
			outputPreview = `[{"type":"tool_result","tool_output":"stored output preview"}]`
		)

		decoded := func(raw json.RawMessage) any {
			var v any
			Expect(json.Unmarshal(raw, &v)).To(Succeed(), string(raw))
			return v
		}

		It("preview mode selects no payload column", func() {
			// The select list itself: neither payload column may be named.
			// Split on the separator so input_preview does not pass for
			// input, and the full list is checked the same way to prove the
			// split sees the columns it should.
			columnsOf := func(sel string) []string {
				parts := strings.Split(sel, ",")
				out := make([]string, 0, len(parts))
				for _, c := range parts {
					out = append(out, strings.TrimSpace(c))
				}
				return out
			}
			previewColumns := columnsOf(postgres.PreviewSpanSelectForTest)
			Expect(previewColumns).NotTo(ContainElement("input"))
			Expect(previewColumns).NotTo(ContainElement("output"))
			Expect(previewColumns).To(ContainElements("input_preview", "output_preview", "usage", "verdict"))
			fullColumns := columnsOf(postgres.SpanSelectColumnsForTest)
			Expect(fullColumns).To(ContainElements("input", "output"))
			Expect(len(fullColumns)).To(Equal(len(previewColumns)+2), "preview is the full list minus the two payload columns")

			// And the rows it produces: previews populated, payload nil,
			// everything else as the full read serves it.
			insertTurn("trc-p")
			insertPreviewedSpan("trc-p", "p-0", 0, input, output, inputPreview, outputPreview)
			insertPreviewedSpan("trc-p", "p-1", 1, input, output, inputPreview, outputPreview)

			for name, seq := range map[string]iter.Seq2[storage.SpanRecord, error]{
				"session": pgDriver.IterateSessionSpans(ctx, sessionID, storage.SpanCursor{}, storage.PayloadPreview),
				"trace":   pgDriver.IterateTraceSpans(ctx, orgID, "trc-p", storage.SpanCursor{}, storage.PayloadPreview),
			} {
				recs := collect(seq)
				Expect(recs).To(HaveLen(2), name)
				for _, rec := range recs {
					Expect(rec.Input).To(BeNil(), "%s: preview mode must not read input", name)
					Expect(rec.Output).To(BeNil(), "%s: preview mode must not read output", name)
					Expect(rec.HasPreview).To(BeTrue(), name)
					Expect(decoded(rec.InputPreview)).To(Equal(decoded(json.RawMessage(inputPreview))), name)
					Expect(decoded(rec.OutputPreview)).To(Equal(decoded(json.RawMessage(outputPreview))), name)
				}
			}

			// The header columns agree with the full read, which still
			// carries the payload.
			full := collect(pgDriver.IterateTraceSpans(ctx, orgID, "trc-p", storage.SpanCursor{}, storage.PayloadFull))
			preview := collect(pgDriver.IterateTraceSpans(ctx, orgID, "trc-p", storage.SpanCursor{}, storage.PayloadPreview))
			Expect(preview).To(HaveLen(len(full)))
			for i := range full {
				Expect(decoded(full[i].Input)).To(Equal(decoded(json.RawMessage(input))))
				Expect(decoded(full[i].Output)).To(Equal(decoded(json.RawMessage(output))))
				full[i].Input, full[i].Output = nil, nil
				Expect(preview[i]).To(Equal(full[i]), "span %d", i)
			}
		})

		It("resumes a preview stream from a cursor", func() {
			n := seedOrderingFixture()
			full := collect(pgDriver.IterateSessionSpans(ctx, sessionID, storage.SpanCursor{}, storage.PayloadPreview))
			Expect(full).To(HaveLen(n))
			for k := range full {
				resumed := collect(pgDriver.IterateSessionSpans(ctx, sessionID, cursorOf(full[k]), storage.PayloadPreview))
				Expect(resumed).To(Equal(full[k+1:]), "resume after %s/%s", full[k].TraceID, full[k].SpanID)
			}
		})

		It("reads a row without stored previews as pending", func() {
			insertTurn("trc-q")
			insertSpan("trc-q", "q-0", 0, base, input)
			recs := collect(pgDriver.IterateTraceSpans(ctx, orgID, "trc-q", storage.SpanCursor{}, storage.PayloadPreview))
			Expect(recs).To(HaveLen(1))
			Expect(recs[0].HasPreview).To(BeFalse())
			Expect(recs[0].InputPreview).To(BeNil())
			Expect(recs[0].OutputPreview).To(BeNil())
			Expect(recs[0].Input).To(BeNil())
		})

		It("serves stored previews on trace detail", func() {
			insertTurn("trc-d")
			insertPreviewedSpan("trc-d", "d-1", 1, input, output, inputPreview, outputPreview)
			insertPreviewedSpan("trc-d", "d-0", 0, input, output, inputPreview, outputPreview)

			turn, spans, _, err := pgDriver.GetTraceDetail(ctx, orgID, "trc-d", storage.PayloadPreview)
			Expect(err).NotTo(HaveOccurred())
			Expect(turn).NotTo(BeNil())
			Expect(spans).To(HaveLen(2))
			Expect(spans[0].SpanID).To(Equal("d-0"), "presentation order is seq order")
			for _, sp := range spans {
				Expect(sp.Input).To(BeNil())
				Expect(sp.Output).To(BeNil())
				Expect(sp.HasPreview).To(BeTrue())
				Expect(decoded(sp.InputPreview)).To(Equal(decoded(json.RawMessage(inputPreview))))
				Expect(decoded(sp.OutputPreview)).To(Equal(decoded(json.RawMessage(outputPreview))))
			}

			// Full mode is the read it always was: the same rows with the
			// payload, in the same order.
			_, fullSpans, _, err := pgDriver.GetTraceDetail(ctx, orgID, "trc-d", storage.PayloadFull)
			Expect(err).NotTo(HaveOccurred())
			Expect(fullSpans).To(HaveLen(2))
			for i := range fullSpans {
				Expect(decoded(fullSpans[i].Input)).To(Equal(decoded(json.RawMessage(input))))
				fullSpans[i].Input, fullSpans[i].Output = nil, nil
				Expect(spans[i]).To(Equal(fullSpans[i]))
			}
		})
	})

	It("stops on context cancellation", func() {
		seedBulk(2000, 500, 1024)

		goroutines := runtime.NumGoroutine()
		cctx, cancel := context.WithCancel(ctx)
		defer cancel()

		const cancelAfter = 10
		var (
			delivered int
			finalErr  error
		)
		for _, err := range pgDriver.IterateSessionSpans(cctx, sessionID, storage.SpanCursor{}, storage.PayloadFull) {
			if err != nil {
				finalErr = err
				continue // keep ranging: the iterator must end on its own
			}
			delivered++
			if delivered == cancelAfter {
				cancel()
			}
		}
		Expect(finalErr).To(HaveOccurred())
		Expect(errors.Is(finalErr, context.Canceled)).To(BeTrue(), "got %v", finalErr)
		Expect(delivered).To(BeNumerically(">=", cancelAfter))
		Expect(delivered).To(BeNumerically("<", 2000), "cancellation must cut the stream short")

		Eventually(func() int32 { return pgDriver.DB().Stat().AcquiredConns() }).Should(BeZero(),
			"the cancelled query must not leak its rows or pooled connection")
		Eventually(runtime.NumGoroutine).Should(BeNumerically("<=", goroutines),
			"the cancelled query must not leak goroutines")
	})
})

// collectCount drains a span iterator, failing on any error.
func collectCount(seq iter.Seq2[storage.SpanRecord, error]) int {
	n := 0
	for _, err := range seq {
		Expect(err).NotTo(HaveOccurred())
		n++
	}
	return n
}
