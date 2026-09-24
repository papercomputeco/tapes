package backfill_test

// `tapes backfill previews`: fill input_preview / output_preview on spans
// derived before the columns existed, from the stored payload, in bounded
// keyset-paged batches — never through the deriver, never touching the
// payload, content_hash or derive_seq.

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/internal/testdb"
	"github.com/papercomputeco/tapes/pkg/backfill"
	"github.com/papercomputeco/tapes/pkg/derive"
	"github.com/papercomputeco/tapes/pkg/llm"
	"github.com/papercomputeco/tapes/pkg/storage"
	"github.com/papercomputeco/tapes/pkg/storage/postgres"
)

var _ = Describe("preview backfill [postgres]", func() {
	var (
		ctx    context.Context
		driver *postgres.Driver
	)

	const (
		orgID    = "00000000-0000-0000-0000-000000000000"
		sessionA = "01900000-0000-7000-8000-0000000000aa"
		sessionB = "01900000-0000-7000-8000-0000000000bb"
	)

	// Longer than the preview bound, so a stored preview is visibly not
	// the payload.
	long := strings.Repeat("é", derive.PreviewRunes+200)

	type seedSpan struct {
		session, trace, span string
		input, output        json.RawMessage
		// previews are stored as given; nil means NULL (the pre-backfill shape).
		inputPreview, outputPreview json.RawMessage
		contentHash                 string
		deriveSeq                   int64
	}

	type spanRow struct {
		session, trace, span        string
		input, output               json.RawMessage
		inputPreview, outputPreview json.RawMessage
		contentHash                 string
		deriveSeq                   int64
	}

	key := func(session, trace, span string) string { return session + "/" + trace + "/" + span }

	// cursorAfter reports whether a sorts strictly after b in the scan's
	// (session_id, trace_id, span_id) order. Canonical lowercase UUID text
	// orders the same way Postgres orders the uuid column.
	cursorAfter := func(a, b storage.SpanBackfillCursor) bool {
		if a.SessionID != b.SessionID {
			return a.SessionID > b.SessionID
		}
		if a.TraceID != b.TraceID {
			return a.TraceID > b.TraceID
		}
		return a.SpanID > b.SpanID
	}

	BeforeEach(func() {
		if testPostgresDSN == "" {
			Skip(testdb.ErrNotConfigured.Error())
		}
		ctx = context.Background()
		var err error
		driver, err = postgres.NewDriver(ctx, testPostgresDSN)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(driver.Close)

		for _, stmt := range []string{
			"TRUNCATE TABLE derive_queue",
			"TRUNCATE TABLE raw_turns RESTART IDENTITY",
			"TRUNCATE TABLE sessions CASCADE",
		} {
			_, err = driver.DB().Exec(ctx, stmt)
			Expect(err).NotTo(HaveOccurred())
		}
		for _, sid := range []string{sessionA, sessionB} {
			_, err = driver.DB().Exec(ctx, `
				INSERT INTO sessions (id, org_id, auth_subject, harness_id, harness_session_id, started_at, last_seen_at)
				VALUES ($1, $2, 'test', 'claude-code', $3, NOW(), NOW())`, sid, orgID, sid)
			Expect(err).NotTo(HaveOccurred())
		}
	})

	// seed writes span rows straight into the projection tables — the shape
	// the deriver left before previews existed — without running it.
	seed := func(spans ...seedSpan) {
		turns := map[string]bool{}
		for _, s := range spans {
			if !turns[s.session+s.trace] {
				turns[s.session+s.trace] = true
				_, err := driver.DB().Exec(ctx, `
					INSERT INTO span_turns_20260615 (org_id, trace_id, session_id, started_at)
					VALUES ($1, $2, $3, NOW())`, orgID, s.trace, s.session)
				Expect(err).NotTo(HaveOccurred())
			}
			_, err := driver.DB().Exec(ctx, `
				INSERT INTO spans_20260615 (
					org_id, trace_id, span_id, session_id, kind, started_at,
					input, output, input_preview, output_preview, content_hash, derive_seq
				) VALUES ($1, $2, $3, $4, 'llm', NOW(), $5, $6, $7, $8, $9, $10)`,
				orgID, s.trace, s.span, s.session,
				[]byte(s.input), []byte(s.output), []byte(s.inputPreview), []byte(s.outputPreview),
				s.contentHash, s.deriveSeq)
			Expect(err).NotTo(HaveOccurred())
		}
	}

	readSpans := func() map[string]spanRow {
		rows, err := driver.DB().Query(ctx, `
			SELECT session_id::text, trace_id, span_id, input, output, input_preview, output_preview, content_hash, derive_seq
			FROM spans_20260615 ORDER BY session_id, trace_id, span_id`)
		Expect(err).NotTo(HaveOccurred())
		defer rows.Close()
		out := map[string]spanRow{}
		for rows.Next() {
			var r spanRow
			Expect(rows.Scan(&r.session, &r.trace, &r.span, &r.input, &r.output,
				&r.inputPreview, &r.outputPreview, &r.contentHash, &r.deriveSeq)).To(Succeed())
			out[key(r.session, r.trace, r.span)] = r
		}
		Expect(rows.Err()).NotTo(HaveOccurred())
		return out
	}

	decode := func(raw json.RawMessage) []llm.ContentBlock {
		var blocks []llm.ContentBlock
		Expect(json.Unmarshal(raw, &blocks)).To(Succeed())
		return blocks
	}

	// expectBackfilled asserts a row carries exactly the previews
	// PreviewBlocks makes of its payload. Compared as decoded blocks: JSONB
	// canonicalizes key order and whitespace on the way in.
	expectBackfilled := func(r spanRow) {
		GinkgoHelper()
		Expect(r.inputPreview).NotTo(BeNil(), "%s must carry an input preview", key(r.session, r.trace, r.span))
		Expect(r.outputPreview).NotTo(BeNil(), "%s must carry an output preview", key(r.session, r.trace, r.span))
		Expect(decode(r.inputPreview)).To(Equal(decode(derive.PreviewBlocks(r.input))))
		Expect(decode(r.outputPreview)).To(Equal(decode(derive.PreviewBlocks(r.output))))
	}

	expectPending := func(r spanRow) {
		GinkgoHelper()
		Expect(r.inputPreview).To(BeNil(), "%s must still be pending", key(r.session, r.trace, r.span))
		Expect(r.outputPreview).To(BeNil(), "%s must still be pending", key(r.session, r.trace, r.span))
	}

	textBlocks := func(text string) json.RawMessage {
		b, err := json.Marshal([]llm.ContentBlock{{Type: "text", Text: text}})
		Expect(err).NotTo(HaveOccurred())
		return b
	}

	run := func(opts backfill.PreviewOptions) *backfill.PreviewResult {
		GinkgoHelper()
		opts.Store = driver
		opts.Pause = -1
		result, err := backfill.Previews(ctx, opts)
		Expect(err).NotTo(HaveOccurred())
		return result
	}

	It("backfills only spans lacking previews", func() {
		kept := json.RawMessage(`[{"type":"text","text":"stored by the deriver"}]`)
		seed(
			// The pre-backfill shape: payload, no previews.
			seedSpan{session: sessionA, trace: "t1", span: "s1", input: textBlocks(long), output: textBlocks("short"), contentHash: "h1", deriveSeq: 11},
			// Output only — the input preview must pin to [].
			seedSpan{session: sessionA, trace: "t1", span: "s2", output: textBlocks(long), contentHash: "h2", deriveSeq: 12},
			// Already carries previews: not selected, not rewritten.
			seedSpan{session: sessionA, trace: "t2", span: "s1", input: textBlocks(long), output: textBlocks(long), inputPreview: kept, outputPreview: kept, contentHash: "h3", deriveSeq: 13},
			// No payload at all: nothing to summarize, left pending by design.
			seedSpan{session: sessionA, trace: "t2", span: "s2", contentHash: "h4", deriveSeq: 14},
		)

		result := run(backfill.PreviewOptions{})
		Expect(result.Backfilled).To(Equal(2))
		Expect(result.Batches).To(Equal(1))
		Expect(result.Last).To(Equal(storage.SpanBackfillCursor{SessionID: sessionA, TraceID: "t1", SpanID: "s2"}))

		spans := readSpans()
		Expect(spans).To(HaveLen(4))
		expectBackfilled(spans[key(sessionA, "t1", "s1")])
		Expect(string(spans[key(sessionA, "t1", "s1")].inputPreview)).NotTo(Equal(string(spans[key(sessionA, "t1", "s1")].input)),
			"the preview is bounded, not a copy of the payload")
		expectBackfilled(spans[key(sessionA, "t1", "s2")])
		Expect(string(spans[key(sessionA, "t1", "s2")].inputPreview)).To(Equal("[]"))

		stored := spans[key(sessionA, "t2", "s1")]
		Expect(decode(stored.inputPreview)).To(Equal(decode(kept)), "a stored preview is never rewritten")
		Expect(decode(stored.outputPreview)).To(Equal(decode(kept)))
		expectPending(spans[key(sessionA, "t2", "s2")])

		// Idempotent: a second run finds nothing.
		again := run(backfill.PreviewOptions{})
		Expect(again.Backfilled).To(BeZero())
		Expect(again.Batches).To(BeZero())
	})

	It("does not overwrite previews written after the row was read", func() {
		// The backfill reads a payload and writes its previews in separate
		// transactions. A derive that lands in between stores previews of
		// the newer payload, and the backfill's stale write must not
		// replace them.
		seed(seedSpan{session: sessionA, trace: "t1", span: "s1", input: textBlocks(long), contentHash: "h1", deriveSeq: 11})
		fresh := json.RawMessage(`[{"type":"text","text":"stored by a later derive"}]`)
		stale := json.RawMessage(`[{"type":"text","text":"computed from the payload read earlier"}]`)
		write := func(preview json.RawMessage) {
			GinkgoHelper()
			Expect(driver.SetSpanPreviews(ctx, []storage.SpanPreviewUpdate{{
				OrgID: orgID, TraceID: "t1", SpanID: "s1", InputPreview: preview, OutputPreview: preview,
			}})).To(Succeed())
		}
		write(fresh)
		write(stale)

		r := readSpans()[key(sessionA, "t1", "s1")]
		Expect(decode(r.inputPreview)).To(Equal(decode(fresh)))
		Expect(decode(r.outputPreview)).To(Equal(decode(fresh)))
	})

	It("resumes from the keyset after interruption", func() {
		all := make([]seedSpan, 0, 5)
		for i := range 5 {
			all = append(all, seedSpan{
				session: sessionA, trace: fmt.Sprintf("t%d", i/2), span: fmt.Sprintf("s%d", i),
				input: textBlocks(long), output: textBlocks(fmt.Sprintf("out %d", i)),
				contentHash: fmt.Sprintf("h%d", i), deriveSeq: int64(20 + i),
			})
		}
		seed(all...)

		// First run: batch of 2, cancelled at the first batch boundary.
		firstCtx, cancel := context.WithCancel(ctx)
		defer cancel()
		first, err := backfill.Previews(firstCtx, backfill.PreviewOptions{
			Store: driver, BatchSize: 2, Pause: -1,
			AfterBatch: func(backfill.PreviewProgress) { cancel() },
		})
		Expect(err).To(MatchError(context.Canceled))
		Expect(first.Backfilled).To(Equal(2))
		Expect(first.Batches).To(Equal(1))
		Expect(first.Last).To(Equal(storage.SpanBackfillCursor{SessionID: sessionA, TraceID: "t0", SpanID: "s1"}))

		mid := readSpans()
		var filled int
		for _, r := range mid {
			if r.inputPreview != nil {
				filled++
			}
		}
		Expect(filled).To(Equal(2), "the cancelled run committed exactly its first batch")

		// Second run picks up after the last row the first run reached.
		var firstBatch []storage.SpanBackfillRow
		second := run(backfill.PreviewOptions{
			BatchSize: 2,
			AfterBatch: func(p backfill.PreviewProgress) {
				if firstBatch == nil {
					firstBatch = p.Batch
				}
			},
		})
		Expect(second.Backfilled).To(Equal(3))
		Expect(second.Batches).To(Equal(2))
		Expect(firstBatch).To(HaveLen(2))
		for _, row := range firstBatch {
			Expect(cursorAfter(row.Cursor(), first.Last)).To(BeTrue(),
				"the second run must start strictly after the first run's last row (%s vs %s)", row.Cursor(), first.Last)
		}

		final := readSpans()
		Expect(final).To(HaveLen(5))
		for _, r := range final {
			expectBackfilled(r)
		}
		Expect(first.Backfilled+second.Backfilled).To(Equal(5), "every row backfilled exactly once")
	})

	It("leaves derive_seq untouched by backfill", func() {
		seed(
			seedSpan{session: sessionA, trace: "t1", span: "s1", input: textBlocks(long), output: textBlocks(long), contentHash: "hash-a", deriveSeq: 101},
			seedSpan{session: sessionA, trace: "t1", span: "s2", input: textBlocks("x"), output: textBlocks("y"), contentHash: "hash-b", deriveSeq: 102},
			seedSpan{session: sessionB, trace: "bt1", span: "s1", input: textBlocks(long), contentHash: "hash-c", deriveSeq: 103},
		)
		before := readSpans()

		result := run(backfill.PreviewOptions{})
		Expect(result.Backfilled).To(Equal(3))

		after := readSpans()
		Expect(after).To(HaveLen(len(before)))
		for k, a := range before {
			b := after[k]
			expectBackfilled(b)
			Expect(b.contentHash).To(Equal(a.contentHash), "%s: content_hash must not change", k)
			Expect(b.deriveSeq).To(Equal(a.deriveSeq), "%s: derive_seq must not change", k)
			Expect(string(b.input)).To(Equal(string(a.input)), "%s: the payload is read, never rewritten", k)
			Expect(string(b.output)).To(Equal(string(a.output)))
		}
	})

	It("restricts to one session", func() {
		seed(
			seedSpan{session: sessionA, trace: "t1", span: "s1", input: textBlocks(long), contentHash: "a1", deriveSeq: 1},
			seedSpan{session: sessionA, trace: "t1", span: "s2", input: textBlocks(long), contentHash: "a2", deriveSeq: 2},
			seedSpan{session: sessionB, trace: "bt1", span: "s1", input: textBlocks(long), contentHash: "b1", deriveSeq: 3},
		)

		result := run(backfill.PreviewOptions{SessionID: sessionB})
		Expect(result.Backfilled).To(Equal(1))
		Expect(result.Last.SessionID).To(Equal(sessionB))

		spans := readSpans()
		expectPending(spans[key(sessionA, "t1", "s1")])
		expectPending(spans[key(sessionA, "t1", "s2")])
		expectBackfilled(spans[key(sessionB, "bt1", "s1")])

		_, err := backfill.Previews(ctx, backfill.PreviewOptions{Store: driver, Pause: -1, SessionID: "not-a-uuid"})
		Expect(err).To(HaveOccurred())
	})

	It("dry run writes nothing", func() {
		seed(
			seedSpan{session: sessionA, trace: "t1", span: "s1", input: textBlocks(long), contentHash: "a1", deriveSeq: 1},
			seedSpan{session: sessionA, trace: "t1", span: "s2", input: textBlocks(long), contentHash: "a2", deriveSeq: 2},
			seedSpan{session: sessionB, trace: "bt1", span: "s1", input: textBlocks(long), contentHash: "b1", deriveSeq: 3},
		)

		var logged []string
		result := run(backfill.PreviewOptions{
			DryRun: true, BatchSize: 2,
			Logf: func(format string, args ...any) { logged = append(logged, fmt.Sprintf(format, args...)) },
		})
		Expect(result.DryRun).To(BeTrue())
		Expect(result.Backfilled).To(Equal(3), "a dry run still walks the whole keyset")
		Expect(result.Batches).To(Equal(2))
		Expect(logged).To(HaveLen(2))
		Expect(logged[0]).To(ContainSubstring("dry-run"))
		Expect(logged[1]).To(ContainSubstring("last=" + sessionB + "/bt1/s1"))

		for _, r := range readSpans() {
			expectPending(r)
		}
	})
})
