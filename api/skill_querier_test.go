package api

import (
	"context"
	"encoding/json"
	"errors"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/storage"
)

// querierStubReader implements the sessionsReader and spanModelReader
// capability interfaces the in-process skill querier depends on. It
// records the org each call receives so specs can prove the bound tenant
// is threaded through — the exact property whose absence (the tokenless
// loopback self-call) made multi-tenant generation read the nil org and
// emit empty skills.
type querierStubReader struct {
	session       *storage.SessionRecord
	sessionOrgs   []string
	summaries     []storage.TraceSummaryRecord
	summariesCall int

	turn          *storage.SpanTurnRecord
	spans         []storage.SpanRecord
	traceOrgs     []string
	getTraceErr   error
	getSessionErr error
}

func (r *querierStubReader) GetSessionRecord(_ context.Context, orgID, _ string) (*storage.SessionRecord, error) {
	r.sessionOrgs = append(r.sessionOrgs, orgID)
	return r.session, r.getSessionErr
}

func (r *querierStubReader) ListSessionRecords(context.Context, string, storage.SessionListOpts) ([]storage.SessionRecord, error) {
	return nil, nil
}

func (r *querierStubReader) GetSessionRecordByHarness(context.Context, string, string, string) (*storage.SessionRecord, error) {
	return nil, nil
}

func (r *querierStubReader) UpdateSessionName(context.Context, string, string, *string) (int64, error) {
	panic("not implemented")
}

func (r *querierStubReader) ListTraceSummaries(_ context.Context, _ string) ([]storage.TraceSummaryRecord, error) {
	r.summariesCall++
	return r.summaries, nil
}

func (r *querierStubReader) GetTraceDetail(_ context.Context, orgID, _ string) (*storage.SpanTurnRecord, []storage.SpanRecord, []storage.SpanLinkRecord, error) {
	r.traceOrgs = append(r.traceOrgs, orgID)
	return r.turn, r.spans, nil, r.getTraceErr
}

func (r *querierStubReader) ListSessionSpanModel(context.Context, string) ([]storage.SpanTurnRecord, []storage.SpanRecord, []storage.SpanLinkRecord, error) {
	return nil, nil, nil, nil
}

func (r *querierStubReader) ListSessionLinks(context.Context, string) ([]storage.SpanLinkRecord, error) {
	return nil, nil
}

func (r *querierStubReader) ListTraceSpans(context.Context, string, string) ([]storage.SpanRecord, error) {
	return nil, nil
}

func (r *querierStubReader) GetSpanRecord(context.Context, string, string, string) (*storage.SpanRecord, error) {
	return nil, nil
}

func (r *querierStubReader) ListRawTurnHeaders(context.Context, string, string, string) ([]storage.RawTurnHeader, error) {
	return nil, nil
}

var _ = Describe("skillTraceQuerier", func() {
	const tenant = "11111111-1111-1111-1111-111111111111"

	newQuerier := func(r *querierStubReader) *skillTraceQuerier {
		return &skillTraceQuerier{sessions: r, spans: r, orgID: tenant}
	}

	Describe("TraceSummaries", func() {
		It("scopes the session lookup to the bound org and maps the rows", func() {
			r := &querierStubReader{
				session: &storage.SessionRecord{ID: "s1"},
				summaries: []storage.TraceSummaryRecord{
					{SpanTurnRecord: storage.SpanTurnRecord{TraceID: "t1", UserPrompt: "hi", ResponsePreview: "yo"}},
				},
			}
			out, err := newQuerier(r).TraceSummaries(context.Background(), "s1")
			Expect(err).NotTo(HaveOccurred())
			Expect(r.sessionOrgs).To(Equal([]string{tenant}))
			Expect(out).To(HaveLen(1))
			Expect(out[0].TraceID).To(Equal("t1"))
			Expect(out[0].UserPrompt).To(Equal("hi"))
			Expect(out[0].ResponsePreview).To(Equal("yo"))
		})

		It("refuses a session absent for the org with a not-found sentinel, without reading its traces", func() {
			r := &querierStubReader{session: nil} // not found under this tenant
			_, err := newQuerier(r).TraceSummaries(context.Background(), "s-other")
			// The sentinel must survive so the handler can map it to a 404
			// rather than a generic 500.
			Expect(errors.Is(err, errSkillSessionNotFound)).To(BeTrue())
			Expect(r.summariesCall).To(Equal(0))
		})

		It("propagates a session lookup error", func() {
			r := &querierStubReader{getSessionErr: errors.New("boom")}
			_, err := newQuerier(r).TraceSummaries(context.Background(), "s1")
			Expect(err).To(MatchError(ContainSubstring("boom")))
		})
	})

	Describe("Trace", func() {
		It("scopes the trace read to the bound org and decodes span output", func() {
			blocks, err := json.Marshal([]map[string]any{{"type": "text", "text": "the answer"}})
			Expect(err).NotTo(HaveOccurred())
			r := &querierStubReader{
				turn: &storage.SpanTurnRecord{TraceID: "t1"},
				spans: []storage.SpanRecord{
					{SpanID: "sp1", Kind: "llm", CallKind: "main", Output: json.RawMessage(blocks)},
				},
			}
			trace, err := newQuerier(r).Trace(context.Background(), "t1")
			Expect(err).NotTo(HaveOccurred())
			Expect(r.traceOrgs).To(Equal([]string{tenant}))
			Expect(trace.Spans).To(HaveLen(1))
			Expect(trace.Spans[0].CallKind).To(Equal("main"))
			Expect(trace.Spans[0].Output).To(HaveLen(1))
			Expect(trace.Spans[0].Output[0].Text).To(Equal("the answer"))
		})

		It("returns a nil trace (preview fallback) when the turn is absent for the org", func() {
			r := &querierStubReader{turn: nil}
			trace, err := newQuerier(r).Trace(context.Background(), "missing")
			Expect(err).NotTo(HaveOccurred())
			Expect(trace).To(BeNil())
		})
	})
})
