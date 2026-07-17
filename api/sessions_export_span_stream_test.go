package api

import (
	"bytes"
	"context"
	"encoding/json"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/storage"
)

// goldenSpanReader is a fixture-backed storage.SpanModelReader for the
// spans-grain streaming export. It serves the per-trace reads
// streamSessionSpanExport performs (turn headers, session links, one
// trace's spans) straight from in-memory fixtures, so the streamed bytes
// can be diffed against the materialized BuildSessionTraces encoding of the
// SAME rows.
type goldenSpanReader struct {
	turns []storage.SpanTurnRecord
	// spans is the whole-session span list in ListSpansBySession order
	// (trace_id ASC, seq ASC); both the materialized and streamed paths
	// derive per-trace order from this single slice's relative ordering.
	spans []storage.SpanRecord
	links []storage.SpanLinkRecord
}

func (r *goldenSpanReader) ListTraceSummaries(_ context.Context, _ string) ([]storage.TraceSummaryRecord, error) {
	out := make([]storage.TraceSummaryRecord, 0, len(r.turns))
	for _, t := range r.turns {
		count := 0
		for _, sp := range r.spans {
			if sp.TraceID == t.TraceID {
				count++
			}
		}
		out = append(out, storage.TraceSummaryRecord{SpanTurnRecord: t, SpanCount: count})
	}
	return out, nil
}

func (r *goldenSpanReader) ListSessionLinks(_ context.Context, _ string) ([]storage.SpanLinkRecord, error) {
	return r.links, nil
}

func (r *goldenSpanReader) ListTraceSpans(_ context.Context, _, traceID string) ([]storage.SpanRecord, error) {
	var out []storage.SpanRecord
	for _, sp := range r.spans {
		if sp.TraceID == traceID {
			out = append(out, sp)
		}
	}
	return out, nil
}

func (r *goldenSpanReader) ListSessionSpanModel(_ context.Context, _ string) ([]storage.SpanTurnRecord, []storage.SpanRecord, []storage.SpanLinkRecord, error) {
	return r.turns, r.spans, r.links, nil
}

func (r *goldenSpanReader) GetTraceDetail(_ context.Context, _, _ string) (*storage.SpanTurnRecord, []storage.SpanRecord, []storage.SpanLinkRecord, error) {
	return nil, nil, nil, nil
}

func (r *goldenSpanReader) GetSpanRecord(context.Context, string, string, string) (*storage.SpanRecord, error) {
	return nil, nil
}

func (r *goldenSpanReader) ListRawTurnHeaders(context.Context, string, string, string) ([]storage.RawTurnHeader, error) {
	return nil, nil
}

var _ = Describe("streamSessionSpanExport (spans-grain streaming render)", func() {
	const org = "11111111-1111-1111-1111-111111111111"
	const sessionID = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"

	// A LastSeenAt well outside the liveness window so sessionItemFromStorage
	// resolves Live=false deterministically regardless of when the test runs
	// — the one time.Now()-dependent field on the wire.
	started := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	sess := storage.SessionRecord{
		ID:         sessionID,
		HarnessID:  "claude",
		StartedAt:  started,
		LastSeenAt: started.Add(time.Minute),
	}

	// materialized is the current, whole-session encoder: the guardrail the
	// streamed output must reproduce byte-for-byte.
	materialized := func(r *goldenSpanReader) []byte {
		var buf bytes.Buffer
		resp := BuildSessionTraces(sessionItemFromStorage(sess, time.Now()), r.turns, r.spans, r.links, PayloadFull)
		Expect(json.NewEncoder(&buf).Encode(resp)).To(Succeed())
		return buf.Bytes()
	}

	streamed := func(r *goldenSpanReader) []byte {
		var buf bytes.Buffer
		Expect(streamSessionSpanExport(context.Background(), r, org, sess, &buf)).To(Succeed())
		return buf.Bytes()
	}

	assertIdentical := func(r *goldenSpanReader) {
		got := streamed(r)
		want := materialized(r)
		// Sanity: the streamed line must round-trip as the composite shape,
		// not just match some arbitrary baseline.
		var line SessionTracesResponse
		Expect(json.Unmarshal(bytes.TrimSpace(got), &line)).To(Succeed())
		Expect(string(got)).To(Equal(string(want)))
	}

	It("matches for a plain two-trace session with one span each", func() {
		assertIdentical(&goldenSpanReader{
			turns: []storage.SpanTurnRecord{
				{TraceID: "t1", UserPrompt: "hi", ResponsePreview: "hello", Status: "completed", Source: "wire", StartedAt: started, TotalInputTokens: 10, TotalOutputTokens: 5},
				{TraceID: "t2", UserPrompt: "thanks", ResponsePreview: "np", Status: "completed", Source: "wire", StartedAt: started.Add(time.Minute), TotalInputTokens: 4, TotalOutputTokens: 2},
			},
			spans: []storage.SpanRecord{
				{TraceID: "t1", SpanID: "s1", Kind: "llm", Name: "call", Status: "ok", CallKind: "main", Model: "claude-test", Seq: 1, StartedAt: started, Input: json.RawMessage(`[{"type":"text","text":"hi"}]`), Output: json.RawMessage(`[{"type":"text","text":"hello"}]`), Usage: json.RawMessage(`{"input_tokens":10,"output_tokens":5}`)},
				{TraceID: "t2", SpanID: "s2", Kind: "llm", Name: "call", Status: "ok", CallKind: "main", Model: "claude-test", Seq: 1, StartedAt: started.Add(time.Minute), Input: json.RawMessage(`[{"type":"text","text":"thanks"}]`), Output: json.RawMessage(`[{"type":"text","text":"np"}]`), Usage: json.RawMessage(`{"input_tokens":4,"output_tokens":2}`)},
			},
		})
	})

	It("matches when a trace has zero spans (pins spans to [])", func() {
		assertIdentical(&goldenSpanReader{
			turns: []storage.SpanTurnRecord{
				{TraceID: "empty", UserPrompt: "", Status: "abandoned", Source: "wire", StartedAt: started},
				{TraceID: "t2", UserPrompt: "hi", Status: "completed", Source: "wire", StartedAt: started.Add(time.Minute)},
			},
			spans: []storage.SpanRecord{
				{TraceID: "t2", SpanID: "s2", Kind: "llm", Status: "ok", CallKind: "main", Model: "m", Seq: 1, StartedAt: started.Add(time.Minute)},
			},
		})
	})

	It("matches for a session with no traces at all (empty traces array)", func() {
		assertIdentical(&goldenSpanReader{})
	})

	It("matches for a single-span, single-trace session with nil payloads", func() {
		assertIdentical(&goldenSpanReader{
			turns: []storage.SpanTurnRecord{
				{TraceID: "only", UserPrompt: "solo", Status: "completed", Source: "wire", StartedAt: started},
			},
			spans: []storage.SpanRecord{
				// nil Input/Output/Usage/Verdict exercise the []/{}/null pins.
				{TraceID: "only", SpanID: "s1", Kind: "tool", Name: "Bash", Status: "ok", CallKind: "shadow", Seq: 1, StartedAt: started},
			},
		})
	})

	It("matches when payloads carry quotes, newlines, HTML-escapable bytes, and unicode", func() {
		assertIdentical(&goldenSpanReader{
			turns: []storage.SpanTurnRecord{
				{TraceID: "t1", UserPrompt: "quote \" and <tag> & amp\nnewline café ☕", ResponsePreview: "résumé", Status: "completed", Source: "wire", StartedAt: started},
			},
			spans: []storage.SpanRecord{
				{
					TraceID: "t1", SpanID: "s1", Kind: "llm", Name: "call", Status: "ok", CallKind: "main", Model: "claude-☕", Seq: 1, StartedAt: started,
					Input:   json.RawMessage(`[{"type":"text","text":"<script>alert(\"x\")</script>\nline2 & more — café ☕"}]`),
					Output:  json.RawMessage(`[{"type":"text","text":"héllo \"world\" <b>b</b>"}]`),
					Usage:   json.RawMessage(`{"input_tokens":1}`),
					Verdict: json.RawMessage(`{"decision":"allow","reason":"looks <ok> & \"fine\""}`),
				},
			},
		})
	})

	It("matches when the trace order differs from lexical trace_id order", func() {
		// Turns are ordered by started_at (the authority), with trace_ids in
		// reverse-lexical order and multiple spans per trace, so the test
		// fails if the stream ever iterated by trace_id or regrouped spans.
		assertIdentical(&goldenSpanReader{
			turns: []storage.SpanTurnRecord{
				{TraceID: "zzz", UserPrompt: "first", Status: "completed", Source: "wire", StartedAt: started},
				{TraceID: "mmm", UserPrompt: "second", Status: "completed", Source: "wire", StartedAt: started.Add(time.Minute)},
				{TraceID: "aaa", UserPrompt: "third", Status: "completed", Source: "wire", StartedAt: started.Add(2 * time.Minute)},
			},
			// spans slice in ListSpansBySession order (trace_id ASC, seq ASC).
			spans: []storage.SpanRecord{
				{TraceID: "aaa", SpanID: "a1", Kind: "llm", Status: "ok", CallKind: "main", Model: "m", Seq: 1, StartedAt: started.Add(2 * time.Minute), Output: json.RawMessage(`[{"type":"text","text":"a1"}]`)},
				{TraceID: "aaa", SpanID: "a2", Kind: "tool", Status: "ok", CallKind: "main", Seq: 2, StartedAt: started.Add(2 * time.Minute)},
				{TraceID: "mmm", SpanID: "m1", Kind: "llm", Status: "ok", CallKind: "main", Model: "m", Seq: 1, StartedAt: started.Add(time.Minute)},
				{TraceID: "zzz", SpanID: "z1", Kind: "llm", Status: "ok", CallKind: "main", Model: "m", Seq: 1, StartedAt: started},
				{TraceID: "zzz", SpanID: "z2", Kind: "llm", Status: "ok", CallKind: "subagent", Model: "m", ThreadID: "th1", Seq: 2, StartedAt: started},
			},
			links: []storage.SpanLinkRecord{
				{Kind: "compaction-seam", FromTraceID: "zzz", FromSpanID: "z2", FromIO: "output", ToTraceID: "mmm", ToSpanID: "m1", ToIO: "input"},
				{Kind: "emits", FromTraceID: "aaa", FromSpanID: "a1", ToTraceID: "aaa", ToSpanID: "a2"},
			},
		})
	})
})
