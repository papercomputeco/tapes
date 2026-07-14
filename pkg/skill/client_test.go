package skill_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/api"
	"github.com/papercomputeco/tapes/pkg/skill"
	"github.com/papercomputeco/tapes/pkg/storage"
)

// The APIClient reads the same span-model wire the server emits, so its
// fixtures are built from the real api response types and renderers
// (api.BuildTraceList / api.BuildTraceDetail / api.SessionListResponse).
// A server-side rename of a wire field the skill generator depends on
// (usage totals, the typed synthetic/call_kind/thread_id fields, or the
// content-block output array) zeroes the decoded value and fails a spec
// here — the coverage gap that let the deck/skill clients drift onto the
// retired flat wire in the first place.
var _ = Describe("APIClient against real API-renderer fixtures", func() {
	started := time.Date(2026, 6, 1, 10, 0, 0, 0, time.UTC)

	Describe("TraceSummaries", func() {
		It("maps usage/main_usage totals and the typed synthetic field", func() {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				Expect(r.URL.Path).To(Equal("/v1/traces"))
				Expect(r.URL.Query().Get("session_id")).To(Equal("sess-1"))
				list := api.BuildTraceList([]storage.TraceSummaryRecord{
					{
						SpanTurnRecord: storage.SpanTurnRecord{
							TraceID: "trace-1", UserPrompt: "do the thing", ResponsePreview: "done",
							Status: "completed", Source: "wire", StartedAt: started,
							TotalInputTokens: 120, TotalOutputTokens: 30,
							MainInputTokens: 100, MainOutputTokens: 25,
						},
						SpanCount: 3,
					},
					{
						SpanTurnRecord: storage.SpanTurnRecord{
							TraceID: "trace-2", Synthetic: "post-compaction",
							Status: "completed", Source: "wire", StartedAt: started.Add(time.Minute),
						},
					},
				})
				Expect(json.NewEncoder(w).Encode(list)).To(Succeed())
			}))
			defer srv.Close()

			summaries, err := skill.NewAPIClient(srv.URL).TraceSummaries(context.Background(), "sess-1")
			Expect(err).NotTo(HaveOccurred())
			Expect(summaries).To(HaveLen(2))

			Expect(summaries[0].TraceID).To(Equal("trace-1"))
			Expect(summaries[0].UserPrompt).To(Equal("do the thing"))
			Expect(summaries[0].TotalInputTokens).To(Equal(int64(120)))
			Expect(summaries[0].TotalOutputTokens).To(Equal(int64(30)))
			Expect(summaries[0].MainInputTokens).To(Equal(int64(100)))
			Expect(summaries[0].MainOutputTokens).To(Equal(int64(25)))
			Expect(summaries[0].Synthetic).To(BeEmpty())

			Expect(summaries[1].Synthetic).To(Equal("post-compaction"))
		})
	})

	Describe("Trace", func() {
		It("maps the typed call_kind/thread_id and the content-block output array", func() {
			turn := storage.SpanTurnRecord{TraceID: "trace-1", Status: "completed", Source: "wire", StartedAt: started}
			spans := []storage.SpanRecord{
				{
					TraceID: "trace-1", SpanID: "sp-1", Kind: "llm", Name: "llm", Seq: 1,
					CallKind: "main", ThreadID: "", StartedAt: started,
					Output: json.RawMessage(`[{"type":"text","text":"the answer"}]`),
				},
				{
					TraceID: "trace-1", SpanID: "sp-2", Kind: "llm", Name: "subagent", Seq: 2,
					CallKind: "offshoot:subagent", ThreadID: "thread-9", StartedAt: started.Add(time.Second),
					Output: json.RawMessage(`[{"type":"text","text":"sub answer"}]`),
				},
			}
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				Expect(r.URL.Path).To(Equal("/v1/traces/trace-1"))
				Expect(json.NewEncoder(w).Encode(api.BuildTraceDetail(turn, spans, nil, api.PayloadFull))).To(Succeed())
			}))
			defer srv.Close()

			trace, err := skill.NewAPIClient(srv.URL).Trace(context.Background(), "trace-1")
			Expect(err).NotTo(HaveOccurred())
			Expect(trace.Spans).To(HaveLen(2))

			Expect(trace.Spans[0].CallKind).To(Equal("main"))
			Expect(trace.Spans[0].ThreadID).To(BeEmpty())
			Expect(trace.Spans[0].Output).To(HaveLen(1))
			Expect(trace.Spans[0].Output[0].Text).To(Equal("the answer"))

			Expect(trace.Spans[1].CallKind).To(Equal("offshoot:subagent"))
			Expect(trace.Spans[1].ThreadID).To(Equal("thread-9"))
			Expect(trace.Spans[1].Output[0].Text).To(Equal("sub answer"))
		})
	})

	Describe("Sessions", func() {
		It("maps the deriver rollup (status, model, preview, spend)", func() {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				Expect(r.URL.Path).To(Equal("/v1/sessions"))
				resp := api.SessionListResponse{Items: []api.SessionItem{{
					ID:         "sess-1",
					HarnessID:  "claude-code",
					StartedAt:  started,
					LastSeenAt: started.Add(time.Hour),
					Rollup: api.SessionRollup{
						Status:     "completed",
						Model:      "claude-opus-4.6",
						Preview:    "fix the flaky test",
						TurnCount:  9,
						KindCounts: map[string]int{},
						Tasks:      []api.TreeTask{},
						Usage:      api.SessionUsage{InputTokens: 1000, OutputTokens: 250, CostUSD: 1.25},
					},
				}}}
				Expect(json.NewEncoder(w).Encode(resp)).To(Succeed())
			}))
			defer srv.Close()

			sessions, err := skill.NewAPIClient(srv.URL).Sessions(context.Background())
			Expect(err).NotTo(HaveOccurred())
			Expect(sessions).To(HaveLen(1))
			Expect(sessions[0].ID).To(Equal("sess-1"))
			Expect(sessions[0].DerivedStatus).To(Equal("completed"))
			Expect(sessions[0].Model).To(Equal("claude-opus-4.6"))
			Expect(sessions[0].Preview).To(Equal("fix the flaky test"))
			Expect(sessions[0].TurnCount).To(Equal(9))
			Expect(sessions[0].TotalCostUSD).To(BeNumerically("~", 1.25, 1e-9))
		})
	})
})
