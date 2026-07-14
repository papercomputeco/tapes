package deck

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/api"
	"github.com/papercomputeco/tapes/pkg/storage"
)

// These specs feed the deck HTTP client the EXACT wire the API server
// emits: fixtures are built from the real api response types and renderers
// (api.SessionItem / api.BuildTraceList / api.BuildTraceDetail), then
// marshaled and served. If a wire field the deck reads moves or is renamed
// on the server side, the decoded DTO zeroes out and these assertions fail
// — which is the point. Self-encoded deck-shaped literals could not catch a
// server/client wire drift.
var _ = Describe("HTTPQuery against real API-renderer fixtures", func() {
	started := time.Date(2026, 6, 1, 10, 0, 0, 0, time.UTC)

	// apiSession builds one api.SessionItem with a populated deriver rollup.
	apiSession := func(id, title, model, status string, in, out int64, cost float64, turns int) api.SessionItem {
		return api.SessionItem{
			ID:         id,
			HarnessID:  "claude-code",
			StartedAt:  started,
			LastSeenAt: started.Add(time.Hour),
			Rollup: api.SessionRollup{
				Status:     status,
				Title:      title,
				Model:      model,
				TurnCount:  turns,
				KindCounts: map[string]int{},
				Tasks:      []api.TreeTask{},
				Usage:      api.SessionUsage{InputTokens: in, OutputTokens: out, CostUSD: cost},
			},
		}
	}

	mustJSON := func(w http.ResponseWriter, v any) {
		Expect(json.NewEncoder(w).Encode(v)).To(Succeed())
	}

	Describe("Overview", func() {
		It("rolls up a page of sessions from the rollup/usage wire", func() {
			var limits, cursors []string
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				Expect(r.URL.Path).To(Equal("/v1/sessions"))
				limits = append(limits, r.URL.Query().Get("limit"))
				cursors = append(cursors, r.URL.Query().Get("cursor"))
				mustJSON(w, api.SessionListResponse{Items: []api.SessionItem{
					apiSession("s1", "one", "m1", StatusCompleted, 100, 50, 0.30, 4),
					apiSession("s2", "two", "m2", StatusFailed, 10, 5, 0.10, 1),
				}})
			}))
			defer srv.Close()

			q := NewHTTPQuery(srv.URL, nil)
			overview, err := q.Overview(context.Background(), Filters{})
			Expect(err).NotTo(HaveOccurred())
			Expect(limits).To(Equal([]string{"25"}))
			Expect(cursors).To(Equal([]string{""}))

			Expect(overview.Sessions).To(HaveLen(2))
			// Non-zero rollup values prove the nested wire decoded — the
			// regression symptom was zeros/blanks here.
			Expect(overview.TotalCost).To(BeNumerically("~", 0.40, 1e-9))
			Expect(overview.TotalTokens).To(Equal(int64(165)))
			Expect(overview.TotalTurns).To(Equal(5))
			Expect(overview.Completed).To(Equal(1))
			Expect(overview.Failed).To(Equal(1))
			Expect(overview.SuccessRate).To(BeNumerically("~", 0.5, 1e-9))
			Expect(overview.CostByModel).To(HaveKey("m1"))
			Expect(overview.CostByModel["m1"].SessionCount).To(Equal(1))
			Expect(overview.Sessions[0].Label).To(Equal("one"))
		})

		It("applies model and status filters client-side", func() {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				mustJSON(w, api.SessionListResponse{Items: []api.SessionItem{
					apiSession("s1", "", "m1", StatusCompleted, 0, 0, 0, 0),
					apiSession("s2", "", "m2", StatusCompleted, 0, 0, 0, 0),
					apiSession("s3", "", "m1", StatusFailed, 0, 0, 0, 0),
				}})
			}))
			defer srv.Close()

			q := NewHTTPQuery(srv.URL, nil)
			overview, err := q.Overview(context.Background(), Filters{Model: "m1", Status: StatusCompleted})
			Expect(err).NotTo(HaveOccurred())
			Expect(overview.Sessions).To(HaveLen(1))
			Expect(overview.Sessions[0].ID).To(Equal("s1"))
		})
	})

	Describe("OverviewPage", func() {
		It("sends cursor and limit and returns next cursor metadata", func() {
			var seenLimit, seenCursor string
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				Expect(r.URL.Path).To(Equal("/v1/sessions"))
				seenLimit = r.URL.Query().Get("limit")
				seenCursor = r.URL.Query().Get("cursor")
				mustJSON(w, api.SessionListResponse{
					Items:      []api.SessionItem{apiSession("s1", "one", "", "", 0, 0, 0, 0)},
					NextCursor: "cursor-2",
				})
			}))
			defer srv.Close()

			q := NewHTTPQuery(srv.URL, nil)
			page, err := q.OverviewPage(context.Background(), Filters{}, "cursor-1", 7)
			Expect(err).NotTo(HaveOccurred())
			Expect(seenLimit).To(Equal("7"))
			Expect(seenCursor).To(Equal("cursor-1"))
			Expect(page.NextCursor).To(Equal("cursor-2"))
			Expect(page.HasMore).To(BeTrue())
			Expect(page.Overview.Sessions).To(HaveLen(1))
		})
	})

	Describe("SessionDetail", func() {
		traceSummary := func(id, prompt, preview string, dur time.Duration, in, out int64, cost float64, spans int, at time.Time) storage.TraceSummaryRecord {
			return storage.TraceSummaryRecord{
				SpanTurnRecord: storage.SpanTurnRecord{
					TraceID: id, UserPrompt: prompt, ResponsePreview: preview,
					Status: "completed", Source: "wire", StartedAt: at, DurationNS: int64(dur),
					TotalInputTokens: in, TotalOutputTokens: out, TotalCostUSD: cost,
				},
				SpanCount: spans,
			}
		}

		newServer := func() *httptest.Server {
			return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/v1/sessions":
					mustJSON(w, api.SessionListResponse{Items: []api.SessionItem{
						apiSession("sess-1", "the session", "claude-opus-4.6", StatusCompleted, 0, 0, 0, 2),
					}})
				case "/v1/sessions/sess-1":
					mustJSON(w, api.SessionDetailResponse{
						Session: apiSession("sess-1", "the session", "claude-opus-4.6", StatusCompleted, 0, 0, 0, 2),
					})
				case "/v1/traces":
					Expect(r.URL.Query().Get("session_id")).To(Equal("sess-1"))
					mustJSON(w, api.BuildTraceList([]storage.TraceSummaryRecord{
						traceSummary("trace-1", "first prompt", "first answer", 20*time.Second, 100, 40, 0.05, 7, started),
						traceSummary("trace-2", "second prompt", "second answer", 5*time.Second, 50, 10, 0.01, 3, started.Add(time.Minute)),
					}))
				default:
					http.NotFound(w, r)
				}
			}))
		}

		It("returns turn summaries plus a turn-grain transcript", func() {
			srv := newServer()
			defer srv.Close()

			q := NewHTTPQuery(srv.URL, nil)
			detail, err := q.SessionDetail(context.Background(), "sess-1")
			Expect(err).NotTo(HaveOccurred())

			Expect(detail.Summary.ID).To(Equal("sess-1"))
			Expect(detail.Summary.Label).To(Equal("the session"))

			Expect(detail.Turns).To(HaveLen(2))
			Expect(detail.Turns[0].TraceID).To(Equal("trace-1"))
			Expect(detail.Turns[0].UserPrompt).To(Equal("first prompt"))
			Expect(detail.Turns[0].SpanCount).To(Equal(7))
			Expect(detail.Turns[0].Duration).To(Equal(20 * time.Second))
			Expect(detail.Turns[0].InputTokens).To(Equal(int64(100)))
			Expect(detail.Turns[0].TotalCost).To(BeNumerically("~", 0.05, 1e-9))

			Expect(detail.Messages).To(HaveLen(4))
			Expect(detail.Messages[0].Text).To(Equal("first prompt"))
			Expect(detail.Messages[1].Text).To(Equal("first answer"))
			Expect(detail.Messages[1].TotalCost).To(BeNumerically("~", 0.05, 1e-9))
			Expect(detail.GroupedMessages).NotTo(BeEmpty())
		})

		It("reuses the cached overview summary instead of refetching the row", func() {
			var sessionGets int
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/v1/sessions":
					mustJSON(w, api.SessionListResponse{Items: []api.SessionItem{
						apiSession("sess-1", "cached", "", "", 0, 0, 0, 0),
					}})
				case "/v1/sessions/sess-1":
					sessionGets++
					mustJSON(w, api.SessionDetailResponse{Session: apiSession("sess-1", "fetched", "", "", 0, 0, 0, 0)})
				case "/v1/traces":
					mustJSON(w, api.TraceListResponse{})
				default:
					http.NotFound(w, r)
				}
			}))
			defer srv.Close()

			q := NewHTTPQuery(srv.URL, nil)
			_, err := q.Overview(context.Background(), Filters{})
			Expect(err).NotTo(HaveOccurred())

			detail, err := q.SessionDetail(context.Background(), "sess-1")
			Expect(err).NotTo(HaveOccurred())
			Expect(detail.Summary.Label).To(Equal("cached"))
			Expect(sessionGets).To(Equal(0))
		})
	})

	Describe("TurnConversation", func() {
		It("builds the conversation from main llm spans and counts the rest", func() {
			turn := storage.SpanTurnRecord{
				TraceID: "trace-1", UserPrompt: "do the thing", Status: "completed", Source: "wire",
				StartedAt: started, DurationNS: int64(12 * time.Second), TotalCostUSD: 0.07,
			}
			spans := []storage.SpanRecord{
				{
					TraceID: "trace-1", SpanID: "sp-1", Kind: "llm", Name: "llm claude-opus-4.6", Seq: 1,
					CallKind: "main", Model: "claude-opus-4.6", StartedAt: started,
					Input:  json.RawMessage(`[{"type":"text","text":"do the thing"}]`),
					Output: json.RawMessage(`[{"type":"text","text":"on it"},{"type":"tool_use","tool_name":"Bash","tool_use_id":"t1"}]`),
					Usage:  json.RawMessage(`{"prompt_tokens":120,"completion_tokens":30}`),
				},
				{
					TraceID: "trace-1", SpanID: "sp-2", Kind: "tool", Name: "Bash", Seq: 2,
					StartedAt: started.Add(2 * time.Second),
				},
				{
					TraceID: "trace-1", SpanID: "sp-3", Kind: "llm", Name: "haiku title", Seq: 3,
					CallKind: "offshoot:topic-detection", StartedAt: started.Add(3 * time.Second),
				},
				{
					TraceID: "trace-1", SpanID: "sp-4", Kind: "event", Name: "claude-md", Seq: 4,
					CallKind: "injected:claude-md", StartedAt: started,
				},
			}
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				Expect(r.URL.Path).To(Equal("/v1/traces/trace-1"))
				mustJSON(w, api.BuildTraceDetail(turn, spans, nil, api.PayloadFull))
			}))
			defer srv.Close()

			q := NewHTTPQuery(srv.URL, DefaultPricing())
			conv, err := q.TurnConversation(context.Background(), "trace-1")
			Expect(err).NotTo(HaveOccurred())

			Expect(conv.Turn.TraceID).To(Equal("trace-1"))
			Expect(conv.Turn.Duration).To(Equal(12 * time.Second))

			Expect(conv.Messages).To(HaveLen(2))
			Expect(conv.Messages[0].Role).To(Equal("user"))
			Expect(conv.Messages[0].Text).To(Equal("do the thing"))
			Expect(conv.Messages[1].Role).To(Equal("assistant"))
			Expect(conv.Messages[1].Text).To(HavePrefix("on it"))
			Expect(conv.Messages[1].InputTokens).To(Equal(int64(120)))
			Expect(conv.Messages[1].OutputTokens).To(Equal(int64(30)))
			Expect(conv.Messages[1].ToolCalls).To(ConsistOf("Bash"))

			Expect(conv.ToolFrequency).To(HaveKeyWithValue("Bash", 1))
			Expect(conv.OffshootCalls).To(Equal(1))
			Expect(conv.InjectedContexts).To(Equal(1))
		})
	})
})
