package deck

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("HTTPQuery", func() {
	started := time.Date(2026, 6, 1, 10, 0, 0, 0, time.UTC)

	Describe("NewHTTPQuery", func() {
		It("assumes http when the api target omits a scheme", func() {
			q := NewHTTPQuery("localhost:8081", nil)
			Expect(q.apiTarget).To(Equal("http://localhost:8081"))
		})

		It("preserves explicit schemes", func() {
			q := NewHTTPQuery("https://example.com/api/", nil)
			Expect(q.apiTarget).To(Equal("https://example.com/api"))
		})
	})

	Describe("Overview", func() {
		It("fetches a single bounded page of sessions and rolls it up", func() {
			var limits, cursors []string
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				Expect(r.URL.Path).To(Equal("/v1/sessions"))
				limits = append(limits, r.URL.Query().Get("limit"))
				cursors = append(cursors, r.URL.Query().Get("cursor"))
				Expect(json.NewEncoder(w).Encode(httpSessionListResponse{
					Items: []httpSessionItem{
						{
							ID: "s1", Name: "one", Model: "m1",
							StartedAt: started, LastSeenAt: started.Add(time.Minute),
							TotalInputTokens: 100, TotalOutputTokens: 50,
							TotalCostUsd: 0.30, TurnCount: 4,
							DerivedStatus: StatusCompleted,
						},
						{
							ID: "s2", Name: "two", Model: "m2",
							StartedAt: started, LastSeenAt: started.Add(2 * time.Minute),
							TotalInputTokens: 10, TotalOutputTokens: 5,
							TotalCostUsd: 0.10, TurnCount: 1,
							DerivedStatus: StatusFailed,
						},
					},
				})).To(Succeed())
			}))
			defer srv.Close()

			q := NewHTTPQuery(srv.URL, nil)
			overview, err := q.Overview(context.Background(), Filters{})
			Expect(err).NotTo(HaveOccurred())
			Expect(limits).To(Equal([]string{"25"}))
			Expect(cursors).To(Equal([]string{""}))

			Expect(overview.Sessions).To(HaveLen(2))
			Expect(overview.TotalCost).To(BeNumerically("~", 0.40, 1e-9))
			Expect(overview.TotalTokens).To(Equal(int64(165)))
			Expect(overview.TotalTurns).To(Equal(5))
			Expect(overview.Completed).To(Equal(1))
			Expect(overview.Failed).To(Equal(1))
			Expect(overview.SuccessRate).To(BeNumerically("~", 0.5, 1e-9))
			Expect(overview.CostByModel).To(HaveKey("m1"))
			Expect(overview.CostByModel["m1"].SessionCount).To(Equal(1))
		})

		It("applies model and status filters client-side", func() {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				Expect(json.NewEncoder(w).Encode(httpSessionListResponse{
					Items: []httpSessionItem{
						{ID: "s1", Model: "m1", DerivedStatus: StatusCompleted, StartedAt: started, LastSeenAt: started},
						{ID: "s2", Model: "m2", DerivedStatus: StatusCompleted, StartedAt: started, LastSeenAt: started},
						{ID: "s3", Model: "m1", DerivedStatus: StatusFailed, StartedAt: started, LastSeenAt: started},
					},
				})).To(Succeed())
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
				Expect(json.NewEncoder(w).Encode(httpSessionListResponse{
					Items:      []httpSessionItem{{ID: "s1", Name: "one"}},
					NextCursor: "cursor-2",
				})).To(Succeed())
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
		newServer := func() *httptest.Server {
			return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/v1/sessions":
					Expect(json.NewEncoder(w).Encode(httpSessionListResponse{
						Items: []httpSessionItem{{
							ID: "sess-1", Name: "the session", Model: "claude-opus-4.6",
							StartedAt: started, LastSeenAt: started.Add(time.Hour),
							DerivedStatus: StatusCompleted, TurnCount: 2,
						}},
					})).To(Succeed())
				case "/v1/sessions/sess-1":
					Expect(json.NewEncoder(w).Encode(httpSessionDetailResponse{
						Session: httpSessionItem{
							ID: "sess-1", Name: "the session", Model: "claude-opus-4.6",
							StartedAt: started, LastSeenAt: started.Add(time.Hour),
							DerivedStatus: StatusCompleted, TurnCount: 2,
						},
					})).To(Succeed())
				case "/v1/traces":
					Expect(r.URL.Query().Get("session_id")).To(Equal("sess-1"))
					Expect(json.NewEncoder(w).Encode(httpTraceListResponse{
						Items: []httpTraceItem{
							{
								TraceID: "trace-1", SessionID: "sess-1",
								UserPrompt: "first prompt", ResponsePreview: "first answer",
								Status: "completed", StartedAt: started,
								DurationNS:       int64(20 * time.Second),
								TotalInputTokens: 100, TotalOutputTokens: 40,
								TotalCostUSD: 0.05, SpanCount: 7,
							},
							{
								TraceID: "trace-2", SessionID: "sess-1",
								UserPrompt: "second prompt", ResponsePreview: "second answer",
								Status: "completed", StartedAt: started.Add(time.Minute),
								DurationNS:       int64(5 * time.Second),
								TotalInputTokens: 50, TotalOutputTokens: 10,
								TotalCostUSD: 0.01, SpanCount: 3,
							},
						},
					})).To(Succeed())
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

			Expect(detail.Messages).To(HaveLen(4))
			Expect(detail.Messages[0].Text).To(Equal("first prompt"))
			Expect(detail.Messages[1].Text).To(Equal("first answer"))
			Expect(detail.Messages[1].TotalCost).To(Equal(0.05))
			Expect(detail.GroupedMessages).NotTo(BeEmpty())
		})

		It("reuses the cached overview summary instead of refetching the row", func() {
			var sessionGets int
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/v1/sessions":
					Expect(json.NewEncoder(w).Encode(httpSessionListResponse{
						Items: []httpSessionItem{{ID: "sess-1", Name: "cached"}},
					})).To(Succeed())
				case "/v1/sessions/sess-1":
					sessionGets++
					Expect(json.NewEncoder(w).Encode(httpSessionDetailResponse{
						Session: httpSessionItem{ID: "sess-1", Name: "fetched"},
					})).To(Succeed())
				case "/v1/traces":
					Expect(json.NewEncoder(w).Encode(httpTraceListResponse{})).To(Succeed())
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
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				Expect(r.URL.Path).To(Equal("/v1/traces/trace-1"))
				Expect(json.NewEncoder(w).Encode(httpTraceDetailResponse{
					Trace: httpTraceItem{
						TraceID: "trace-1", UserPrompt: "do the thing",
						StartedAt: started, DurationNS: int64(12 * time.Second),
						TotalCostUSD: 0.07, SpanCount: 4,
					},
					Spans: []httpSpanItem{
						{
							SpanID: "sp-1", Kind: "llm", Name: "llm claude-opus-4.6", Seq: 1,
							StartedAt: started,
							Metadata:  map[string]any{"call_kind": "main", "model": "claude-opus-4.6"},
							Input:     map[string]json.RawMessage{"content": json.RawMessage(`[{"type":"text","text":"do the thing"}]`)},
							Output:    map[string]json.RawMessage{"content": json.RawMessage(`[{"type":"text","text":"on it"},{"type":"tool_use","tool_name":"Bash","tool_use_id":"t1"}]`)},
							Metrics:   json.RawMessage(`{"prompt_tokens":120,"completion_tokens":30}`),
						},
						{
							SpanID: "sp-2", Kind: "tool", Name: "Bash", Seq: 2,
							StartedAt: started.Add(2 * time.Second),
							Metrics:   json.RawMessage(`{}`),
						},
						{
							SpanID: "sp-3", Kind: "llm", Name: "haiku title", Seq: 3,
							StartedAt: started.Add(3 * time.Second),
							Metadata:  map[string]any{"call_kind": "offshoot:topic-detection"},
							Metrics:   json.RawMessage(`{}`),
						},
						{
							SpanID: "sp-4", Kind: "event", Name: "claude-md", Seq: 4,
							StartedAt: started,
							Metadata:  map[string]any{"call_kind": "injected:claude-md"},
							Metrics:   json.RawMessage(`{}`),
						},
					},
				})).To(Succeed())
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
