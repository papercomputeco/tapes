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

var _ = Describe("effectiveSinceCutoff", func() {
	It("returns zero when no bounds are set", func() {
		Expect(effectiveSinceCutoff(Filters{}).IsZero()).To(BeTrue())
	})

	It("derives the cutoff from a relative since", func() {
		got := effectiveSinceCutoff(Filters{Since: time.Hour})
		Expect(got).To(BeTemporally("~", time.Now().Add(-time.Hour), time.Second))
	})

	It("uses from when it is the only bound", func() {
		from := time.Now().Add(-3 * time.Hour)
		Expect(effectiveSinceCutoff(Filters{From: &from})).To(BeTemporally("~", from, time.Second))
	})

	It("prefers since when it is later than from", func() {
		older := time.Now().Add(-12 * time.Hour)
		got := effectiveSinceCutoff(Filters{Since: time.Hour, From: &older})
		Expect(got).To(BeTemporally("~", time.Now().Add(-time.Hour), time.Second))
	})

	It("prefers from when it is later than since", func() {
		from := time.Now().Add(-3 * time.Hour)
		got := effectiveSinceCutoff(Filters{Since: 24 * time.Hour, From: &from})
		Expect(got).To(BeTemporally("~", from, time.Second))
	})
})

// The pushdown specs guard against regressing to "page through the whole
// corpus client-side": time bounds must reach /v1/sessions as query params
// so the server narrows the page before building rows.
var _ = Describe("HTTPQuery filter pushdown", func() {
	It("pushes time bounds down to /v1/sessions", func() {
		var paths []string
		var since, until string
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			paths = append(paths, r.URL.Path)
			since = r.URL.Query().Get("since")
			until = r.URL.Query().Get("until")
			Expect(json.NewEncoder(w).Encode(httpSessionListResponse{})).To(Succeed())
		}))
		defer srv.Close()

		from := time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC)
		to := time.Date(2026, 4, 2, 0, 0, 0, 0, time.UTC)
		q := NewHTTPQuery(srv.URL, nil)
		_, err := q.Overview(context.Background(), Filters{From: &from, To: &to})
		Expect(err).NotTo(HaveOccurred())

		Expect(paths).To(ConsistOf("/v1/sessions"))
		Expect(since).To(Equal(from.UTC().Format(time.RFC3339)))
		Expect(until).To(Equal(to.UTC().Format(time.RFC3339)))
	})

	It("omits query params the sessions endpoint does not understand", func() {
		var seen *http.Request
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			seen = r
			Expect(json.NewEncoder(w).Encode(httpSessionListResponse{})).To(Succeed())
		}))
		defer srv.Close()

		q := NewHTTPQuery(srv.URL, nil)
		_, err := q.Overview(context.Background(), Filters{Model: "claude-opus-4.6", Project: "tapes"})
		Expect(err).NotTo(HaveOccurred())

		Expect(seen).NotTo(BeNil())
		for _, key := range []string{"since", "until", "model", "project"} {
			Expect(seen.URL.Query().Has(key)).To(BeFalse(), "unexpected %q query param", key)
		}
	})
})

var _ = Describe("summaryFromSessionItem", func() {
	started := time.Date(2026, 6, 1, 10, 0, 0, 0, time.UTC)
	lastSeen := started.Add(45 * time.Minute)

	It("maps a sessions row onto the summary shape", func() {
		summary := summaryFromSessionItem(httpSessionItem{
			ID:         "9f0d8a4e-0000-0000-0000-000000000001",
			HarnessID:  "claude-code",
			Cwd:        "/Users/dev/workspace/tapes",
			StartedAt:  started,
			LastSeenAt: lastSeen,
			Rollup: httpSessionRollup{
				Title:     "port deck to traces",
				Status:    StatusCompleted,
				Model:     "claude-opus-4.6",
				TurnCount: 12,
				Usage:     httpSessionUsage{InputTokens: 1000, OutputTokens: 250, CostUSD: 1.25},
			},
		})

		Expect(summary.Label).To(Equal("port deck to traces"))
		Expect(summary.Project).To(Equal("tapes"))
		Expect(summary.AgentName).To(Equal("claude-code"))
		Expect(summary.Status).To(Equal(StatusCompleted))
		Expect(summary.StartTime).To(Equal(started))
		Expect(summary.EndTime).To(Equal(lastSeen))
		Expect(summary.Duration).To(Equal(45 * time.Minute))
		Expect(summary.InputTokens).To(Equal(int64(1000)))
		Expect(summary.OutputTokens).To(Equal(int64(250)))
		Expect(summary.TotalCost).To(Equal(1.25))
		Expect(summary.MessageCount).To(Equal(12))
	})

	It("prefers ended_at over last_seen_at when set", func() {
		ended := started.Add(10 * time.Minute)
		summary := summaryFromSessionItem(httpSessionItem{
			StartedAt:  started,
			LastSeenAt: lastSeen,
			EndedAt:    &ended,
		})
		Expect(summary.EndTime).To(Equal(ended))
		Expect(summary.Duration).To(Equal(10 * time.Minute))
	})

	It("falls back to the preview, then the id, for the label", func() {
		Expect(summaryFromSessionItem(httpSessionItem{
			ID:     "9f0d8a4e-0000-0000-0000-000000000001",
			Rollup: httpSessionRollup{Preview: "\nfix the flaky proxy test\nplease"},
		}).Label).To(Equal("fix the flaky proxy test"))

		Expect(summaryFromSessionItem(httpSessionItem{
			ID: "9f0d8a4e-0000-0000-0000-000000000001",
		}).Label).To(Equal("9f0d8a4e-..."))
	})
})

var _ = Describe("messagesFromTurns", func() {
	started := time.Date(2026, 6, 1, 10, 0, 0, 0, time.UTC)

	It("renders each turn as a prompt/response pair carrying the turn rollups", func() {
		turns := []TurnSummary{
			{
				TraceID:         "trace-1",
				UserPrompt:      "what changed?",
				ResponsePreview: "three files",
				StartedAt:       started,
				Duration:        30 * time.Second,
				InputTokens:     100,
				OutputTokens:    40,
				TotalCost:       0.05,
			},
			{
				TraceID:         "trace-2",
				UserPrompt:      "ship it",
				ResponsePreview: "done",
				StartedAt:       started.Add(2 * time.Minute),
				Duration:        10 * time.Second,
				InputTokens:     50,
				OutputTokens:    10,
				TotalCost:       0.01,
			},
		}

		messages := messagesFromTurns(turns, "claude-opus-4.6")
		Expect(messages).To(HaveLen(4))

		Expect(messages[0].Role).To(Equal("user"))
		Expect(messages[0].Text).To(Equal("what changed?"))
		Expect(messages[0].TraceID).To(Equal("trace-1"))
		Expect(messages[0].Timestamp).To(Equal(started))

		Expect(messages[1].Role).To(Equal("assistant"))
		Expect(messages[1].Text).To(Equal("three files"))
		Expect(messages[1].TotalTokens).To(Equal(int64(140)))
		Expect(messages[1].TotalCost).To(Equal(0.05))
		Expect(messages[1].Model).To(Equal("claude-opus-4.6"))
		Expect(messages[1].Delta).To(Equal(30 * time.Second))

		// The second turn's user delta is the idle gap after turn one ended.
		Expect(messages[2].Delta).To(Equal(90 * time.Second))
		Expect(messages[3].TraceID).To(Equal("trace-2"))
	})
})

var _ = Describe("projectFromCwd", func() {
	It("uses the basename of the working directory", func() {
		Expect(projectFromCwd("/Users/dev/workspace/tapes")).To(Equal("tapes"))
		Expect(projectFromCwd("/srv/app/")).To(Equal("app"))
	})

	It("returns empty for empty or root cwds", func() {
		Expect(projectFromCwd("")).To(Equal(""))
		Expect(projectFromCwd("/")).To(Equal(""))
	})
})
