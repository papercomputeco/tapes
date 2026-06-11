package api

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"time"

	"github.com/gofiber/fiber/v2"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	tapeslogger "github.com/papercomputeco/tapes/pkg/logger"
	"github.com/papercomputeco/tapes/pkg/storage"
	"github.com/papercomputeco/tapes/pkg/storage/inmemory"
)

type traceDriver struct {
	storage.Driver

	traces       []storage.TraceRecord
	spans        []storage.SpanRecord
	links        []storage.SpanLinkRecord
	listErr      error
	getErr       error
	lastListOrg  string
	lastGetOrg   string
	lastTraceID  string
	lastLimit    int
	lastCursorID string
}

func (d *traceDriver) ListTraceRecords(_ context.Context, orgID string, limit int, _ *time.Time, cursorTraceID *string) ([]storage.TraceRecord, error) {
	d.lastListOrg = orgID
	d.lastLimit = limit
	if cursorTraceID != nil {
		d.lastCursorID = *cursorTraceID
	}
	if d.listErr != nil {
		return nil, d.listErr
	}
	if limit > 0 && len(d.traces) > limit {
		return d.traces[:limit], nil
	}
	return d.traces, nil
}

func (d *traceDriver) GetTrace(_ context.Context, orgID, traceID string) (*storage.TraceRecord, []storage.SpanRecord, []storage.SpanLinkRecord, error) {
	d.lastGetOrg = orgID
	d.lastTraceID = traceID
	if d.getErr != nil {
		return nil, nil, nil, d.getErr
	}
	for i := range d.traces {
		if d.traces[i].TraceID == traceID {
			return &d.traces[i], d.spans, d.links, nil
		}
	}
	return nil, nil, nil, nil
}

func newTraceTestServer(driver storage.Driver) *Server {
	server, err := NewServer(Config{ListenAddr: ":0"}, driver, tapeslogger.NewNoop())
	Expect(err).NotTo(HaveOccurred())
	return server
}

func doTraceReq(server *Server, path, org string) ([]byte, int) {
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, path, nil)
	Expect(err).NotTo(HaveOccurred())
	if org != "" {
		req.Header.Set(orgIDHeader, org)
	}
	resp, err := server.app.Test(req)
	Expect(err).NotTo(HaveOccurred())
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	Expect(err).NotTo(HaveOccurred())
	return raw, resp.StatusCode
}

var _ = Describe("span trace API", func() {
	var trace storage.TraceRecord

	BeforeEach(func() {
		trace = storage.TraceRecord{
			ID:                "turn-1",
			SessionID:         "session-1",
			TraceID:           "trc_abc",
			HarnessID:         "pi",
			HarnessSessionID:  "pi-session",
			UserPrompt:        "build a thing",
			Status:            "ok",
			StartedAt:         time.Date(2026, 6, 10, 12, 0, 0, 0, time.UTC),
			TotalInputTokens:  10,
			TotalOutputTokens: 5,
			SpanCount:         2,
			Metadata:          map[string]any{"model": "gpt-test"},
		}
	})

	It("lists traces and threads tenant org to the storage capability", func() {
		drv := &traceDriver{Driver: inmemory.NewDriver(), traces: []storage.TraceRecord{trace}}
		server := newTraceTestServer(drv)
		raw, status := doTraceReq(server, "/v1/traces?limit=1", "11111111-1111-1111-1111-111111111111")
		Expect(status).To(Equal(fiber.StatusOK))
		Expect(drv.lastListOrg).To(Equal("11111111-1111-1111-1111-111111111111"))
		Expect(drv.lastLimit).To(Equal(2), "handler fetches one extra row for pagination")

		var body TraceListResponse
		Expect(json.Unmarshal(raw, &body)).To(Succeed())
		Expect(body.Items).To(HaveLen(1))
		Expect(body.Items[0].TraceID).To(Equal("trc_abc"))
		Expect(body.Items[0].UserPrompt).To(Equal("build a thing"))
	})

	It("returns a trace with child ids computed from parent_span_id", func() {
		drv := &traceDriver{
			Driver: inmemory.NewDriver(),
			traces: []storage.TraceRecord{trace},
			spans: []storage.SpanRecord{
				{ID: "1", TraceID: "trc_abc", SpanID: "agent_1", Kind: "agent", Name: "agent-request", Status: "ok", Input: json.RawMessage(`{}`), Output: json.RawMessage(`{}`), Metadata: json.RawMessage(`{}`), Metrics: json.RawMessage(`{}`)},
				{ID: "2", TraceID: "trc_abc", SpanID: "llm_1", ParentSpanID: "agent_1", Kind: "llm", Name: "gpt-test", Status: "ok", Input: json.RawMessage(`{}`), Output: json.RawMessage(`{}`), Metadata: json.RawMessage(`{}`), Metrics: json.RawMessage(`{}`)},
			},
		}
		server := newTraceTestServer(drv)
		raw, status := doTraceReq(server, "/v1/traces/trc_abc", "")
		Expect(status).To(Equal(fiber.StatusOK))
		Expect(drv.lastGetOrg).To(Equal(nilOrgID))
		Expect(drv.lastTraceID).To(Equal("trc_abc"))

		var body TraceDetailResponse
		Expect(json.Unmarshal(raw, &body)).To(Succeed())
		Expect(body.Trace.TraceID).To(Equal("trc_abc"))
		Expect(body.Spans).To(HaveLen(2))
		Expect(body.Spans[0].ChildrenIDs).To(ContainElement("llm_1"))
	})

	It("returns 501 when the backend has no span read capability", func() {
		server := newTraceTestServer(inmemory.NewDriver())
		_, status := doTraceReq(server, "/v1/traces", "")
		Expect(status).To(Equal(fiber.StatusNotImplemented))
	})
})
