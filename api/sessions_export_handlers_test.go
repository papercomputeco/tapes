package api

import (
	"context"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/gofiber/fiber/v2"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	tapeslogger "github.com/papercomputeco/tapes/pkg/logger"
	"github.com/papercomputeco/tapes/pkg/storage"
	"github.com/papercomputeco/tapes/pkg/storage/inmemory"
)

// exportStubDriver wraps a real storage.Driver and implements the
// sessionsReader and spanModelReader capability interfaces needed by the
// export handlers and the in-process skillTraceQuerier they use.
type exportStubDriver struct {
	storage.Driver

	sessionsByOrg map[string]map[string]storage.SessionRecord // orgID -> sessionID -> record
	summaries     map[string][]storage.TraceSummaryRecord     // sessionID -> turns

	getSessionCalls int
	lastGetOrg      string
	lastGetID       string
}

func (d *exportStubDriver) ListSessionRecords(_ context.Context, _ string, _ storage.SessionListOpts) ([]storage.SessionRecord, error) {
	return nil, nil
}

func (d *exportStubDriver) GetSessionRecord(_ context.Context, orgID, id string) (*storage.SessionRecord, error) {
	d.getSessionCalls++
	d.lastGetOrg = orgID
	d.lastGetID = id
	byOrg, ok := d.sessionsByOrg[orgID]
	if !ok {
		return nil, nil
	}
	rec, ok := byOrg[id]
	if !ok {
		return nil, nil
	}
	return &rec, nil
}

func (d *exportStubDriver) GetSessionRecordByHarness(_ context.Context, _, _, _ string) (*storage.SessionRecord, error) {
	return nil, nil
}

func (d *exportStubDriver) ListTraceSummaries(_ context.Context, sessionID string) ([]storage.TraceSummaryRecord, error) {
	return d.summaries[sessionID], nil
}

func (d *exportStubDriver) GetTraceDetail(_ context.Context, _, _ string) (*storage.SpanTurnRecord, []storage.SpanRecord, []storage.SpanLinkRecord, error) {
	return nil, nil, nil, nil
}

func (d *exportStubDriver) ListSessionSpanModel(context.Context, string) ([]storage.SpanTurnRecord, []storage.SpanRecord, []storage.SpanLinkRecord, error) {
	return nil, nil, nil, nil
}

func (d *exportStubDriver) GetSpanRecord(context.Context, string, string, string) (*storage.SpanRecord, error) {
	return nil, nil
}

func (d *exportStubDriver) ListRawTurnHeaders(context.Context, string, string, string) ([]storage.RawTurnHeader, error) {
	return nil, nil
}

func newExportServer(driver storage.Driver) *Server {
	server, err := NewServer(Config{ListenAddr: ":0"}, driver, tapeslogger.NewNoop())
	Expect(err).NotTo(HaveOccurred())
	return server
}

// getRaw issues a GET against the server and returns the raw response
// (status, headers, body) without assuming any particular content type.
func getRaw(server *Server, path, org string) (*http.Response, []byte) {
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, path, nil)
	Expect(err).NotTo(HaveOccurred())
	if org != "" {
		req.Header.Set(orgIDHeader, org)
	}
	resp, err := server.app.Test(req, -1)
	Expect(err).NotTo(HaveOccurred())
	body, err := io.ReadAll(resp.Body)
	Expect(err).NotTo(HaveOccurred())
	defer resp.Body.Close()
	return resp, body
}

var _ = Describe("GET /v1/sessions/:id/export", func() {
	const org = "11111111-1111-1111-1111-111111111111"
	const sessionID = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"

	var record storage.SessionRecord

	BeforeEach(func() {
		started := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
		record = storage.SessionRecord{
			ID:         sessionID,
			HarnessID:  "claude",
			StartedAt:  started,
			LastSeenAt: started.Add(time.Minute),
		}
	})

	newDriverWithSession := func() *exportStubDriver {
		return &exportStubDriver{
			Driver: inmemory.NewDriver(),
			sessionsByOrg: map[string]map[string]storage.SessionRecord{
				org: {sessionID: record},
			},
			summaries: map[string][]storage.TraceSummaryRecord{
				sessionID: {
					{SpanTurnRecord: storage.SpanTurnRecord{
						TraceID: "t1", UserPrompt: "hi", ResponsePreview: "hello",
						StartedAt: record.StartedAt, TotalInputTokens: 10, TotalOutputTokens: 5,
					}},
					{SpanTurnRecord: storage.SpanTurnRecord{
						TraceID: "t2", UserPrompt: "thanks", ResponsePreview: "np",
						StartedAt: record.StartedAt.Add(time.Minute), TotalInputTokens: 4, TotalOutputTokens: 2,
					}},
				},
			},
		}
	}

	// T-3: 200 + headers + line count + full fidelity.
	It("returns 200 NDJSON with one line per turn and the expected headers", func() {
		drv := newDriverWithSession()
		server := newExportServer(drv)

		resp, body := getRaw(server, "/v1/sessions/"+sessionID+"/export", org)
		Expect(resp.StatusCode).To(Equal(fiber.StatusOK))
		Expect(resp.Header.Get("Content-Type")).To(ContainSubstring("application/x-ndjson"))

		lines := nonEmptyLinesAPI(string(body))
		Expect(lines).To(HaveLen(2))
		Expect(string(body)).To(ContainSubstring(`"trace_id":"t1"`))
		Expect(string(body)).To(ContainSubstring(`"trace_id":"t2"`))
		Expect(string(body)).To(ContainSubstring(`"session_id":"` + sessionID + `"`))

		// GetSessionRecord is called twice by design: once by the handler's
		// own pre-flight 404 gate (so no headers/body are ever sent for a
		// session outside the org), and once more inside
		// skillTraceQuerier.TraceSummaries, which independently re-scopes
		// every read to the bound org.
		Expect(drv.getSessionCalls).To(Equal(2))
		Expect(drv.lastGetOrg).To(Equal(org))
		Expect(drv.lastGetID).To(Equal(sessionID))
	})

	// T-4: filename includes the session id.
	It("sets Content-Disposition with a filename containing the session id", func() {
		drv := newDriverWithSession()
		server := newExportServer(drv)

		resp, _ := getRaw(server, "/v1/sessions/"+sessionID+"/export", org)
		Expect(resp.StatusCode).To(Equal(fiber.StatusOK))
		disposition := resp.Header.Get("Content-Disposition")
		Expect(disposition).To(ContainSubstring("attachment"))
		Expect(disposition).To(ContainSubstring(sessionID))
		Expect(disposition).To(ContainSubstring(".jsonl"))
	})

	// T-5: cross-org -> 404, no disclosure.
	It("returns 404 when the session belongs to a different org", func() {
		drv := newDriverWithSession()
		server := newExportServer(drv)

		otherOrg := "22222222-2222-2222-2222-222222222222"
		resp, body := getRaw(server, "/v1/sessions/"+sessionID+"/export", otherOrg)
		Expect(resp.StatusCode).To(Equal(fiber.StatusNotFound))
		Expect(string(body)).NotTo(ContainSubstring("t1"))
		Expect(drv.lastGetOrg).To(Equal(otherOrg))
	})

	It("returns 404 when the session does not exist", func() {
		drv := newDriverWithSession()
		server := newExportServer(drv)

		resp, _ := getRaw(server, "/v1/sessions/ffffffff-ffff-ffff-ffff-ffffffffffff/export", org)
		Expect(resp.StatusCode).To(Equal(fiber.StatusNotFound))
	})

	It("returns 400 for a malformed session id", func() {
		drv := newDriverWithSession()
		server := newExportServer(drv)

		resp, _ := getRaw(server, "/v1/sessions/not-a-uuid/export", org)
		Expect(resp.StatusCode).To(Equal(fiber.StatusBadRequest))
	})

	// T-6: missing capability -> 501.
	It("returns 501 when the driver does not implement the sessions/span read surface", func() {
		base := inmemory.NewDriver()
		server := newExportServer(base)

		resp, _ := getRaw(server, "/v1/sessions/"+sessionID+"/export", org)
		Expect(resp.StatusCode).To(Equal(fiber.StatusNotImplemented))
	})
})

func nonEmptyLinesAPI(s string) []string {
	var out []string
	for line := range strings.SplitSeq(s, "\n") {
		if strings.TrimSpace(line) != "" {
			out = append(out, line)
		}
	}
	return out
}
