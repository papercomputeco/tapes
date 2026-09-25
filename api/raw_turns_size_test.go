package api

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	tapeslogger "github.com/papercomputeco/tapes/pkg/logger"
	"github.com/papercomputeco/tapes/pkg/storage"
	"github.com/papercomputeco/tapes/pkg/storage/inmemory"
)

// rawTurnHeaderStub is the driver behind the raw-turn listing specs: one
// session and the wire-log headers the store returns for it. The
// embedded nil interfaces satisfy the capability checks; only the two
// methods the listing calls are real, so a handler that reached for the
// payloads would nil-panic here rather than quietly measure them.
type rawTurnHeaderStub struct {
	storage.Driver
	sessionsReader
	storage.SpanModelReader

	session storage.SessionRecord
	headers []storage.RawTurnHeader
}

func (d *rawTurnHeaderStub) GetSessionRecord(_ context.Context, _, id string) (*storage.SessionRecord, error) {
	if id != d.session.ID {
		return nil, nil
	}
	sess := d.session
	return &sess, nil
}

// ListRawTurnHeaders pages the stub's headers the way the store does:
// id order, strictly after afterID, at most limit of them.
func (d *rawTurnHeaderStub) ListRawTurnHeaders(_ context.Context, _, harnessID, harnessSessionID string, afterID int64, limit int) ([]storage.RawTurnHeader, error) {
	if harnessID != d.session.HarnessID || harnessSessionID != d.session.HarnessSessionID {
		return nil, nil
	}
	var page []storage.RawTurnHeader
	for _, h := range d.headers {
		if h.ID <= afterID {
			continue
		}
		if len(page) == limit {
			break
		}
		page = append(page, h)
	}
	return page, nil
}

var _ = Describe("GET /v1/sessions/{id}/raw_turns sizes", func() {
	const sessionID = "7a1d2c3e-4b5f-4a6b-8c7d-9e0f1a2b3c4d"

	session := storage.SessionRecord{
		ID:               sessionID,
		HarnessID:        "claude-code",
		HarnessSessionID: "harness-session-1",
	}

	// listRawTurns serves the listing off the stub and decodes the items as
	// generic JSON, so a field's presence or absence on the wire is visible —
	// a typed decode would zero a missing field and hide the omission.
	listRawTurns := func(headers []storage.RawTurnHeader) []map[string]any {
		GinkgoHelper()
		driver := &rawTurnHeaderStub{Driver: inmemory.NewDriver(), session: session, headers: headers}
		server, err := NewServer(Config{ListenAddr: ":0"}, driver, tapeslogger.NewNoop())
		Expect(err).NotTo(HaveOccurred())

		req, err := http.NewRequestWithContext(context.Background(), http.MethodGet,
			"/v1/sessions/"+sessionID+"/raw_turns", nil)
		Expect(err).NotTo(HaveOccurred())
		resp, err := server.app.Test(req)
		Expect(err).NotTo(HaveOccurred())
		defer resp.Body.Close()
		Expect(resp.StatusCode).To(Equal(http.StatusOK))

		var body struct {
			Items []map[string]any `json:"items"`
		}
		Expect(json.NewDecoder(resp.Body).Decode(&body)).To(Succeed())
		return body.Items
	}

	It("raw turn sizes come from meta, not payload length", func() {
		// The store hands the handler the capture-time sizes it read from
		// the row's meta. The listing must report those verbatim — it has no
		// payload to measure and must not go looking for one.
		items := listRawTurns([]storage.RawTurnHeader{{
			ID:            1,
			Source:        storage.RawTurnSourceWire,
			Provider:      "anthropic",
			RequestID:     "req-1",
			ReceivedAt:    time.Date(2026, 9, 24, 12, 0, 0, 0, time.UTC),
			Meta:          json.RawMessage(`{"request_id":"req-1","request_bytes":12345,"response_bytes":678}`),
			RequestBytes:  12345,
			ResponseBytes: 678,
		}})

		Expect(items).To(HaveLen(1))
		Expect(items[0]).To(HaveKeyWithValue("request_bytes", float64(12345)))
		Expect(items[0]).To(HaveKeyWithValue("response_bytes", float64(678)))
		Expect(items[0]).To(HaveKeyWithValue("meta", HaveKeyWithValue("request_bytes", float64(12345))),
			"meta rides along verbatim beside the typed size")
	})

	It("reports raw response bytes and the dropped flag", func() {
		items := listRawTurns([]storage.RawTurnHeader{
			{
				ID:               1,
				Source:           storage.RawTurnSourceWire,
				RequestID:        "req-kept",
				ReceivedAt:       time.Date(2026, 9, 24, 12, 0, 0, 0, time.UTC),
				RequestBytes:     10,
				ResponseBytes:    20,
				RawResponseBytes: 4096,
			},
			{
				ID:                 2,
				Source:             storage.RawTurnSourceWire,
				RequestID:          "req-dropped",
				ReceivedAt:         time.Date(2026, 9, 24, 12, 0, 1, 0, time.UTC),
				RequestBytes:       10,
				ResponseBytes:      20,
				RawResponseDropped: true,
			},
		})

		Expect(items).To(HaveLen(2))

		Expect(items[0]).To(HaveKeyWithValue("raw_response_bytes", float64(4096)))
		Expect(items[0]).To(HaveKeyWithValue("raw_response_dropped", false),
			"the flag is always present, so a retained response reads as explicitly not dropped")

		Expect(items[1]).To(HaveKeyWithValue("raw_response_bytes", float64(0)),
			"zero raw bytes is always reported, never omitted")
		Expect(items[1]).To(HaveKeyWithValue("raw_response_dropped", true),
			"a dropped verbatim response is a fidelity gap the listing must disclose")
	})
})
