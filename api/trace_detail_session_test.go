package api

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/logger"
	"github.com/papercomputeco/tapes/pkg/storage"
	"github.com/papercomputeco/tapes/pkg/storage/inmemory"
)

// stubSpanModel serves one canned trace. The embedded nil interface
// satisfies storage.SpanModelReader; only the method under test is real.
type stubSpanModel struct {
	storage.Driver
	storage.SpanModelReader
	turn storage.SpanTurnRecord
}

func (s *stubSpanModel) GetTraceDetail(_ context.Context, _, traceID string) (*storage.SpanTurnRecord, []storage.SpanRecord, []storage.SpanLinkRecord, error) {
	if traceID != s.turn.TraceID {
		return nil, nil, nil, nil
	}
	turn := s.turn
	return &turn, nil, nil, nil
}

var _ = Describe("standalone trace reads", func() {
	It("stamps the owning session on GET /v1/traces/:trace_id", func() {
		driver := &stubSpanModel{
			Driver: inmemory.NewDriver(),
			turn: storage.SpanTurnRecord{
				TraceID:   "trc_feedface",
				SessionID: "018f-session",
				StartedAt: time.Now().UTC(),
			},
		}
		server, err := NewServer(Config{ListenAddr: ":0"}, driver, logger.NewNoop())
		Expect(err).NotTo(HaveOccurred())

		request := httptest.NewRequest(http.MethodGet, "/v1/traces/trc_feedface", nil)
		response, err := server.app.Test(request)
		Expect(err).NotTo(HaveOccurred())
		defer response.Body.Close()
		body, err := io.ReadAll(response.Body)
		Expect(err).NotTo(HaveOccurred())
		Expect(response.StatusCode).To(Equal(200), string(body))

		var payload map[string]json.RawMessage
		Expect(json.Unmarshal(body, &payload)).To(Succeed())
		Expect(payload).To(HaveKey("session_id"),
			"a standalone lookup's caller does not know the session; the response must say")
		Expect(string(payload["session_id"])).To(Equal(`"018f-session"`))
	})

	It("keeps the session-scoped composite free of the duplicate", func() {
		detail := BuildTraceDetail(storage.SpanTurnRecord{
			TraceID:   "trc_feedface",
			SessionID: "018f-session",
		}, nil, nil, PayloadFull)
		encoded, err := json.Marshal(detail)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(encoded)).NotTo(ContainSubstring("session_id"),
			"inside a session's traces the id belongs to the session envelope; the composite schema carries no such field at all")
	})
})
