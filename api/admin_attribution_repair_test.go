package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	tapeslogger "github.com/papercomputeco/tapes/pkg/logger"
	"github.com/papercomputeco/tapes/pkg/storage"
	"github.com/papercomputeco/tapes/pkg/storage/inmemory"
)

type attributionRepairStub struct {
	storage.Driver
	req    storage.RawTurnAttributionRepairRequest
	result storage.RawTurnAttributionRepairResult
	err    error
}

func (s *attributionRepairStub) RepairRawTurnAttribution(
	_ context.Context,
	_ string,
	req storage.RawTurnAttributionRepairRequest,
) (storage.RawTurnAttributionRepairResult, error) {
	s.req = req
	return s.result, s.err
}

var _ = Describe("raw-turn attribution repair admin endpoint", func() {
	newServer := func(driver storage.Driver) *Server {
		s, err := NewServer(Config{ListenAddr: ":0"}, driver, tapeslogger.NewNoop())
		Expect(err).NotTo(HaveOccurred())
		return s
	}

	request := func(s *Server, body string) *http.Response {
		req, err := http.NewRequestWithContext(context.Background(), http.MethodPost,
			"/v1/admin/raw-turns/attribution-repair", bytes.NewBufferString(body))
		Expect(err).NotTo(HaveOccurred())
		req.Header.Set("Content-Type", "application/json")
		resp, err := s.app.Test(req)
		Expect(err).NotTo(HaveOccurred())
		return resp
	}

	It("threads an exact correlation selector to storage under the single-tenant sentinel", func() {
		stub := &attributionRepairStub{Driver: inmemory.NewDriver(), result: storage.RawTurnAttributionRepairResult{Recorded: true}}
		resp := request(newServer(stub), `{"paper_proxy_request_id":"proxy-1","harness_id":"codex","harness_session_id":"child","thread_id":"thread","reason":"hook evidence"}`)
		defer resp.Body.Close()
		Expect(resp.StatusCode).To(Equal(http.StatusOK))
		Expect(stub.req.OrgID).To(Equal(singleTenantOrgID),
			"repairs scope to the deployment's single tenant, never a caller-asserted org")
		Expect(stub.req.PaperProxyRequestID).To(Equal("proxy-1"))
	})

	It("rejects ambiguous selector shapes before calling storage", func() {
		stub := &attributionRepairStub{Driver: inmemory.NewDriver()}
		resp := request(newServer(stub), `{"raw_turn_id":1,"paper_proxy_request_id":"proxy-1","harness_id":"codex","harness_session_id":"child","reason":"evidence"}`)
		defer resp.Body.Close()
		Expect(resp.StatusCode).To(Equal(http.StatusBadRequest))
		Expect(stub.req).To(Equal(storage.RawTurnAttributionRepairRequest{}))
	})

	It("rejects a session that names itself as its parent", func() {
		stub := &attributionRepairStub{Driver: inmemory.NewDriver()}
		resp := request(newServer(stub), `{"raw_turn_id":1,"harness_id":"codex","harness_session_id":"child","parent_harness_session_id":"child","reason":"evidence"}`)
		defer resp.Body.Close()
		Expect(resp.StatusCode).To(Equal(http.StatusBadRequest))
		Expect(stub.req).To(Equal(storage.RawTurnAttributionRepairRequest{}))
	})

	It("maps missing and ambiguous correlations", func() {
		stub := &attributionRepairStub{Driver: inmemory.NewDriver(), err: storage.ErrRawTurnNotFound}
		resp := request(newServer(stub), `{"raw_turn_id":1,"harness_id":"codex","harness_session_id":"child","reason":"evidence"}`)
		resp.Body.Close()
		Expect(resp.StatusCode).To(Equal(http.StatusNotFound))

		stub.err = storage.ErrRawTurnAmbiguous
		resp = request(newServer(stub), `{"paper_proxy_request_id":"proxy-1","harness_id":"codex","harness_session_id":"child","reason":"evidence"}`)
		defer resp.Body.Close()
		Expect(resp.StatusCode).To(Equal(http.StatusConflict))
	})

	It("returns not implemented for a driver without the repair capability", func() {
		resp := request(newServer(inmemory.NewDriver()), `{"raw_turn_id":1,"harness_id":"codex","harness_session_id":"child","reason":"evidence"}`)
		defer resp.Body.Close()
		Expect(resp.StatusCode).To(Equal(http.StatusNotImplemented))
	})

	It("returns 202 with the partial result when the correction recorded but projections are pending", func() {
		pending := storage.RawTurnAttributionRepairResult{
			Recorded: true,
			ProjectionsPending: []storage.RepairPendingSession{
				{HarnessID: "codex", HarnessSessionID: "child"},
			},
		}
		stub := &attributionRepairStub{
			Driver: inmemory.NewDriver(),
			result: pending,
			err:    fmt.Errorf("%w: rederive effective session: boom", storage.ErrRepairProjectionsPending),
		}
		resp := request(newServer(stub), `{"raw_turn_id":1,"harness_id":"codex","harness_session_id":"child","reason":"evidence"}`)
		defer resp.Body.Close()
		Expect(resp.StatusCode).To(Equal(http.StatusAccepted))
		var body storage.RawTurnAttributionRepairResult
		Expect(json.NewDecoder(resp.Body).Decode(&body)).To(Succeed())
		Expect(body.Recorded).To(BeTrue())
		Expect(body.ProjectionsPending).To(Equal(pending.ProjectionsPending),
			"the response must name the sessions still awaiting the derive worker")
	})

	It("returns 200 with source_cleanup_pending when only the cosmetic source cleanup failed", func() {
		stub := &attributionRepairStub{
			Driver: inmemory.NewDriver(),
			result: storage.RawTurnAttributionRepairResult{Recorded: true, SourceCleanupPending: true},
		}
		resp := request(newServer(stub), `{"raw_turn_id":1,"harness_id":"codex","harness_session_id":"child","reason":"evidence"}`)
		defer resp.Body.Close()
		Expect(resp.StatusCode).To(Equal(http.StatusOK),
			"an applied repair with a leftover empty source row is a success, not a 500")
		var body storage.RawTurnAttributionRepairResult
		Expect(json.NewDecoder(resp.Body).Decode(&body)).To(Succeed())
		Expect(body.Recorded).To(BeTrue())
		Expect(body.SourceCleanupPending).To(BeTrue(),
			"the response must disclose the leftover source row")
		Expect(body.ProjectionsPending).To(BeEmpty())
	})

	It("maps unexpected storage failures to internal server error", func() {
		stub := &attributionRepairStub{Driver: inmemory.NewDriver(), err: errors.New("boom")}
		resp := request(newServer(stub), `{"raw_turn_id":1,"harness_id":"codex","harness_session_id":"child","reason":"evidence"}`)
		defer resp.Body.Close()
		Expect(resp.StatusCode).To(Equal(http.StatusInternalServerError))
	})
})
