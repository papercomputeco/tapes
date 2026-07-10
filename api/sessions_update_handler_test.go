package api

// Specs for PATCH /v1/sessions/:id (handleUpdateSession) land here in the
// tester phase, driven against sessionsStubDriver (see
// sessions_handlers_test.go) — no Postgres required. Covers: trim/empty->NULL
// normalization, length>200->400, missing name field->400, 200 + updated
// summary via GetSessionRecord, cross-org/unknown id->404 via
// rowsAffected==0, and the 501-when-unsupported path (CC-2, CC-3, CC-4).

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/gofiber/fiber/v2"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/llm"
	tapeslogger "github.com/papercomputeco/tapes/pkg/logger"
	"github.com/papercomputeco/tapes/pkg/storage"
	"github.com/papercomputeco/tapes/pkg/storage/inmemory"
)

// patchSession issues a PATCH request against the server with the given raw
// JSON body (verbatim, so callers can omit fields or send explicit null),
// optionally with the X-Tapes-Org-Id header, and decodes the response as a
// SessionDetailResponse on 200 or an llm.ErrorResponse otherwise.
func patchSession(server *Server, path, org, rawBody string) (SessionDetailResponse, llm.ErrorResponse, int) {
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPatch, path, bytes.NewBufferString(rawBody))
	Expect(err).NotTo(HaveOccurred())
	req.Header.Set("Content-Type", "application/json")
	if org != "" {
		req.Header.Set(orgIDHeader, org)
	}
	resp, err := server.app.Test(req)
	Expect(err).NotTo(HaveOccurred())
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	Expect(err).NotTo(HaveOccurred())

	var body SessionDetailResponse
	var errBody llm.ErrorResponse
	switch {
	case resp.StatusCode == fiber.StatusOK:
		Expect(json.Unmarshal(raw, &body)).To(Succeed())
	case len(raw) > 0:
		// A handler that still panics ("not implemented") is recovered by
		// fiber's recover middleware into a plain-text 500, not JSON; a
		// real 4xx/5xx from the handler is JSON (llm.ErrorResponse). Fall
		// back to the raw text so callers still see *something* useful
		// (and so a not-yet-implemented handler fails on the assertions
		// below rather than here on a decode error).
		if err := json.Unmarshal(raw, &errBody); err != nil {
			errBody.Error = string(raw)
		}
	}
	return body, errBody, resp.StatusCode
}

var _ = Describe("PATCH /v1/sessions/:id (handleUpdateSession)", func() {
	const sessionID = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
	const org = "11111111-1111-1111-1111-111111111111"

	var record storage.SessionRecord

	newSessionsServer := func(driver storage.Driver) *Server {
		server, err := NewServer(Config{ListenAddr: ":0"}, driver, tapeslogger.NewNoop())
		Expect(err).NotTo(HaveOccurred())
		return server
	}

	BeforeEach(func() {
		started := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
		record = storage.SessionRecord{
			ID:               sessionID,
			HarnessID:        "claude",
			HarnessSessionID: "sess-xyz",
			Name:             "My corrected title",
			StartedAt:        started,
			LastSeenAt:       started,
			DerivedStatus:    "completed",
		}
	})

	It("test_update_session_name_persists", func() {
		// Given a driver that reports one row affected and echoes the
		// updated record back on the post-write re-read
		drv := &sessionsStubDriver{
			Driver:             inmemory.NewDriver(),
			updateRowsAffected: 1,
			getRecord:          &record,
		}
		server := newSessionsServer(drv)

		// When the client PATCHes a valid name
		_, _, status := patchSession(server, "/v1/sessions/"+sessionID, org, `{"name":"My corrected title"}`)

		// Then the request succeeds and the driver saw the trimmed name
		// plus the org id threaded from context
		Expect(status).To(Equal(fiber.StatusOK))
		Expect(drv.updateCalls).To(Equal(1))
		Expect(drv.lastUpdateOrgID).To(Equal(org))
		Expect(drv.lastUpdateID).To(Equal(sessionID))
		Expect(drv.lastUpdateName).NotTo(BeNil())
		Expect(*drv.lastUpdateName).To(Equal("My corrected title"))
	})

	It("test_update_session_name_trims_whitespace", func() {
		// Given a driver that accepts the update
		drv := &sessionsStubDriver{
			Driver:             inmemory.NewDriver(),
			updateRowsAffected: 1,
			getRecord:          &record,
		}
		server := newSessionsServer(drv)

		// When the client PATCHes a name padded with leading/trailing
		// whitespace
		_, _, status := patchSession(server, "/v1/sessions/"+sessionID, org, `{"name":"   spaced title   "}`)

		// Then the server trims before calling the driver (CC-3)
		Expect(status).To(Equal(fiber.StatusOK))
		Expect(drv.updateCalls).To(Equal(1))
		Expect(drv.lastUpdateName).NotTo(BeNil())
		Expect(*drv.lastUpdateName).To(Equal("spaced title"))
	})

	It("test_update_session_name_rejects_over_length", func() {
		// Given a driver that would otherwise accept the update
		drv := &sessionsStubDriver{
			Driver:             inmemory.NewDriver(),
			updateRowsAffected: 1,
			getRecord:          &record,
		}
		server := newSessionsServer(drv)

		// When the client PATCHes a name over 200 characters after
		// trimming
		overLong := "  " + strings.Repeat("a", 201) + "  "
		_, errBody, status := patchSession(server, "/v1/sessions/"+sessionID, org, `{"name":"`+overLong+`"}`)

		// Then the server rejects it before ever calling the driver
		// (CC-3, EST-9/10)
		Expect(status).To(Equal(fiber.StatusBadRequest))
		Expect(errBody.Error).NotTo(BeEmpty())
		Expect(drv.updateCalls).To(BeZero(), "validation must precede any storage call")
	})

	It("test_update_session_name_missing_field_400", func() {
		// Given a driver that would otherwise accept the update
		drv := &sessionsStubDriver{
			Driver:             inmemory.NewDriver(),
			updateRowsAffected: 1,
			getRecord:          &record,
		}
		server := newSessionsServer(drv)

		// When the client PATCHes a body with no "name" field at all
		_, errBody, status := patchSession(server, "/v1/sessions/"+sessionID, org, `{}`)

		// Then the request is rejected as having nothing to update
		// (EST-6) — this is distinct from an explicit null/empty, which
		// is a valid "clear" and must not 400.
		Expect(status).To(Equal(fiber.StatusBadRequest))
		Expect(errBody.Error).NotTo(BeEmpty())
		Expect(drv.updateCalls).To(BeZero(), "validation must precede any storage call")
	})

	It("test_update_session_name_returns_updated_summary", func() {
		// Given a driver that reports success and, on the post-write
		// re-read, returns the record reflecting the new name
		updated := record
		updated.Name = "My corrected title"
		drv := &sessionsStubDriver{
			Driver:             inmemory.NewDriver(),
			updateRowsAffected: 1,
			getRecord:          &updated,
		}
		server := newSessionsServer(drv)

		// When the client PATCHes the name
		body, _, status := patchSession(server, "/v1/sessions/"+sessionID, org, `{"name":"My corrected title"}`)

		// Then the handler re-reads via GetSessionRecord and returns the
		// updated session summary shape (EST-2)
		Expect(status).To(Equal(fiber.StatusOK))
		Expect(drv.getCalls).To(Equal(1), "success must re-read the record rather than echoing the request")
		Expect(drv.lastGetOrgID).To(Equal(org))
		Expect(drv.lastGetID).To(Equal(sessionID))
		Expect(body.Session.ID).To(Equal(sessionID))
		Expect(body.Session.Name).To(Equal("My corrected title"))
	})

	It("test_update_session_name_cross_org_404", func() {
		// Given a driver whose storage update predicate matched zero rows
		// (org mismatch or unknown id)
		drv := &sessionsStubDriver{
			Driver:             inmemory.NewDriver(),
			updateRowsAffected: 0,
			getRecord:          &record,
		}
		server := newSessionsServer(drv)

		// When the client PATCHes a valid name
		_, errBody, status := patchSession(server, "/v1/sessions/"+sessionID, org, `{"name":"My corrected title"}`)

		// Then the handler reports not-found rather than a false success
		// (CC-2, EST-7), and never falls through to the re-read
		Expect(status).To(Equal(fiber.StatusNotFound))
		Expect(errBody.Error).NotTo(BeEmpty())
		Expect(drv.updateCalls).To(Equal(1))
		Expect(drv.getCalls).To(BeZero(), "a rowsAffected==0 result must not trigger the success re-read")
	})
})
