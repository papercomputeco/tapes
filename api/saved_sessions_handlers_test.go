package api

import (
	"bytes"
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

// savedStubDriver implements the savedSessionsStore capability with canned
// per-id responses, recording arguments so specs can assert org and subject
// threading. Follows the sessionsStubDriver pattern above.
type savedStubDriver struct {
	storage.Driver

	saveRecs    map[string]*storage.SavedSessionRecord // nil value = not found
	saveErr     error
	saveCalls   int
	lastOrgID   string
	lastSavedBy string

	unsaveDeleted bool
	unsaveErr     error
	unsaveCalls   int

	listRecs []storage.SavedSessionRecord
	listErr  error
}

func (d *savedStubDriver) SaveSession(_ context.Context, orgID, sessionID, savedBy string) (*storage.SavedSessionRecord, error) {
	d.saveCalls++
	d.lastOrgID = orgID
	d.lastSavedBy = savedBy
	return d.saveRecs[sessionID], d.saveErr
}

func (d *savedStubDriver) UnsaveSession(_ context.Context, orgID, _ string) (bool, error) {
	d.unsaveCalls++
	d.lastOrgID = orgID
	return d.unsaveDeleted, d.unsaveErr
}

func (d *savedStubDriver) ListSavedSessions(_ context.Context, orgID string) ([]storage.SavedSessionRecord, error) {
	d.lastOrgID = orgID
	return d.listRecs, d.listErr
}

func savedRequest(server *Server, method, path, org, subject string, body any) (map[string]any, int) {
	var reader io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		Expect(err).NotTo(HaveOccurred())
		reader = bytes.NewReader(raw)
	}
	req, err := http.NewRequestWithContext(context.Background(), method, path, reader)
	Expect(err).NotTo(HaveOccurred())
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if org != "" {
		req.Header.Set(orgIDHeader, org)
	}
	if subject != "" {
		req.Header.Set(authSubjectHeader, subject)
	}
	resp, err := server.app.Test(req)
	Expect(err).NotTo(HaveOccurred())
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	Expect(err).NotTo(HaveOccurred())
	decoded := map[string]any{}
	if len(raw) > 0 {
		Expect(json.Unmarshal(raw, &decoded)).To(Succeed())
	}
	return decoded, resp.StatusCode
}

var _ = Describe("saved sessions endpoints", func() {
	const (
		org = "11111111-1111-1111-1111-111111111111"
		sid = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
	)
	savedAt := time.Date(2026, 7, 11, 12, 0, 0, 0, time.UTC)

	newServer := func(driver storage.Driver) *Server {
		server, err := NewServer(Config{ListenAddr: ":0"}, driver, tapeslogger.NewNoop())
		Expect(err).NotTo(HaveOccurred())
		return server
	}

	It("PUT /v1/sessions/:id/save records the caller's subject and returns the marker", func() {
		drv := &savedStubDriver{
			Driver: inmemory.NewDriver(),
			saveRecs: map[string]*storage.SavedSessionRecord{
				sid: {SessionID: sid, SavedBy: "user_alice", SavedAt: savedAt},
			},
		}
		server := newServer(drv)

		body, status := savedRequest(server, http.MethodPut, "/v1/sessions/"+sid+"/save", org, "user_alice", nil)
		Expect(status).To(Equal(fiber.StatusOK))
		Expect(body["session_id"]).To(Equal(sid))
		Expect(body["saved_by"]).To(Equal("user_alice"))
		Expect(drv.saveCalls).To(Equal(1))
		Expect(drv.lastOrgID).To(Equal(org))
		Expect(drv.lastSavedBy).To(Equal("user_alice"))
	})

	It("PUT /v1/sessions/:id/save 404s for an unknown session", func() {
		drv := &savedStubDriver{Driver: inmemory.NewDriver(), saveRecs: map[string]*storage.SavedSessionRecord{}}
		server := newServer(drv)

		body, status := savedRequest(server, http.MethodPut, "/v1/sessions/"+sid+"/save", org, "user_alice", nil)
		Expect(status).To(Equal(fiber.StatusNotFound))
		Expect(body["error"]).To(Equal("session not found"))
	})

	It("DELETE /v1/sessions/:id/save is a 204 even when already absent", func() {
		drv := &savedStubDriver{Driver: inmemory.NewDriver(), unsaveDeleted: false}
		server := newServer(drv)

		_, status := savedRequest(server, http.MethodDelete, "/v1/sessions/"+sid+"/save", org, "", nil)
		Expect(status).To(Equal(fiber.StatusNoContent))
		Expect(drv.unsaveCalls).To(Equal(1))
	})

	It("GET /v1/saved_sessions lists the org's markers", func() {
		drv := &savedStubDriver{
			Driver:   inmemory.NewDriver(),
			listRecs: []storage.SavedSessionRecord{{SessionID: sid, SavedBy: "user_alice", SavedAt: savedAt}},
		}
		server := newServer(drv)

		body, status := savedRequest(server, http.MethodGet, "/v1/saved_sessions", org, "", nil)
		Expect(status).To(Equal(fiber.StatusOK))
		items, ok := body["items"].([]any)
		Expect(ok).To(BeTrue())
		Expect(items).To(HaveLen(1))
		Expect(drv.lastOrgID).To(Equal(org))
	})

	It("PUT /v1/sessions/save batch-saves and reports unknown ids", func() {
		other := "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
		drv := &savedStubDriver{
			Driver: inmemory.NewDriver(),
			saveRecs: map[string]*storage.SavedSessionRecord{
				sid: {SessionID: sid, SavedBy: "user_alice", SavedAt: savedAt},
			},
		}
		server := newServer(drv)

		body, status := savedRequest(server, http.MethodPut, "/v1/sessions/save", org, "user_alice",
			map[string]any{"session_ids": []string{sid, other}})
		Expect(status).To(Equal(fiber.StatusOK))
		items, ok := body["items"].([]any)
		Expect(ok).To(BeTrue())
		Expect(items).To(HaveLen(1))
		notFound, ok := body["not_found"].([]any)
		Expect(ok).To(BeTrue())
		Expect(notFound).To(ConsistOf(other))
		Expect(drv.saveCalls).To(Equal(2))
	})

	It("PUT /v1/sessions/save 400s on an empty id list", func() {
		drv := &savedStubDriver{Driver: inmemory.NewDriver()}
		server := newServer(drv)

		body, status := savedRequest(server, http.MethodPut, "/v1/sessions/save", org, "",
			map[string]any{"session_ids": []string{}})
		Expect(status).To(Equal(fiber.StatusBadRequest))
		Expect(body["error"]).To(Equal("session_ids is required and must be non-empty"))
	})
})
