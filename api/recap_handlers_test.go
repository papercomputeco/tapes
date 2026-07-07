package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"time"

	"github.com/gofiber/fiber/v2"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	tapeslogger "github.com/papercomputeco/tapes/pkg/logger"
	"github.com/papercomputeco/tapes/pkg/storage"
	"github.com/papercomputeco/tapes/pkg/storage/inmemory"
)

// recapStubDriver implements the unexported recapStore capability plus the
// session/span read surface the generate handler's transcript querier needs,
// so specs exercise the full endpoint without Postgres or a real LLM.
type recapStubDriver struct {
	storage.Driver

	sessions map[string]storage.SessionRecord
	// Keyed by (org, session) — mirroring the composite PK — so specs catch a
	// handler that stops threading the caller's org into recap reads/writes.
	recaps  map[[2]string]storage.SessionRecapRecord
	lastOrg string
}

func newRecapStub() *recapStubDriver {
	return &recapStubDriver{
		sessions: map[string]storage.SessionRecord{},
		recaps:   map[[2]string]storage.SessionRecapRecord{},
	}
}

// --- recapStore ---

func (d *recapStubDriver) UpsertSessionRecap(_ context.Context, orgID string, rec storage.SessionRecapRecord) (*storage.SessionRecapRecord, error) {
	d.lastOrg = orgID
	d.recaps[[2]string{orgID, rec.SessionID}] = rec
	out := rec
	return &out, nil
}

func (d *recapStubDriver) GetSessionRecap(_ context.Context, orgID, sessionID string) (*storage.SessionRecapRecord, error) {
	d.lastOrg = orgID
	if r, ok := d.recaps[[2]string{orgID, sessionID}]; ok {
		out := r
		return &out, nil
	}
	return nil, nil
}

// --- sessionsReader ---

func (d *recapStubDriver) ListSessionRecords(_ context.Context, _ string, _ storage.SessionListOpts) ([]storage.SessionRecord, error) {
	return nil, nil
}

func (d *recapStubDriver) GetSessionRecord(_ context.Context, orgID, id string) (*storage.SessionRecord, error) {
	d.lastOrg = orgID
	if s, ok := d.sessions[id]; ok {
		out := s
		return &out, nil
	}
	return nil, nil
}

func (d *recapStubDriver) GetSessionRecordByHarness(_ context.Context, _, _, _ string) (*storage.SessionRecord, error) {
	return nil, nil
}

// --- storage.SpanModelReader (transcript source for the generate path) ---

func (d *recapStubDriver) ListSessionSpanModel(_ context.Context, _ string) ([]storage.SpanTurnRecord, []storage.SpanRecord, []storage.SpanLinkRecord, error) {
	return nil, nil, nil, nil
}

func (d *recapStubDriver) ListTraceSummaries(_ context.Context, sessionID string) ([]storage.TraceSummaryRecord, error) {
	return []storage.TraceSummaryRecord{
		{SpanTurnRecord: storage.SpanTurnRecord{
			TraceID:    "trc-1",
			SessionID:  sessionID,
			UserPrompt: "Fix the flaky login test",
			// No spans served below, so the transcript builder falls back to
			// this derive-time preview as the turn's response.
			ResponsePreview: "Pinned the clock in the session fixture.",
			StartedAt:       time.Now().Add(-time.Hour),
		}},
	}, nil
}

func (d *recapStubDriver) GetTraceDetail(_ context.Context, _, _ string) (*storage.SpanTurnRecord, []storage.SpanRecord, []storage.SpanLinkRecord, error) {
	return nil, nil, nil, nil
}

func (d *recapStubDriver) GetSpanRecord(_ context.Context, _, _, _ string) (*storage.SpanRecord, error) {
	return nil, nil
}

func (d *recapStubDriver) ListRawTurnHeaders(_ context.Context, _, _, _ string) ([]storage.RawTurnHeader, error) {
	return nil, nil
}

// seedRecapSession seeds a settled session with the given turn count.
func seedRecapSession(d *recapStubDriver, id string, turns int) {
	ended := time.Now().Add(-time.Hour)
	d.sessions[id] = storage.SessionRecord{
		ID:         id,
		StartedAt:  ended.Add(-time.Hour),
		LastSeenAt: ended,
		EndedAt:    &ended,
		TurnCount:  turns,
	}
}

// fakeOpenAI serves the chat-completions shape the openai caller expects,
// with the given content as the assistant message. calls counts requests.
func fakeOpenAI(content string, calls *int) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		*calls++
		resp := map[string]any{
			"choices": []map[string]any{
				{"message": map[string]any{"content": content}},
			},
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}))
}

const recapLLMJSON = `{"narrative": "The user set out to fix a flaky login test; the agent pinned the clock and the suite went green.", "observation": "Flaky tests here tend to be unpinned clocks."}`

func newRecapServer(stub *recapStubDriver, llmURL string) *Server {
	cfg := Config{ListenAddr: ":0"}
	if llmURL != "" {
		cfg.SkillLLMProvider = defaultSkillLLMProvider
		cfg.SkillLLMModel = "gpt-test"
		cfg.SkillLLMAPIKey = "test-key"
		cfg.SkillLLMBaseURL = llmURL
	}
	server, err := NewServer(cfg, stub, tapeslogger.NewNoop())
	Expect(err).NotTo(HaveOccurred())
	return server
}

var _ = Describe("Session recap handlers", func() {
	org := "22222222-2222-2222-2222-222222222222"

	It("404s GET when no recap has been generated", func() {
		stub := newRecapStub()
		seedRecapSession(stub, "sess-1", 3)
		server := newRecapServer(stub, "")
		body, status := doJSON(server, http.MethodGet, "/v1/sessions/sess-1/recap", "", org, "")
		Expect(status).To(Equal(fiber.StatusNotFound))
		Expect(body["error"]).To(ContainSubstring("no recap"))
		Expect(stub.lastOrg).To(Equal(org), "the read must be scoped to the requested tenant")
	})

	It("returns the stored recap on GET in snake_case", func() {
		stub := newRecapStub()
		stub.recaps[[2]string{org, "sess-1"}] = storage.SessionRecapRecord{
			SessionID:   "sess-1",
			Narrative:   "Fixed the flaky login test.",
			Observation: "Unpinned clocks cause flakes.",
			TurnCount:   3,
			Model:       "gpt-test",
			GeneratedAt: time.Date(2026, 7, 7, 0, 0, 0, 0, time.UTC),
		}
		server := newRecapServer(stub, "")
		body, status := doJSON(server, http.MethodGet, "/v1/sessions/sess-1/recap", "", org, "")
		Expect(status).To(Equal(fiber.StatusOK))
		Expect(body).To(HaveKeyWithValue("session_id", "sess-1"))
		Expect(body).To(HaveKeyWithValue("narrative", "Fixed the flaky login test."))
		Expect(body).To(HaveKeyWithValue("observation", "Unpinned clocks cause flakes."))
		Expect(body).To(HaveKeyWithValue("turn_count", BeNumerically("==", 3)))
		Expect(body).To(HaveKeyWithValue("generated_at", "2026-07-07T00:00:00Z"))
	})

	It("404s POST for a session the org cannot see", func() {
		stub := newRecapStub()
		server := newRecapServer(stub, "")
		body, status := doJSON(server, http.MethodPost, "/v1/sessions/sess-missing/recap", "", org, "")
		Expect(status).To(Equal(fiber.StatusNotFound))
		Expect(body["error"]).To(ContainSubstring("session not found"))
	})

	It("never serves another org's recap", func() {
		stub := newRecapStub()
		stub.recaps[[2]string{org, "sess-1"}] = storage.SessionRecapRecord{
			SessionID: "sess-1",
			Narrative: "Org A's recap.",
			TurnCount: 3,
		}
		server := newRecapServer(stub, "")
		otherOrg := "33333333-3333-3333-3333-333333333333"
		body, status := doJSON(server, http.MethodGet, "/v1/sessions/sess-1/recap", "", otherOrg, "")
		Expect(status).To(Equal(fiber.StatusNotFound))
		Expect(body["error"]).To(ContainSubstring("no recap"))
		Expect(stub.lastOrg).To(Equal(otherOrg), "the read must be scoped to the caller's org, not the recap owner's")
	})

	It("returns the stored recap without an LLM call when the turn count is unchanged", func() {
		stub := newRecapStub()
		seedRecapSession(stub, "sess-1", 3)
		stub.recaps[[2]string{org, "sess-1"}] = storage.SessionRecapRecord{
			SessionID: "sess-1",
			Narrative: "Cached narrative.",
			TurnCount: 3,
		}
		// No LLM configured at all: reaching the LLM path would 422/500, so a
		// 200 with the stored narrative proves the cache short-circuited.
		server := newRecapServer(stub, "")
		body, status := doJSON(server, http.MethodPost, "/v1/sessions/sess-1/recap", "", org, "")
		Expect(status).To(Equal(fiber.StatusOK))
		Expect(body).To(HaveKeyWithValue("narrative", "Cached narrative."))
	})

	It("generates, persists, and returns a recap stamped with the session's turn count", func() {
		calls := 0
		llm := fakeOpenAI(recapLLMJSON, &calls)
		defer llm.Close()

		stub := newRecapStub()
		seedRecapSession(stub, "sess-1", 3)
		server := newRecapServer(stub, llm.URL)

		body, status := doJSON(server, http.MethodPost, "/v1/sessions/sess-1/recap", "", org, "")
		Expect(status).To(Equal(fiber.StatusCreated))
		Expect(calls).To(Equal(1))
		Expect(body["narrative"]).To(ContainSubstring("flaky login test"))
		Expect(body).To(HaveKeyWithValue("observation", "Flaky tests here tend to be unpinned clocks."))
		Expect(body).To(HaveKeyWithValue("turn_count", BeNumerically("==", 3)))
		Expect(body).To(HaveKeyWithValue("model", "gpt-test"))
		Expect(stub.recaps[[2]string{org, "sess-1"}].TurnCount).To(Equal(3))
	})

	It("regenerates when the session has accrued turns past the stored recap", func() {
		calls := 0
		llm := fakeOpenAI(recapLLMJSON, &calls)
		defer llm.Close()

		stub := newRecapStub()
		seedRecapSession(stub, "sess-1", 5)
		stub.recaps[[2]string{org, "sess-1"}] = storage.SessionRecapRecord{
			SessionID: "sess-1",
			Narrative: "Stale narrative.",
			TurnCount: 3,
		}
		server := newRecapServer(stub, llm.URL)

		body, status := doJSON(server, http.MethodPost, "/v1/sessions/sess-1/recap", "", org, "")
		Expect(status).To(Equal(fiber.StatusCreated))
		Expect(calls).To(Equal(1))
		Expect(body["narrative"]).NotTo(Equal("Stale narrative."))
		Expect(body).To(HaveKeyWithValue("turn_count", BeNumerically("==", 5)))
		Expect(stub.recaps[[2]string{org, "sess-1"}].TurnCount).To(Equal(5))
	})

	It("422s generate when no LLM key resolves for the tenant", func() {
		// Neutralize the env fallback so the spec is deterministic regardless
		// of the developer's shell.
		orig, had := os.LookupEnv("OPENAI_API_KEY")
		Expect(os.Unsetenv("OPENAI_API_KEY")).To(Succeed())
		DeferCleanup(func() {
			if had {
				_ = os.Setenv("OPENAI_API_KEY", orig)
			}
		})

		stub := newRecapStub()
		seedRecapSession(stub, "sess-1", 3)
		server := newRecapServer(stub, "")
		body, status := doJSON(server, http.MethodPost, "/v1/sessions/sess-1/recap", "", org, "")
		Expect(status).To(Equal(fiber.StatusUnprocessableEntity))
		Expect(body["error"]).To(ContainSubstring("search/embedding"))
	})

	It("501s on a backend without recap support", func() {
		server, err := NewServer(Config{ListenAddr: ":0"}, inmemory.NewDriver(), tapeslogger.NewNoop())
		Expect(err).NotTo(HaveOccurred())
		_, status := doJSON(server, http.MethodGet, "/v1/sessions/sess-1/recap", "", org, "")
		Expect(status).To(Equal(fiber.StatusNotImplemented))
		_, status = doJSON(server, http.MethodPost, "/v1/sessions/sess-1/recap", "", org, "")
		Expect(status).To(Equal(fiber.StatusNotImplemented))
	})
})
