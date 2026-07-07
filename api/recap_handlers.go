package api

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/gofiber/fiber/v2"

	"github.com/papercomputeco/tapes/pkg/llm"
	"github.com/papercomputeco/tapes/pkg/recap"
	"github.com/papercomputeco/tapes/pkg/skill"
	"github.com/papercomputeco/tapes/pkg/storage"
)

// Session-recap endpoints (PCC-241): POST /v1/sessions/:id/recap runs one LLM
// pass over the session transcript and persists the result; GET returns the
// stored recap. One recap per session — a stored recap whose turn_count still
// matches the session is current and immutable, so repeat POSTs return it
// without an LLM call, and only a session that has accrued turns since (the
// console's "Update recap" prompt) regenerates.

// defaultSkillLLMProvider is the provider the shared skill/recap LLM path
// falls back to when the deployment doesn't configure one — the platform's
// search/embedding provider default.
const defaultSkillLLMProvider = "openai"

// recapStore is the capability interface the recap API needs from the storage
// driver. Backends that don't implement it (e.g. inmemory) get a 501, same as
// skills.
type recapStore interface {
	UpsertSessionRecap(ctx context.Context, orgID string, rec storage.SessionRecapRecord) (*storage.SessionRecapRecord, error)
	GetSessionRecap(ctx context.Context, orgID, sessionID string) (*storage.SessionRecapRecord, error)
}

// recapStoreOr501 type-asserts the driver to recapStore, writing a 501 and
// returning ok=false when the backend doesn't support recaps.
func (s *Server) recapStoreOr501(c *fiber.Ctx) (recapStore, bool) {
	store, ok := s.driver.(recapStore)
	if !ok {
		_ = c.Status(fiber.StatusNotImplemented).JSON(llm.ErrorResponse{Error: "session recaps not supported by this backend"})
		return nil, false
	}
	return store, true
}

// recapResponse is the wire shape (snake_case, matching the console's
// recap-schemas.ts). observation is omitted when the pass found nothing
// noteworthy.
type recapResponse struct {
	SessionID   string `json:"session_id"`
	Narrative   string `json:"narrative"`
	Observation string `json:"observation,omitempty"`
	TurnCount   int    `json:"turn_count"`
	Model       string `json:"model,omitempty"`
	GeneratedAt string `json:"generated_at"`
}

func recapFromRecord(rec storage.SessionRecapRecord) recapResponse {
	return recapResponse{
		SessionID:   rec.SessionID,
		Narrative:   rec.Narrative,
		Observation: rec.Observation,
		TurnCount:   rec.TurnCount,
		Model:       rec.Model,
		GeneratedAt: rec.GeneratedAt.UTC().Format(time.RFC3339),
	}
}

// sessionIsLive mirrors the console's running heuristic (sessions/utils.ts):
// no ended_at and last seen within the running window. It selects the
// narrative's tense — present for a session still working, past otherwise.
const sessionRunningWindow = 5 * time.Minute

func sessionIsLive(sess *storage.SessionRecord, now time.Time) bool {
	if sess.EndedAt != nil {
		return false
	}
	return now.Sub(sess.LastSeenAt) < sessionRunningWindow
}

// handleGenerateSessionRecap runs the pkg/recap LLM extractor over the
// session's transcript, persists the result keyed on the session, and returns
// it. The server is authoritative on turn_count — it reads the session row,
// never a client-sent value.
func (s *Server) handleGenerateSessionRecap(c *fiber.Ctx) error {
	store, ok := s.recapStoreOr501(c)
	if !ok {
		return nil
	}
	sessions, ok := s.driver.(sessionsReader)
	if !ok {
		return c.Status(fiber.StatusNotImplemented).JSON(llm.ErrorResponse{Error: "session recaps not supported by this backend"})
	}

	orgID := orgIDFromCtx(c)
	sessionID := c.Params("id")

	sess, err := sessions.GetSessionRecord(c.Context(), orgID, sessionID)
	if err != nil {
		s.logger.Error("load session for recap", "error", err)
		return c.Status(fiber.StatusInternalServerError).JSON(llm.ErrorResponse{Error: "failed to load session"})
	}
	if sess == nil {
		return c.Status(fiber.StatusNotFound).JSON(llm.ErrorResponse{Error: "session not found"})
	}

	// Cache check: a stored recap that still covers the session's turn count
	// is current — return it without an LLM call. Repeat clicks are free, and
	// a settled session's recap is effectively immutable.
	existing, err := store.GetSessionRecap(c.Context(), orgID, sess.ID)
	if err != nil {
		s.logger.Error("load existing recap", "error", err)
		return c.Status(fiber.StatusInternalServerError).JSON(llm.ErrorResponse{Error: "failed to load existing recap"})
	}
	if existing != nil && existing.TurnCount == sess.TurnCount {
		return c.Status(fiber.StatusOK).JSON(recapFromRecord(*existing))
	}

	// Same LLM configuration as skill generation — the tenant's shared
	// search/embedding credential; no recap-specific keys.
	llmCfg := skill.LLMCallerConfig{
		Provider: s.config.SkillLLMProvider,
		Model:    s.config.SkillLLMModel,
		APIKey:   s.config.SkillLLMAPIKey,
		BaseURL:  s.config.SkillLLMBaseURL,
	}
	if strings.TrimSpace(llmCfg.Provider) == "" {
		llmCfg.Provider = defaultSkillLLMProvider
	}
	llmCaller, err := skill.NewLLMCaller(llmCfg)
	if err != nil {
		if errors.Is(err, skill.ErrNoAPIKey) {
			return c.Status(fiber.StatusUnprocessableEntity).JSON(llm.ErrorResponse{
				Error: "recap generation requires the search/embedding feature to be enabled for this tenant",
			})
		}
		s.logger.Error("configure llm for recap generation", "error", err)
		return c.Status(fiber.StatusInternalServerError).JSON(llm.ErrorResponse{Error: "llm provider not configured"})
	}

	// Transcripts read through the in-process, org-scoped querier — the same
	// tenancy gate skill generation uses.
	query, ok := s.skillTraceQuerier(orgID)
	if !ok {
		return c.Status(fiber.StatusNotImplemented).JSON(llm.ErrorResponse{Error: "session recaps not supported by this backend"})
	}

	live := sessionIsLive(sess, time.Now().UTC())
	r, err := recap.NewGenerator(query, llmCaller).Generate(c.Context(), sess.ID, live)
	if err != nil {
		s.logger.Error("generate session recap", "error", err)
		return c.Status(fiber.StatusInternalServerError).JSON(llm.ErrorResponse{Error: "failed to generate recap"})
	}

	rec := storage.SessionRecapRecord{
		SessionID:   sess.ID,
		Narrative:   r.Narrative,
		Observation: r.Observation,
		TurnCount:   sess.TurnCount,
		Model:       llmCfg.Model,
		GeneratedAt: time.Now().UTC(),
	}
	saved, err := store.UpsertSessionRecap(c.Context(), orgID, rec)
	if err != nil {
		s.logger.Error("persist session recap", "error", err)
		return c.Status(fiber.StatusInternalServerError).JSON(llm.ErrorResponse{Error: "failed to persist recap"})
	}

	return c.Status(fiber.StatusCreated).JSON(recapFromRecord(*saved))
}

// handleGetSessionRecap returns the stored recap for a session, or 404 when
// none has been generated yet ("no recap" is a state the console renders as
// the generate button, not an error).
func (s *Server) handleGetSessionRecap(c *fiber.Ctx) error {
	store, ok := s.recapStoreOr501(c)
	if !ok {
		return nil
	}

	rec, err := store.GetSessionRecap(c.Context(), orgIDFromCtx(c), c.Params("id"))
	if err != nil {
		s.logger.Error("get session recap", "error", err)
		return c.Status(fiber.StatusInternalServerError).JSON(llm.ErrorResponse{Error: "failed to fetch recap"})
	}
	if rec == nil {
		return c.Status(fiber.StatusNotFound).JSON(llm.ErrorResponse{Error: "no recap for session"})
	}
	return c.JSON(recapFromRecord(*rec))
}
