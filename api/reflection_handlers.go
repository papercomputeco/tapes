package api

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/gofiber/fiber/v2"

	"github.com/papercomputeco/tapes/pkg/llm"
	"github.com/papercomputeco/tapes/pkg/reflection"
	"github.com/papercomputeco/tapes/pkg/skill"
	"github.com/papercomputeco/tapes/pkg/storage"
)

// Session-reflection endpoints (PCC-241): POST /v1/sessions/:id/reflection runs one LLM
// pass over the session transcript and persists the result; GET returns the
// stored reflection. One reflection per session — a stored reflection whose turn_count still
// matches the session is current and immutable, so repeat POSTs return it
// without an LLM call, and only a session that has accrued turns since (the
// console's "Update reflection" prompt) regenerates.

// defaultSkillLLMProvider is the provider the shared skill/reflection LLM path
// falls back to when the deployment doesn't configure one — the platform's
// search/embedding provider default.
const defaultSkillLLMProvider = "openai"

// reflectionStore is the capability interface the reflection API needs from the storage
// driver. Backends that don't implement it (e.g. inmemory) get a 501, same as
// skills.
type reflectionStore interface {
	UpsertSessionReflection(ctx context.Context, orgID string, rec storage.SessionReflectionRecord) (*storage.SessionReflectionRecord, error)
	GetSessionReflection(ctx context.Context, orgID, sessionID string) (*storage.SessionReflectionRecord, error)
}

// reflectionStoreOr501 type-asserts the driver to reflectionStore, writing a 501 and
// returning ok=false when the backend doesn't support reflections.
func (s *Server) reflectionStoreOr501(c *fiber.Ctx) (reflectionStore, bool) {
	store, ok := s.driver.(reflectionStore)
	if !ok {
		_ = c.Status(fiber.StatusNotImplemented).JSON(llm.ErrorResponse{Error: "session reflections not supported by this backend"})
		return nil, false
	}
	return store, true
}

// reflectionResponse is the wire shape (snake_case, matching the console's
// reflection-schemas.ts). observation is omitted when the pass found nothing
// noteworthy.
type reflectionResponse struct {
	SessionID   string `json:"session_id"`
	Narrative   string `json:"narrative"`
	Observation string `json:"observation,omitempty"`
	TurnCount   int    `json:"turn_count"`
	Model       string `json:"model,omitempty"`
	GeneratedAt string `json:"generated_at"`
}

func reflectionFromRecord(rec storage.SessionReflectionRecord) reflectionResponse {
	return reflectionResponse{
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

// handleGenerateSessionReflection runs the pkg/reflection LLM extractor over the
// session's transcript, persists the result keyed on the session, and returns
// it. The server is authoritative on turn_count — it reads the session row,
// never a client-sent value.
func (s *Server) handleGenerateSessionReflection(c *fiber.Ctx) error {
	store, ok := s.reflectionStoreOr501(c)
	if !ok {
		return nil
	}
	sessions, ok := s.driver.(sessionsReader)
	if !ok {
		return c.Status(fiber.StatusNotImplemented).JSON(llm.ErrorResponse{Error: "session reflections not supported by this backend"})
	}

	orgID := orgIDFromCtx(c)
	sessionID := c.Params("id")

	sess, err := sessions.GetSessionRecord(c.Context(), orgID, sessionID)
	if err != nil {
		s.logger.Error("load session for reflection", "error", err)
		return c.Status(fiber.StatusInternalServerError).JSON(llm.ErrorResponse{Error: "failed to load session"})
	}
	if sess == nil {
		return c.Status(fiber.StatusNotFound).JSON(llm.ErrorResponse{Error: "session not found"})
	}

	// Cache check: a stored reflection that still covers the session's turn count
	// is current — return it without an LLM call. Repeat clicks are free, and
	// a settled session's reflection is effectively immutable.
	existing, err := store.GetSessionReflection(c.Context(), orgID, sess.ID)
	if err != nil {
		s.logger.Error("load existing reflection", "error", err)
		return c.Status(fiber.StatusInternalServerError).JSON(llm.ErrorResponse{Error: "failed to load existing reflection"})
	}
	if existing != nil && existing.TurnCount == sess.TurnCount {
		return c.Status(fiber.StatusOK).JSON(reflectionFromRecord(*existing))
	}

	// Same LLM configuration as skill generation — the tenant's shared
	// search/embedding credential; no reflection-specific keys.
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
				Error: "reflection generation requires the search/embedding feature to be enabled for this tenant",
			})
		}
		s.logger.Error("configure llm for reflection generation", "error", err)
		return c.Status(fiber.StatusInternalServerError).JSON(llm.ErrorResponse{Error: "llm provider not configured"})
	}

	// Transcripts read through the in-process, org-scoped querier — the same
	// tenancy gate skill generation uses.
	query, ok := s.skillTraceQuerier(orgID)
	if !ok {
		return c.Status(fiber.StatusNotImplemented).JSON(llm.ErrorResponse{Error: "session reflections not supported by this backend"})
	}

	live := sessionIsLive(sess, time.Now().UTC())
	r, err := reflection.NewGenerator(query, llmCaller).Generate(c.Context(), sess.ID, live)
	if err != nil {
		s.logger.Error("generate session reflection", "error", err)
		return c.Status(fiber.StatusInternalServerError).JSON(llm.ErrorResponse{Error: "failed to generate reflection"})
	}

	rec := storage.SessionReflectionRecord{
		SessionID:   sess.ID,
		Narrative:   r.Narrative,
		Observation: r.Observation,
		TurnCount:   sess.TurnCount,
		Model:       llmCfg.Model,
		GeneratedAt: time.Now().UTC(),
	}
	saved, err := store.UpsertSessionReflection(c.Context(), orgID, rec)
	if err != nil {
		s.logger.Error("persist session reflection", "error", err)
		return c.Status(fiber.StatusInternalServerError).JSON(llm.ErrorResponse{Error: "failed to persist reflection"})
	}

	return c.Status(fiber.StatusCreated).JSON(reflectionFromRecord(*saved))
}

// handleGetSessionReflection returns the stored reflection for a session, or 404 when
// none has been generated yet ("no reflection" is a state the console renders as
// the generate button, not an error).
func (s *Server) handleGetSessionReflection(c *fiber.Ctx) error {
	store, ok := s.reflectionStoreOr501(c)
	if !ok {
		return nil
	}

	rec, err := store.GetSessionReflection(c.Context(), orgIDFromCtx(c), c.Params("id"))
	if err != nil {
		s.logger.Error("get session reflection", "error", err)
		return c.Status(fiber.StatusInternalServerError).JSON(llm.ErrorResponse{Error: "failed to fetch reflection"})
	}
	if rec == nil {
		return c.Status(fiber.StatusNotFound).JSON(llm.ErrorResponse{Error: "no reflection for session"})
	}
	return c.JSON(reflectionFromRecord(*rec))
}
