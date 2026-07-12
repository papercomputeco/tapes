package api

import (
	"context"
	"time"

	"github.com/gofiber/fiber/v2"

	"github.com/papercomputeco/tapes/pkg/llm"
	"github.com/papercomputeco/tapes/pkg/storage"
)

// savedSessionsStore is the capability interface for the org-wide saved
// sessions feature (PCC-815). The Postgres driver implements it; the
// handlers return 501 for drivers that don't — mirroring skills. Saved is
// a shared team shortlist: one marker per (org, session), saved_by is
// attribution only, and anyone in the org may unsave.
type savedSessionsStore interface {
	SaveSession(ctx context.Context, orgID, sessionID, savedBy string) (*storage.SavedSessionRecord, error)
	UnsaveSession(ctx context.Context, orgID, sessionID string) (bool, error)
	ListSavedSessions(ctx context.Context, orgID string) ([]storage.SavedSessionRecord, error)
}

// maxBatchSaveIDs bounds the batch save body — one console page is 25 rows,
// so 200 (the sessions-list max page size) is already generous.
const maxBatchSaveIDs = 200

// SavedSessionItem is one org-wide saved marker on the wire.
type SavedSessionItem struct {
	SessionID string    `json:"session_id"`
	SavedBy   string    `json:"saved_by,omitempty"`
	SavedAt   time.Time `json:"saved_at"`
}

// SavedSessionListResponse is the GET /v1/saved_sessions envelope.
type SavedSessionListResponse struct {
	Items []SavedSessionItem `json:"items"`
}

// saveSessionsBatchRequest is the PUT /v1/sessions/save body.
type saveSessionsBatchRequest struct {
	SessionIDs []string `json:"session_ids"`
}

// SaveSessionsBatchResponse reports per-id outcomes for a batch save:
// saved markers for the ids that exist, unknown ids under not_found.
type SaveSessionsBatchResponse struct {
	Items    []SavedSessionItem `json:"items"`
	NotFound []string           `json:"not_found,omitempty"`
}

func savedItemFromStorage(rec storage.SavedSessionRecord) SavedSessionItem {
	return SavedSessionItem{SessionID: rec.SessionID, SavedBy: rec.SavedBy, SavedAt: rec.SavedAt}
}

// savedStoreOr501 type-asserts the driver, writing a 501 when the backend
// doesn't support saved sessions.
func (s *Server) savedStoreOr501(c *fiber.Ctx) (savedSessionsStore, bool) {
	store, ok := s.driver.(savedSessionsStore)
	if !ok {
		_ = c.Status(fiber.StatusNotImplemented).JSON(llm.ErrorResponse{Error: "saved sessions not supported by this backend"})
		return nil, false
	}
	return store, true
}

// handleSaveSession handles PUT /v1/sessions/:id/save.
//
//	@Summary		Save a session org-wide
//	@Description	Idempotently marks a session as saved for the whole org. The caller's gateway-stamped subject is recorded as attribution (first saver wins).
//	@Tags			sessions
//	@Produce		json
//	@Param			id	path		string	true	"Session id (UUID)"
//	@Success		200	{object}	SavedSessionItem
//	@Failure		404	{object}	llm.ErrorResponse	"No such session in the org"
//	@Failure		500	{object}	llm.ErrorResponse	"Failed to save session"
//	@Failure		501	{object}	llm.ErrorResponse	"Saved sessions not supported by this backend"
//	@Router			/v1/sessions/{id}/save [put]
func (s *Server) handleSaveSession(c *fiber.Ctx) error {
	store, ok := s.savedStoreOr501(c)
	if !ok {
		return nil
	}
	rec, err := store.SaveSession(c.Context(), orgIDFromCtx(c), c.Params("id"), authSubjectFromCtx(c))
	if err != nil {
		s.logger.Error("save session", "error", err)
		return c.Status(fiber.StatusInternalServerError).JSON(llm.ErrorResponse{Error: "failed to save session"})
	}
	if rec == nil {
		return c.Status(fiber.StatusNotFound).JSON(llm.ErrorResponse{Error: "session not found"})
	}
	return c.JSON(savedItemFromStorage(*rec))
}

// handleSaveSessionsBatch handles PUT /v1/sessions/save — the console's
// checkbox multi-select flow lands here as one round trip.
//
//	@Summary		Save several sessions org-wide
//	@Description	Idempotently marks each listed session as saved for the whole org. Unknown ids are reported under not_found rather than failing the batch.
//	@Tags			sessions
//	@Accept			json
//	@Produce		json
//	@Param			request	body		saveSessionsBatchRequest	true	"Session ids to save"
//	@Success		200		{object}	SaveSessionsBatchResponse
//	@Failure		400		{object}	llm.ErrorResponse	"Empty or oversized session_ids"
//	@Failure		500		{object}	llm.ErrorResponse	"Failed to save sessions"
//	@Failure		501		{object}	llm.ErrorResponse	"Saved sessions not supported by this backend"
//	@Router			/v1/sessions/save [put]
func (s *Server) handleSaveSessionsBatch(c *fiber.Ctx) error {
	store, ok := s.savedStoreOr501(c)
	if !ok {
		return nil
	}
	var req saveSessionsBatchRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(llm.ErrorResponse{Error: "invalid request body"})
	}
	if len(req.SessionIDs) == 0 {
		return c.Status(fiber.StatusBadRequest).JSON(llm.ErrorResponse{Error: "session_ids is required and must be non-empty"})
	}
	if len(req.SessionIDs) > maxBatchSaveIDs {
		return c.Status(fiber.StatusBadRequest).JSON(llm.ErrorResponse{Error: "too many session_ids"})
	}

	orgID := orgIDFromCtx(c)
	subject := authSubjectFromCtx(c)
	resp := SaveSessionsBatchResponse{Items: []SavedSessionItem{}}
	for _, id := range req.SessionIDs {
		rec, err := store.SaveSession(c.Context(), orgID, id, subject)
		if err != nil {
			s.logger.Error("batch save session", "error", err, "session_id", id)
			return c.Status(fiber.StatusInternalServerError).JSON(llm.ErrorResponse{Error: "failed to save sessions"})
		}
		if rec == nil {
			resp.NotFound = append(resp.NotFound, id)
			continue
		}
		resp.Items = append(resp.Items, savedItemFromStorage(*rec))
	}
	return c.JSON(resp)
}

// handleUnsaveSession handles DELETE /v1/sessions/:id/save.
//
//	@Summary		Unsave a session org-wide
//	@Description	Removes the shared saved marker for everyone in the org. Idempotent — deleting an absent marker is still a 204.
//	@Tags			sessions
//	@Param			id	path	string	true	"Session id (UUID)"
//	@Success		204
//	@Failure		500	{object}	llm.ErrorResponse	"Failed to unsave session"
//	@Failure		501	{object}	llm.ErrorResponse	"Saved sessions not supported by this backend"
//	@Router			/v1/sessions/{id}/save [delete]
func (s *Server) handleUnsaveSession(c *fiber.Ctx) error {
	store, ok := s.savedStoreOr501(c)
	if !ok {
		return nil
	}
	if _, err := store.UnsaveSession(c.Context(), orgIDFromCtx(c), c.Params("id")); err != nil {
		s.logger.Error("unsave session", "error", err)
		return c.Status(fiber.StatusInternalServerError).JSON(llm.ErrorResponse{Error: "failed to unsave session"})
	}
	return c.SendStatus(fiber.StatusNoContent)
}

// handleListSavedSessions handles GET /v1/saved_sessions.
//
//	@Summary		List the org's saved sessions
//	@Description	Every org-wide saved marker, newest-saved-first. The console joins these ids against the sessions list to render saved state.
//	@Tags			sessions
//	@Produce		json
//	@Success		200	{object}	SavedSessionListResponse
//	@Failure		500	{object}	llm.ErrorResponse	"Failed to list saved sessions"
//	@Failure		501	{object}	llm.ErrorResponse	"Saved sessions not supported by this backend"
//	@Router			/v1/saved_sessions [get]
func (s *Server) handleListSavedSessions(c *fiber.Ctx) error {
	store, ok := s.savedStoreOr501(c)
	if !ok {
		return nil
	}
	recs, err := store.ListSavedSessions(c.Context(), orgIDFromCtx(c))
	if err != nil {
		s.logger.Error("list saved sessions", "error", err)
		return c.Status(fiber.StatusInternalServerError).JSON(llm.ErrorResponse{Error: "failed to list saved sessions"})
	}
	items := make([]SavedSessionItem, len(recs))
	for i, rec := range recs {
		items[i] = savedItemFromStorage(rec)
	}
	return c.JSON(SavedSessionListResponse{Items: items})
}
