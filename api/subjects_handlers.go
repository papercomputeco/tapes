package api

import (
	"context"
	"strconv"
	"time"

	"github.com/gofiber/fiber/v3"

	"github.com/papercomputeco/tapes/pkg/llm"
	"github.com/papercomputeco/tapes/pkg/storage"
)

// subjectsLister is the capability behind GET /v1/subjects. The Postgres
// driver implements it; the handler returns 501 for drivers that don't.
type subjectsLister interface {
	ListSubjects(ctx context.Context, orgID, q string, limit int) ([]storage.Subject, error)
}

// SubjectItem is one auth_subject that has captured sessions.
type SubjectItem struct {
	// Subject is the auth_subject value on the sessions.
	Subject string `json:"subject"`
	// SessionCount is how many captured sessions carry this subject.
	SessionCount int64 `json:"session_count"`
	// LastSeenAt is the most recent activity on any of those sessions.
	LastSeenAt time.Time `json:"last_seen_at"`
}

// SubjectListResponse is the GET /v1/subjects body.
type SubjectListResponse struct {
	Items []SubjectItem `json:"items"`
}

const maxSubjectListLimit = 200

func (s *Server) handleListSubjects(c fiber.Ctx) error {
	lister, ok := s.driver.(subjectsLister)
	if !ok {
		return c.Status(fiber.StatusNotImplemented).JSON(llm.ErrorResponse{Error: "subjects not supported by this backend"})
	}
	limit := storage.DefaultListLimit
	if raw := c.Query("limit"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 {
			return c.Status(fiber.StatusBadRequest).JSON(llm.ErrorResponse{Error: "limit must be a positive integer"})
		}
		limit = min(n, maxSubjectListLimit)
	}
	subjects, err := lister.ListSubjects(c.Context(), singleTenantOrgID, c.Query("q"), limit)
	if err != nil {
		s.logger.Error("list subjects", "error", err)
		return c.Status(fiber.StatusInternalServerError).JSON(llm.ErrorResponse{Error: "failed to list subjects"})
	}
	items := make([]SubjectItem, 0, len(subjects))
	for _, sub := range subjects {
		items = append(items, SubjectItem{Subject: sub.Subject, SessionCount: sub.SessionCount, LastSeenAt: sub.LastSeenAt})
	}
	return c.JSON(SubjectListResponse{Items: items})
}
