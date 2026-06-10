package api

import (
	"context"
	"errors"
	"os"
	"strings"

	"github.com/gofiber/fiber/v2"

	"github.com/papercomputeco/tapes/pkg/backfill"
	"github.com/papercomputeco/tapes/pkg/deck"
	"github.com/papercomputeco/tapes/pkg/derive"
	"github.com/papercomputeco/tapes/pkg/llm"
	"github.com/papercomputeco/tapes/pkg/storage"
)

type seedDemoRequest struct {
	Overwrite bool `json:"overwrite,omitempty"`
}

type backfillUsageRequest struct {
	TranscriptDir string `json:"transcript_dir"`
	DryRun        bool   `json:"dry_run,omitempty"`
	Verbose       bool   `json:"verbose,omitempty"`
	Sessions      bool   `json:"sessions,omitempty"`
	OrgID         string `json:"org_id,omitempty"`
	AuthSubject   string `json:"auth_subject,omitempty"`
}

func (s *Server) handleSeedDemo(c *fiber.Ctx) error {
	var req seedDemoRequest
	if len(c.Body()) > 0 {
		if err := c.BodyParser(&req); err != nil {
			return c.Status(fiber.StatusBadRequest).JSON(llm.ErrorResponse{Error: "invalid payload: " + err.Error()})
		}
	}

	if req.Overwrite {
		return c.Status(fiber.StatusBadRequest).JSON(llm.ErrorResponse{Error: "overwrite is only supported for fresh local databases before the API starts"})
	}

	sessions, messages, err := deck.SeedDemoToDriver(c.Context(), s.driver)
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(llm.ErrorResponse{Error: err.Error()})
	}

	return c.JSON(deck.SeedDemoResponse{Sessions: sessions, Messages: messages})
}

func (s *Server) handleBackfillUsage(c *fiber.Ctx) error {
	var req backfillUsageRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(llm.ErrorResponse{Error: "invalid payload: " + err.Error()})
	}

	if strings.TrimSpace(req.TranscriptDir) == "" {
		return c.Status(fiber.StatusBadRequest).JSON(llm.ErrorResponse{Error: "transcript_dir is required"})
	}
	if info, err := os.Stat(req.TranscriptDir); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return c.Status(fiber.StatusBadRequest).JSON(llm.ErrorResponse{Error: "transcript_dir does not exist"})
		}
		return c.Status(fiber.StatusBadRequest).JSON(llm.ErrorResponse{Error: err.Error()})
	} else if !info.IsDir() {
		return c.Status(fiber.StatusBadRequest).JSON(llm.ErrorResponse{Error: "transcript_dir must be a directory"})
	}

	b := backfill.NewBackfillerWithDriver(s.driver, backfill.Options{
		DryRun:      req.DryRun,
		Verbose:     req.Verbose,
		Sessions:    req.Sessions,
		OrgID:       req.OrgID,
		AuthSubject: req.AuthSubject,
	})
	result, err := b.Run(c.Context(), req.TranscriptDir)
	if err != nil {
		s.logger.Error("backfill usage", "error", err)
		return c.Status(fiber.StatusInternalServerError).JSON(llm.ErrorResponse{Error: err.Error()})
	}

	return c.JSON(result)
}

// handleBackfillSessionStatus recomputes derived_status for existing
// sessions whose rows predate the ingest-time computation. Live ingest
// keeps status current, so this is a one-shot for legacy/pre-feature data;
// it is idempotent and safe to re-run.
func (s *Server) handleBackfillSessionStatus(c *fiber.Ctx) error {
	bf, ok := s.driver.(storage.SessionStatusBackfiller)
	if !ok {
		return c.Status(fiber.StatusNotImplemented).JSON(llm.ErrorResponse{Error: "driver does not support session-status backfill"})
	}
	res, err := bf.BackfillSessionStatus(c.Context())
	if err != nil {
		s.logger.Error("backfill session status", "error", err)
		return c.Status(fiber.StatusInternalServerError).JSON(llm.ErrorResponse{Error: err.Error()})
	}
	return c.JSON(fiber.Map{"scanned": res.Scanned, "updated": res.Updated})
}

// deriveRunner is the optional driver capability behind
// POST /v1/admin/derive/run.
type deriveRunner interface {
	RederiveFromRaw(ctx context.Context, project string) (map[string]*derive.RederiveReport, error)
}

// handleDeriveRun rebuilds the derived node layer (typing, edges,
// projection) from the immutable raw-turn store. Idempotent and
// re-runnable: this is the lever that makes data-model iteration cheap
// — a classifier or projection change redeploys, re-runs, and every
// captured session reclassifies without re-capture.
func (s *Server) handleDeriveRun(c *fiber.Ctx) error {
	runner, ok := s.driver.(deriveRunner)
	if !ok {
		return c.Status(fiber.StatusNotImplemented).JSON(llm.ErrorResponse{Error: "driver does not host the raw-turn layer"})
	}

	reports, err := runner.RederiveFromRaw(c.Context(), "")
	if err != nil {
		s.logger.Error("derive run", "error", err)
		return c.Status(fiber.StatusInternalServerError).JSON(llm.ErrorResponse{Error: err.Error()})
	}

	return c.JSON(fiber.Map{"orgs": reports})
}

// handleDeriveVerify proves the raw→derived round-trip: re-derive node
// chains from the immutable raw_turns layer and check every derived
// hash against the node store. Read-only and idempotent; safe to run
// any time. 501 when the driver doesn't host the raw layer.
func (s *Server) handleDeriveVerify(c *fiber.Ctx) error {
	rawStore, ok := s.driver.(storage.RawTurnStore)
	if !ok {
		return c.Status(fiber.StatusNotImplemented).JSON(llm.ErrorResponse{Error: "driver does not host the raw-turn layer"})
	}

	result, err := derive.VerifyRederive(c.Context(), rawStore, s.driver, "")
	if err != nil {
		s.logger.Error("derive verify", "error", err)
		return c.Status(fiber.StatusInternalServerError).JSON(llm.ErrorResponse{Error: err.Error()})
	}

	return c.JSON(fiber.Map{
		"verified": result.Verified(),
		"report":   result,
	})
}
