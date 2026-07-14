package api

import (
	"context"
	"errors"

	"github.com/gofiber/fiber/v2"

	"github.com/papercomputeco/tapes/pkg/derive"
	"github.com/papercomputeco/tapes/pkg/llm"
	"github.com/papercomputeco/tapes/pkg/seed"
)

type seedDemoRequest struct {
	Overwrite bool `json:"overwrite,omitempty"`
}

// handleSeedDemo handles POST /v1/admin/seed/demo.
//
// Seeding replays bundled capture corpora through the normal ingest
// write path (raw turns + sessions) and then derives the seeded
// sessions, so demo data is indistinguishable from live capture and
// exercises the full raw → derive → span pipeline. The operation is
// idempotent: re-seeding dedupes at the raw layer and the derive pass
// upserts the same projection.
//
//	@Summary		Seed demo sessions (operator)
//	@ID			seedDemo
//	@Description	Replays the bundled demo capture corpora through the ingest write path into the caller's org, then derives the seeded sessions. Idempotent: raw-turn dedup makes repeat seeds no-ops.
//	@Tags			admin
//	@Accept			json
//	@Produce		json
//	@Param			request	body		seedDemoRequest	false	"Seed options (overwrite is no longer supported)"
//	@Success		200		{object}	seed.Result
//	@Failure		400		{object}	llm.ErrorResponse	"Invalid payload or unsupported option"
//	@Failure		500		{object}	llm.ErrorResponse	"Seeding failed"
//	@Failure		501		{object}	llm.ErrorResponse	"Driver does not host the raw-turn layer"
//	@Router			/v1/admin/seed/demo [post]
func (s *Server) handleSeedDemo(c *fiber.Ctx) error {
	var req seedDemoRequest
	if len(c.Body()) > 0 {
		if err := c.BodyParser(&req); err != nil {
			return c.Status(fiber.StatusBadRequest).JSON(llm.ErrorResponse{Error: "invalid payload: " + err.Error()})
		}
	}

	if req.Overwrite {
		return c.Status(fiber.StatusBadRequest).JSON(llm.ErrorResponse{Error: "overwrite is no longer supported; seeding is idempotent against the raw layer"})
	}

	report, err := seed.Run(c.Context(), s.driver, s.logger, orgIDFromCtx(c))
	if err != nil {
		if errors.Is(err, seed.ErrUnsupportedDriver) {
			return c.Status(fiber.StatusNotImplemented).JSON(llm.ErrorResponse{Error: err.Error()})
		}
		s.logger.Error("seed demo", "error", err)
		return c.Status(fiber.StatusInternalServerError).JSON(llm.ErrorResponse{Error: err.Error()})
	}

	return c.JSON(report)
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
