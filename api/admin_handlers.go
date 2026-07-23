package api

import (
	"context"
	"errors"
	"strings"

	"github.com/gofiber/fiber/v2"

	"github.com/papercomputeco/tapes/pkg/derive"
	"github.com/papercomputeco/tapes/pkg/llm"
	"github.com/papercomputeco/tapes/pkg/seed"
	"github.com/papercomputeco/tapes/pkg/storage"
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

// deriveRunResponse is the derive-run result, keyed by org.
//
// Declared as a type rather than assembled inline so the published schema is
// generated from the shape the handler actually returns; a fiber.Map would
// leave swag with nothing to describe and the endpoint documented as an opaque
// object.
type deriveRunResponse struct {
	Orgs map[string]*derive.RederiveReport `json:"orgs"`
}

// handleDeriveRun rebuilds the derived span projection (traces, spans,
// links, and the session rollups) from the immutable raw-turn store. The
// persisted node layer is retired; the merkle DAG lives only in memory at
// derive time. Idempotent and re-runnable: this is the lever that makes
// data-model iteration cheap — a classifier or projection change
// redeploys, re-runs, and every captured session reclassifies without
// re-capture.
//
//	@Summary		Re-derive the span projection (operator)
//	@ID			runDerive
//	@Description	Rebuilds traces, spans, links, and session rollups for every org from the immutable raw-turn store. Idempotent: re-running reproduces the same projection and prunes rows the current derive no longer emits.
//	@Description
//	@Description	This is how a projection or classifier change reaches already-captured data — it re-derives rather than re-captures. Cost scales with the raw layer, so it is an operator lever, not a request-path call.
//	@Tags			admin
//	@Produce		json
//	@Success		200	{object}	deriveRunResponse	"Per-org derive reports"
//	@Failure		500	{object}	llm.ErrorResponse	"Derive failed"
//	@Failure		501	{object}	llm.ErrorResponse	"Driver does not host the raw-turn layer"
//	@Router			/v1/admin/derive/run [post]
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

	return c.JSON(deriveRunResponse{Orgs: reports})
}

type rawTurnAttributionRepairer interface {
	RepairRawTurnAttribution(context.Context, string, storage.RawTurnAttributionRepairRequest) (storage.RawTurnAttributionRepairResult, error)
}

// handleRawTurnAttributionRepair records an append-only attribution overlay
// and synchronously rebuilds both affected session projections.
//
//	@Summary		Repair raw-turn attribution (operator)
//	@ID			repairRawTurnAttribution
//	@Description	Records an audited correction without modifying raw_turns, then re-derives the previous and effective sessions. Select exactly one row by raw_turn_id or paper_proxy_request_id.
//	@Tags			admin
//	@Accept			json
//	@Produce		json
//	@Param			request	body		storage.RawTurnAttributionRepairRequest	true	"Attribution repair"
//	@Success		200		{object}	storage.RawTurnAttributionRepairResult	"Repair applied; source_cleanup_pending discloses a cosmetic leftover source session row that nothing retries automatically"
//	@Success		202		{object}	storage.RawTurnAttributionRepairResult	"Correction recorded; projections_pending lists sessions the derive worker will converge"
//	@Failure		400		{object}	llm.ErrorResponse	"Invalid payload or replacement attribution"
//	@Failure		404		{object}	llm.ErrorResponse	"Raw turn not found"
//	@Failure		409		{object}	llm.ErrorResponse	"Correlation selector is ambiguous"
//	@Failure		500		{object}	llm.ErrorResponse	"Repair failed"
//	@Failure		501		{object}	llm.ErrorResponse	"Driver does not support attribution repair"
//	@Router			/v1/admin/raw-turns/attribution-repair [post]
func (s *Server) handleRawTurnAttributionRepair(c *fiber.Ctx) error {
	repairer, ok := s.driver.(rawTurnAttributionRepairer)
	if !ok {
		return c.Status(fiber.StatusNotImplemented).JSON(llm.ErrorResponse{Error: "driver does not support raw-turn attribution repair"})
	}
	var req storage.RawTurnAttributionRepairRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(llm.ErrorResponse{Error: "invalid payload: " + err.Error()})
	}
	if err := validateRawTurnAttributionRepairRequest(req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(llm.ErrorResponse{Error: err.Error()})
	}
	req.OrgID = orgIDFromCtx(c)
	result, err := repairer.RepairRawTurnAttribution(c.Context(), "", req)
	if err != nil {
		switch {
		case errors.Is(err, storage.ErrRawTurnNotFound):
			return c.Status(fiber.StatusNotFound).JSON(llm.ErrorResponse{Error: err.Error()})
		case errors.Is(err, storage.ErrRawTurnAmbiguous):
			return c.Status(fiber.StatusConflict).JSON(llm.ErrorResponse{Error: err.Error()})
		case errors.Is(err, storage.ErrRepairProjectionsPending):
			// The correction committed and is effective; only the synchronous
			// projection rebuild is outstanding, and the derive queue converges
			// it. 202 with the result (projections_pending names the stale
			// sessions) rather than a 500 that would misreport a recorded
			// repair as a failure and invite a redundant retry.
			s.logger.Warn("repair raw-turn attribution settling asynchronously", "error", err)
			return c.Status(fiber.StatusAccepted).JSON(result)
		default:
			s.logger.Error("repair raw-turn attribution", "error", err)
			return c.Status(fiber.StatusInternalServerError).JSON(llm.ErrorResponse{Error: err.Error()})
		}
	}
	return c.JSON(result)
}

func validateRawTurnAttributionRepairRequest(req storage.RawTurnAttributionRepairRequest) error {
	if (req.RawTurnID > 0) == (strings.TrimSpace(req.PaperProxyRequestID) != "") {
		return errors.New("exactly one of raw_turn_id or paper_proxy_request_id is required")
	}
	if req.HarnessID == "" {
		return errors.New("harness_id is required")
	}
	if req.HarnessSessionID == "" {
		return errors.New("harness_session_id is required")
	}
	if req.ParentHarnessSessionID != nil && *req.ParentHarnessSessionID == "" {
		return errors.New("parent_harness_session_id must be omitted instead of empty")
	}
	if req.ParentHarnessSessionID != nil && *req.ParentHarnessSessionID == req.HarnessSessionID {
		return errors.New("a session cannot parent itself")
	}
	if strings.TrimSpace(req.Reason) == "" {
		return errors.New("reason is required")
	}
	return nil
}
