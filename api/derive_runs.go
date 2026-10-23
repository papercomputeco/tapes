package api

import (
	"context"
	"errors"
	"os"
	"strconv"
	"time"

	"github.com/gofiber/fiber/v3"

	"github.com/papercomputeco/tapes/pkg/derive"
	"github.com/papercomputeco/tapes/pkg/llm"
	oas "github.com/papercomputeco/tapes/pkg/tapesoapi"
	"github.com/papercomputeco/tapes/pkg/tapesoapi/oasfiber"
)

// Asynchronous derive runs.
//
// POST /v1/admin/derive/run re-derives the whole store inside one request, so
// it lasts exactly as long as the connection carrying it: a gateway timeout, a
// client that sleeps, or a replica rolling loses the work with no trace. A
// derive run is the same re-derive as a durable job instead. Creating one
// snapshots the session set into the database and returns at once; the work
// happens in the background, records each session as it finishes, and resumes
// from the first unfinished session in whichever process picks it up next.
//
// Like every /v1/admin/ route these are served by the listener the deployment
// chose for admin routes (see admin_listener.go).
const (
	// DeriveRunsPath creates (POST) and lists (GET) derive runs.
	DeriveRunsPath = "/v1/admin/derive/runs"

	// deriveRunPath reads one derive run.
	deriveRunPath = DeriveRunsPath + "/:id"

	// deriveRunCancelPath cancels one derive run.
	deriveRunCancelPath = deriveRunPath + "/cancel"
)

// DefaultDeriveRunPoll is how often an idle supervisor looks for an active run
// to execute or resume. Starting a run on this process wakes it immediately;
// the poll is what picks up a run left behind by a process that went away.
const DefaultDeriveRunPoll = 30 * time.Second

const (
	defaultDeriveRunListLimit = 20
	maxDeriveRunListLimit     = 200
)

// deriveRunStore is the optional driver capability behind derive runs.
type deriveRunStore interface {
	StartDeriveRun(ctx context.Context, project string) (derive.Run, bool, error)
	GetDeriveRun(ctx context.Context, id string) (derive.Run, error)
	ListDeriveRuns(ctx context.Context, limit int) ([]derive.Run, error)
	CancelDeriveRun(ctx context.Context, id string) (derive.Run, error)
	ListActiveDeriveRunIDs(ctx context.Context) ([]string, error)
	ExecuteDeriveRun(ctx context.Context, id, worker string) (derive.Run, error)
}

// deriveRunRequest is the optional body of a create.
type deriveRunRequest struct {
	// Project is recorded on the nodes the run derives; empty records none.
	// It does not narrow which sessions the run covers.
	Project string `json:"project,omitempty"`
}

// deriveRunList is the list response.
type deriveRunList struct {
	Runs []derive.Run `json:"runs"`
}

// mountDeriveRuns registers and describes the derive-run routes.
func (s *Server) mountDeriveRuns(router *oasfiber.Router) {
	runID := func(doc *oasfiber.DocBuilder) *oasfiber.DocBuilder {
		return doc.PathParam("id", oas.String(), oas.ParamDescription("Derive run id (UUID)"))
	}

	router.Post(DeriveRunsPath, s.handleStartDeriveRun,
		oasfiber.Doc("startDeriveRun").
			Summary("Start an asynchronous derive run (operator)").
			Description("Snapshots the set of persisted sessions and re-derives each of them in the "+
				"background, recording progress per session so a run interrupted by a restart "+
				"resumes at its first unfinished session. At most one run is active at a time: "+
				"starting another while one is active returns that run with 200 instead of 202. "+
				"Poll it at the Location returned.").
			Tag("admin").
			OptionalJSONBody("Run options", s.schema(deriveRunRequest{})).
			JSONResponse(202, "Run created", s.schema(derive.Run{})).
			JSONResponse(200, "A run is already active; this is it", s.schema(derive.Run{})).
			JSONResponse(400, "Invalid payload", s.errorSchema()).
			JSONResponse(500, "Run could not be created", s.errorSchema()).
			JSONResponse(501, "Driver does not support derive runs", s.errorSchema()))

	router.Get(DeriveRunsPath, s.handleListDeriveRuns,
		oasfiber.Doc("listDeriveRuns").
			Summary("List derive runs (operator)").
			Description("Returns the most recent derive runs, newest first.").
			Tag("admin").
			QueryParam("limit", oas.Integer(oas.Minimum(1), oas.Maximum(maxDeriveRunListLimit)),
				oas.ParamDescription("Maximum number of runs (default 20, max 200)")).
			JSONResponse(200, "Recent runs", s.schema(deriveRunList{})).
			JSONResponse(400, "Malformed limit", s.errorSchema()).
			JSONResponse(500, "Runs could not be listed", s.errorSchema()).
			JSONResponse(501, "Driver does not support derive runs", s.errorSchema()))

	router.Post(deriveRunCancelPath, s.handleCancelDeriveRun,
		runID(oasfiber.Doc("cancelDeriveRun").
			Summary("Cancel a derive run (operator)").
			Description("Stops an active run after the session in progress. Sessions already "+
				"re-derived stay re-derived. Canceling a run that is no longer active returns it "+
				"unchanged.").
			Tag("admin")).
			JSONResponse(200, "The run after the cancel", s.schema(derive.Run{})).
			JSONResponse(404, "Run not found", s.errorSchema()).
			JSONResponse(500, "Run could not be canceled", s.errorSchema()).
			JSONResponse(501, "Driver does not support derive runs", s.errorSchema()))

	router.Get(deriveRunPath, s.handleGetDeriveRun,
		runID(oasfiber.Doc("getDeriveRun").
			Summary("Read a derive run (operator)").
			Description("Reports a run's state and progress: sessions done, total, and failed, the "+
				"aggregated derive report, and a sample of per-session errors.").
			Tag("admin")).
			JSONResponse(200, "The run", s.schema(derive.Run{})).
			JSONResponse(404, "Run not found", s.errorSchema()).
			JSONResponse(500, "Run could not be read", s.errorSchema()).
			JSONResponse(501, "Driver does not support derive runs", s.errorSchema()))
}

func (s *Server) deriveRuns(c fiber.Ctx) (deriveRunStore, bool) {
	store, ok := s.driver.(deriveRunStore)
	if !ok {
		_ = c.Status(fiber.StatusNotImplemented).JSON(llm.ErrorResponse{Error: "driver does not support derive runs"})
	}
	return store, ok
}

// handleStartDeriveRun creates a derive run, or answers with the one already
// active: 202 for a new run, 200 for an existing one. Either way the body is
// the run and Location names it, so a caller that only wants "a run is going"
// can treat both alike and poll.
func (s *Server) handleStartDeriveRun(c fiber.Ctx) error {
	store, ok := s.deriveRuns(c)
	if !ok {
		return nil
	}
	var req deriveRunRequest
	if len(c.Body()) > 0 {
		if err := c.Bind().Body(&req); err != nil {
			return c.Status(fiber.StatusBadRequest).JSON(llm.ErrorResponse{Error: "invalid payload: " + err.Error()})
		}
	}
	run, created, err := store.StartDeriveRun(c.Context(), req.Project)
	if err != nil {
		s.logger.Error("start derive run", "error", err)
		return c.Status(fiber.StatusInternalServerError).JSON(llm.ErrorResponse{Error: err.Error()})
	}
	s.kickDeriveRuns()
	c.Set(fiber.HeaderLocation, DeriveRunsPath+"/"+run.ID)
	if created {
		s.logger.Info("derive run created", "run_id", run.ID, "sessions_total", run.SessionsTotal)
		return c.Status(fiber.StatusAccepted).JSON(run)
	}
	return c.Status(fiber.StatusOK).JSON(run)
}

func (s *Server) handleGetDeriveRun(c fiber.Ctx) error {
	store, ok := s.deriveRuns(c)
	if !ok {
		return nil
	}
	run, err := store.GetDeriveRun(c.Context(), c.Params("id"))
	return s.deriveRunResponse(c, run, err, "get derive run")
}

func (s *Server) handleCancelDeriveRun(c fiber.Ctx) error {
	store, ok := s.deriveRuns(c)
	if !ok {
		return nil
	}
	run, err := store.CancelDeriveRun(c.Context(), c.Params("id"))
	if err == nil {
		s.logger.Info("derive run cancel requested", "run_id", run.ID, "state", run.State)
	}
	return s.deriveRunResponse(c, run, err, "cancel derive run")
}

func (s *Server) handleListDeriveRuns(c fiber.Ctx) error {
	store, ok := s.deriveRuns(c)
	if !ok {
		return nil
	}
	limit := defaultDeriveRunListLimit
	if raw := c.Query("limit"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 1 || parsed > maxDeriveRunListLimit {
			return c.Status(fiber.StatusBadRequest).JSON(llm.ErrorResponse{Error: "limit must be an integer between 1 and 200"})
		}
		limit = parsed
	}
	runs, err := store.ListDeriveRuns(c.Context(), limit)
	if err != nil {
		s.logger.Error("list derive runs", "error", err)
		return c.Status(fiber.StatusInternalServerError).JSON(llm.ErrorResponse{Error: err.Error()})
	}
	return c.JSON(deriveRunList{Runs: runs})
}

func (s *Server) deriveRunResponse(c fiber.Ctx, run derive.Run, err error, op string) error {
	switch {
	case errors.Is(err, derive.ErrRunNotFound):
		return c.Status(fiber.StatusNotFound).JSON(llm.ErrorResponse{Error: err.Error()})
	case err != nil:
		s.logger.Error(op, "error", err)
		return c.Status(fiber.StatusInternalServerError).JSON(llm.ErrorResponse{Error: err.Error()})
	default:
		return c.JSON(run)
	}
}

// kickDeriveRuns wakes the supervisor without blocking; a wake already pending
// covers this one.
func (s *Server) kickDeriveRuns() {
	select {
	case s.deriveRunWake <- struct{}{}:
	default:
	}
}

// StartDeriveRunSupervisor executes derive runs in the background until ctx
// ends. It runs a run created on this process as soon as it is created, and
// polls every interval for an active run no process is executing — one whose
// executor went away mid-run — and resumes it. Execution is serialized across
// processes by each run's advisory lock, so every replica can run a
// supervisor.
//
// When ctx ends mid-run the session in progress is abandoned and left
// unfinished, so the next process to pick the run up re-derives it.
func (s *Server) StartDeriveRunSupervisor(ctx context.Context, interval time.Duration) {
	store, ok := s.driver.(deriveRunStore)
	if !ok {
		return
	}
	if interval <= 0 {
		interval = DefaultDeriveRunPoll
	}
	go s.superviseDeriveRuns(ctx, store, interval)
}

func (s *Server) superviseDeriveRuns(ctx context.Context, store deriveRunStore, interval time.Duration) {
	worker := s.deriveRunWorkerName()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		s.executeActiveDeriveRuns(ctx, store, worker)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		case <-s.deriveRunWake:
		}
	}
}

func (s *Server) executeActiveDeriveRuns(ctx context.Context, store deriveRunStore, worker string) {
	ids, err := store.ListActiveDeriveRunIDs(ctx)
	if err != nil {
		if ctx.Err() == nil {
			s.logger.Warn("list active derive runs", "error", err)
		}
		return
	}
	for _, id := range ids {
		run, err := store.ExecuteDeriveRun(ctx, id, worker)
		switch {
		case errors.Is(err, derive.ErrRunBusy):
			continue
		case ctx.Err() != nil:
			s.logger.Info("derive run interrupted; it resumes on the next process to pick it up", "run_id", id)
			return
		case err != nil:
			s.logger.Error("derive run stopped; it resumes on the next poll", "run_id", id, "error", err)
		default:
			s.logger.Info("derive run finished",
				"run_id", run.ID, "state", run.State,
				"sessions_done", run.SessionsDone, "sessions_failed", run.SessionsFailed)
		}
	}
}

// deriveRunWorkerName identifies this process on the run record: the pod name
// when the deployment supplies one, else the host name, plus the instance id
// so a restarted process is distinguishable from its predecessor.
func (s *Server) deriveRunWorkerName() string {
	name := s.instance.PodName
	if name == "" {
		name, _ = os.Hostname()
	}
	if name == "" {
		return s.instance.InstanceID
	}
	return name + "/" + s.instance.InstanceID
}
