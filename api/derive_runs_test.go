package api

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"time"

	"github.com/gofiber/fiber/v3"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/derive"
	tapeslogger "github.com/papercomputeco/tapes/pkg/logger"
	"github.com/papercomputeco/tapes/pkg/storage/inmemory"
)

// deriveRunStub is an in-memory deriveRunStore: one run slot, executed by
// marking it completed.
type deriveRunStub struct {
	*inmemory.Driver

	mu       sync.Mutex
	runs     []derive.Run
	busy     bool
	executed []string
}

func (s *deriveRunStub) StartDeriveRun(_ context.Context, project string) (derive.Run, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, run := range s.runs {
		if run.State.Active() {
			return run, false, nil
		}
	}
	run := derive.Run{
		ID: "run-" + string(rune('a'+len(s.runs))), Project: project, State: derive.RunPending,
		SessionsTotal: 3, Errors: []string{}, CreatedAt: time.Now(), UpdatedAt: time.Now(),
	}
	s.runs = append(s.runs, run)
	return run, true, nil
}

func (s *deriveRunStub) find(id string) (int, bool) {
	for i, run := range s.runs {
		if run.ID == id {
			return i, true
		}
	}
	return 0, false
}

func (s *deriveRunStub) GetDeriveRun(_ context.Context, id string) (derive.Run, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	i, ok := s.find(id)
	if !ok {
		return derive.Run{}, derive.ErrRunNotFound
	}
	return s.runs[i], nil
}

func (s *deriveRunStub) ListDeriveRuns(_ context.Context, limit int) ([]derive.Run, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := []derive.Run{}
	for i := len(s.runs) - 1; i >= 0 && len(out) < limit; i-- {
		out = append(out, s.runs[i])
	}
	return out, nil
}

func (s *deriveRunStub) CancelDeriveRun(_ context.Context, id string) (derive.Run, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	i, ok := s.find(id)
	if !ok {
		return derive.Run{}, derive.ErrRunNotFound
	}
	if s.runs[i].State.Active() {
		s.runs[i].State = derive.RunCanceled
	}
	return s.runs[i], nil
}

func (s *deriveRunStub) ListActiveDeriveRunIDs(_ context.Context) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var ids []string
	for _, run := range s.runs {
		if run.State.Active() {
			ids = append(ids, run.ID)
		}
	}
	return ids, nil
}

func (s *deriveRunStub) ExecuteDeriveRun(_ context.Context, id, worker string) (derive.Run, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.busy {
		return derive.Run{}, derive.ErrRunBusy
	}
	i, ok := s.find(id)
	if !ok {
		return derive.Run{}, derive.ErrRunNotFound
	}
	s.runs[i].State = derive.RunCompleted
	s.runs[i].SessionsDone = s.runs[i].SessionsTotal
	s.runs[i].Worker = worker
	s.executed = append(s.executed, id)
	return s.runs[i], nil
}

func (s *deriveRunStub) executedRuns() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.executed...)
}

var _ deriveRunStore = (*deriveRunStub)(nil)

var _ = Describe("derive runs on the internal listener", func() {
	const token = "derive-run-token"

	var (
		stub     *deriveRunStub
		server   *Server
		internal *InternalServer
	)

	build := func(driver any) {
		var err error
		switch d := driver.(type) {
		case *deriveRunStub:
			server, err = NewServer(Config{ListenAddr: ":0"}, d, tapeslogger.NewNoop())
		case *inmemory.Driver:
			server, err = NewServer(Config{ListenAddr: ":0"}, d, tapeslogger.NewNoop())
		}
		Expect(err).NotTo(HaveOccurred())
		internal, err = server.NewInternalServer(InternalConfig{ListenAddr: DefaultInternalListen, Token: token})
		Expect(err).NotTo(HaveOccurred())
	}

	call := func(app *fiber.App, method, path, body string, authed bool) (*http.Response, []byte) {
		var reader io.Reader
		if body != "" {
			reader = strings.NewReader(body)
		}
		req := httptest.NewRequest(method, path, reader)
		if body != "" {
			req.Header.Set(fiber.HeaderContentType, fiber.MIMEApplicationJSON)
		}
		if authed {
			req.Header.Set(fiber.HeaderAuthorization, "Bearer "+token)
		}
		response, err := app.Test(req, fiber.TestConfig{Timeout: 5 * time.Second})
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(response.Body.Close)
		raw, err := io.ReadAll(response.Body)
		Expect(err).NotTo(HaveOccurred())
		return response, raw
	}

	decodeRun := func(raw []byte) derive.Run {
		var run derive.Run
		Expect(json.Unmarshal(raw, &run)).To(Succeed())
		return run
	}

	BeforeEach(func() {
		stub = &deriveRunStub{Driver: inmemory.NewDriver()}
		build(stub)
	})

	It("creates a run with 202 and answers a second create with the same run and 200", func() {
		response, raw := call(internal.app, http.MethodPost, DeriveRunsPath, "", true)
		Expect(response.StatusCode).To(Equal(http.StatusAccepted))
		created := decodeRun(raw)
		Expect(created.ID).NotTo(BeEmpty())
		Expect(created.State).To(Equal(derive.RunPending))
		Expect(response.Header.Get(fiber.HeaderLocation)).To(Equal(DeriveRunsPath + "/" + created.ID))

		response, raw = call(internal.app, http.MethodPost, DeriveRunsPath, `{"project":"p"}`, true)
		Expect(response.StatusCode).To(Equal(http.StatusOK))
		Expect(decodeRun(raw).ID).To(Equal(created.ID))
	})

	It("reads, lists, and cancels runs", func() {
		_, raw := call(internal.app, http.MethodPost, DeriveRunsPath, "", true)
		id := decodeRun(raw).ID

		response, raw := call(internal.app, http.MethodGet, DeriveRunsPath+"/"+id, "", true)
		Expect(response.StatusCode).To(Equal(http.StatusOK))
		Expect(decodeRun(raw).ID).To(Equal(id))

		response, raw = call(internal.app, http.MethodGet, DeriveRunsPath+"?limit=5", "", true)
		Expect(response.StatusCode).To(Equal(http.StatusOK))
		var list deriveRunList
		Expect(json.Unmarshal(raw, &list)).To(Succeed())
		Expect(list.Runs).To(HaveLen(1))

		response, raw = call(internal.app, http.MethodPost, DeriveRunsPath+"/"+id+"/cancel", "", true)
		Expect(response.StatusCode).To(Equal(http.StatusOK))
		Expect(decodeRun(raw).State).To(Equal(derive.RunCanceled))
	})

	It("answers an unknown run with 404 and a bad limit with 400", func() {
		response, _ := call(internal.app, http.MethodGet, DeriveRunsPath+"/nope", "", true)
		Expect(response.StatusCode).To(Equal(http.StatusNotFound))
		response, _ = call(internal.app, http.MethodPost, DeriveRunsPath+"/nope/cancel", "", true)
		Expect(response.StatusCode).To(Equal(http.StatusNotFound))
		response, _ = call(internal.app, http.MethodGet, DeriveRunsPath+"?limit=0", "", true)
		Expect(response.StatusCode).To(Equal(http.StatusBadRequest))
	})

	It("rejects a malformed body", func() {
		response, _ := call(internal.app, http.MethodPost, DeriveRunsPath, `{"project":`, true)
		Expect(response.StatusCode).To(Equal(http.StatusBadRequest))
	})

	It("requires the bearer token", func() {
		response, _ := call(internal.app, http.MethodPost, DeriveRunsPath, "", false)
		Expect(response.StatusCode).To(Equal(http.StatusUnauthorized))
		Expect(stub.runs).To(BeEmpty())
	})

	// The gateway rewrites a public prefix onto the API listener's root, so
	// a route there is a route anyone with gateway access can call.
	It("is not served on the API listener", func() {
		response, _ := call(server.app, http.MethodPost, DeriveRunsPath, "", true)
		Expect(response.StatusCode).To(Equal(http.StatusNotFound))
		Expect(stub.runs).To(BeEmpty())
	})

	It("answers 501 when the driver cannot host runs", func() {
		build(inmemory.NewDriver())
		response, _ := call(internal.app, http.MethodPost, DeriveRunsPath, "", true)
		Expect(response.StatusCode).To(Equal(http.StatusNotImplemented))
	})

	It("executes a created run in the background, independent of the request", func(ctx SpecContext) {
		runCtx, cancel := context.WithCancel(ctx)
		DeferCleanup(cancel)
		// A long poll: only the wake from the create can start it promptly.
		server.StartDeriveRunSupervisor(runCtx, time.Hour)

		_, raw := call(internal.app, http.MethodPost, DeriveRunsPath, "", true)
		id := decodeRun(raw).ID

		Eventually(stub.executedRuns).WithTimeout(5 * time.Second).Should(ConsistOf(id))
		_, raw = call(internal.app, http.MethodGet, DeriveRunsPath+"/"+id, "", true)
		run := decodeRun(raw)
		Expect(run.State).To(Equal(derive.RunCompleted))
		Expect(run.Worker).NotTo(BeEmpty())
	})

	It("resumes an active run it finds on its poll", func(ctx SpecContext) {
		run, _, err := stub.StartDeriveRun(ctx, "")
		Expect(err).NotTo(HaveOccurred())

		runCtx, cancel := context.WithCancel(ctx)
		DeferCleanup(cancel)
		server.StartDeriveRunSupervisor(runCtx, 10*time.Millisecond)

		Eventually(stub.executedRuns).WithTimeout(5 * time.Second).Should(ConsistOf(run.ID))
	})

	It("leaves a run another process holds alone", func(ctx SpecContext) {
		stub.busy = true
		_, _, err := stub.StartDeriveRun(ctx, "")
		Expect(err).NotTo(HaveOccurred())

		runCtx, cancel := context.WithCancel(ctx)
		DeferCleanup(cancel)
		server.StartDeriveRunSupervisor(runCtx, 10*time.Millisecond)

		Consistently(stub.executedRuns).WithTimeout(100 * time.Millisecond).Should(BeEmpty())
	})
})
