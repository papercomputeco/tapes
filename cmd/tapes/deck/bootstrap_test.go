package deckcmder

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/start"
)

// fakeSpawn returns a spawn function suitable for bootstrapConfig.spawn that
// records the supplied SpawnError sentinel to the manager (mimicking what
// the real daemon does on failure) and then returns the given error.
func fakeSpawn(reason start.SpawnErrorReason, detail string) func(context.Context, *start.Manager, start.SpawnOptions) (*start.State, error) {
	return func(_ context.Context, m *start.Manager, opts start.SpawnOptions) (*start.State, error) {
		_ = m.RecordSpawnError(start.SpawnError{
			Reason: reason,
			DSN:    opts.PostgresDSN,
			Detail: detail,
		})
		return nil, errors.New("daemon process exited during startup")
	}
}

var _ = Describe("bootstrapAPI", func() {
	var tmpDir string

	BeforeEach(func() {
		var err error
		tmpDir, err = os.MkdirTemp("", "deck-bootstrap-*")
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		Expect(os.RemoveAll(tmpDir)).To(Succeed())
	})

	Describe("step 1: explicit api-target", func() {
		It("returns the configured target without touching the start manager", func() {
			apiURL, cleanup, err := bootstrapAPI(context.Background(), bootstrapConfig{
				apiTarget:         "https://api.paper.cloud",
				apiTargetIsCustom: true,
				configDir:         tmpDir,
				out:               &bytes.Buffer{},
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(apiURL).To(Equal("https://api.paper.cloud"))
			Expect(cleanup).NotTo(BeNil())
			cleanup()

			_, statErr := os.Stat(tmpDir + "/start.json")
			Expect(os.IsNotExist(statErr)).To(BeTrue())
		})

		It("normalizes a bare hostname", func() {
			apiURL, cleanup, err := bootstrapAPI(context.Background(), bootstrapConfig{
				apiTarget:         "api.example.com:8081",
				apiTargetIsCustom: true,
				configDir:         tmpDir,
				out:               &bytes.Buffer{},
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(apiURL).To(Equal("http://api.example.com:8081"))
			cleanup()
		})
	})

	Describe("step 2: existing healthy daemon", func() {
		It("returns the daemon's APIURL and registers a deck session", func() {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusOK)
			}))
			DeferCleanup(server.Close)

			manager, err := start.NewManager(tmpDir)
			Expect(err).NotTo(HaveOccurred())
			Expect(manager.SaveState(&start.State{
				DaemonPID: os.Getpid(),
				APIURL:    server.URL,
				ProxyURL:  server.URL,
			})).To(Succeed())

			apiURL, cleanup, err := bootstrapAPI(context.Background(), bootstrapConfig{
				configDir: tmpDir,
				out:       &bytes.Buffer{},
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(apiURL).To(Equal(server.URL))

			state, err := manager.LoadState()
			Expect(err).NotTo(HaveOccurred())
			Expect(state.Agents).To(HaveLen(1))
			Expect(state.Agents[0].Name).To(Equal("deck"))
			Expect(state.Agents[0].PID).To(Equal(os.Getpid()))

			cleanup()

			afterCleanup, err := manager.LoadState()
			Expect(err).NotTo(HaveOccurred())
			Expect(afterCleanup.Agents).To(BeEmpty())
		})

		It("clears stale state when the recorded daemon is dead", func() {
			manager, err := start.NewManager(tmpDir)
			Expect(err).NotTo(HaveOccurred())
			// PID 999999999 is essentially guaranteed not to exist; APIURL is
			// pointing at a closed port. StateHealthy returns false → state cleared.
			Expect(manager.SaveState(&start.State{
				DaemonPID: 999999999,
				APIURL:    "http://127.0.0.1:1",
				ProxyURL:  "http://127.0.0.1:1",
			})).To(Succeed())

			_, _, _ = bootstrapAPI(context.Background(), bootstrapConfig{
				configDir: tmpDir,
				out:       &bytes.Buffer{},
				hasDocker: func() bool { return false },
				spawn:     fakeSpawn(start.ReasonPostgresUnreachable, "connection refused"),
			})

			state, err := manager.LoadState()
			Expect(err).NotTo(HaveOccurred())
			Expect(state).To(BeNil())
		})
	})

	Describe("DSN mismatch with running daemon", func() {
		It("refuses to attach and returns a styled actionable error", func() {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusOK)
			}))
			DeferCleanup(server.Close)

			manager, err := start.NewManager(tmpDir)
			Expect(err).NotTo(HaveOccurred())
			Expect(manager.SaveState(&start.State{
				DaemonPID:   os.Getpid(),
				APIURL:      server.URL,
				ProxyURL:    server.URL,
				PostgresDSN: "postgres://running/db_a",
			})).To(Succeed())

			_, _, err = bootstrapAPI(context.Background(), bootstrapConfig{
				configDir:   tmpDir,
				postgresDSN: "postgres://requested/db_b",
				out:         &bytes.Buffer{},
			})
			Expect(err).To(HaveOccurred())
			msg := err.Error()
			Expect(msg).To(ContainSubstring("Running tapes daemon is bound to a different Postgres"))
			Expect(msg).To(ContainSubstring("postgres://running/db_a"))
			Expect(msg).To(ContainSubstring("postgres://requested/db_b"))
			Expect(msg).To(ContainSubstring("pkill -f 'tapes start'"))
		})
	})

	Describe("spawn fails with postgres_unreachable + non-local DSN", func() {
		It("refuses to bootstrap when the configured DSN points elsewhere", func() {
			_, _, err := bootstrapAPI(context.Background(), bootstrapConfig{
				configDir:   tmpDir,
				postgresDSN: "postgres://user:pw@staging.example.com:5432/db?sslmode=disable",
				out:         &bytes.Buffer{},
				hasDocker:   func() bool { return true },
				spawn:       fakeSpawn(start.ReasonPostgresUnreachable, "dial tcp staging.example.com:5432: connect: connection refused"),
			})
			Expect(err).To(HaveOccurred())
			msg := err.Error()
			Expect(msg).To(ContainSubstring("Configured Postgres is unreachable"))
			Expect(msg).To(ContainSubstring("staging.example.com"))
			Expect(msg).To(ContainSubstring(`tapes config set storage.postgres_dsn ""`))
		})

		It("refuses to bootstrap when the configured DSN uses a non-default port", func() {
			_, _, err := bootstrapAPI(context.Background(), bootstrapConfig{
				configDir:   tmpDir,
				postgresDSN: "postgres://user:pw@localhost:6543/db?sslmode=disable",
				out:         &bytes.Buffer{},
				hasDocker:   func() bool { return true },
				spawn:       fakeSpawn(start.ReasonPostgresUnreachable, "dial tcp 127.0.0.1:6543: connect: connection refused"),
			})
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("Configured Postgres is unreachable"))
		})
	})

	Describe("spawn fails with postgres_unreachable + no Docker", func() {
		It("returns the styled actionable error when Docker is missing", func() {
			_, _, err := bootstrapAPI(context.Background(), bootstrapConfig{
				configDir: tmpDir,
				out:       &bytes.Buffer{},
				hasDocker: func() bool { return false },
				spawn:     fakeSpawn(start.ReasonPostgresUnreachable, "dial tcp 127.0.0.1:5432: connect: connection refused"),
			})
			Expect(err).To(HaveOccurred())
			msg := err.Error()
			Expect(msg).To(ContainSubstring("Postgres is not reachable and Docker is not installed"))
			Expect(msg).To(ContainSubstring("Install Docker"))
			Expect(msg).To(ContainSubstring("tapes config set storage.postgres_dsn"))
		})
	})

	Describe("spawn fails with postgres_unreachable + local default + docker", func() {
		It("calls local.Up and retries the spawn once", func() {
			localUpCalls := 0
			spawnCalls := 0
			_, _, err := bootstrapAPI(context.Background(), bootstrapConfig{
				configDir: tmpDir,
				out:       &bytes.Buffer{},
				hasDocker: func() bool { return true },
				localUp: func(_ context.Context, _ string, _ io.Writer) error {
					localUpCalls++
					return nil
				},
				spawn: func(_ context.Context, m *start.Manager, opts start.SpawnOptions) (*start.State, error) {
					spawnCalls++
					if spawnCalls == 1 {
						_ = m.RecordSpawnError(start.SpawnError{
							Reason: start.ReasonPostgresUnreachable,
							DSN:    opts.PostgresDSN,
							Detail: "dial tcp 127.0.0.1:5432: connect: connection refused",
						})
						return nil, errors.New("daemon process exited during startup")
					}
					_ = m.ClearSpawnError()
					return nil, errors.New("retry-also-failed-but-we-only-care-about-the-control-flow")
				},
			})
			Expect(err).To(HaveOccurred())
			Expect(localUpCalls).To(Equal(1))
			Expect(spawnCalls).To(Equal(2))
		})
	})

	Describe("spawn fails with non-postgres reason", func() {
		It("surfaces the underlying error without invoking local.Up", func() {
			localUpCalls := 0
			_, _, err := bootstrapAPI(context.Background(), bootstrapConfig{
				configDir: tmpDir,
				out:       &bytes.Buffer{},
				hasDocker: func() bool { return true },
				localUp: func(_ context.Context, _ string, _ io.Writer) error {
					localUpCalls++
					return nil
				},
				spawn: fakeSpawn(start.ReasonOther, "listen tcp 127.0.0.1:0: bind: address already in use"),
			})
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("spawning tapes daemon"))
			Expect(localUpCalls).To(Equal(0))
		})
	})

	Describe("corrupt start.json", func() {
		It("clears the file and falls through instead of erroring", func() {
			Expect(os.WriteFile(tmpDir+"/start.json", []byte("{not json"), 0o600)).To(Succeed())

			_, _, _ = bootstrapAPI(context.Background(), bootstrapConfig{
				configDir: tmpDir,
				out:       &bytes.Buffer{},
				hasDocker: func() bool { return false },
				spawn:     fakeSpawn(start.ReasonPostgresUnreachable, "connection refused"),
			})

			_, statErr := os.Stat(tmpDir + "/start.json")
			Expect(os.IsNotExist(statErr)).To(BeTrue())
		})
	})
})
