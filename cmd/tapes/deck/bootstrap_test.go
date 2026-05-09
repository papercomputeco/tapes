package deckcmder

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"os"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/start"
)

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

			// State file should not exist — we never spawned anything.
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

			// We don't expect bootstrap to succeed (no Postgres, no Docker test
			// fixture), but we do expect step 2 to clear the stale state.
			_, _, _ = bootstrapAPI(context.Background(), bootstrapConfig{
				configDir: tmpDir,
				out:       &bytes.Buffer{},
			})

			state, err := manager.LoadState()
			Expect(err).NotTo(HaveOccurred())
			Expect(state).To(BeNil())
		})
	})

	Describe("step 3: ensurePostgres without Docker", func() {
		It("returns the styled actionable error when Docker is missing and Postgres is unreachable", func() {
			_, _, err := bootstrapAPI(context.Background(), bootstrapConfig{
				configDir:   tmpDir,
				postgresDSN: "postgres://x:y@127.0.0.1:1/db?sslmode=disable",
				out:         &bytes.Buffer{},
				hasDocker:   func() bool { return false },
			})
			Expect(err).To(HaveOccurred())
			msg := err.Error()
			Expect(msg).To(ContainSubstring("Postgres is not reachable and Docker is not installed"))
			Expect(msg).To(ContainSubstring("Install Docker"))
			Expect(msg).To(ContainSubstring("tapes config set storage.postgres_dsn"))
		})
	})

	Describe("corrupt start.json", func() {
		It("clears the file and falls through instead of erroring", func() {
			Expect(os.WriteFile(tmpDir+"/start.json", []byte("{not json"), 0o600)).To(Succeed())

			_, _, _ = bootstrapAPI(context.Background(), bootstrapConfig{
				configDir:   tmpDir,
				postgresDSN: "postgres://x:y@127.0.0.1:1/db?sslmode=disable",
				out:         &bytes.Buffer{},
				hasDocker:   func() bool { return false },
			})

			_, statErr := os.Stat(tmpDir + "/start.json")
			Expect(os.IsNotExist(statErr)).To(BeTrue())
		})
	})

	Describe("postgresReachable", func() {
		It("returns false on empty DSN", func() {
			Expect(postgresReachable(context.Background(), "")).To(BeFalse())
		})

		It("returns false on an unreachable port", func() {
			// Port 1 is privileged + closed; a 1s timeout keeps the test fast.
			Expect(postgresReachable(context.Background(), "postgres://x:y@127.0.0.1:1/db?sslmode=disable")).To(BeFalse())
		})
	})
})
