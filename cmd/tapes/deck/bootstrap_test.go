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
				configDir:    tmpDir,
				out:          &bytes.Buffer{},
				apiReachable: func(context.Context, string) bool { return false },
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

	Describe("step 3: configured api-target is reachable", func() {
		It("returns the configured target when /ping answers", func() {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusOK)
			}))
			DeferCleanup(server.Close)

			apiURL, cleanup, err := bootstrapAPI(context.Background(), bootstrapConfig{
				apiTarget: server.URL,
				configDir: tmpDir,
				out:       &bytes.Buffer{},
				apiReachable: func(_ context.Context, url string) bool {
					return url == server.URL
				},
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(apiURL).To(Equal(server.URL))
			cleanup()
		})
	})

	Describe("nothing is reachable", func() {
		It("returns the styled actionable error pointing at tapes local up", func() {
			_, _, err := bootstrapAPI(context.Background(), bootstrapConfig{
				apiTarget:    "http://localhost:8081",
				configDir:    tmpDir,
				out:          &bytes.Buffer{},
				apiReachable: func(context.Context, string) bool { return false },
			})
			Expect(err).To(HaveOccurred())
			msg := err.Error()
			Expect(msg).To(ContainSubstring("No tapes API is reachable"))
			Expect(msg).To(ContainSubstring("tapes local up"))
			Expect(msg).To(ContainSubstring("tapes deck --api-target"))
		})
	})

	Describe("corrupt start.json", func() {
		It("clears the file and falls through instead of erroring", func() {
			Expect(os.WriteFile(tmpDir+"/start.json", []byte("{not json"), 0o600)).To(Succeed())

			_, _, _ = bootstrapAPI(context.Background(), bootstrapConfig{
				configDir:    tmpDir,
				out:          &bytes.Buffer{},
				apiReachable: func(context.Context, string) bool { return false },
			})

			_, statErr := os.Stat(tmpDir + "/start.json")
			Expect(os.IsNotExist(statErr)).To(BeTrue())
		})
	})
})
