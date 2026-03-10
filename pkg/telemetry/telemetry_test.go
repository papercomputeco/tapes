package telemetry_test

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"path/filepath"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/logger"
	"github.com/papercomputeco/tapes/pkg/telemetry"
)

var _ = Describe("Telemetry", func() {
	Describe("Manager", func() {
		var tmpDir string

		BeforeEach(func() {
			tmpDir = GinkgoT().TempDir()
			Expect(os.MkdirAll(filepath.Join(tmpDir, ".tapes"), 0o755)).To(Succeed())
		})

		Describe("LoadOrCreate", func() {
			It("creates a new state file on first run", func() {
				mgr, err := telemetry.NewManager(filepath.Join(tmpDir, ".tapes"))
				Expect(err).NotTo(HaveOccurred())

				state, isFirstRun, err := mgr.LoadOrCreate()
				Expect(err).NotTo(HaveOccurred())
				Expect(isFirstRun).To(BeTrue())
				Expect(state.UUID).NotTo(BeEmpty())
				Expect(state.FirstRun).NotTo(BeEmpty())
			})

			It("loads existing state on subsequent runs", func() {
				mgr, err := telemetry.NewManager(filepath.Join(tmpDir, ".tapes"))
				Expect(err).NotTo(HaveOccurred())

				state1, isFirstRun, err := mgr.LoadOrCreate()
				Expect(err).NotTo(HaveOccurred())
				Expect(isFirstRun).To(BeTrue())

				state2, isFirstRun, err := mgr.LoadOrCreate()
				Expect(err).NotTo(HaveOccurred())
				Expect(isFirstRun).To(BeFalse())
				Expect(state2.UUID).To(Equal(state1.UUID))
				Expect(state2.FirstRun).To(Equal(state1.FirstRun))
			})

			It("writes the state file with 0600 permissions", func() {
				mgr, err := telemetry.NewManager(filepath.Join(tmpDir, ".tapes"))
				Expect(err).NotTo(HaveOccurred())

				_, _, err = mgr.LoadOrCreate()
				Expect(err).NotTo(HaveOccurred())

				statePath := filepath.Join(tmpDir, ".tapes", "telemetry.json")
				info, err := os.Stat(statePath)
				Expect(err).NotTo(HaveOccurred())
				Expect(info.Mode().Perm()).To(Equal(os.FileMode(0o600)))
			})

			It("stores valid JSON with expected fields", func() {
				mgr, err := telemetry.NewManager(filepath.Join(tmpDir, ".tapes"))
				Expect(err).NotTo(HaveOccurred())

				_, _, err = mgr.LoadOrCreate()
				Expect(err).NotTo(HaveOccurred())

				data, err := os.ReadFile(filepath.Join(tmpDir, ".tapes", "telemetry.json"))
				Expect(err).NotTo(HaveOccurred())

				var state telemetry.State
				Expect(json.Unmarshal(data, &state)).To(Succeed())
				Expect(state.UUID).NotTo(BeEmpty())
				Expect(state.FirstRun).NotTo(BeEmpty())
			})

			It("generates a valid UUID v4", func() {
				mgr, err := telemetry.NewManager(filepath.Join(tmpDir, ".tapes"))
				Expect(err).NotTo(HaveOccurred())

				state, _, err := mgr.LoadOrCreate()
				Expect(err).NotTo(HaveOccurred())
				Expect(state.UUID).To(MatchRegexp(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`))
			})

			It("returns an error for corrupt JSON", func() {
				statePath := filepath.Join(tmpDir, ".tapes", "telemetry.json")
				Expect(os.WriteFile(statePath, []byte("{invalid json"), 0o600)).To(Succeed())

				mgr, err := telemetry.NewManager(filepath.Join(tmpDir, ".tapes"))
				Expect(err).NotTo(HaveOccurred())

				_, _, err = mgr.LoadOrCreate()
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("parsing telemetry state"))
			})

			It("returns an error when directory is not writable", func() {
				if os.Getuid() == 0 {
					Skip("root ignores directory permissions")
				}

				readOnlyDir := filepath.Join(tmpDir, "readonly")
				Expect(os.MkdirAll(readOnlyDir, 0o555)).To(Succeed())

				mgr, err := telemetry.NewManager(readOnlyDir)
				Expect(err).NotTo(HaveOccurred())

				_, _, err = mgr.LoadOrCreate()
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("writing telemetry state"))
			})
		})
	})

	Describe("IsCI", func() {
		It("returns true when GITHUB_ACTIONS is set", func() {
			GinkgoT().Setenv("GITHUB_ACTIONS", "true")
			Expect(telemetry.IsCI()).To(BeTrue())
		})

		It("returns true when CI is set", func() {
			GinkgoT().Setenv("CI", "true")
			Expect(telemetry.IsCI()).To(BeTrue())
		})

		It("returns true when GITLAB_CI is set", func() {
			GinkgoT().Setenv("GITLAB_CI", "true")
			Expect(telemetry.IsCI()).To(BeTrue())
		})

		It("returns true when BUILDKITE is set", func() {
			GinkgoT().Setenv("BUILDKITE", "true")
			Expect(telemetry.IsCI()).To(BeTrue())
		})

		It("returns false when no CI env vars are set", func() {
			for _, env := range []string{"CI", "GITHUB_ACTIONS", "GITLAB_CI", "CIRCLECI", "TRAVIS", "JENKINS_URL", "BUILDKITE", "CODEBUILD_BUILD_ID"} {
				GinkgoT().Setenv(env, "")
			}
			Expect(telemetry.IsCI()).To(BeFalse())
		})
	})

	Describe("Context", func() {
		It("round-trips a client through context", func() {
			ctx := context.Background()
			Expect(telemetry.FromContext(ctx)).To(BeNil())

			ctx = telemetry.WithContext(ctx, nil)
			Expect(telemetry.FromContext(ctx)).To(BeNil())
		})

		It("returns nil from a plain background context", func() {
			Expect(telemetry.FromContext(context.Background())).To(BeNil())
		})
	})

	Describe("Client nil safety", func() {
		It("does not panic when calling capture methods on nil client", func() {
			var client *telemetry.Client
			Expect(func() {
				client.CaptureInstall()
				client.CaptureCommandRun("test")
				client.CaptureInit("default")
				client.CaptureSessionCreated("openai")
				client.CaptureSearch(5)
				client.CaptureServerStarted("api")
				client.CaptureMCPTool("tool")
				client.CaptureSyncPush()
				client.CaptureSyncPull()
				client.CaptureError("test", "runtime")
			}).NotTo(Panic())
		})

		It("does not panic when closing a nil client", func() {
			var client *telemetry.Client
			Expect(client.Close()).To(Succeed())
		})
	})

	Describe("CommonProperties", func() {
		It("includes os and arch", func() {
			props := telemetry.CommonProperties()
			Expect(props).To(HaveKey("os"))
			Expect(props).To(HaveKey("arch"))
			Expect(props["os"]).NotTo(BeEmpty())
			Expect(props["arch"]).NotTo(BeEmpty())
		})

		It("returns a fresh map each call", func() {
			props1 := telemetry.CommonProperties()
			props2 := telemetry.CommonProperties()
			props1.Set("extra", "mutated")
			Expect(props2).NotTo(HaveKey("extra"))
		})
	})

	Describe("NewClient", func() {
		var l *slog.Logger

		BeforeEach(func() {
			l = logger.NewNoop()
		})

		It("returns nil when PostHogAPIKey is empty", func() {
			orig := telemetry.PostHogAPIKey
			telemetry.PostHogAPIKey = ""
			defer func() { telemetry.PostHogAPIKey = orig }()

			client, err := telemetry.NewClient("test-uuid-1234", l)
			Expect(err).NotTo(HaveOccurred())
			Expect(client).To(BeNil())
		})

		It("creates a client when PostHogAPIKey is set", func() {
			orig := telemetry.PostHogAPIKey
			telemetry.PostHogAPIKey = "phc_test_key"
			defer func() { telemetry.PostHogAPIKey = orig }()

			client, err := telemetry.NewClient("test-uuid-1234", l)
			Expect(err).NotTo(HaveOccurred())
			Expect(client).NotTo(BeNil())
			Expect(client.Close()).To(Succeed())
		})
	})
})
