package telemetry_test

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

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

		It("returns false when no CI env vars are set", func() {
			// Clear all CI env vars to ensure clean state.
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

			// A nil client can be stored and retrieved.
			ctx = telemetry.WithContext(ctx, nil)
			Expect(telemetry.FromContext(ctx)).To(BeNil())
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
		})
	})
})
