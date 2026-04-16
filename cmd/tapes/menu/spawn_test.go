//go:build darwin

package menucmder

import (
	"io"
	"log/slog"
	"os"
	"os/exec"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/start"
)

var _ = Describe("Spawn", func() {
	var tmpDir string

	BeforeEach(func() {
		var err error
		tmpDir, err = os.MkdirTemp("", "tapes-spawn-menu-*")
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		Expect(os.RemoveAll(tmpDir)).To(Succeed())
	})

	It("does nothing when an existing menu PID is alive", func() {
		// A long-running sleep stands in for the live menu process.
		sleepCmd := exec.Command("sleep", "60")
		Expect(sleepCmd.Start()).To(Succeed())
		DeferCleanup(func() {
			_ = sleepCmd.Process.Kill()
			_ = sleepCmd.Wait()
		})

		manager, err := start.NewManager(tmpDir)
		Expect(err).NotTo(HaveOccurred())
		Expect(manager.SaveState(&start.State{
			DaemonPID: os.Getpid(),
			ProxyURL:  "http://localhost:9000",
			APIURL:    "http://localhost:9001",
			MenuPID:   sleepCmd.Process.Pid,
		})).To(Succeed())

		Spawn(manager, tmpDir, false, discardLogger())

		// The recorded MenuPID must be untouched — no respawn happened.
		loaded, err := manager.LoadState()
		Expect(err).NotTo(HaveOccurred())
		Expect(loaded.MenuPID).To(Equal(sleepCmd.Process.Pid))
	})

	It("spawns a new menu when no PID is recorded", func() {
		manager, err := start.NewManager(tmpDir)
		Expect(err).NotTo(HaveOccurred())

		Spawn(manager, tmpDir, true, discardLogger())

		loaded, err := manager.LoadState()
		Expect(err).NotTo(HaveOccurred())
		if loaded != nil && loaded.MenuPID != 0 {
			// Clean up the spawned process — the test binary will exit fast since
			// it does not have a "menu" subcommand, but it may still be alive.
			if proc, findErr := os.FindProcess(loaded.MenuPID); findErr == nil {
				_ = proc.Kill()
			}
		}
	})

	It("spawns a new menu when the recorded PID is dead", func() {
		manager, err := start.NewManager(tmpDir)
		Expect(err).NotTo(HaveOccurred())
		Expect(manager.SaveState(&start.State{
			DaemonPID: os.Getpid(),
			MenuPID:   999999999, // unlikely to be alive
		})).To(Succeed())

		Spawn(manager, tmpDir, false, discardLogger())

		loaded, err := manager.LoadState()
		Expect(err).NotTo(HaveOccurred())
		Expect(loaded).NotTo(BeNil())
		Expect(loaded.MenuPID).NotTo(Equal(999999999))
		if loaded.MenuPID != 0 {
			if proc, findErr := os.FindProcess(loaded.MenuPID); findErr == nil {
				_ = proc.Kill()
			}
		}
	})
})

var _ = Describe("saveMenuPID", func() {
	var tmpDir string

	BeforeEach(func() {
		var err error
		tmpDir, err = os.MkdirTemp("", "tapes-save-menu-pid-*")
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		Expect(os.RemoveAll(tmpDir)).To(Succeed())
	})

	It("creates state when none exists", func() {
		manager, err := start.NewManager(tmpDir)
		Expect(err).NotTo(HaveOccurred())

		saveMenuPID(manager, 42, discardLogger())

		loaded, err := manager.LoadState()
		Expect(err).NotTo(HaveOccurred())
		Expect(loaded).NotTo(BeNil())
		Expect(loaded.MenuPID).To(Equal(42))
	})

	It("preserves existing state fields", func() {
		manager, err := start.NewManager(tmpDir)
		Expect(err).NotTo(HaveOccurred())
		Expect(manager.SaveState(&start.State{
			DaemonPID:        os.Getpid(),
			ProxyURL:         "http://localhost:9000",
			APIURL:           "http://localhost:9001",
			ShutdownWhenIdle: true,
			Agents: []start.AgentSession{
				{Name: "claude", PID: 100, StartedAt: time.Now()},
			},
		})).To(Succeed())

		saveMenuPID(manager, 55, discardLogger())

		loaded, err := manager.LoadState()
		Expect(err).NotTo(HaveOccurred())
		Expect(loaded.MenuPID).To(Equal(55))
		Expect(loaded.DaemonPID).To(Equal(os.Getpid()))
		Expect(loaded.ProxyURL).To(Equal("http://localhost:9000"))
		Expect(loaded.ShutdownWhenIdle).To(BeTrue())
		Expect(loaded.Agents).To(HaveLen(1))
	})
})

var _ = Describe("processAlive", func() {
	It("returns false for non-positive PIDs", func() {
		Expect(processAlive(0)).To(BeFalse())
		Expect(processAlive(-1)).To(BeFalse())
	})

	It("returns true for the current process", func() {
		Expect(processAlive(os.Getpid())).To(BeTrue())
	})
})

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}
