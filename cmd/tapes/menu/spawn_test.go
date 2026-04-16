//go:build darwin

package menucmder

import (
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
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

	It("does not respawn when the recorded PID is alive", func() {
		// A long-running sleep stands in for the live menu process.
		sleepCmd := exec.Command("sleep", "60")
		Expect(sleepCmd.Start()).To(Succeed())
		DeferCleanup(func() {
			_ = sleepCmd.Process.Kill()
			_ = sleepCmd.Wait()
		})

		pidPath := filepath.Join(tmpDir, pidFileName)
		Expect(os.WriteFile(pidPath, []byte(strconv.Itoa(sleepCmd.Process.Pid)), 0o600)).To(Succeed())

		Spawn(tmpDir, false, discardLogger())

		// PID file untouched.
		Expect(readPID(pidPath)).To(Equal(sleepCmd.Process.Pid))
	})

	It("spawns when no pid file exists", func() {
		Spawn(tmpDir, true, discardLogger())

		pidPath := filepath.Join(tmpDir, pidFileName)
		pid := readPID(pidPath)
		if pid != 0 {
			if proc, err := os.FindProcess(pid); err == nil {
				_ = proc.Kill()
			}
		}
	})

	It("spawns when the recorded PID is dead", func() {
		pidPath := filepath.Join(tmpDir, pidFileName)
		Expect(os.WriteFile(pidPath, []byte("999999999"), 0o600)).To(Succeed())

		Spawn(tmpDir, false, discardLogger())

		newPID := readPID(pidPath)
		Expect(newPID).NotTo(Equal(999999999))
		if newPID != 0 {
			if proc, err := os.FindProcess(newPID); err == nil {
				_ = proc.Kill()
			}
		}
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
