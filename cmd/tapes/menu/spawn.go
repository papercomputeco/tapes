//go:build darwin

package menucmder

import (
	"context"
	"log/slog"
	"os"
	"os/exec"
	"syscall"
)

// Spawn launches the menu bar app as a separate process if one is not already
// running. It is idempotent: callers can invoke it on every server start and
// only the first call will fork a new process. The menu owns its lifecycle via
// a PID file (typically ~/.tapes/menu.pid) and survives across restarts of the
// invoking server — it is only killed when the user quits it from the menu.
func Spawn(configDir string, debug bool, log *slog.Logger) {
	pidPath, err := pidFilePath(configDir)
	if err != nil {
		log.Warn("could not resolve menu pid file", "error", err)
		return
	}

	if pid := readPID(pidPath); pid > 0 && processAlive(pid) {
		return
	}

	execPath, err := os.Executable()
	if err != nil {
		log.Warn("could not resolve executable for menu", "error", err)
		return
	}

	args := []string{"menu"}
	if debug {
		args = append(args, "--debug")
	}
	if configDir != "" {
		args = append(args, "--config-dir", configDir)
	}

	// The menu outlives the caller's lifecycle by design — use a Background
	// context so it survives `tapes serve` shutdown.
	// #nosec G204 -- args are constructed from known constants.
	cmd := exec.CommandContext(context.Background(), execPath, args...)
	cmd.Stdout = nil
	cmd.Stderr = nil

	if err := cmd.Start(); err != nil {
		log.Warn("could not start menu bar app", "error", err)
		return
	}

	log.Info("started menu bar app", "pid", cmd.Process.Pid)

	if err := writePID(pidPath, cmd.Process.Pid); err != nil {
		log.Warn("could not persist menu pid", "error", err)
	}

	go func() {
		_ = cmd.Wait()
		// Clean up the pid file once the menu exits so a future Spawn does not
		// see a stale PID and skip relaunching.
		if err := removePID(pidPath); err != nil {
			log.Warn("could not remove menu pid file", "error", err)
		}
	}()
}

func processAlive(pid int) bool {
	if pid <= 0 {
		return false
	}
	proc, err := os.FindProcess(pid)
	if err != nil {
		return false
	}
	return proc.Signal(syscall.Signal(0)) == nil
}
