//go:build darwin

package menucmder

import (
	"context"
	"log/slog"
	"os"
	"os/exec"
	"syscall"

	"github.com/papercomputeco/tapes/pkg/start"
)

// Spawn launches the menu bar app as a separate process if one is not already
// running. It is idempotent: callers can invoke it on every server start and
// only the first call will fork a new process. The menu's lifecycle is
// independent of the calling server — it survives across restarts and is only
// killed when the user quits it from the menu itself.
func Spawn(manager *start.Manager, configDir string, debug bool, log *slog.Logger) {
	if existingMenuAlive(manager) {
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
	saveMenuPID(manager, cmd.Process.Pid, log)

	go func() { _ = cmd.Wait() }()
}

func existingMenuAlive(manager *start.Manager) bool {
	state, err := manager.LoadState()
	if err != nil || state == nil || state.MenuPID == 0 {
		return false
	}
	return processAlive(state.MenuPID)
}

func saveMenuPID(manager *start.Manager, pid int, log *slog.Logger) {
	lock, err := manager.Lock()
	if err != nil {
		log.Warn("could not lock state to save menu PID", "error", err)
		return
	}
	defer func() { _ = lock.Release() }()

	state, err := manager.LoadState()
	if err != nil {
		log.Warn("could not load state to save menu PID", "error", err)
		return
	}
	if state == nil {
		state = &start.State{}
	}
	state.MenuPID = pid
	if err := manager.SaveState(state); err != nil {
		log.Warn("could not save menu PID to state", "error", err)
	}
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
