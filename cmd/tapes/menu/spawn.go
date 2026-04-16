//go:build darwin

package menucmder

import (
	"log/slog"
	"os"
	"os/exec"
)

// Spawn launches the menu bar app as a separate process. It is safe to call
// on every server start — the spawned binary acquires an exclusive flock on
// ~/.tapes/menu.lock and exits silently if a menu is already running, so at
// most one instance is ever visible.
//
// On non-darwin platforms this is a no-op via the build tag.
func Spawn(configDir string, log *slog.Logger) {
	execPath, err := os.Executable()
	if err != nil {
		log.Warn("could not resolve executable for menu", "error", err)
		return
	}

	args := []string{"menu"}
	if configDir != "" {
		args = append(args, "--config-dir", configDir)
	}

	// The menu outlives the caller's lifecycle by design, so we deliberately
	// do not bind it to a cancellable context — `tapes serve` exiting must
	// not kill the menu.
	// #nosec G204 -- args are constructed from known constants.
	cmd := exec.Command(execPath, args...) //nolint:noctx
	cmd.Stdout = nil
	cmd.Stderr = nil

	if err := cmd.Start(); err != nil {
		log.Warn("could not start menu bar app", "error", err)
		return
	}

	log.Info("spawned menu bar app candidate", "pid", cmd.Process.Pid)

	// Reap the child if it exits quickly (e.g. another menu already holds the
	// lock and this candidate exits silently).
	go func() { _ = cmd.Wait() }()
}
