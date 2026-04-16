//go:build darwin

package menucmder

import (
	"context"
	"log/slog"
	"os"
	"os/exec"
)

// Spawn launches the menu bar app as a separate process. It is safe to call
// on every server start — the spawned binary acquires an exclusive flock on
// ~/.tapes/menu.pid and exits silently if a menu is already running, so at
// most one instance is ever visible. Using flock instead of a pid-alive
// check eliminates PID-reuse and TOCTOU races a plain pid file would suffer.
//
// On non-darwin platforms this is a no-op via the build tag.
func Spawn(configDir string, debug bool, log *slog.Logger) {
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

	log.Info("spawned menu bar app candidate", "pid", cmd.Process.Pid)

	// Reap the child if it exits quickly (e.g. another menu already holds the
	// lock and this candidate exits silently). Without this the process would
	// linger as a zombie until the parent exits.
	go func() { _ = cmd.Wait() }()
}
