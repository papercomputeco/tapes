//go:build darwin

package menucmder

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"

	"github.com/papercomputeco/tapes/pkg/dotdir"
)

const pidFileName = "menu.pid"

// pidFilePath resolves the absolute path to the menu pid file. The file lives
// inside the resolved tapes dotdir (typically ~/.tapes/menu.pid), so the menu
// owns its lifecycle independent of the start daemon.
func pidFilePath(configDir string) (string, error) {
	manager := dotdir.NewManager()
	dir, err := manager.Target(configDir)
	if err != nil {
		return "", fmt.Errorf("resolving tapes dir: %w", err)
	}
	if dir == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return "", fmt.Errorf("resolving home dir: %w", err)
		}
		dir = filepath.Join(home, ".tapes")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", fmt.Errorf("creating tapes dir: %w", err)
	}
	return filepath.Join(dir, pidFileName), nil
}

// acquireMenuLock opens the pid file and grabs an exclusive, non-blocking
// flock on the file descriptor. The kernel auto-releases the lock when the
// process exits, so a hard kill of the menu cannot leave the lock stranded —
// this is what eliminates the PID-reuse races a plain pid file would suffer.
//
// Returns an open *os.File whose lifetime callers must hold for the duration
// of the lock; closing the file releases the lock. A nil file with a nil
// error means the lock is already held by another menu instance.
func acquireMenuLock(configDir string) (*os.File, error) {
	path, err := pidFilePath(configDir)
	if err != nil {
		return nil, err
	}

	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, fmt.Errorf("opening pid file: %w", err)
	}

	// File descriptors are small ints; the uintptr→int conversion cannot overflow.
	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil { //nolint:gosec
		_ = file.Close()
		if err == syscall.EWOULDBLOCK {
			return nil, nil
		}
		return nil, fmt.Errorf("locking pid file: %w", err)
	}

	return file, nil
}

// writePID overwrites the pid file with the given pid for human-readable
// inspection. The flock owned by the caller via acquireMenuLock continues to
// guard the file — this is just informational content.
func writePID(file *os.File, pid int) error {
	if _, err := file.Seek(0, 0); err != nil {
		return fmt.Errorf("seeking pid file: %w", err)
	}
	if err := file.Truncate(0); err != nil {
		return fmt.Errorf("truncating pid file: %w", err)
	}
	if _, err := fmt.Fprintf(file, "%d\n", pid); err != nil {
		return fmt.Errorf("writing pid: %w", err)
	}
	return file.Sync()
}

// readPID reads the pid recorded in the pid file. Returns 0 when the file is
// missing or unparseable. Used by tests; runtime callers do not need it
// because the flock is the source of truth for liveness.
func readPID(path string) int {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0
	}
	var pid int
	if _, err := fmt.Sscanf(string(data), "%d", &pid); err != nil {
		return 0
	}
	return pid
}
