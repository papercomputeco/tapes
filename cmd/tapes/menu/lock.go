//go:build darwin

package menucmder

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"

	"github.com/papercomputeco/tapes/pkg/dotdir"
)

const lockFileName = "menu.lock"

func lockFilePath(configDir string) (string, error) {
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
	return filepath.Join(dir, lockFileName), nil
}

// acquireMenuLock grabs an exclusive, non-blocking flock on the menu lock
// file. The kernel auto-releases the lock on process exit, so SIGKILL cannot
// strand it. Callers must hold the returned *os.File for the lock's lifetime;
// closing releases it. A nil file with nil error means another menu holds it.
func acquireMenuLock(configDir string) (*os.File, error) {
	path, err := lockFilePath(configDir)
	if err != nil {
		return nil, err
	}

	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, fmt.Errorf("opening lock file: %w", err)
	}

	// File descriptors are small ints; the uintptr→int conversion cannot overflow.
	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil { //nolint:gosec
		_ = file.Close()
		if err == syscall.EWOULDBLOCK {
			return nil, nil
		}
		return nil, fmt.Errorf("locking lock file: %w", err)
	}

	return file, nil
}
