package menucmder

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/papercomputeco/tapes/pkg/dotdir"
)

const pidFileName = "menu.pid"

// pidFilePath resolves the absolute path to the menu PID file. The file lives
// inside the resolved tapes dotdir (typically ~/.tapes/menu.pid), so the menu
// owns its own lifecycle state independent of the start daemon.
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

// readPID returns the PID stored in the menu pid file, or 0 if the file is
// missing or unreadable.
func readPID(path string) int {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil {
		return 0
	}
	return pid
}

// writePID atomically writes pid to path.
func writePID(path string, pid int) error {
	data := []byte(strconv.Itoa(pid) + "\n")
	tmp, err := os.CreateTemp(filepath.Dir(path), "menu-pid-*.tmp")
	if err != nil {
		return fmt.Errorf("creating temp pid file: %w", err)
	}
	tmpPath := tmp.Name()
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpPath)
		return fmt.Errorf("writing temp pid file: %w", err)
	}
	if err := tmp.Chmod(0o600); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpPath)
		return fmt.Errorf("chmod temp pid file: %w", err)
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("closing temp pid file: %w", err)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("persisting pid file: %w", err)
	}
	return nil
}

// removePID deletes the pid file. A missing file is not an error.
func removePID(path string) error {
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("removing pid file: %w", err)
	}
	return nil
}
