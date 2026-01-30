// Package dotdir manages the .tapes/ and ~/.tapes directories.
//
// The checkout state represents a point in a conversation DAG that the user has
// "checked out" for resuming chat sessions. The state is persisted as a JSON file
// in the ~/.tapes/ directory.
package dotdir

import (
	"fmt"
	"os"
	"path/filepath"
)

const (
	// dirName is the name of the tapes directory.
	dirName = ".tapes"
)

type Manager struct{}

func NewManager() *Manager {
	return &Manager{}
}

// Target returns the target absolute path to a .tapes/ directory.
// Order of precedence is as follows:
//  1. Provided override
//  2. Local ./.tapes/ dir
//  3. Home ~/.tapes/ dir
//  4. If none found, attempt to create ~/.tapes/ dir
func (m *Manager) Target(overrideDir string) (string, error) {
	var dir string

	switch {
	case overrideDir != "":
		dir = overrideDir

	case m.localDirExists():
		cwd, err := os.Getwd()
		if err != nil {
			return "", fmt.Errorf("getting current directory: %w", err)
		}
		dir = filepath.Join(cwd, dirName)

	default:
		home, err := os.UserHomeDir()
		if err != nil {
			return "", fmt.Errorf("getting home directory: %w", err)
		}
		dir = filepath.Join(home, dirName)
	}

	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", fmt.Errorf("creating tapes directory %s: %w", dir, err)
	}

	return filepath.Abs(dir)
}

// localDirExists checks whether a .tapes/ directory exists in the current
// working directory.
func (m *Manager) localDirExists() bool {
	cwd, err := os.Getwd()
	if err != nil {
		return false
	}

	info, err := os.Stat(filepath.Join(cwd, dirName))
	return err == nil && info.IsDir()
}
