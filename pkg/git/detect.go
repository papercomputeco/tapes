// Package git provides utilities for detecting git repository information.
package git

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

// RepoName returns the name of the current git repository.
// It runs "git rev-parse --show-toplevel" and returns the base directory name.
// If not inside a git repo, it falls back to the base name of the working directory.
func RepoName() string {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	out, err := exec.CommandContext(ctx, "git", "rev-parse", "--show-toplevel").Output()
	if err == nil {
		top := strings.TrimSpace(string(out))
		if top != "" {
			return filepath.Base(top)
		}
	}

	wd, err := os.Getwd()
	if err != nil {
		return ""
	}
	return filepath.Base(wd)
}
