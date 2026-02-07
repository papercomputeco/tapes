package sqlitepath

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
)

func ResolveSQLitePath(override string) (string, error) {
	if override != "" {
		return override, nil
	}

	if envPath := strings.TrimSpace(os.Getenv("TAPES_SQLITE")); envPath != "" {
		return envPath, nil
	}
	if envPath := strings.TrimSpace(os.Getenv("TAPES_DB")); envPath != "" {
		return envPath, nil
	}

	for _, candidate := range sqliteCandidates() {
		if _, err := os.Stat(candidate); err == nil {
			return candidate, nil
		}
	}

	return "", errors.New("could not find tapes SQLite database; pass --sqlite")
}

func sqliteCandidates() []string {
	candidates := []string{
		"tapes.db",
		"tapes.sqlite",
		filepath.Join(".tapes", "tapes.db"),
		filepath.Join(".tapes", "tapes.sqlite"),
	}

	home, err := os.UserHomeDir()
	if err == nil {
		candidates = append([]string{
			filepath.Join(home, ".tapes", "tapes.db"),
			filepath.Join(home, ".tapes", "tapes.sqlite"),
		}, candidates...)
	}

	if xdgHome := strings.TrimSpace(os.Getenv("XDG_DATA_HOME")); xdgHome != "" {
		candidates = append([]string{
			filepath.Join(xdgHome, "tapes", "tapes.db"),
			filepath.Join(xdgHome, "tapes", "tapes.sqlite"),
		}, candidates...)
	}

	return candidates
}
