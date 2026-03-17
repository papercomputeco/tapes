package sqlitepath

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
)

// ResolveSQLitePathWithFallback resolves the SQLite path with a default
// fallback of "tapes.sqlite" when no database is found.
func ResolveSQLitePathWithFallback(override string) string {
	path, err := ResolveSQLitePath(override)
	if err == nil {
		return path
	}

	return "tapes.sqlite"
}

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
		"tapes.sqlite",
		"tapes.db",
		filepath.Join(".tapes", "tapes.sqlite"),
		filepath.Join(".tapes", "tapes.db"),
	}

	home, err := os.UserHomeDir()
	if err == nil {
		candidates = append([]string{
			filepath.Join(home, ".tapes", "tapes.sqlite"),
			filepath.Join(home, ".tapes", "tapes.db"),
		}, candidates...)
	}

	if xdgHome := strings.TrimSpace(os.Getenv("XDG_DATA_HOME")); xdgHome != "" {
		candidates = append([]string{
			filepath.Join(xdgHome, "tapes", "tapes.sqlite"),
			filepath.Join(xdgHome, "tapes", "tapes.db"),
		}, candidates...)
	}

	return candidates
}
