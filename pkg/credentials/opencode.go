package credentials

import (
	"encoding/json"
	"os"
	"path/filepath"
)

// ReadOpenCodeAuthFile reads ~/.local/share/opencode/auth.json and returns its
// contents and path. Returns nil, "" if the file cannot be read.
func ReadOpenCodeAuthFile() ([]byte, string) {
	// OpenCode stores auth at $XDG_DATA_HOME/opencode/auth.json,
	// defaulting to ~/.local/share/opencode/auth.json.
	var dataDir string
	if xdg := os.Getenv("XDG_DATA_HOME"); xdg != "" {
		dataDir = xdg
	} else {
		home, err := os.UserHomeDir()
		if err != nil {
			return nil, ""
		}
		dataDir = filepath.Join(home, ".local", "share")
	}

	authPath := filepath.Join(dataDir, "opencode", "auth.json")
	data, err := os.ReadFile(authPath)
	if err != nil {
		return nil, ""
	}

	return data, authPath
}

// PatchOpenCodeAuth removes OAuth entries for the given providers from the
// opencode auth JSON, forcing opencode to fall back to API keys from its
// config or environment. Returns the updated bytes and true on success.
// Returns nil, false if the JSON cannot be processed.
func PatchOpenCodeAuth(data []byte, providers []string) ([]byte, bool) {
	var auth map[string]json.RawMessage
	if err := json.Unmarshal(data, &auth); err != nil {
		return nil, false
	}

	for _, p := range providers {
		delete(auth, p)
	}

	updated, err := json.MarshalIndent(auth, "", "  ")
	if err != nil {
		return nil, false
	}

	return updated, true
}
