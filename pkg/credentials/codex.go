package credentials

import (
	"encoding/json"
	"os"
	"path/filepath"
)

// ReadCodexAuthFile reads ~/.codex/auth.json and returns its contents and path.
// Returns nil, "" if the file cannot be read.
func ReadCodexAuthFile() ([]byte, string) {
	home, err := os.UserHomeDir()
	if err != nil {
		return nil, ""
	}

	authPath := filepath.Join(home, ".codex", "auth.json")
	data, err := os.ReadFile(authPath)
	if err != nil {
		return nil, ""
	}

	return data, authPath
}

// PatchCodexAuthKey sets OPENAI_API_KEY in the codex auth JSON and returns the
// updated bytes. It also removes OAuth "tokens" so codex falls back to the API
// key instead of using OAuth tokens that may lack required scopes.
// Returns nil, false if the JSON cannot be processed.
func PatchCodexAuthKey(data []byte, apiKey string) ([]byte, bool) {
	var auth map[string]json.RawMessage
	if err := json.Unmarshal(data, &auth); err != nil {
		return nil, false
	}

	keyJSON, err := json.Marshal(apiKey)
	if err != nil {
		return nil, false
	}
	auth["OPENAI_API_KEY"] = keyJSON

	// Remove OAuth tokens so codex uses the API key instead of OAuth
	// credentials that may lack required scopes (e.g. api.responses.write).
	delete(auth, "tokens")

	updated, err := json.MarshalIndent(auth, "", "  ")
	if err != nil {
		return nil, false
	}

	return updated, true
}
